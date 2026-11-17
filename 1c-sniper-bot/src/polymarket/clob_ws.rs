//! Polymarket CLOB market-channel WebSocket ingest.
//!
//! Subscribes to the `market` channel for a set of (asset_id, slug, side)
//! triples, maintains per-asset book state, and emits `BookUpdate` on every
//! book change. Requires no auth.
//!
//! Spec highlights (docs.polymarket.com/developers/CLOB/websocket):
//! - URL: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
//! - Subscribe: `{"assets_ids":[...], "type":"market", "custom_feature_enabled":true}`
//! - Heartbeat: client sends text `"PING"` every ≤10s; server disconnects after ~10s idle.
//! - Event types handled: `book` (full snapshot), `price_change` (deltas).
//!   Other types (`tick_size_change`, `last_trade_price`, `best_bid_ask`,
//!   `new_market`, `market_resolved`) are logged and ignored for phase 1.
//! - Messages may arrive as a JSON array (batched) or a bare object; both are
//!   handled.

use crate::types::{now_micros, BookUpdate, Side};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::Connector;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_BACKOFF: Duration = Duration::from_secs(1);
const PING_INTERVAL: Duration = Duration::from_secs(8); // safety margin under the 10s limit
// W2: if no book/price_change/last_trade event arrives within this window while the socket stays up
// (delta-dead-but-socket-alive), break the stream loop so the outer run loop reconnects+resubscribes.
// Chosen > PING_INTERVAL so a legitimately slow-but-alive book does not churn the connection.
const BOOK_STALE_RECONNECT: Duration = Duration::from_secs(11);
const PRICE_SCALE: f64 = 10_000.0;

/// One subscription: a token id with its (slug, side) context.
#[derive(Clone, Debug)]
pub struct Subscription {
    pub asset_id: String,
    pub slug: String,
    pub side: Side,
}

/// Build a shared `Connector::Rustls` populated with the OS native root
/// certificates. Call once at process startup from inside
/// `tokio::task::spawn_blocking` — `rustls_native_certs::load_native_certs()`
/// walks the macOS keychain / Linux CA bundle synchronously and can take
/// hundreds of milliseconds (or many seconds under contention). Reusing the
/// returned connector for every `connect_async_tls_with_config` call avoids
/// paying that cost on every per-market WS reconnect (lifecycle churn). See
/// feat/clob-ws-reap-fix branch notes.
pub fn build_shared_tls_connector() -> anyhow::Result<Connector> {
    use rustls::{ClientConfig, RootCertStore};
    use std::sync::Arc;
    let mut root_store = RootCertStore::empty();
    let result = rustls_native_certs::load_native_certs();
    if !result.errors.is_empty() {
        tracing::warn!(errors = ?result.errors, "native root CA loading errors (non-fatal)");
    }
    if result.certs.is_empty() {
        anyhow::bail!("no native root CA certificates found; cannot build TLS connector");
    }
    let total = result.certs.len();
    let (added, ignored) = root_store.add_parsable_certificates(result.certs);
    tracing::info!(
        added,
        ignored,
        total,
        "loaded native root CAs for shared TLS connector"
    );
    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Ok(Connector::Rustls(Arc::new(config)))
}

pub struct ClobWsIngest {
    ws_url: String,
    subs: Vec<Subscription>,
    registry: Option<std::sync::Arc<crate::polymarket::book_registry::BookRegistry>>,
    tls_connector: Option<Connector>,
    /// Microsecond Unix timestamp of the most-recent BOOK-BEARING WS message
    /// (`book`/`price_change`/`last_trade_price`) — NOT the keepalive PONG (W1,
    /// 2026-06-06). Exposed via `last_event_micros()` for the per-coin staleness
    /// watchdog (Task 3.3) to detect a dead book. Initialized to 0 (no events
    /// yet) so the watchdog skips ticks before the first message.
    last_event_micros: Arc<AtomicU64>,
    shutdown_token: Option<tokio_util::sync::CancellationToken>,
    /// Optional trade-print fan-out (2026-06-13, lift-composition
    /// discriminator): every parsed `last_trade_price` is forwarded with its
    /// price/size/aggressor. `try_send` — a slow consumer drops prints, never
    /// stalls the book path.
    trade_tx: Option<mpsc::Sender<TradePrint>>,
    /// Unix-micros timestamp after which the W2 book-stale watchdog stops
    /// forcing reconnects (0 = never, the default). Past a market's close the
    /// book legitimately goes sparse, then silent at on-chain resolution, so
    /// the watchdog's premise (live market ⇒ book event within 11s) no longer
    /// holds — without a cutoff, a subscription held through the post-close
    /// window churns a reconnect every ~13s until teardown (observed
    /// 2026-07-06).
    book_stale_reconnect_until_us: u64,
    /// Per-ingest book-stale reconnect THRESHOLD (default `BOOK_STALE_RECONNECT`
    /// = 11s). A 99¢ resting-bid maker raises this (~40s): its book goes
    /// legitimately quiet for tens of seconds pre-close on flat BTC, and each
    /// stale-reconnect freezes the registry ~1-3s and heals with a full
    /// SNAPSHOT that skips the ask-lift delta — so the driver places into an
    /// already-formed wall (back-of-wall, 2026-07-15). Safety is unaffected:
    /// the 5s `ws_stale_threshold_ms` kill gate still halts trading on a dead
    /// book, so a longer threshold governs only socket self-heal.
    book_stale_reconnect_after_us: u64,
}

impl ClobWsIngest {
    pub fn new(ws_url: impl Into<String>, subs: Vec<Subscription>) -> Self {
        Self {
            ws_url: ws_url.into(),
            subs,
            registry: None,
            tls_connector: None,
            last_event_micros: Arc::new(AtomicU64::new(0)),
            shutdown_token: None,
            trade_tx: None,
            book_stale_reconnect_until_us: 0,
            book_stale_reconnect_after_us: BOOK_STALE_RECONNECT.as_micros() as u64,
        }
    }

    /// Construct an ingest that mirrors every top-of-book update into `registry`
    /// as well as emitting the usual `BookUpdate` on the mpsc channel.
    pub fn new_with_registry(
        ws_url: impl Into<String>,
        subs: Vec<Subscription>,
        registry: std::sync::Arc<crate::polymarket::book_registry::BookRegistry>,
    ) -> Self {
        Self {
            ws_url: ws_url.into(),
            subs,
            registry: Some(registry),
            tls_connector: None,
            last_event_micros: Arc::new(AtomicU64::new(0)),
            shutdown_token: None,
            trade_tx: None,
            book_stale_reconnect_until_us: 0,
            book_stale_reconnect_after_us: BOOK_STALE_RECONNECT.as_micros() as u64,
        }
    }

    /// Clone the shared atomic timestamp of the most-recent BOOK-BEARING WS
    /// message (`book`/`price_change`/`last_trade_price`; NOT the keepalive
    /// PONG — W1). Used by the per-coin watchdog (Task 3.3 Step 3) to detect a
    /// dead book. Value is microseconds since the Unix epoch; `0` means no
    /// book event observed yet.
    pub fn last_event_micros(&self) -> Arc<AtomicU64> {
        self.last_event_micros.clone()
    }

    /// Inject a pre-built TLS connector. Required in production wiring so
    /// every per-market reconnect reuses one `rustls::ClientConfig` instead of
    /// re-loading native root certs synchronously on a tokio worker (which
    /// was the root cause of the 28 s `quoter_wake` extreme at market
    /// transitions — see feat/clob-ws-reap-fix branch notes). When `None` the
    /// ingest falls back to `connect_async` which builds a fresh connector
    /// per call (acceptable only in tests / single-shot live probes).
    pub fn with_tls_connector(mut self, connector: Connector) -> Self {
        self.tls_connector = Some(connector);
        self
    }

    /// Forward every parsed `last_trade_price` (price/size/aggressor) to
    /// `tx`. Lossy by design (`try_send`): prints are telemetry, the book
    /// path must never block on them.
    pub fn with_trade_prints(mut self, tx: mpsc::Sender<TradePrint>) -> Self {
        self.trade_tx = Some(tx);
        self
    }

    /// Inject a cancellation token. When fired, `run` exits its outer reconnect
    /// loop AND the inner `connect_and_stream` select! arm returns cleanly.
    /// Without this, `run` loops forever; a LifecycleSupervisor
    /// uses this to reap per-slot WS ingests at slot_end + grace.
    pub fn with_shutdown_token(mut self, token: tokio_util::sync::CancellationToken) -> Self {
        self.shutdown_token = Some(token);
        self
    }

    /// Quiesce the book-stale reconnect watchdog from `unix_micros` on.
    /// Callers whose subscription outlives the market's active window (e.g. a
    /// post-close hold to resolution) pass the close timestamp: past it the
    /// socket stays up on PINGs alone instead of churning reconnects against
    /// a legitimately silent book. Staleness before the cutoff reconnects as
    /// usual; unset (0) keeps the watchdog always armed.
    pub fn with_book_stale_reconnect_until(mut self, unix_micros: u64) -> Self {
        self.book_stale_reconnect_until_us = unix_micros;
        self
    }

    /// Override the book-stale reconnect THRESHOLD (default 11s =
    /// `BOOK_STALE_RECONNECT`). Raise it for a subscription whose book
    /// legitimately goes quiet within its active window: a 99¢
    /// resting-bid book is silent for tens of seconds on flat BTC pre-close,
    /// and a stale-reconnect drops the ask-lift delta (→ back-of-wall). Safety
    /// is independent — the 5s ws-stale kill gate still halts trading on a dead
    /// book, so this governs only socket self-heal, never trade admission.
    pub fn with_book_stale_reconnect_after(mut self, after: Duration) -> Self {
        self.book_stale_reconnect_after_us = after.as_micros() as u64;
        self
    }

    /// Run the ingest loop forever. Publishes `BookUpdate`s as the book state
    /// changes. Reconnects with a fixed back-off on any error.
    pub async fn run(self, tx: mpsc::Sender<BookUpdate>) -> Result<()> {
        if self.subs.is_empty() {
            anyhow::bail!("ClobWsIngest: no subscriptions");
        }
        tracing::info!(url = %self.ws_url, n_subs = self.subs.len(), "clob_ws ingest connecting");
        loop {
            if let Some(tok) = &self.shutdown_token {
                if tok.is_cancelled() {
                    tracing::info!("clob_ws shutdown_token cancelled; exiting run");
                    return Ok(());
                }
            }
            let connect_fut = self.connect_and_stream(&tx);
            match &self.shutdown_token {
                Some(tok) => {
                    tokio::select! {
                        biased;
                        _ = tok.cancelled() => {
                            tracing::info!("clob_ws shutdown_token cancelled mid-connect; exiting run");
                            return Ok(());
                        }
                        res = connect_fut => match res {
                            Ok(()) => tracing::warn!("clob_ws closed cleanly; reconnecting"),
                            Err(e) => tracing::error!(?e, "clob_ws error; reconnecting"),
                        }
                    }
                }
                None => match connect_fut.await {
                    Ok(()) => tracing::warn!("clob_ws closed cleanly; reconnecting"),
                    Err(e) => tracing::error!(?e, "clob_ws error; reconnecting"),
                },
            }
            tokio::time::sleep(RECONNECT_BACKOFF).await;
        }
    }

    async fn connect_and_stream(&self, tx: &mpsc::Sender<BookUpdate>) -> Result<()> {
        // Prefer the shared connector when one was injected; falling back to
        // `connect_async` re-loads the OS native cert store synchronously
        // inside the async call, blocking the tokio worker for 100 ms — 28 s
        // depending on keychain contention. See `build_shared_tls_connector`.
        let connect_fut = async {
            match &self.tls_connector {
                Some(conn) => {
                    tokio_tungstenite::connect_async_tls_with_config(
                        &self.ws_url,
                        None,
                        true,
                        Some(conn.clone()),
                    )
                    .await
                }
                None => {
                    tokio_tungstenite::connect_async_with_config(&self.ws_url, None, true).await
                }
            }
        };
        let (mut ws, _resp) = tokio::time::timeout(CONNECT_TIMEOUT, connect_fut)
            .await
            .context("clob_ws connect timeout")?
            .context("clob_ws connect")?;
        tracing::info!("clob_ws connected");

        // Subscribe.
        let assets_ids: Vec<&str> = self.subs.iter().map(|s| s.asset_id.as_str()).collect();
        let sub_msg = serde_json::json!({
            "assets_ids": assets_ids,
            "type": "market",
            "custom_feature_enabled": true,
        });
        ws.send(Message::Text(sub_msg.to_string().into()))
            .await
            .context("clob_ws send subscribe")?;

        // Per-asset context + book state.
        let mut state: HashMap<String, AssetState> = HashMap::new();
        for s in &self.subs {
            state.insert(
                s.asset_id.clone(),
                AssetState {
                    slug: s.slug.clone(),
                    side: s.side,
                    book: Book::default(),
                },
            );
        }

        let mut heartbeat = tokio::time::interval(PING_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate tick so we don't PING before subscribing.
        heartbeat.tick().await;

        // W2: a book-only freshness clock + watchdog, independent of the cross-connection
        // `last_event_micros` Arc (which persists across reconnects). Initialized to "now" at connect
        // so the timer measures freshness since THIS connection subscribed.
        let mut last_book_event_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let mut book_watchdog = tokio::time::interval(Duration::from_secs(2));
        book_watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        book_watchdog.tick().await; // skip the immediate tick
        let mut quiesce_logged = false;

        loop {
            tokio::select! {
                biased;
                _ = heartbeat.tick() => {
                    if let Err(e) = ws.send(Message::Text("PING".into())).await {
                        return Err(e).context("clob_ws send PING");
                    }
                }
                msg = ws.next() => {
                    let Some(msg) = msg else { break };
                    let msg = msg.context("clob_ws read")?;
                    match msg {
                        Message::Text(t) => {
                            // W1: the staleness clock is a BOOK-freshness clock. Stamp it only when the
                            // message carried a book/price_change/last_trade event — NEVER on the server's
                            // 8s text "PONG" keepalive, which previously masked a dead delta stream.
                            if handle_text_message(
                                t.as_str(),
                                &mut state,
                                tx,
                                self.registry.as_ref(),
                                self.trade_tx.as_ref(),
                            )
                            .await
                            {
                                let now_us = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .map(|d| d.as_micros() as u64)
                                    .unwrap_or(0);
                                self.last_event_micros.store(now_us, Ordering::Relaxed);
                                last_book_event_us = now_us;
                            }
                        }
                        Message::Ping(p) => {
                            ws.send(Message::Pong(p)).await.context("clob_ws ws-layer pong")?;
                        }
                        Message::Close(frame) => {
                            tracing::warn!(?frame, "clob_ws received close frame");
                            break;
                        }
                        _ => {}
                    }
                }
                _ = book_watchdog.tick() => {
                    let now_us = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_micros() as u64)
                        .unwrap_or(0);
                    let elapsed_us = now_us.saturating_sub(last_book_event_us);
                    if should_stale_reconnect(
                        now_us,
                        last_book_event_us,
                        self.book_stale_reconnect_until_us,
                        self.book_stale_reconnect_after_us,
                    ) {
                        // Diagnostic (front-of-wall investigation, 2026-07-15):
                        // top-of-book at the stale event. If it is UNCHANGED
                        // across the reconnect gap it was genuine market-quiet
                        // (safe to widen the threshold); if it JUMPED (e.g. an
                        // ask vanished / a wall formed) a delta was missed in
                        // the gap and the reconnect cost a touch.
                        let top_of_book: Vec<String> = state
                            .values()
                            .map(|a| {
                                format!(
                                    "{} {:?} bid={:?} ask={:?}",
                                    a.slug,
                                    a.side,
                                    a.book.best_bid(),
                                    a.book.best_ask()
                                )
                            })
                            .collect();
                        tracing::warn!(
                            elapsed_ms = elapsed_us / 1_000,
                            ?top_of_book,
                            "clob_ws book-delta stream stale (socket alive, no book events) — reconnecting+resubscribing"
                        );
                        break;
                    }
                    if elapsed_us > self.book_stale_reconnect_after_us && !quiesce_logged {
                        quiesce_logged = true;
                        tracing::info!(
                            elapsed_ms = elapsed_us / 1_000,
                            "clob_ws book quiet past stale-reconnect cutoff — expected post-close; holding socket"
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

/// W2 watchdog decision: force a reconnect when the book has been silent past
/// `stale_after_us` (per-ingest, default `BOOK_STALE_RECONNECT`) — unless
/// `quiet_after_us` (0 = never) has passed, at which point silence is the
/// market's expected post-close state, not a dead delta stream, and churning
/// the connection buys nothing.
fn should_stale_reconnect(
    now_us: u64,
    last_book_event_us: u64,
    quiet_after_us: u64,
    stale_after_us: u64,
) -> bool {
    if quiet_after_us != 0 && now_us >= quiet_after_us {
        return false;
    }
    now_us.saturating_sub(last_book_event_us) > stale_after_us
}

struct AssetState {
    slug: String,
    side: Side,
    book: Book,
}

#[derive(Default)]
struct Book {
    // BTreeMap: O(1) best via next/next_back, sorted iteration for free.
    bids: BTreeMap<u32, f64>,
    asks: BTreeMap<u32, f64>,
}

impl Book {
    fn apply_snapshot(&mut self, bids: &[Level], asks: &[Level]) {
        self.bids.clear();
        self.asks.clear();
        for l in bids {
            if l.size > 0.0 {
                self.bids.insert(scale_price(l.price), l.size);
            }
        }
        for l in asks {
            if l.size > 0.0 {
                self.asks.insert(scale_price(l.price), l.size);
            }
        }
    }

    fn apply_change(&mut self, side: BookSide, price: f64, size: f64) {
        let k = scale_price(price);
        let book = match side {
            BookSide::Buy => &mut self.bids,
            BookSide::Sell => &mut self.asks,
        };
        if size <= 0.0 {
            book.remove(&k);
        } else {
            book.insert(k, size);
        }
    }

    fn best_bid(&self) -> Option<f64> {
        self.bids
            .keys()
            .next_back()
            .map(|&k| k as f64 / PRICE_SCALE)
    }

    fn best_ask(&self) -> Option<f64> {
        self.asks.keys().next().map(|&k| k as f64 / PRICE_SCALE)
    }

    fn asks_sorted(&self) -> Vec<crate::polymarket::book_registry::Level> {
        self.asks
            .iter()
            .map(|(&k, &size)| crate::polymarket::book_registry::Level {
                price: k as f64 / PRICE_SCALE,
                size,
            })
            .collect()
    }

    fn bids_sorted(&self) -> Vec<crate::polymarket::book_registry::Level> {
        self.bids
            .iter()
            .rev()
            .map(|(&k, &size)| crate::polymarket::book_registry::Level {
                price: k as f64 / PRICE_SCALE,
                size,
            })
            .collect()
    }
}

fn scale_price(p: f64) -> u32 {
    (p * PRICE_SCALE).round() as u32
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BookSide {
    Buy,
    Sell,
}

impl BookSide {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "BUY" | "buy" => Some(BookSide::Buy),
            "SELL" | "sell" => Some(BookSide::Sell),
            _ => None,
        }
    }
}

#[derive(Deserialize)]
struct Level {
    #[serde(deserialize_with = "deser_decimal_str")]
    price: f64,
    #[serde(deserialize_with = "deser_decimal_str")]
    size: f64,
}

#[derive(Deserialize)]
struct Change {
    #[serde(deserialize_with = "deser_decimal_str")]
    price: f64,
    side: String,
    #[serde(deserialize_with = "deser_decimal_str")]
    size: f64,
}

#[derive(Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
enum Event {
    Book(BookEvent),
    PriceChange(PriceChangeEvent),
    LastTradePrice(LastTradePriceEvent),
    // Anything else deserializes to Unknown below via the untagged wrapper.
}

#[derive(Deserialize)]
struct BookEvent {
    asset_id: String,
    #[serde(default)]
    bids: Vec<Level>,
    #[serde(default)]
    asks: Vec<Level>,
}

#[derive(Deserialize)]
struct PriceChangeEvent {
    asset_id: String,
    changes: Vec<Change>,
}

/// `last_trade_price`: `asset_id` feeds the SlotSignals trade-rate windows;
/// price/size/side are forwarded on the optional trade-print channel
/// (2026-06-13 — lift-composition discriminator needs aggressor-tagged
/// prints; before this they were parsed and dropped). All optional fields
/// are defensive: a missing field degrades the print, never the parse.
#[derive(Deserialize)]
struct LastTradePriceEvent {
    asset_id: String,
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    size: Option<String>,
    /// Aggressor side ("BUY"/"SELL").
    #[serde(default)]
    side: Option<String>,
}

/// A venue trade print, slug/side-resolved. `side` is the BOOK side of the
/// printed asset (Up/Down token); `aggressor` is the taker direction
/// ("BUY"/"SELL") as reported by the venue.
#[derive(Clone, Debug)]
pub struct TradePrint {
    pub slug: String,
    pub side: crate::types::Side,
    pub price: f64,
    pub size: f64,
    pub aggressor: Option<String>,
    pub ts_micros: u64,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum EventOrUnknown {
    Known(Event),
    Unknown(serde_json::Value),
}

/// Polymarket WS sends either an array of events or a single event object.
/// Accept both. Dispatch on the first non-whitespace byte so we only attempt
/// ONE serde_json parse per message (vs the prior try-array-then-object
/// fallback which paid two full parses on single-object messages).
fn parse_events(text: &str) -> Vec<EventOrUnknown> {
    let first = match text.as_bytes().iter().find(|&&b| !b.is_ascii_whitespace()) {
        Some(&b) => b,
        None => return Vec::new(),
    };
    match first {
        b'[' => serde_json::from_str::<Vec<EventOrUnknown>>(text).unwrap_or_default(),
        b'{' => match serde_json::from_str::<EventOrUnknown>(text) {
            Ok(v) => vec![v],
            Err(_) => Vec::new(),
        },
        _ => Vec::new(),
    }
}

async fn handle_text_message(
    text: &str,
    state: &mut HashMap<String, AssetState>,
    tx: &mpsc::Sender<BookUpdate>,
    registry: Option<&std::sync::Arc<crate::polymarket::book_registry::BookRegistry>>,
    trade_tx: Option<&mpsc::Sender<TradePrint>>,
) -> bool {
    let parse_start = std::time::Instant::now();
    let events = parse_events(text);
    crate::monitoring::latency::record_elapsed("ws_parse", parse_start);
    let mut book_bearing = false;
    for evt in events {
        match evt {
            EventOrUnknown::Known(Event::Book(b)) => {
                book_bearing = true;
                if let Some(a) = state.get_mut(&b.asset_id) {
                    a.book.apply_snapshot(&b.bids, &b.asks);
                    emit(a, tx, registry).await;
                }
            }
            EventOrUnknown::Known(Event::PriceChange(pc)) => {
                book_bearing = true;
                if let Some(a) = state.get_mut(&pc.asset_id) {
                    for c in &pc.changes {
                        if let Some(s) = BookSide::parse(&c.side) {
                            a.book.apply_change(s, c.price, c.size);
                        }
                    }
                    emit(a, tx, registry).await;
                }
            }
            EventOrUnknown::Known(Event::LastTradePrice(t)) => {
                book_bearing = true;
                if let Some(a) = state.get(&t.asset_id) {
                    if let Some(reg) = registry {
                        reg.record_trade(&a.slug, now_micros());
                    }
                    if let (Some(ttx), Some(px), Some(sz)) = (
                        trade_tx,
                        t.price.as_deref().and_then(|s| s.parse::<f64>().ok()),
                        t.size.as_deref().and_then(|s| s.parse::<f64>().ok()),
                    ) {
                        let _ = ttx.try_send(TradePrint {
                            slug: a.slug.clone(),
                            side: a.side,
                            price: px,
                            size: sz,
                            aggressor: t.side.clone(),
                            ts_micros: now_micros(),
                        });
                    }
                }
            }
            EventOrUnknown::Unknown(v) => {
                if let Some(et) = v.get("event_type").and_then(|x| x.as_str()) {
                    tracing::debug!(event_type = et, "clob_ws: ignored event");
                }
            }
        }
    }
    book_bearing
}

async fn emit(
    a: &AssetState,
    tx: &mpsc::Sender<BookUpdate>,
    registry: Option<&std::sync::Arc<crate::polymarket::book_registry::BookRegistry>>,
) {
    let ts = now_micros();
    let best_bid = a.book.best_bid();
    let best_ask = a.book.best_ask();
    if let Some(reg) = registry {
        let reg_start = std::time::Instant::now();
        reg.update(
            &a.slug,
            a.side,
            crate::polymarket::book_registry::BookSnapshot {
                bids: a.book.bids_sorted(),
                asks: a.book.asks_sorted(),
                ts_micros: ts,
            },
        );
        crate::monitoring::latency::record_elapsed("registry_update", reg_start);
    }
    let update = BookUpdate {
        slug: a.slug.clone(),
        side: a.side,
        best_bid,
        best_ask,
        ts_micros: ts,
    };
    if let Err(e) = tx.try_send(update) {
        use tokio::sync::mpsc::error::TrySendError;
        match e {
            TrySendError::Full(_) => tracing::warn!("clob_ws: book update channel full; dropping"),
            TrySendError::Closed(_) => tracing::warn!("clob_ws: book update channel closed"),
        }
    }
}

fn deser_decimal_str<'de, D: serde::Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    // Zero-copy: visit the &str directly without allocating an intermediate
    // String. Polymarket sends `price`/`size` as JSON strings, so each level
    // used to pay one String allocation per field. 2 fields × 1–3 changes per
    // event × 100–500 events/s = thousands of allocs/sec we can skip.
    use serde::de::{Error, Visitor};
    use std::fmt;
    struct V;
    impl Visitor<'_> for V {
        type Value = f64;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a decimal number encoded as a JSON string")
        }
        fn visit_str<E: Error>(self, v: &str) -> Result<f64, E> {
            v.parse::<f64>().map_err(E::custom)
        }
        fn visit_borrowed_str<E: Error>(self, v: &str) -> Result<f64, E> {
            self.visit_str(v)
        }
    }
    d.deserialize_str(V)
}

#[cfg(test)]
mod bench {
    //! Synthetic hot-path benchmark for latency instrumentation.
    //!
    //! Drives N book/price_change messages through `handle_text_message` at
    //! peak throughput, then dumps p50/p90/p99 for every instrumented stage.
    //! Reproducible, no network. Run with:
    //!
    //!   cargo test --release --lib --features= -- --ignored --nocapture bench_hotpath_synthetic
    //!
    //! A typical 5-min BTC-updown-5m market sees ~40–80 price_change events/s
    //! during active periods. We push 20 000 events in this harness so the
    //! p99 buckets are populated.

    use super::*;
    use crate::polymarket::book_registry::BookRegistry;
    use std::sync::Arc;

    const LEVELS: u32 = 100;

    fn build_ws_snapshot_json(asset: &str, seed: u64) -> String {
        // `LEVELS` per side — realistic Polymarket depth during active markets.
        let mut bids = String::new();
        let mut asks = String::new();
        for i in 0..LEVELS {
            let bp = 0.50 - (i as f64) * 0.001;
            let ap = 0.50 + (i as f64) * 0.001 + 0.001;
            let sz = 50.0 + ((seed + i as u64) % 97) as f64;
            if i > 0 {
                bids.push(',');
                asks.push(',');
            }
            bids.push_str(&format!(r#"{{"price":"{bp:.2}","size":"{sz:.1}"}}"#));
            asks.push_str(&format!(r#"{{"price":"{ap:.2}","size":"{sz:.1}"}}"#));
        }
        format!(r#"[{{"event_type":"book","asset_id":"{asset}","bids":[{bids}],"asks":[{asks}]}}]"#)
    }

    fn build_ws_price_change_json(asset: &str, seed: u64) -> String {
        // 1–3 changes per event; BUY or SELL; realistic shape.
        let n = 1 + (seed % 3);
        let mut changes = String::new();
        for i in 0..n {
            let side = if (seed + i).is_multiple_of(2) {
                "BUY"
            } else {
                "SELL"
            };
            let base = if side == "BUY" { 0.49 } else { 0.51 };
            let p = base - (i as f64) * 0.01;
            let sz = if (seed + i).is_multiple_of(7) {
                0.0
            } else {
                10.0 + ((seed + i) % 50) as f64
            };
            if i > 0 {
                changes.push(',');
            }
            changes.push_str(&format!(
                r#"{{"price":"{p:.2}","side":"{side}","size":"{sz:.1}"}}"#
            ));
        }
        format!(r#"[{{"event_type":"price_change","asset_id":"{asset}","changes":[{changes}]}}]"#)
    }

    #[tokio::test]
    #[ignore = "synthetic benchmark — run with --ignored"]
    async fn bench_hotpath_synthetic() {
        crate::monitoring::latency::reset_all();

        let asset = "BENCH_ASSET";
        let slug = "btc-updown-5m-bench";
        let registry = Arc::new(BookRegistry::new());
        let (tx, mut rx) = mpsc::channel::<BookUpdate>(8192);
        let mut state = HashMap::new();
        state.insert(
            asset.to_string(),
            AssetState {
                slug: slug.to_string(),
                side: Side::Up,
                book: Book::default(),
            },
        );

        // Drain the mpsc so try_send doesn't fill up and corrupt the measurement
        // with drop warnings.
        tokio::spawn(async move { while rx.recv().await.is_some() {} });

        // 1 snapshot to seed, then 20_000 price_changes. Matches the real WS
        // where the first message per asset is a book snapshot.
        let seed_msg = build_ws_snapshot_json(asset, 0);
        handle_text_message(&seed_msg, &mut state, &tx, Some(&registry), None).await;

        const N: u64 = 100_000;
        let wall = std::time::Instant::now();
        for i in 1..=N {
            let msg = build_ws_price_change_json(asset, i);
            handle_text_message(&msg, &mut state, &tx, Some(&registry), None).await;

            // Pattern A — `ask_ladder` (allocates a Vec<(f64,f64)>).
            // Every book_update triggers this; at 100 ev/s × 2 sides × 20
            // active markets = ~4000 reads/s in production.
            let _lad = registry.ask_ladder(slug, Side::Up);

            // Pattern B — direct iterate on the Arc<BookSnapshot>'s asks slice,
            // no Vec alloc. Simulates the opp-cross scan after the proposed
            // quoter refactor (best ask + sum of depth ≤ best+tick).
            {
                let t0 = std::time::Instant::now();
                if let Some(snap) = registry.get(slug, Side::Up) {
                    let cap = snap
                        .asks
                        .first()
                        .map(|l| l.price + 0.01)
                        .unwrap_or(f64::NAN);
                    let _depth: f64 = snap
                        .asks
                        .iter()
                        .take_while(|l| l.price <= cap)
                        .map(|l| l.size)
                        .sum();
                }
                crate::monitoring::latency::record_elapsed("direct_scan", t0);
            }
        }
        let wall_elapsed = wall.elapsed();
        let throughput = N as f64 / wall_elapsed.as_secs_f64();

        eprintln!(
            "\n=== bench_hotpath_synthetic: {N} price_change events in {:.2}s ({throughput:.0} ev/s) ===",
            wall_elapsed.as_secs_f64()
        );
        eprintln!(
            "{:<20} {:>10} {:>10} {:>10} {:>10} {:>10}",
            "stage", "count", "p50_us", "p90_us", "p99_us", "max_us"
        );
        for stage in crate::monitoring::latency::STAGES {
            if let Some(s) = crate::monitoring::latency::snapshot_stage(stage) {
                eprintln!(
                    "{:<20} {:>10} {:>10.3} {:>10.3} {:>10.3} {:>10.3}",
                    stage,
                    s.count,
                    s.p50_ns as f64 / 1000.0,
                    s.p90_ns as f64 / 1000.0,
                    s.p99_ns as f64 / 1000.0,
                    s.max_ns as f64 / 1000.0,
                );
            }
        }
        eprintln!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    fn sub(asset: &str, slug: &str, side: Side) -> Subscription {
        Subscription {
            asset_id: asset.into(),
            slug: slug.into(),
            side,
        }
    }

    #[test]
    fn scale_price_is_exact_at_tick() {
        assert_eq!(scale_price(0.01), 100);
        assert_eq!(scale_price(0.99), 9900);
        assert_eq!(scale_price(1.00), 10_000);
        assert_eq!(scale_price(0.505), 5050);
    }

    #[test]
    fn book_best_from_empty_is_none() {
        let b = Book::default();
        assert!(b.best_bid().is_none());
        assert!(b.best_ask().is_none());
    }

    #[test]
    fn book_snapshot_picks_top_of_each_side() {
        let mut b = Book::default();
        b.apply_snapshot(
            &[
                Level {
                    price: 0.10,
                    size: 100.0,
                },
                Level {
                    price: 0.20,
                    size: 50.0,
                },
                Level {
                    price: 0.30,
                    size: 10.0,
                }, // best bid
            ],
            &[
                Level {
                    price: 0.70,
                    size: 5.0,
                }, // best ask
                Level {
                    price: 0.80,
                    size: 20.0,
                },
                Level {
                    price: 0.90,
                    size: 100.0,
                },
            ],
        );
        assert_abs_diff_eq!(b.best_bid().unwrap(), 0.30, epsilon = 1e-9);
        assert_abs_diff_eq!(b.best_ask().unwrap(), 0.70, epsilon = 1e-9);
    }

    #[test]
    fn book_ignores_zero_size_in_snapshot() {
        let mut b = Book::default();
        b.apply_snapshot(
            &[
                Level {
                    price: 0.25,
                    size: 0.0,
                }, // should be skipped
                Level {
                    price: 0.30,
                    size: 10.0,
                },
            ],
            &[Level {
                price: 0.70,
                size: 5.0,
            }],
        );
        assert_abs_diff_eq!(b.best_bid().unwrap(), 0.30, epsilon = 1e-9);
    }

    #[test]
    fn book_price_change_zero_size_removes_level() {
        let mut b = Book::default();
        b.apply_snapshot(
            &[
                Level {
                    price: 0.30,
                    size: 10.0,
                },
                Level {
                    price: 0.20,
                    size: 50.0,
                },
            ],
            &[Level {
                price: 0.70,
                size: 5.0,
            }],
        );
        b.apply_change(BookSide::Buy, 0.30, 0.0); // remove top bid
        assert_abs_diff_eq!(b.best_bid().unwrap(), 0.20, epsilon = 1e-9);
    }

    #[test]
    fn book_price_change_updates_size() {
        let mut b = Book::default();
        b.apply_change(BookSide::Sell, 0.60, 25.0);
        b.apply_change(BookSide::Sell, 0.55, 10.0); // becomes best ask
        assert_abs_diff_eq!(b.best_ask().unwrap(), 0.55, epsilon = 1e-9);
        b.apply_change(BookSide::Sell, 0.55, 0.0);
        assert_abs_diff_eq!(b.best_ask().unwrap(), 0.60, epsilon = 1e-9);
    }

    #[test]
    fn parse_events_accepts_single_object() {
        let s = r#"{"event_type":"book","asset_id":"A","bids":[],"asks":[]}"#;
        let evs = parse_events(s);
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], EventOrUnknown::Known(Event::Book(_))));
    }

    #[test]
    fn parse_events_accepts_array() {
        let s = r#"[
            {"event_type":"book","asset_id":"A","bids":[],"asks":[]},
            {"event_type":"price_change","asset_id":"A","changes":[]}
        ]"#;
        let evs = parse_events(s);
        assert_eq!(evs.len(), 2);
    }

    #[test]
    fn parse_events_routes_unknown_types_to_unknown() {
        // `tick_size_change` is in the WS event lexicon but Phase 2 does
        // not consume it — it must still route to Unknown for the ignored
        // path. (`last_trade_price` was the prior fixture event for this
        // test but is now a Known event used to drive SlotSignals.)
        let s = r#"{"event_type":"tick_size_change","asset_id":"A","new_tick_size":"0.01"}"#;
        let evs = parse_events(s);
        assert_eq!(evs.len(), 1);
        assert!(matches!(evs[0], EventOrUnknown::Unknown(_)));
    }

    #[test]
    fn parse_events_routes_last_trade_price_as_known() {
        let s = r#"{"event_type":"last_trade_price","asset_id":"A","price":"0.5","size":"5","side":"BUY","fee_rate_bps":"0"}"#;
        let evs = parse_events(s);
        assert_eq!(evs.len(), 1);
        assert!(matches!(
            evs[0],
            EventOrUnknown::Known(Event::LastTradePrice(_))
        ));
    }

    #[test]
    fn parse_events_handles_malformed_gracefully() {
        let evs = parse_events("not json");
        assert_eq!(evs.len(), 0);
    }

    #[test]
    fn book_event_parses_string_decimals() {
        let s = r#"[{
            "event_type": "book",
            "asset_id": "A",
            "market": "0x00",
            "timestamp": "1",
            "hash": "h",
            "bids": [{"price":"0.30","size":"10.5"}],
            "asks": [{"price":"0.70","size":"5"}]
        }]"#;
        let evs = parse_events(s);
        assert_eq!(evs.len(), 1);
        match &evs[0] {
            EventOrUnknown::Known(Event::Book(b)) => {
                assert_eq!(b.asset_id, "A");
                assert_eq!(b.bids.len(), 1);
                assert_abs_diff_eq!(b.bids[0].price, 0.30, epsilon = 1e-9);
                assert_abs_diff_eq!(b.bids[0].size, 10.5, epsilon = 1e-9);
            }
            _ => panic!("expected Book"),
        }
    }

    #[tokio::test]
    async fn handle_book_event_drives_book_update() {
        let mut state = HashMap::new();
        state.insert(
            "A".to_string(),
            AssetState {
                slug: "eth-updown-15m-0".into(),
                side: Side::Up,
                book: Book::default(),
            },
        );
        let (tx, mut rx) = mpsc::channel::<BookUpdate>(32);

        let msg = r#"[{
            "event_type": "book",
            "asset_id": "A",
            "bids": [{"price":"0.30","size":"10"}, {"price":"0.20","size":"20"}],
            "asks": [{"price":"0.70","size":"5"}, {"price":"0.80","size":"15"}]
        }]"#;
        handle_text_message(msg, &mut state, &tx, None, None).await;

        let u = rx.recv().await.expect("got update");
        assert_eq!(u.slug, "eth-updown-15m-0");
        assert_eq!(u.side, Side::Up);
        assert_abs_diff_eq!(u.best_bid.unwrap(), 0.30, epsilon = 1e-9);
        assert_abs_diff_eq!(u.best_ask.unwrap(), 0.70, epsilon = 1e-9);
    }

    #[tokio::test]
    async fn handle_price_change_shifts_best() {
        let mut state = HashMap::new();
        state.insert(
            "A".to_string(),
            AssetState {
                slug: "btc-updown-15m-0".into(),
                side: Side::Down,
                book: Book::default(),
            },
        );
        let (tx, mut rx) = mpsc::channel::<BookUpdate>(32);

        // Seed with a snapshot.
        let seed = r#"[{"event_type":"book","asset_id":"A",
            "bids":[{"price":"0.30","size":"10"}],
            "asks":[{"price":"0.70","size":"5"}]}]"#;
        handle_text_message(seed, &mut state, &tx, None, None).await;
        let _ = rx.recv().await;

        // New best ask appears.
        let update = r#"[{"event_type":"price_change","asset_id":"A",
            "changes":[{"price":"0.65","side":"SELL","size":"10"}]}]"#;
        handle_text_message(update, &mut state, &tx, None, None).await;
        let u = rx.recv().await.expect("update after change");
        assert_abs_diff_eq!(u.best_ask.unwrap(), 0.65, epsilon = 1e-9);
        assert_abs_diff_eq!(u.best_bid.unwrap(), 0.30, epsilon = 1e-9);
    }

    #[tokio::test]
    async fn handle_last_trade_price_does_not_emit_book_update() {
        let mut state = HashMap::new();
        state.insert(
            "A".to_string(),
            AssetState {
                slug: "eth-updown-15m-0".into(),
                side: Side::Up,
                book: Book::default(),
            },
        );
        let (tx, mut rx) = mpsc::channel::<BookUpdate>(32);
        let msg = r#"[{"event_type":"last_trade_price","asset_id":"A","price":"0.5","size":"5","side":"BUY","fee_rate_bps":"0"}]"#;
        handle_text_message(msg, &mut state, &tx, None, None).await;
        // Trade events drive SlotSignals trade-rate windows but never the
        // book-state broadcast — top-of-book may not have moved.
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn handle_last_trade_price_drives_record_trade_when_registry_attached() {
        use crate::polymarket::book_registry::BookRegistry;
        use std::sync::Arc;

        let mut state = HashMap::new();
        state.insert(
            "A".to_string(),
            AssetState {
                slug: "eth-updown-15m-0".into(),
                side: Side::Up,
                book: Book::default(),
            },
        );
        let registry = Arc::new(BookRegistry::new());
        let (tx, _rx) = mpsc::channel::<BookUpdate>(32);
        let msg = r#"[{"event_type":"last_trade_price","asset_id":"A","price":"0.5","size":"5","side":"BUY","fee_rate_bps":"0"},
                       {"event_type":"last_trade_price","asset_id":"A","price":"0.51","size":"7","side":"SELL","fee_rate_bps":"0"}]"#;
        handle_text_message(msg, &mut state, &tx, Some(&registry), None).await;

        let sig = registry.signals_for("eth-updown-15m-0");
        let s = sig.lock().unwrap();
        assert_eq!(s.trades_5s.count(), 2);
        assert_eq!(s.trades_30s.count(), 2);
        assert_eq!(s.trades_60s.count(), 2);
    }

    #[tokio::test]
    async fn last_trade_price_forwards_aggressor_tagged_print_when_channel_attached() {
        let mut state = HashMap::new();
        state.insert(
            "A".to_string(),
            AssetState {
                slug: "btc-updown-5m-0".into(),
                side: Side::Down,
                book: Book::default(),
            },
        );
        let (tx, _rx) = mpsc::channel::<BookUpdate>(8);
        let (ttx, mut trx) = mpsc::channel::<TradePrint>(8);
        let msg = r#"{"event_type":"last_trade_price","asset_id":"A","price":"0.60","size":"8.13","side":"BUY","fee_rate_bps":"0"}"#;
        handle_text_message(msg, &mut state, &tx, None, Some(&ttx)).await;
        let p = trx.try_recv().expect("print forwarded");
        assert_eq!(p.slug, "btc-updown-5m-0");
        assert_eq!(p.side, Side::Down);
        assert_abs_diff_eq!(p.price, 0.60, epsilon = 1e-9);
        assert_abs_diff_eq!(p.size, 8.13, epsilon = 1e-9);
        assert_eq!(p.aggressor.as_deref(), Some("BUY"));
        // degraded print (no price) is dropped, not panicked on
        let msg2 = r#"{"event_type":"last_trade_price","asset_id":"A"}"#;
        handle_text_message(msg2, &mut state, &tx, None, Some(&ttx)).await;
        assert!(trx.try_recv().is_err(), "price-less print not forwarded");
    }

    #[tokio::test]
    async fn handle_book_event_updates_registry() {
        use crate::polymarket::book_registry::BookRegistry;
        use std::sync::Arc;

        let registry = Arc::new(BookRegistry::new());
        let mut state = HashMap::new();
        state.insert(
            "A".to_string(),
            AssetState {
                slug: "eth-updown-15m-0".into(),
                side: Side::Up,
                book: Book::default(),
            },
        );
        let (tx, mut rx) = mpsc::channel::<BookUpdate>(32);

        let msg = r#"[{
            "event_type":"book",
            "asset_id":"A",
            "bids":[{"price":"0.30","size":"10"}],
            "asks":[{"price":"0.70","size":"5"}]
        }]"#;
        handle_text_message(msg, &mut state, &tx, Some(&registry), None).await;

        let _ = rx.recv().await;
        let snap = registry
            .get("eth-updown-15m-0", Side::Up)
            .expect("registry populated");
        assert_abs_diff_eq!(snap.best_ask().unwrap(), 0.70, epsilon = 1e-9);
        assert_abs_diff_eq!(snap.best_bid().unwrap(), 0.30, epsilon = 1e-9);
    }

    #[tokio::test]
    async fn handle_price_change_updates_registry_incrementally() {
        use crate::polymarket::book_registry::BookRegistry;
        use std::sync::Arc;

        let registry = Arc::new(BookRegistry::new());
        let mut state = HashMap::new();
        state.insert(
            "A".to_string(),
            AssetState {
                slug: "btc-updown-15m-0".into(),
                side: Side::Down,
                book: Book::default(),
            },
        );
        let (tx, mut rx) = mpsc::channel::<BookUpdate>(32);

        let seed = r#"[{"event_type":"book","asset_id":"A",
            "bids":[{"price":"0.30","size":"10"}],
            "asks":[{"price":"0.70","size":"5"}]}]"#;
        handle_text_message(seed, &mut state, &tx, Some(&registry), None).await;
        let _ = rx.recv().await;

        let update = r#"[{"event_type":"price_change","asset_id":"A",
            "changes":[{"price":"0.65","side":"SELL","size":"10"}]}]"#;
        handle_text_message(update, &mut state, &tx, Some(&registry), None).await;
        let _ = rx.recv().await;

        let snap = registry
            .get("btc-updown-15m-0", Side::Down)
            .expect("registry populated");
        assert_abs_diff_eq!(snap.best_ask().unwrap(), 0.65, epsilon = 1e-9);
    }

    #[tokio::test]
    async fn handle_book_event_captures_full_depth_in_registry() {
        use crate::polymarket::book_registry::BookRegistry;
        use std::sync::Arc;

        let registry = Arc::new(BookRegistry::new());
        let mut state = HashMap::new();
        state.insert(
            "A".to_string(),
            AssetState {
                slug: "eth-updown-15m-0".into(),
                side: Side::Up,
                book: Book::default(),
            },
        );
        let (tx, mut rx) = mpsc::channel::<BookUpdate>(32);

        let msg = r#"[{
            "event_type":"book",
            "asset_id":"A",
            "bids":[
                {"price":"0.30","size":"10"},
                {"price":"0.29","size":"20"}
            ],
            "asks":[
                {"price":"0.70","size":"5"},
                {"price":"0.71","size":"15"},
                {"price":"0.72","size":"25"}
            ]
        }]"#;
        handle_text_message(msg, &mut state, &tx, Some(&registry), None).await;
        let _ = rx.recv().await;

        let snap = registry
            .get("eth-updown-15m-0", Side::Up)
            .expect("registry populated");
        assert_eq!(snap.bids.len(), 2);
        assert_eq!(snap.asks.len(), 3);
        assert_abs_diff_eq!(snap.asks[0].price, 0.70, epsilon = 1e-9);
        assert_abs_diff_eq!(snap.asks[0].size, 5.0, epsilon = 1e-9);
        assert_abs_diff_eq!(snap.asks[1].price, 0.71, epsilon = 1e-9);
        assert_abs_diff_eq!(snap.asks[2].price, 0.72, epsilon = 1e-9);
        assert_abs_diff_eq!(snap.bids[0].price, 0.30, epsilon = 1e-9);
        assert_abs_diff_eq!(snap.bids[1].price, 0.29, epsilon = 1e-9);
    }

    #[test]
    fn ingest_refuses_empty_subs() {
        // run() is async, but new() is sync and subs are empty → caller error path.
        let ing = ClobWsIngest::new("wss://example", vec![]);
        // Can't call run without a runtime; just check construction succeeds.
        assert_eq!(ing.subs.len(), 0);
    }

    #[test]
    fn book_stale_reconnect_exceeds_ping_interval() {
        // The self-heal timeout must sit above the 8s ping so a legitimately slow-but-alive book
        // does not churn the connection; the deterministic async reconnect path itself is verified
        // by the solo live canary, not a fake-socket unit test.
        assert!(super::BOOK_STALE_RECONNECT > super::PING_INTERVAL);
    }

    #[test]
    fn stale_gap_reconnects_while_watchdog_armed() {
        const US: u64 = 1_000_000;
        // 12s gap, no quiet-after cutoff: legacy behavior, reconnect.
        assert!(super::should_stale_reconnect(100 * US, 88 * US, 0, DEF));
        // Cutoff set but not yet reached: still armed.
        assert!(super::should_stale_reconnect(100 * US, 88 * US, 200 * US, DEF));
    }

    #[test]
    fn stale_gap_past_quiet_after_does_not_reconnect() {
        const US: u64 = 1_000_000;
        // Post-close (now past the cutoff) the book is legitimately sparse →
        // silent at resolution; a 12s+ gap must NOT churn the connection
        // (2026-07-06: 6-9 forced reconnects/slot in a
        // post-close hold).
        assert!(!super::should_stale_reconnect(300 * US, 88 * US, 200 * US, DEF));
        // Boundary: exactly at the cutoff counts as quiesced.
        assert!(!super::should_stale_reconnect(200 * US, 88 * US, 200 * US, DEF));
    }

    #[test]
    fn fresh_book_never_reconnects() {
        const US: u64 = 1_000_000;
        assert!(!super::should_stale_reconnect(100 * US, 95 * US, 0, DEF));
        assert!(!super::should_stale_reconnect(100 * US, 95 * US, 200 * US, DEF));
    }

    /// Default threshold constant for the pre-existing tests.
    const DEF: u64 = super::BOOK_STALE_RECONNECT.as_micros() as u64;

    #[test]
    fn configurable_stale_threshold_tolerates_quiet_book() {
        const US: u64 = 1_000_000;
        // Per-ingest LONGER threshold (40s): a quiet-but-alive book must NOT
        // reconnect on a 12s gap (front-of-wall fix — a quiet 99¢
        // book pre-close is normal; the 5s kill-health gate already guards
        // stale-book trade safety, so the 11s reconnect is pure churn here).
        assert!(!super::should_stale_reconnect(100 * US, 88 * US, 0, 40 * US)); // 12s < 40s
        // …but a genuinely dead 41s feed still self-heals.
        assert!(super::should_stale_reconnect(129 * US, 88 * US, 0, 40 * US)); // 41s > 40s
        // Default threshold (11s) preserves legacy behavior: 12s reconnects.
        assert!(super::should_stale_reconnect(100 * US, 88 * US, 0, DEF)); // 12s > 11s
    }

    #[test]
    fn build_shared_tls_connector_succeeds_on_dev_machine() {
        // Load native roots. On every supported dev machine there must be at
        // least one CA in the trust store; this is the pre-warm path the
        // runtime exercises at startup. Also ensures `Connector::Rustls` is
        // the chosen variant (no accidental fallback to Plain).
        let conn = build_shared_tls_connector().expect("native roots present");
        match conn {
            Connector::Rustls(_) => {}
            _ => panic!("expected Rustls connector; other TLS feature must be off"),
        }
    }

    #[test]
    fn with_tls_connector_injects_cached_config() {
        // Pin: the builder method takes a `Connector` by value and stashes it
        // on the ingest so `connect_and_stream` can reuse it on every
        // reconnect. Without this, each market transition re-loaded the
        // native cert store on a tokio worker (28 s `quoter_wake` extreme).
        let ing = ClobWsIngest::new("wss://example", vec![]);
        assert!(ing.tls_connector.is_none());
        let conn = build_shared_tls_connector().expect("load native roots");
        let ing = ing.with_tls_connector(conn);
        assert!(ing.tls_connector.is_some());
    }

    #[test]
    fn subscription_fields_preserved() {
        let s = sub("AID", "eth-updown-15m-1", Side::Up);
        assert_eq!(s.asset_id, "AID");
        assert_eq!(s.slug, "eth-updown-15m-1");
        assert_eq!(s.side, Side::Up);
    }

    #[tokio::test]
    async fn book_bearing_messages_stamp_the_clock_but_pong_and_unknown_do_not() {
        let (tx, _rx) = mpsc::channel(16);
        let mut state: HashMap<String, AssetState> = HashMap::new();

        // price_change → book-bearing → true
        let pc = r#"{"event_type":"price_change","asset_id":"a","changes":[{"price":"0.50","side":"BUY","size":"10"}]}"#;
        assert!(handle_text_message(pc, &mut state, &tx, None, None).await);

        // book snapshot → book-bearing → true
        let book = r#"{"event_type":"book","asset_id":"a","bids":[{"price":"0.49","size":"5"}],"asks":[{"price":"0.51","size":"5"}]}"#;
        assert!(handle_text_message(book, &mut state, &tx, None, None).await);

        // last_trade_price → book-bearing (proof the feed is live) → true
        let ltp = r#"{"event_type":"last_trade_price","asset_id":"a"}"#;
        assert!(handle_text_message(ltp, &mut state, &tx, None, None).await);

        // server text "PONG" keepalive → NOT book-bearing → false
        assert!(!handle_text_message("PONG", &mut state, &tx, None, None).await);

        // unknown event type (e.g. tick_size_change) → NOT book-bearing → false
        let unk = r#"{"event_type":"tick_size_change","asset_id":"a","new_tick_size":"0.01"}"#;
        assert!(!handle_text_message(unk, &mut state, &tx, None, None).await);
    }

    #[tokio::test]
    async fn shutdown_token_cancels_run_before_first_connect() {
        use tokio_util::sync::CancellationToken;
        let subs = vec![Subscription {
            asset_id: "TEST".into(),
            slug: "test-slug".into(),
            side: Side::Up,
        }];
        let shutdown = CancellationToken::new();
        // Point at an unreachable URL so connect would otherwise loop forever.
        let ingest =
            ClobWsIngest::new("ws://127.0.0.1:1", subs).with_shutdown_token(shutdown.clone());
        let (tx, _rx) = tokio::sync::mpsc::channel::<crate::types::BookUpdate>(8);

        shutdown.cancel();
        let res = tokio::time::timeout(std::time::Duration::from_secs(2), ingest.run(tx)).await;
        assert!(
            res.is_ok(),
            "run did not return within 2s after shutdown cancel"
        );
        assert!(res.unwrap().is_ok(), "run returned Ok on shutdown");
    }

    #[tokio::test]
    #[ignore = "live network; run with: cargo test -- --ignored live_ws_emits_book_updates"]
    async fn live_ws_emits_book_updates() {
        use crate::polymarket::gamma::GammaClient;

        // Pick an upcoming ETH market ~30 min out (markets are minted
        // ~22-24 h in advance per observed gamma cadence) so we're
        // subscribing to a token with a real populated book.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let target = (now / 900) * 900 + 1800;
        let slug = format!("eth-updown-15m-{target}");
        let gamma = GammaClient::new("https://gamma-api.polymarket.com").unwrap();
        let meta = gamma
            .fetch_by_slug(&slug)
            .await
            .expect("gamma fetch")
            .expect("market should exist 30 min out");

        let subs = vec![
            Subscription {
                asset_id: meta.token_id_up.clone(),
                slug: slug.clone(),
                side: Side::Up,
            },
            Subscription {
                asset_id: meta.token_id_down.clone(),
                slug: slug.clone(),
                side: Side::Down,
            },
        ];
        let ingest =
            ClobWsIngest::new("wss://ws-subscriptions-clob.polymarket.com/ws/market", subs);
        let (tx, mut rx) = mpsc::channel::<BookUpdate>(256);
        let h = tokio::spawn(ingest.run(tx));

        let mut count: usize = 0;
        let mut any_ask = false;
        let deadline = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = &mut deadline => break,
                Some(u) = rx.recv() => {
                    count += 1;
                    if u.best_ask.is_some() {
                        any_ask = true;
                    }
                }
            }
        }
        h.abort();

        assert!(count >= 1, "expected ≥1 BookUpdate in 5s, got 0");
        assert!(any_ask, "expected at least one update with a best_ask");
        println!("live_ws_emits_book_updates: {count} updates in 5s");
    }
}
