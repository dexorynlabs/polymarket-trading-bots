//! User-channel WS — feeds the LiveClob fills broadcast and the
//! per-slug MakerRebateLedger.
//!
//! Schema sources (canonical AsyncAPI + TS SDK; see
//! `tests/fixtures/v2_fill_event.notes.md` for full provenance):
//! - https://docs.polymarket.com/api-reference/wss/user.md (AsyncAPI SoT)
//! - https://docs.polymarket.com/market-data/websocket/user-channel.md
//!   (narrative + JSON examples)
//! - https://github.com/Polymarket/clob-client/blob/main/src/types.ts
//!   (`MakerOrder`, `Trade` interfaces)
//!
//! Wire facts pinned by the schema:
//! - Top-level discriminator is `type` (uppercase: TRADE / PLACEMENT /
//!   UPDATE / CANCELLATION). The lower-case `event_type` field is also
//!   present on every event ("trade" / "order"). We deserialize via
//!   `#[serde(rename = "type", alias = "event_type")]` so V1-shaped
//!   fixtures (which used the lowercase form) still parse.
//! - All amounts (`price`, `size`, `matched_amount`, `fee_rate_bps`,
//!   `amount`) are string-decimals on the wire — NOT JSON numbers.
//! - `side` is `"BUY"` / `"SELL"` strings; `trader_side` is
//!   `"TAKER"` / `"MAKER"`.
//! - `timestamp` is documented as ms-since-epoch but the JSON examples
//!   show 10-digit Unix seconds. We defensively detect via magnitude.
//! - `market` is the on-chain `condition_id` (0x-prefixed hex), NOT a
//!   slug. Slug resolution happens elsewhere (registry).
//!
//! Audit deltas baked in (preserved from V1):
//! - #5 subscribe `type` lowercase "user"; auth in subscribe message;
//!   only MINED + CONFIRMED produce a Fill.
//! - B5: `fee_rate_bps` is a bps integer (string), not USD.
//! - B6: emit one Fill per `maker_orders[]` entry, keyed by maker
//!   `order_id` (hex), sized by `matched_amount`. Filter
//!   `maker_orders[]` by `owner == our_api_key` UUID — Polymarket
//!   includes other participants' filled orders in the same event.
//! - I2: defensive seconds-vs-ms timestamp detection.
//!
//! Phase 9 + 10B additions:
//! - Exhaustive event-type triage (TRADE / MATCH / MAKER_REBATE /
//!   REWARD / CANCEL_ACK / ORDER_PLACED / unknown).
//! - MAKER_REBATE / REWARD route into `MakerRebateLedger` (NOT `fills_tx`).
//!   These event types do NOT exist on the user channel today per the
//!   AsyncAPI; the arms are forward-compatibility scaffolding.
//! - Pinned emit ordering: `position_store.record_fill(&fill)` BEFORE
//!   `fills_tx.send(fill)`. For multi-leg TRADE, all sub-fills land in
//!   PositionStore atomically before any broadcast.
//! - Auth health-check at startup: see `crate::execution::auth_probe::run_auth_probe`.
//!   The probe runs from `runtime.rs` in live mode before any market spawn,
//!   so credential failure surfaces at boot rather than at first order.

use crate::types::{Coin, Fill, LiquidityRole, RestingOrderId, Side};
use crate::execution::auth::ApiCreds;
use crate::execution::maker_rebate_ledger::MakerRebateLedger;
use crate::execution::order_manager::{CancellationEvent, OrderManager};
use crate::execution::position::PositionStore;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

/// Maximum trade.ids to remember for dedup. Polymarket emits 3 status
/// updates per trade (MATCHED → MINED → CONFIRMED) within ~10 s; with
/// at most a few fills/second, 256 covers ≥ 1 minute of history.
const TRADE_DEDUP_CAP: usize = 256;

/// Bounded-size LRU dedup. Returns true if `id` is unseen (caller
/// should emit), false if it has been seen already (caller should skip
/// — duplicate). Empty id always returns true (cannot dedup; pass
/// through unchanged). On first sighting, `id` is appended; the oldest
/// entry is evicted once the queue exceeds `max`.
///
/// Polymarket's user channel emits the same TRADE event multiple times
/// for a single match — once per `status` transition (MATCHED → MINED
/// → CONFIRMED). Without this dedup the fill consumer doubles shares
/// in `PositionStore` and double-charges the `SpendLedger`. Verified
/// against the 2026-05-05 ws_user_probe capture: trade UUID
/// `4effc8d7-9409-46db-aee7-2e60c42e34a0` arrived three times in 11 s.
pub(crate) fn record_and_check_unseen(seen: &mut VecDeque<String>, id: &str, max: usize) -> bool {
    if id.is_empty() {
        return true;
    }
    if seen.iter().any(|s| s == id) {
        return false;
    }
    seen.push_back(id.to_string());
    while seen.len() > max {
        seen.pop_front();
    }
    true
}

/// Maps `asset_id` → (slug, coin, outcome_side). Built by lifecycle's
/// market-spawn hook; consulted by the user-channel WS bridge to
/// resolve incoming trade events into bot-domain Fills.
pub type AssetIdRegistry = Arc<Mutex<HashMap<String, AssetMeta>>>;

#[derive(Clone, Debug)]
pub struct AssetMeta {
    pub slug: String,
    pub coin: Coin,
    pub outcome_side: Side,
}

/// One element of `maker_orders[]` on a TRADE event.
///
/// Field source: AsyncAPI `TradeMakerOrder` schema in
/// https://docs.polymarket.com/api-reference/wss/user.md (verified
/// 2026-04-28). `#[serde(default)]` on every optional means the parse
/// tolerates partial / future-extended payloads.
#[derive(Deserialize, Debug, Default, Clone)]
pub struct MakerOrderRef {
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub matched_amount: Option<String>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub asset_id: Option<String>,
    #[serde(default)]
    pub fee_rate_bps: Option<String>,
    /// API-key UUID of the maker order's owner. The user channel
    /// includes ALL maker_orders matched in the trade, including other
    /// participants' orders. We filter to `owner == our_api_key`.
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub maker_address: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
}

/// Top-level user-channel event.
///
/// Per AsyncAPI two event kinds are documented today: TRADE and ORDER
/// (with PLACEMENT / UPDATE / CANCELLATION subtypes via `type`). The
/// rebate / reward arms in `event_to_fills` are forward-compatibility
/// scaffolding — see notes file for provenance.
#[derive(Deserialize, Debug, Default)]
pub struct UserChannelEvent {
    /// Top-level discriminator. Values seen:
    /// - "TRADE" (TRADE event)
    /// - "PLACEMENT" / "UPDATE" / "CANCELLATION" (ORDER event subtypes)
    /// - Forward: "MATCH", "MAKER_REBATE", "REWARD", "CANCEL_ACK",
    ///   "ORDER_PLACED" — not in AsyncAPI today, but the triage
    ///   anticipates them per spec §3 Patch F.
    ///
    /// Wire fact: per AsyncAPI both `type` (uppercase) and `event_type`
    /// (lowercase, "trade"/"order") appear on every event. We read
    /// `type` for the dispatch. The companion `event_type_lowercase`
    /// field below captures the lowercase form purely for forensics
    /// (e.g. logging both in case of an unknown event). serde rejects
    /// duplicate aliases at the same field, so they must be split.
    /// 06-12: `default` because the venue's `auto_redeem` settlement event
    /// carries ONLY the lowercase `event_type` discriminator — a required
    /// `type` failed the whole parse and dropped the payout at the wire.
    /// Dispatch falls back to `event_kind` via [`UserChannelEvent::kind`].
    #[serde(default, rename = "type")]
    pub event_type: String,

    /// Companion lowercase classifier ("trade" or "order"). Optional
    /// because synthesized fixtures may omit it; present on the real
    /// wire per AsyncAPI. Not used for dispatch — `event_type` (the
    /// uppercase `type` field) is the canonical discriminator.
    #[serde(default, rename = "event_type")]
    pub event_kind: Option<String>,

    // TRADE-shaped fields. Source: AsyncAPI `TradeEvent` properties.
    #[serde(default)]
    pub status: Option<String>,
    /// On TRADE this is the on-chain `condition_id` (0x-hex). On
    /// MAKER_REBATE / REWARD scaffolding we may also accept it as a
    /// slug-bearing field (see `slug` below).
    #[serde(default)]
    pub market: Option<String>,
    #[serde(default)]
    pub asset_id: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub price: Option<String>,
    /// B5: bps integer (string). Convert to USD downstream:
    /// `fee_usd = (bps / 10_000) × shares × price`.
    #[serde(default)]
    pub fee_rate_bps: Option<String>,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub taker_order_id: Option<String>,
    #[serde(default)]
    pub trader_side: Option<String>,
    #[serde(default)]
    pub maker_orders: Vec<MakerOrderRef>,
    /// AsyncAPI says ms-since-epoch; examples are 10-digit seconds.
    /// `event_to_fills` defensively detects via magnitude.
    #[serde(default)]
    pub timestamp: Option<String>,

    // MAKER_REBATE / REWARD scaffolding fields. Source: spec §3 Patch F
    // (no docs / SDK source — these events are not on the user channel
    // today). TODO: validate against real fixture once Polymarket
    // emits them or once we capture one.
    /// Slug bearing the rebate / reward credit. Used by
    /// `MakerRebateLedger::record_*`. Falls back to `market` if absent.
    #[serde(default)]
    pub slug: Option<String>,
    /// USD credit value as a decimal string.
    #[serde(default)]
    pub amount: Option<String>,
    // auto_redeem settlement fields (06-12, observed live — no AsyncAPI doc).
    /// On-chain settlement tx hash of the venue-initiated redeem.
    #[serde(default)]
    pub txn_hash: Option<String>,
    /// CTF condition id (`0x`-hex) of the settled market. Join key back to
    /// a `Position` via `PositionStore::settle_by_condition_id`.
    #[serde(default)]
    pub condition_id: Option<String>,
    // Deliberately NOT using `#[serde(flatten)] HashMap<String, Value>`:
    // serde's default behavior (which is what we want) is to silently
    // ignore unknown fields when `deny_unknown_fields` is absent. A
    // catch-all hashmap would only matter for re-serializing or
    // logging the raw payload — neither is needed here. The unknown-
    // event arm in `event_to_fills` warn-logs the payload via the
    // outer raw-text capture in `run_once`, not via this struct.
}

impl UserChannelEvent {
    /// Canonical dispatch discriminator: the uppercase `type` field when
    /// present, else the lowercase `event_type` uppercased. The fallback
    /// exists for venue events that omit `type` entirely (observed live
    /// 06-12 on `auto_redeem`).
    pub fn kind(&self) -> String {
        if !self.event_type.is_empty() {
            self.event_type.to_ascii_uppercase()
        } else {
            self.event_kind.as_deref().unwrap_or("").to_ascii_uppercase()
        }
    }
}

/// Venue-initiated settlement, forwarded to the strategy AFTER the store
/// settle so the strategy can record realized PnL (telemetry). `tx_hash` is
/// the on-chain settlement transaction — unlike the relayer correlation id
/// on our own redeem submits, this IS proof of on-chain settlement.
#[derive(Clone, Debug)]
pub struct AutoRedeem {
    pub slug: String,
    pub condition_id: String,
    pub amount_usd: f64,
    pub tx_hash: String,
    pub ts_ms: u64,
}

/// Extract `(condition_id, amount_usd, txn_hash)` from a venue `auto_redeem`
/// settlement event. `None` for any other event kind, or when the payout
/// can't be attributed (missing/unparseable amount, missing condition id).
/// `txn_hash` may legitimately be absent on a future wire shape — it falls
/// back to empty string rather than dropping the settlement.
pub fn auto_redeem_from_event(e: &UserChannelEvent) -> Option<(String, f64, String)> {
    if e.kind() != "AUTO_REDEEM" {
        return None;
    }
    let cond = e.condition_id.as_deref()?.to_string();
    let amount_usd: f64 = e.amount.as_deref()?.parse().ok()?;
    let tx = e.txn_hash.clone().unwrap_or_default();
    Some((cond, amount_usd, tx))
}

pub async fn run(
    ws_url: String,
    creds: ApiCreds,
    registry: AssetIdRegistry,
    fills_tx: broadcast::Sender<Fill>,
    position_store: Arc<std::sync::Mutex<PositionStore>>,
    rebate_ledger: Arc<MakerRebateLedger>,
    tls_connector: Option<tokio_tungstenite::Connector>,
) -> Result<()> {
    run_with_provisional_fills(
        ws_url,
        creds,
        registry,
        fills_tx,
        None,
        position_store,
        rebate_ledger,
        tls_connector,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_with_provisional_fills(
    ws_url: String,
    creds: ApiCreds,
    registry: AssetIdRegistry,
    fills_tx: broadcast::Sender<Fill>,
    provisional_fills_tx: Option<broadcast::Sender<Fill>>,
    position_store: Arc<std::sync::Mutex<PositionStore>>,
    rebate_ledger: Arc<MakerRebateLedger>,
    tls_connector: Option<tokio_tungstenite::Connector>,
    auto_redeem_tx: Option<broadcast::Sender<AutoRedeem>>,
) -> Result<()> {
    loop {
        match run_once(
            &ws_url,
            &creds,
            &registry,
            &fills_tx,
            provisional_fills_tx.as_ref(),
            &position_store,
            &rebate_ledger,
            tls_connector.as_ref(),
            auto_redeem_tx.as_ref(),
        )
        .await
        {
            Ok(()) => tracing::warn!("user_ws closed cleanly; reconnecting in 1s"),
            Err(e) => tracing::warn!(?e, "user_ws errored; reconnecting in 1s"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_once(
    ws_url: &str,
    creds: &ApiCreds,
    registry: &AssetIdRegistry,
    fills_tx: &broadcast::Sender<Fill>,
    provisional_fills_tx: Option<&broadcast::Sender<Fill>>,
    position_store: &Arc<std::sync::Mutex<PositionStore>>,
    rebate_ledger: &Arc<MakerRebateLedger>,
    tls_connector: Option<&tokio_tungstenite::Connector>,
    auto_redeem_tx: Option<&broadcast::Sender<AutoRedeem>>,
) -> Result<()> {
    use std::time::Duration;
    use tokio::time::MissedTickBehavior;

    let url = format!("{ws_url}/ws/user");
    // `disable_nagle=true` → TCP_NODELAY on the underlying socket; tungstenite
    // leaves Nagle ON by default which can add ~40ms tail latency to small
    // request/response frames (cancel ACKs, PING/PONG). The shared
    // `tls_connector` (when provided) avoids `rustls_native_certs::load_native_certs`
    // re-walking the OS keychain on every reconnect — same pattern as
    // `clob_ws::build_shared_tls_connector`.
    let (ws, _) =
        tokio_tungstenite::connect_async_tls_with_config(&url, None, true, tls_connector.cloned())
            .await
            .with_context(|| format!("connect {url}"))?;
    let (sink, mut stream) = ws.split();
    let sink = Arc::new(Mutex::new(sink));

    // Subscribe via the shared sink.
    {
        let sub = serde_json::json!({
            "type": "user",
            "auth": {
                "apiKey": creds.key,
                "secret": creds.secret,
                "passphrase": creds.passphrase,
            },
        });
        let mut s = sink.lock().await;
        s.send(Message::Text(sub.to_string().into()))
            .await
            .context("send subscribe")?;
    }

    // Phase 9: auth health-check. Spec §3 Patch F mandates a place+cancel
    // of a tiny order with a 5s deadline post-subscribe to confirm event
    // reception. Stubbed pending live order placement authorization
    // (Phase 1 step 3). Returns Ok(()) so we don't fail-open at startup.
    if let Err(e) = auth_health_check(std::time::Duration::from_secs(5)).await {
        tracing::warn!(?e, "user_ws: auth_health_check returned err (stub)");
    }

    // I1: client-initiated 10s PING. Polymarket idle-timeouts after ~30s
    // without one. Send literal text "PING" (matches the canonical Go
    // reference and the market-channel behavior — NOT WS-frame Ping).
    let ping_sink = sink.clone();
    let ping_handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let mut s = ping_sink.lock().await;
            if s.send(Message::Text("PING".into())).await.is_err() {
                break;
            }
        }
    });

    // 2026-05-05 dedup: Polymarket emits 3 events for the same trade
    // (MATCHED → MINED → CONFIRMED). Our existing status gate at
    // `trade_event_to_fills` drops MATCHED but accepts both MINED and
    // CONFIRMED, so without this LRU the same fill would be recorded
    // twice in `PositionStore` and charged twice in `SpendLedger`.
    // State is per-`run_once`-invocation (i.e. lost on reconnect), which
    // is acceptable: a CONFIRMED arriving after a reconnect would
    // re-emit, but that's an extreme edge (10 s window where the WS
    // also drops). Bounded to `TRADE_DEDUP_CAP` entries.
    let mut seen_trade_ids: VecDeque<String> = VecDeque::with_capacity(TRADE_DEDUP_CAP);
    let mut seen_provisional_trade_ids: VecDeque<String> = VecDeque::with_capacity(TRADE_DEDUP_CAP);

    while let Some(msg) = stream.next().await {
        let msg = msg.context("ws recv")?;
        let text = match msg {
            Message::Text(t) => t,
            Message::Ping(p) => {
                // Server-initiated WS-frame Ping → respond Pong.
                let mut s = sink.lock().await;
                let _ = s.send(Message::Pong(p)).await;
                continue;
            }
            Message::Close(_) => break,
            _ => continue,
        };
        // D10 (06-11 shakedown): the server's 10s textual "PONG" keepalive is not an event —
        // skip it before parsing (clob_ws ignores it the same way in `parse_events`). WARNing on
        // it (one line per PING) is noise that would mask a genuinely malformed frame.
        if is_keepalive_pong(text.as_str()) {
            continue;
        }
        let event: UserChannelEvent = match serde_json::from_str(text.as_str()) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    ?e,
                    raw = %text.as_str().chars().take(200).collect::<String>(),
                    "user_ws parse failed"
                );
                continue;
            }
        };

        // 06-12: venue-initiated settlement. The venue auto-redeems resolved
        // positions on-chain and notifies via `auto_redeem`; settle the
        // matching position by condition id (zeroes shares + marks
        // `redeem_submitted` — the redeem-lane fallback gate reads
        // both), log loudly (operator evidence that capital returned), and
        // forward to the strategy for realized-PnL telemetry.
        if let Some((cond, amount_usd, tx_hash)) = auto_redeem_from_event(&event) {
            let now_ms = crate::types::now_micros() / 1_000;
            let credit = position_store
                .lock()
                .unwrap()
                .settle_by_condition_id(&cond, amount_usd, now_ms);
            match credit {
                Some(c) => {
                    tracing::info!(
                        slug = %c.slug,
                        amount_usd,
                        winner = ?c.winner,
                        tx_hash = %tx_hash,
                        "user_ws: venue auto_redeem settled position"
                    );
                    if let Some(tx) = auto_redeem_tx {
                        let _ = tx.send(AutoRedeem {
                            slug: c.slug,
                            condition_id: cond,
                            amount_usd,
                            tx_hash,
                            ts_ms: now_ms,
                        });
                    }
                }
                None => tracing::info!(
                    condition_id = %cond,
                    amount_usd,
                    tx_hash = %tx_hash,
                    "user_ws: venue auto_redeem for untracked condition (prior run's slot?) — USDC returned on-chain, no position to settle"
                ),
            }
            continue;
        }

        // Forensic raw-event logging (2026-05-08). After the 20:50 ETH-5m
        // slot we observed two attribution-suspicious fills: +6.15 sh Up
        // @ $0.39 (price unrelated to any of our placed bids) and -21 sh
        // Up @ $0.25 (sell against 15.14 sh of held inventory). Without
        // raw event payload we couldn't determine whether the parser
        // mis-attributed a counterparty leg or the venue genuinely
        // executed against us. This emits structured fields from every
        // TRADE event so the next slot's logs let us reconstruct.
        // Behavior-neutral; subscribe to target `recon::user_ws::trade_raw`
        // (or set `RUST_LOG=recon::user_ws::trade_raw=info`) to capture.
        if event.event_type == "TRADE" {
            let owners: Vec<&str> = event
                .maker_orders
                .iter()
                .map(|m| m.owner.as_deref().unwrap_or(""))
                .collect();
            let order_ids: Vec<&str> = event
                .maker_orders
                .iter()
                .map(|m| m.order_id.as_deref().unwrap_or(""))
                .collect();
            let maker_addrs: Vec<&str> = event
                .maker_orders
                .iter()
                .map(|m| m.maker_address.as_deref().unwrap_or(""))
                .collect();
            tracing::info!(
                target: "recon::user_ws::trade_raw",
                trade_id = event.id.as_deref().unwrap_or(""),
                asset_id = event.asset_id.as_deref().unwrap_or(""),
                side = event.side.as_deref().unwrap_or(""),
                trader_side = event.trader_side.as_deref().unwrap_or(""),
                status = event.status.as_deref().unwrap_or(""),
                price = event.price.as_deref().unwrap_or(""),
                size = event.size.as_deref().unwrap_or(""),
                fee_rate_bps = event.fee_rate_bps.as_deref().unwrap_or(""),
                taker_order_id = event.taker_order_id.as_deref().unwrap_or(""),
                maker_orders_count = event.maker_orders.len(),
                maker_order_ids = ?order_ids,
                maker_orders_owners = ?owners,
                maker_orders_addresses = ?maker_addrs,
                raw_truncated = %text.as_str().chars().take(500).collect::<String>(),
                "raw_trade_event"
            );
        }

        if let Some(tx) = provisional_fills_tx {
            let provisional = event_to_provisional_fills(&event, registry, &creds.key).await;
            if !provisional.is_empty() {
                let trade_id = event.id.as_deref().unwrap_or("");
                if record_and_check_unseen(
                    &mut seen_provisional_trade_ids,
                    trade_id,
                    TRADE_DEDUP_CAP,
                ) {
                    for fill in provisional {
                        let _ = tx.send(fill);
                    }
                }
            }
        }

        // Task 3.4: `order_manager = None` here. Live runtime wiring
        // (constructing the OrderManager and threading it through
        // `run`/`run_once` signatures) lands in Phase 4 — this task
        // only delivers the dispatch site so the type + behavior are
        // ready for that wiring.
        let fills = event_to_fills(&event, registry, &creds.key, rebate_ledger, None).await;

        // 2026-05-05 dedup gate. Skip if this trade.id was already
        // emitted by an earlier status transition. See comment at
        // `seen_trade_ids` declaration above.
        if !fills.is_empty() {
            let trade_id = event.id.as_deref().unwrap_or("");
            if !record_and_check_unseen(&mut seen_trade_ids, trade_id, TRADE_DEDUP_CAP) {
                tracing::trace!(
                    trade_id = %trade_id,
                    status = ?event.status,
                    fill_count = fills.len(),
                    "user_ws: dedup skip (trade.id already emitted earlier; status update only)"
                );
                continue;
            }
        }

        // Pinned emit ordering (spec §3 Patch F):
        // 1. ALL fills for this event land in PositionStore atomically.
        // 2. Lock dropped before any broadcast.
        // 3. Then fills are broadcast on `fills_tx`.
        //
        // PositionStore::record_fill is invoked HERE, before fills_tx.send,
        // per spec §3 Patch F's emit-ordering pin. Downstream consumers of
        // fills_tx in live mode MUST NOT also call record_fill — see
        // runtime.rs's fills consumer (`process_resting_fill_ledger_only`)
        // for the live "ledger-only" pattern. Phase 10C fix; paper mode
        // is untouched (no user_ws task).
        //
        // 2026-05-08 Fix A: clamp over-sells against held inventory at
        // this wire boundary (`clamp_fill_to_held`). Pre-fix, an over-
        // sized SELL clamped position-side via `record_fill` (zeroing
        // the side) but the broadcast carried the raw shares; the
        // ledger-only consumer's phantom guard saw `held=0` and dropped
        // the legit portion's proceeds. Now we determine the clamp
        // BEFORE record_fill, broadcast only the legit shares, and the
        // downstream guard sees an exact-fit fill.
        let to_broadcast: Vec<Fill> = if fills.is_empty() {
            Vec::new()
        } else {
            let mut store = position_store.lock().unwrap();
            let mut acc: Vec<Fill> = Vec::with_capacity(fills.len());
            for fill in fills {
                let held = store
                    .get(&fill.slug)
                    .map(|p| match fill.side {
                        Side::Up => p.up_shares,
                        Side::Down => p.down_shares,
                    })
                    .unwrap_or(0.0);
                let raw_shares = fill.shares;
                match clamp_fill_to_held(fill, held) {
                    None => {
                        tracing::warn!(
                            target: "recon::user_ws::oversell",
                            raw_shares,
                            held,
                            "user_ws: phantom sell dropped at wire (no inventory on this side)"
                        );
                    }
                    Some(clamped) => {
                        if (-clamped.shares - (-raw_shares).max(0.0)).abs() > 1e-9
                            && raw_shares < 0.0
                        {
                            tracing::warn!(
                                target: "recon::user_ws::oversell",
                                slug = %clamped.slug,
                                side = ?clamped.side,
                                raw_shares,
                                clamped_shares = clamped.shares,
                                held,
                                price = clamped.price,
                                "user_ws: over-sell clamped to held inventory"
                            );
                        }
                        store.record_fill(&clamped);
                        // Stamp this TRADE's condition_id onto the position so
                        // auto_redeem can settle + credit it (see the fn's doc).
                        stamp_condition_id_from_trade(
                            &mut store,
                            &clamped.slug,
                            event.market.as_deref(),
                        );
                        acc.push(clamped);
                    }
                }
            }
            acc
        };
        for fill in to_broadcast {
            let _ = fills_tx.send(fill);
        }
    }

    ping_handle.abort();
    Ok(())
}

/// Auth health-check stub.
///
/// **Spec contract (§3 Patch F):** after subscribing, place + cancel a
/// tiny order with a `deadline` budget; expect at least one user-channel
/// event within that window. If none arrives, return `Err(...)` so the
/// caller can fail closed.
///
/// **Current status:** stubbed pending Phase 1 step 3 (live order
/// placement) user authorization. Returns `Ok(())` unconditionally with
/// a `warn!` flagging the deferred status. Operators see this once at
/// startup.
///
/// **TODO(phase-1-step-3):** implement once the user authorizes a live
/// place+cancel. Until then, the WS subscription is treated as healthy
/// purely on the basis of a successful TCP connect + subscribe-write.
async fn auth_health_check(deadline: std::time::Duration) -> Result<()> {
    tracing::warn!(
        ?deadline,
        "user_ws: auth_health_check stubbed (Phase 1 step 3 deferred)"
    );
    Ok(())
}

/// Map a wire `trader_side` string to our domain `LiquidityRole` enum.
///
/// Polymarket's user channel emits `trader_side: "MAKER" | "TAKER"` on
/// TRADE events. We accept either case and treat anything else
/// (including absent / empty / future values) as `Unknown` so the
/// downstream rebate / TAKER-rebalance gate can fall back gracefully
/// rather than misattribute.
#[inline]
/// D10: true for the server's textual "PONG" keepalive frame (sent in reply to our 10s text
/// PING) — not an event, must not reach the JSON parser / its WARN.
fn is_keepalive_pong(text: &str) -> bool {
    text.trim() == "PONG"
}

fn parse_liquidity_role(trader_side: Option<&str>) -> LiquidityRole {
    match trader_side.map(str::trim) {
        Some(s) if s.eq_ignore_ascii_case("MAKER") => LiquidityRole::Maker,
        Some(s) if s.eq_ignore_ascii_case("TAKER") => LiquidityRole::Taker,
        _ => LiquidityRole::Unknown,
    }
}

/// Translate a single user-channel event into zero or more `Fill`s,
/// routing rebate / reward events into `MakerRebateLedger` along the way.
///
/// **Triage (spec §3 Patch F):**
/// - `TRADE` → emit per-leg `Fill`s via [`trade_event_to_fills`].
/// - `MATCH` → log-only (per-leg state echo; TRADE already covers the
///   position-changing portion).
/// - `MAKER_REBATE` → route to `MakerRebateLedger::record_maker_rebate`
///   (USD credit, NOT a share change). NOT in AsyncAPI today; arm is
///   forward-compatibility scaffolding.
/// - `REWARD` → route to `MakerRebateLedger::record_reward`. Also forward-
///   compatibility scaffolding.
/// - `CANCELLATION` / `CANCEL_ACK` → routes into `OrderManager::handle_cancellation`
///   when one is provided (Task 3.4 — see `order_manager.rs` for the
///   contract). Both names are accepted because the cancel_latency_probe
///   confirms the venue currently emits both, and recon's spec uses
///   `CANCELLATION` while the probe uses `CANCEL_ACK`. No Fill emission.
/// - `ORDER_PLACED` / `PLACEMENT` / `UPDATE` → submitter handles its
///   own id-/cancel-tracking; no Fill emission, no warn.
/// - Unknown → warn-log with type + drop.
///
/// `event_type` matching is case-insensitive (uppercased) so a
/// future fixture using lowercase shapes still routes correctly.
///
/// `order_manager` is `None` in paper mode and in legacy callers that
/// don't yet thread the manager through (most tests). Live runtime
/// wiring lands in Phase 4 — Task 3.4 only delivers the type + the
/// dispatch site, not the runtime plumbing.
pub async fn event_to_fills(
    e: &UserChannelEvent,
    registry: &AssetIdRegistry,
    our_api_key: &str,
    rebate_ledger: &Arc<MakerRebateLedger>,
    order_manager: Option<&Arc<OrderManager>>,
) -> Vec<Fill> {
    let kind = e.kind();
    match kind.as_str() {
        "TRADE" => trade_event_to_fills(e, registry, our_api_key).await,
        // Settlement, not a fill — handled upstream in `run_once` (store
        // settle + strategy forward). Defensive no-op for direct callers.
        "AUTO_REDEEM" => Vec::new(),
        "MATCH" => {
            tracing::debug!(asset = ?e.asset_id, id = ?e.id,
                "user_ws: MATCH event — log only (TRADE handles position)");
            Vec::new()
        }
        "MAKER_REBATE" => {
            // Routes USD credit into the per-slug ledger. Forward-compat
            // scaffolding — not present in AsyncAPI as of 2026-04-28.
            // Falls back to `market` if `slug` is absent (defensive).
            let slug = e.slug.as_deref().or(e.market.as_deref());
            if let (Some(slug), Some(amount_str)) = (slug, e.amount.as_deref()) {
                match amount_str.parse::<f64>() {
                    Ok(usd) => {
                        rebate_ledger.record_maker_rebate(slug, usd);
                    }
                    Err(_) => tracing::warn!(
                        slug,
                        amount_str,
                        "user_ws: MAKER_REBATE amount unparseable — dropping"
                    ),
                }
            } else {
                tracing::warn!(slug = ?slug, amount = ?e.amount,
                    "user_ws: MAKER_REBATE missing slug or amount — dropping");
            }
            Vec::new()
        }
        "REWARD" => {
            let slug = e.slug.as_deref().or(e.market.as_deref());
            if let (Some(slug), Some(amount_str)) = (slug, e.amount.as_deref()) {
                match amount_str.parse::<f64>() {
                    Ok(usd) => {
                        rebate_ledger.record_reward(slug, usd);
                    }
                    Err(_) => tracing::warn!(
                        slug,
                        amount_str,
                        "user_ws: REWARD amount unparseable — dropping"
                    ),
                }
            } else {
                tracing::warn!(slug = ?slug, amount = ?e.amount,
                    "user_ws: REWARD missing slug or amount — dropping");
            }
            Vec::new()
        }
        // Task 3.4: CANCELLATION (and the equivalent CANCEL_ACK echo
        // observed by cancel_latency_probe) routes into the
        // OrderManager when present. Spec §15 Module 4 mandates this:
        // the WS confirm is the canonical Live → Cancelled signal
        // (p99 ~507 ms vs DELETE p99 7.77 s). When `order_manager` is
        // None (paper mode / legacy tests) we silently drop, matching
        // pre-Task-3.4 behavior.
        "CANCELLATION" | "CANCEL_ACK" => {
            if let Some(om) = order_manager {
                if let Some(order_id) = e.id.as_deref() {
                    // Magnitude-detect timestamp the same way
                    // trade_event_to_fills does (sec vs ms vs micros).
                    // CANCELLATION events use the same `timestamp`
                    // field per AsyncAPI ORDER schema.
                    let ts: u64 = e
                        .timestamp
                        .as_deref()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    let ts_micros = if ts < 1_000_000_000_000 {
                        ts.saturating_mul(1_000_000)
                    } else {
                        ts.saturating_mul(1_000)
                    };
                    om.handle_cancellation(CancellationEvent {
                        order_id: order_id.to_string(),
                        ts_micros,
                        slug: e.slug.clone(),
                        side: None,
                    });
                } else {
                    // No order id on the wire — can't dispatch.
                    // Defensive: log at trace so a future schema shift
                    // (id absent on a real CANCELLATION) shows up.
                    tracing::trace!(
                        "user_ws: CANCELLATION/CANCEL_ACK without `id` field — dropping"
                    );
                }
            }
            Vec::new()
        }
        // Other order lifecycle events. The submitter's cancel_index +
        // pending-order map already track placements via the REST POST
        // round-trip; the WS event is informational here.
        "ORDER_PLACED" | "PLACEMENT" | "UPDATE" => Vec::new(),
        // Robustness: empty discriminator (deserialized from a payload
        // missing both `type` and `event_type`) drops silently — the
        // outer parse-fail branch in `run_once` already warn-logs that
        // case.
        "" => Vec::new(),
        other => {
            tracing::warn!(event_type = %other,
                "user_ws: unknown event type — dropping");
            Vec::new()
        }
    }
}

/// Best-effort provisional exposure stream for TRADE/MATCHED events. These fills are deliberately
/// NOT recorded into PositionStore and are NOT emitted on the settled `fills_tx`; callers may use
/// them only for decision exposure while awaiting MINED/CONFIRMED.
pub async fn event_to_provisional_fills(
    e: &UserChannelEvent,
    registry: &AssetIdRegistry,
    our_api_key: &str,
) -> Vec<Fill> {
    let kind = e.event_type.to_ascii_uppercase();
    if kind == "TRADE" {
        trade_event_to_provisional_fills(e, registry, our_api_key).await
    } else {
        Vec::new()
    }
}

/// TRADE-event Fill emission. Encapsulates V1's wire-correct logic
/// (MAKER path: per-`maker_orders[]` filtered by owner; TAKER path:
/// best-effort single Fill from top-level fields).
///
/// MAKER path: emit one Fill per `maker_orders[i]` whose `owner` matches
/// our API key UUID. Each Fill keys on the maker order's hex `order_id`,
/// sized by `matched_amount`. Trade events can match multiple resting
/// bids — both ours and other participants'; the owner filter is
/// load-bearing for PositionStore correctness.
///
/// TAKER path: emit a single best-effort Fill from top-level fields.
/// Note: `place_taker_buy` currently discards the FOK order ID
/// (live_clob.rs:276-278), so `taker_order_id` won't match anything in
/// submitter's cancel_index. PositionStore + SpendLedger key on
/// slug/side/shares/price/fee — the Fill still does real accounting if
/// the registry is populated.
async fn trade_event_to_fills(
    e: &UserChannelEvent,
    registry: &AssetIdRegistry,
    our_api_key: &str,
) -> Vec<Fill> {
    let status = e.status.as_deref().unwrap_or("");
    // Empty status falls through to the maker/taker dispatch below; this
    // keeps the synthesized `{"type":"TRADE",...}` fixtures (no `status`
    // field) compatible with legitimate "MATCHED" gating that real
    // payloads always carry. The legitimate gate (MINED/CONFIRMED only)
    // applies once status is known.
    if !status.is_empty() && status != "MINED" && status != "CONFIRMED" {
        if status == "FAILED" {
            tracing::warn!(asset = ?e.asset_id, id = ?e.id, "trade FAILED on chain; dropping");
        } else if status == "RETRYING" {
            tracing::warn!(asset = ?e.asset_id, id = ?e.id, "trade RETRYING; awaiting resolution");
        }
        return Vec::new();
    }

    trade_event_to_fills_unchecked(e, registry, our_api_key).await
}

async fn trade_event_to_provisional_fills(
    e: &UserChannelEvent,
    registry: &AssetIdRegistry,
    our_api_key: &str,
) -> Vec<Fill> {
    if e.status.as_deref().unwrap_or("") != "MATCHED" {
        return Vec::new();
    }
    trade_event_to_fills_unchecked(e, registry, our_api_key).await
}

async fn trade_event_to_fills_unchecked(
    e: &UserChannelEvent,
    registry: &AssetIdRegistry,
    our_api_key: &str,
) -> Vec<Fill> {
    let trader_side = e.trader_side.as_deref().unwrap_or("");
    let side_str = e.side.as_deref().unwrap_or("");
    let ts: u64 = e
        .timestamp
        .as_deref()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // I2: docs are inconsistent (AsyncAPI says ms, examples are 10-digit
    // seconds). Defensive detect by magnitude.
    let ts_micros = if ts < 1_000_000_000_000 {
        ts.saturating_mul(1_000_000)
    } else {
        ts.saturating_mul(1_000)
    };

    let mut out = Vec::new();

    // MAKER path: filter maker_orders[] by ownership.
    if trader_side == "MAKER" || (trader_side != "TAKER" && !e.maker_orders.is_empty()) {
        let registry_snapshot = registry.lock().await;
        for mo in &e.maker_orders {
            // Only emit Fills for our own resting orders. maker_orders[]
            // can include other participants' orders that filled in the
            // same trade.
            if let Some(owner) = &mo.owner {
                if owner != our_api_key {
                    continue;
                }
            }
            // If owner field is absent (defensive serde with None
            // default), fall through and emit — Polymarket's user-
            // channel only sends events for our account, so a missing-
            // owner entry is most likely ours. Documented degraded mode
            // (V1 contract pinned by tests).
            let asset_id = match mo.asset_id.as_deref() {
                Some(a) => a.to_string(),
                None => continue,
            };
            let meta = match registry_snapshot.get(&asset_id) {
                Some(m) => m.clone(),
                None => {
                    // Diagnostic warn (added 2026-05-05): a TRADE event
                    // referenced an asset_id we couldn't resolve to a
                    // slug. Pre-fix this happened on every fill because
                    // lifecycle didn't populate the registry. Post-fix
                    // this should be vanishingly rare — surface loudly
                    // so a future regression in the same area doesn't
                    // silently drop fills again.
                    tracing::warn!(
                        asset_id = %asset_id,
                        order_id = ?mo.order_id,
                        "user_ws: TRADE maker leg references unknown asset_id; \
                         fill dropped (registry not populated for this market?)"
                    );
                    continue;
                }
            };
            let raw_size: f64 = match mo.matched_amount.as_deref().and_then(|s| s.parse().ok()) {
                Some(v) => v,
                None => continue,
            };
            let price: f64 = match mo.price.as_deref().and_then(|s| s.parse().ok()) {
                Some(v) => v,
                None => continue,
            };
            let fee_bps: f64 = mo
                .fee_rate_bps
                .as_deref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let maker_side = mo.side.as_deref().filter(|s| !s.is_empty());
            let signed_shares = match maker_side {
                Some("SELL") => -raw_size,
                Some(_) => raw_size, // explicit BUY (or any present non-SELL)
                None => {
                    // No per-order side on the wire (degraded maker entry; the
                    // 2026-06-04 complementary-mint legs had order_id=null too).
                    // Do NOT inherit `side_str`: that is the TAKER's perspective
                    // and is "SELL" on a mint leg even though our RESTING maker
                    // order is a BUY (incident-2026-06-04 §7). Every resting
                    // maker order our accounts place is a BUY, so a maker fill is
                    // a BUY. A future resting-ASK strategy must revisit this.
                    tracing::warn!(
                        order_id = ?mo.order_id, top_level_side = side_str,
                        "user_ws: maker leg missing per-order side; signing as BUY \
                         (resting-bid invariant — see incident-2026-06-04 §7)"
                    );
                    raw_size
                }
            };
            let fee_usd = (fee_bps / 10_000.0) * raw_size * price;
            let order_id = mo.order_id.clone().unwrap_or_default();
            out.push(Fill {
                slug: meta.slug,
                coin: meta.coin,
                side: meta.outcome_side,
                token_id: asset_id,
                id: RestingOrderId::from_string(order_id),
                shares: signed_shares,
                price,
                usd: (price * signed_shares).abs(),
                fee_usd,
                ts_micros,
                liquidity_role: parse_liquidity_role(e.trader_side.as_deref()),
            });
        }
    } else if trader_side == "TAKER" {
        // TAKER path: best-effort single Fill. See doc comment above.
        let asset_id = match e.asset_id.as_ref() {
            Some(a) => a.clone(),
            None => return out,
        };
        let meta = {
            let g = registry.lock().await;
            match g.get(&asset_id) {
                Some(m) => m.clone(),
                None => {
                    tracing::warn!(
                        asset_id = %asset_id,
                        taker_order_id = ?e.taker_order_id,
                        "user_ws: TRADE TAKER leg references unknown asset_id; \
                         fill dropped (registry not populated for this market?)"
                    );
                    return out;
                }
            }
        };
        let raw_size: f64 = match e.size.as_deref().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return out,
        };
        let price: f64 = match e.price.as_deref().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return out,
        };
        let fee_bps: f64 = e
            .fee_rate_bps
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let signed_shares = if side_str == "SELL" {
            -raw_size
        } else {
            raw_size
        };
        let fee_usd = (fee_bps / 10_000.0) * raw_size * price;
        let id_str = e
            .taker_order_id
            .clone()
            .or_else(|| e.id.clone())
            .unwrap_or_default();
        out.push(Fill {
            slug: meta.slug,
            coin: meta.coin,
            side: meta.outcome_side,
            token_id: asset_id,
            id: RestingOrderId::from_string(id_str),
            shares: signed_shares,
            price,
            usd: (price * signed_shares).abs(),
            fee_usd,
            ts_micros,
            liquidity_role: parse_liquidity_role(e.trader_side.as_deref()),
        });
    }
    // else: trader_side empty AND maker_orders empty AND not TAKER →
    // can't determine our role; drop silently (registry would have
    // dropped it anyway).

    out
}

/// Clamp a single sell `Fill` against the inventory currently held on
/// that (slug, side). Returns:
/// - `Some(fill)` for any BUY (passed through unchanged).
/// - `Some(fill)` for a SELL whose `|shares| <= held` (passed through
///   unchanged).
/// - `Some(clamped)` for a SELL with `|shares| > held > 0`. Shares,
///   USD, and fee are prorated by `held / |shares|`.
/// - `None` for a SELL against zero inventory ("phantom sell").
///
/// Why: the live user-channel sometimes reports SELL fills that exceed
/// our held inventory (observed 20:50 ETH-5m: -21 sh against 15.14 sh
/// held). Without this clamp, `PositionStore::record_fill` zeroes the
/// side ([position.rs:166–183](exec::position)), then the live ledger
/// consumer's phantom guard ([runtime.rs:240](runtime)) sees `held=0`
/// and drops the entire ledger record, silently losing ~$3.79 of legit
/// proceeds. By clamping at the wire boundary we book the legit
/// portion and drop only the over-amount.
pub(crate) fn clamp_fill_to_held(fill: Fill, held: f64) -> Option<Fill> {
    if fill.shares >= 0.0 {
        return Some(fill);
    }
    if held <= 0.0 {
        return None;
    }
    let want = -fill.shares;
    if want <= held {
        return Some(fill);
    }
    // Over-sell: clamp shares to held; prorate usd and fee.
    let scale = held / want;
    let mut clamped = fill;
    clamped.shares = -held;
    clamped.usd = held * clamped.price;
    clamped.fee_usd *= scale;
    Some(clamped)
}

/// Stamp the CTF `condition_id` (a TRADE event's `market` field — on a TRADE
/// that field IS the on-chain condition_id, per `UserChannelEvent::market`)
/// onto the position `record_fill` just created, so a later venue `auto_redeem`
/// can join it via `PositionStore::settle_by_condition_id`.
///
/// Why: the live `PositionStore` is fed ONLY by `record_fill`, which knows the
/// slug but never the on-chain condition_id. Left empty, every
/// `settle_by_condition_id` misses (position.rs skips positions whose
/// condition_id is empty), the `auto_redeem` is never forwarded to the
/// strategy, `record_class(TradeCredit, ..)` never fires, and
/// `net_trade_pnl_24h_at` stays debit-only — so a hold-to-resolution maker's
/// loss-kill trips on gross buy-spend, not realized loss (a
/// 2026-07-18 live self-termination). Strategies that hydrate condition_id
/// another way (legacy `recon`'s `ensure_market_metadata`) are unaffected:
/// this only fills an EMPTY id and ignores an absent/empty market, so it never
/// clobbers a good id. No-op for an untracked slug (`record_fill` always runs
/// first in `run_once`).
fn stamp_condition_id_from_trade(store: &mut PositionStore, slug: &str, market: Option<&str>) {
    let Some(cid) = market.filter(|m| !m.is_empty()) else {
        return;
    };
    if let Some(p) = store.get_mut(slug) {
        if p.condition_id().is_empty() {
            p.set_condition_id(cid.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Coin;

    const OUR_KEY: &str = "our-api-key-uuid";

    #[test]
    fn keepalive_pong_is_filtered_not_parsed() {
        // D10 (06-11 shakedown): 22 + 11 "user_ws parse failed … raw=PONG" WARNs — one per 10s
        // PING. The textual keepalive must be skipped before the JSON parse.
        assert!(is_keepalive_pong("PONG"));
        assert!(is_keepalive_pong(" PONG\n"));
        assert!(!is_keepalive_pong("{\"event_type\":\"trade\"}"));
        assert!(!is_keepalive_pong("PONGX"));
    }

    #[test]
    fn auto_redeem_event_parses_without_type_field() {
        // 06-12 live: the venue's auto_redeem event carries ONLY the
        // lowercase `event_type` discriminator (no uppercase `type`) —
        // pre-fix serde failed with "missing field `type`" and the
        // settlement payout was dropped at the wire.
        let raw = r#"{"event_type":"auto_redeem", "proxy_wallet":"0x365f0ca36ae1f641e02fe3b7743673da42a13a70", "txn_hash":"0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "amount":"55", "condition_id":"0xabc123"}"#;
        let e: UserChannelEvent = serde_json::from_str(raw).expect("auto_redeem must parse");
        assert_eq!(e.event_type, "");
        assert_eq!(e.event_kind.as_deref(), Some("auto_redeem"));
        assert_eq!(e.amount.as_deref(), Some("55"));
        assert_eq!(e.kind(), "AUTO_REDEEM");

        let (cond, amount_usd, tx) =
            auto_redeem_from_event(&e).expect("auto_redeem fields must extract");
        assert_eq!(cond, "0xabc123");
        assert!((amount_usd - 55.0).abs() < 1e-9);
        assert_eq!(
            tx,
            "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
    }

    #[test]
    fn kind_prefers_type_and_falls_back_to_event_kind() {
        let trade: UserChannelEvent =
            serde_json::from_str(r#"{"type":"TRADE","event_type":"trade"}"#).unwrap();
        assert_eq!(trade.kind(), "TRADE");
        let no_type: UserChannelEvent = serde_json::from_str(r#"{"event_type":"order"}"#).unwrap();
        assert_eq!(no_type.kind(), "ORDER");
    }

    #[test]
    fn auto_redeem_from_event_rejects_non_redeem_and_partial_payloads() {
        let trade: UserChannelEvent =
            serde_json::from_str(r#"{"type":"TRADE","event_type":"trade","amount":"5"}"#).unwrap();
        assert!(auto_redeem_from_event(&trade).is_none());
        // Missing condition_id → can't attribute the payout to a slug; drop.
        let partial: UserChannelEvent =
            serde_json::from_str(r#"{"event_type":"auto_redeem","amount":"55"}"#).unwrap();
        assert!(auto_redeem_from_event(&partial).is_none());
        // Unparseable amount → drop.
        let bad_amount: UserChannelEvent = serde_json::from_str(
            r#"{"event_type":"auto_redeem","amount":"x","condition_id":"0xab"}"#,
        )
        .unwrap();
        assert!(auto_redeem_from_event(&bad_amount).is_none());
    }

    #[test]
    fn dedup_first_occurrence_returns_true() {
        let mut seen: VecDeque<String> = VecDeque::new();
        assert!(record_and_check_unseen(&mut seen, "trade-1", 8));
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0], "trade-1");
    }

    #[test]
    fn dedup_repeat_id_returns_false() {
        // Polymarket emits 3 status updates per trade (MATCHED → MINED
        // → CONFIRMED). The status gate inside trade_event_to_fills
        // drops MATCHED but accepts both MINED and CONFIRMED — so the
        // SAME trade.id arrives twice. Dedup must skip the second.
        let mut seen: VecDeque<String> = VecDeque::new();
        assert!(record_and_check_unseen(&mut seen, "trade-1", 8));
        assert!(
            !record_and_check_unseen(&mut seen, "trade-1", 8),
            "second occurrence of same id must return false"
        );
        assert_eq!(seen.len(), 1, "duplicate must NOT be appended");
    }

    #[test]
    fn dedup_empty_id_passes_through() {
        // Empty id can't be deduped — pass through unchanged. Real
        // Polymarket payloads always carry a trade UUID per AsyncAPI;
        // synthesized fixtures sometimes omit it.
        let mut seen: VecDeque<String> = VecDeque::new();
        assert!(record_and_check_unseen(&mut seen, "", 8));
        assert!(seen.is_empty(), "empty id must not be added to LRU");
        // Calling twice with empty id stays a no-op.
        assert!(record_and_check_unseen(&mut seen, "", 8));
        assert!(seen.is_empty());
    }

    #[test]
    fn dedup_lru_evicts_oldest_when_full() {
        // After (max + 1) distinct ids the oldest must be evicted —
        // bounds memory under long-running connections.
        let mut seen: VecDeque<String> = VecDeque::new();
        assert!(record_and_check_unseen(&mut seen, "id-1", 3));
        assert!(record_and_check_unseen(&mut seen, "id-2", 3));
        assert!(record_and_check_unseen(&mut seen, "id-3", 3));
        assert!(record_and_check_unseen(&mut seen, "id-4", 3));
        assert_eq!(seen.len(), 3, "queue cap respected");
        assert_eq!(seen[0], "id-2", "oldest (id-1) evicted");
        // id-1 evicted → it's "unseen" again, returns true.
        assert!(record_and_check_unseen(&mut seen, "id-1", 3));
        assert_eq!(seen.len(), 3, "still capped at 3");
    }

    #[test]
    fn dedup_distinct_ids_all_pass_through() {
        // Sanity: the LRU doesn't accidentally suppress different trade
        // ids that happen to share a prefix or have similar content.
        let mut seen: VecDeque<String> = VecDeque::new();
        assert!(record_and_check_unseen(&mut seen, "abc-1", 8));
        assert!(record_and_check_unseen(&mut seen, "abc-2", 8));
        assert!(record_and_check_unseen(&mut seen, "abc-3", 8));
        assert_eq!(seen.len(), 3);
    }

    fn empty_event() -> UserChannelEvent {
        // V2 default: type "TRADE" (uppercase). All other fields default
        // to None / empty per `#[derive(Default)]`.
        UserChannelEvent {
            event_type: "TRADE".into(),
            ..UserChannelEvent::default()
        }
    }

    fn ledger() -> Arc<MakerRebateLedger> {
        Arc::new(MakerRebateLedger::default())
    }

    fn maker(
        order_id: &str,
        amt: &str,
        price: &str,
        asset: &str,
        bps: Option<&str>,
        owner: Option<&str>,
    ) -> MakerOrderRef {
        MakerOrderRef {
            order_id: Some(order_id.to_string()),
            matched_amount: Some(amt.to_string()),
            price: Some(price.to_string()),
            asset_id: Some(asset.to_string()),
            fee_rate_bps: bps.map(|s| s.to_string()),
            owner: owner.map(|s| s.to_string()),
            ..MakerOrderRef::default()
        }
    }

    async fn registry_with(asset_id: &str, slug: &str, coin: Coin, side: Side) -> AssetIdRegistry {
        let r: AssetIdRegistry = Arc::new(Mutex::new(HashMap::new()));
        r.lock().await.insert(
            asset_id.into(),
            AssetMeta {
                slug: slug.into(),
                coin,
                outcome_side: side,
            },
        );
        r
    }

    #[tokio::test]
    async fn drops_non_trade_events() {
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            event_type: "PING".into(),
            ..empty_event()
        };
        assert!(event_to_fills(&e, &r, OUR_KEY, &ledger(), None)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn drops_unmined_status() {
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MATCHED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            trader_side: Some("MAKER".into()),
            maker_orders: vec![maker(
                "0xord",
                "50",
                "0.50",
                "100",
                Some("1000"),
                Some(OUR_KEY),
            )],
            ..empty_event()
        };
        assert!(event_to_fills(&e, &r, OUR_KEY, &ledger(), None)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn matched_status_emits_provisional_fill_but_not_settled_fill() {
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MATCHED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            trader_side: Some("MAKER".into()),
            timestamp: Some("1780594201".into()),
            maker_orders: vec![maker("0xord", "5", "0.76", "100", Some("0"), Some(OUR_KEY))],
            ..empty_event()
        };

        assert!(event_to_fills(&e, &r, OUR_KEY, &ledger(), None)
            .await
            .is_empty());
        let provisional = event_to_provisional_fills(&e, &r, OUR_KEY).await;
        assert_eq!(provisional.len(), 1);
        assert_eq!(provisional[0].slug, "btc-updown-5m-1");
        assert_eq!(provisional[0].side, Side::Up);
        assert_eq!(provisional[0].shares, 5.0);
        assert_eq!(provisional[0].price, 0.76);
        assert_eq!(provisional[0].id.as_str(), "0xord");
    }

    #[tokio::test]
    async fn mined_status_is_settled_not_provisional() {
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            trader_side: Some("MAKER".into()),
            maker_orders: vec![maker("0xord", "5", "0.76", "100", Some("0"), Some(OUR_KEY))],
            ..empty_event()
        };

        assert_eq!(
            event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await.len(),
            1
        );
        assert!(event_to_provisional_fills(&e, &r, OUR_KEY).await.is_empty());
    }

    #[tokio::test]
    async fn drops_when_asset_missing_from_registry() {
        let r = registry_with("OTHER", "x", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            trader_side: Some("MAKER".into()),
            maker_orders: vec![maker(
                "0xord",
                "50",
                "0.50",
                "100",
                Some("1000"),
                Some(OUR_KEY),
            )],
            ..empty_event()
        };
        assert!(event_to_fills(&e, &r, OUR_KEY, &ledger(), None)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn maker_buy_emits_positive_shares_with_bps_fee_usd() {
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            trader_side: Some("MAKER".into()),
            timestamp: Some("1700000000".into()),
            maker_orders: vec![maker(
                "0xord-hex",
                "50",
                "0.50",
                "100",
                Some("1000"),
                Some(OUR_KEY),
            )],
            ..empty_event()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await;
        assert_eq!(fills.len(), 1);
        let f = &fills[0];
        assert_eq!(f.shares, 50.0);
        assert_eq!(f.price, 0.50);
        // 1000 bps × 50sh × $0.50 = $2.50
        assert!((f.fee_usd - 2.50).abs() < 1e-9, "fee_usd = {}", f.fee_usd);
        assert_eq!(f.token_id, "100");
        assert_eq!(f.slug, "btc-updown-5m-1");
        // I2: 10-digit timestamp interpreted as seconds → 1.7e15 micros.
        assert_eq!(f.ts_micros, 1_700_000_000_000_000);
        // B6: id is the hex maker order_id, NOT the trade UUID.
        assert_eq!(f.id.as_str(), "0xord-hex");
    }

    #[tokio::test]
    async fn maker_sell_yields_negative_shares() {
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let mut order = maker("0xord", "50", "0.55", "100", Some("0"), Some(OUR_KEY));
        order.side = Some("SELL".into());
        let e = UserChannelEvent {
            status: Some("CONFIRMED".into()),
            asset_id: Some("100".into()),
            side: Some("SELL".into()),
            trader_side: Some("MAKER".into()),
            maker_orders: vec![order],
            ..empty_event()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await;
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].shares, -50.0);
    }

    #[tokio::test]
    async fn maker_order_buy_side_overrides_top_level_sell() {
        // Real 2026-06-04 shape: a taker SELL hit our resting
        // BUY maker orders. Inventory sign must come from our maker
        // order side, not the top-level taker trade side.
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Down).await;
        let mut first = maker("0x878b", "5", "0.60", "100", Some("0"), Some(OUR_KEY));
        first.side = Some("BUY".into());
        let mut second = maker("0xf46a", "5", "0.59", "100", Some("0"), Some(OUR_KEY));
        second.side = Some("BUY".into());
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            id: Some("da6b895f-3f17-4e78-bae4-013de9eac9fa".into()),
            asset_id: Some("100".into()),
            side: Some("SELL".into()),
            trader_side: Some("MAKER".into()),
            maker_orders: vec![first, second],
            ..empty_event()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await;
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].shares, 5.0);
        assert_eq!(fills[0].side, Side::Down);
        assert_eq!(fills[1].shares, 5.0);
        assert_eq!(fills[1].side, Side::Down);
    }

    #[tokio::test]
    async fn maker_leg_with_absent_side_on_mint_sell_is_a_buy() {
        // REAL 2026-06-04 incident shape: a complementary-mint DN "SELL" trade
        // where our resting BUY-DN maker legs carry NO per-order `side`
        // (`order_id=null` / "unattributed" on the wire). The maker fill is
        // economically a BUY — shares MUST be positive. Pre-fix this returned
        // -5.0 (the DN under-count behind the manual rebalance).
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Down).await;
        let first = maker("0x878b", "5", "0.60", "100", Some("0"), Some(OUR_KEY)); // no .side
        let second = maker("0xf46a", "5", "0.59", "100", Some("0"), Some(OUR_KEY)); // no .side
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            id: Some("da6b895f-3f17-4e78-bae4-013de9eac9fa".into()),
            asset_id: Some("100".into()),
            side: Some("SELL".into()),
            trader_side: Some("MAKER".into()),
            maker_orders: vec![first, second],
            ..empty_event()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await;
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].shares, 5.0, "maker BUY-DN must be positive");
        assert_eq!(fills[1].shares, 5.0, "maker BUY-DN must be positive");
        assert_eq!(fills[0].side, Side::Down);
    }

    #[tokio::test]
    async fn maker_orders_owner_mismatch_is_filtered_out() {
        // Polymarket emits a single trade event listing all maker_orders
        // that filled (including other participants'). We only keep ours.
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            trader_side: Some("MAKER".into()),
            maker_orders: vec![
                maker(
                    "0xthem",
                    "30",
                    "0.50",
                    "100",
                    Some("1000"),
                    Some("some-other-wallet"),
                ),
                maker("0xus", "20", "0.50", "100", Some("1000"), Some(OUR_KEY)),
            ],
            ..empty_event()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await;
        assert_eq!(fills.len(), 1, "should only emit our own maker fill");
        assert_eq!(fills[0].id.as_str(), "0xus");
        assert_eq!(fills[0].shares, 20.0);
    }

    #[tokio::test]
    async fn multiple_our_maker_orders_emit_multiple_fills() {
        // Two of our resting bids matched in one trade.
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            trader_side: Some("MAKER".into()),
            maker_orders: vec![
                maker("0xa", "30", "0.50", "100", Some("1000"), Some(OUR_KEY)),
                maker("0xb", "20", "0.49", "100", Some("1000"), Some(OUR_KEY)),
            ],
            ..empty_event()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await;
        assert_eq!(fills.len(), 2);
        let ids: Vec<_> = fills.iter().map(|f| f.id.as_str().to_string()).collect();
        assert_eq!(ids, vec!["0xa", "0xb"]);
    }

    #[tokio::test]
    async fn maker_orders_absent_owner_falls_through_and_emits() {
        // Documented degraded mode: when maker_orders[i].owner is absent
        // (Polymarket payload occasionally omits the field), fall through
        // and emit the Fill — the user-channel only ever broadcasts events
        // for our account, so a missing-owner entry is most likely ours.
        // This test pins the contract; a future "tighten" that requires
        // Some(owner) would silently drop legitimate fills and trip here.
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            trader_side: Some("MAKER".into()),
            maker_orders: vec![maker("0xord", "10", "0.50", "100", Some("1000"), None)],
            ..empty_event()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await;
        assert_eq!(
            fills.len(),
            1,
            "absent owner should fall through (degraded mode)"
        );
        assert_eq!(fills[0].id.as_str(), "0xord");
        assert_eq!(fills[0].shares, 10.0);
    }

    #[tokio::test]
    async fn taker_path_emits_best_effort_fill() {
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            size: Some("30".into()),
            price: Some("0.55".into()),
            fee_rate_bps: Some("1000".into()),
            trader_side: Some("TAKER".into()),
            taker_order_id: Some("0xtaker".into()),
            timestamp: Some("1700000000000".into()), // 13-digit ms
            ..empty_event()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await;
        assert_eq!(fills.len(), 1);
        let f = &fills[0];
        assert_eq!(f.shares, 30.0);
        assert_eq!(f.price, 0.55);
        // 1000 bps × 30sh × $0.55 = $1.65
        assert!((f.fee_usd - 1.65).abs() < 1e-9);
        // Best-effort: taker_order_id used as the Fill id (won't match
        // submitter's cancel_index since place_taker_buy discards it).
        assert_eq!(f.id.as_str(), "0xtaker");
        // I2: 13-digit timestamp interpreted as ms → 1.7e15 micros.
        assert_eq!(f.ts_micros, 1_700_000_000_000_000);
    }

    #[tokio::test]
    async fn defensive_timestamp_handles_seconds_input() {
        let r = registry_with("100", "x", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            trader_side: Some("MAKER".into()),
            timestamp: Some("1700000000".into()),
            maker_orders: vec![maker("0x", "1", "0.5", "100", None, Some(OUR_KEY))],
            ..empty_event()
        };
        let f = &event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await[0];
        assert_eq!(f.ts_micros, 1_700_000_000_000_000);
    }

    #[tokio::test]
    async fn defensive_timestamp_handles_ms_input() {
        let r = registry_with("100", "x", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            trader_side: Some("MAKER".into()),
            timestamp: Some("1700000000123".into()),
            maker_orders: vec![maker("0x", "1", "0.5", "100", None, Some(OUR_KEY))],
            ..empty_event()
        };
        let f = &event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await[0];
        assert_eq!(f.ts_micros, 1_700_000_000_123_000);
    }

    #[tokio::test]
    async fn maker_rebate_event_routes_to_ledger() {
        // Phase 10B: MAKER_REBATE event routes USD credit into MakerRebateLedger.
        // Forward-compatibility scaffolding — not in AsyncAPI today.
        let r = registry_with("100", "x", Coin::Btc, Side::Up).await;
        let l = ledger();
        let e = UserChannelEvent {
            event_type: "MAKER_REBATE".into(),
            slug: Some("btc-updown-5m-test".into()),
            amount: Some("0.50".into()),
            ..UserChannelEvent::default()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &l, None).await;
        assert!(fills.is_empty());
        let snap = l.snapshot();
        assert!((snap["btc-updown-5m-test"].maker_rebate_usd - 0.50).abs() < 1e-9);
        assert_eq!(snap["btc-updown-5m-test"].events, 1);
    }

    #[tokio::test]
    async fn reward_event_routes_to_ledger() {
        let r = registry_with("100", "x", Coin::Btc, Side::Up).await;
        let l = ledger();
        let e = UserChannelEvent {
            event_type: "REWARD".into(),
            slug: Some("btc-updown-5m-test".into()),
            amount: Some("0.30".into()),
            ..UserChannelEvent::default()
        };
        let _ = event_to_fills(&e, &r, OUR_KEY, &l, None).await;
        let snap = l.snapshot();
        assert!((snap["btc-updown-5m-test"].reward_usd - 0.30).abs() < 1e-9);
    }

    #[tokio::test]
    async fn unknown_event_type_drops_silently() {
        let r = registry_with("100", "x", Coin::Btc, Side::Up).await;
        let l = ledger();
        let e = UserChannelEvent {
            event_type: "FUTURE_EVENT".into(),
            ..UserChannelEvent::default()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &l, None).await;
        assert!(fills.is_empty());
        assert!(l.snapshot().is_empty());
    }

    #[tokio::test]
    async fn cancel_ack_is_silently_dropped() {
        let r = registry_with("100", "x", Coin::Btc, Side::Up).await;
        let l = ledger();
        let e = UserChannelEvent {
            event_type: "CANCEL_ACK".into(),
            ..UserChannelEvent::default()
        };
        assert!(event_to_fills(&e, &r, OUR_KEY, &l, None).await.is_empty());
        assert!(l.snapshot().is_empty());
    }

    #[test]
    fn deserialize_uppercase_type_field() {
        // Wire fact: `type` (uppercase) is the canonical discriminator.
        let raw = r#"{"type": "TRADE"}"#;
        let e: UserChannelEvent = serde_json::from_str(raw).expect("v2 parse");
        assert_eq!(e.event_type, "TRADE");
        assert_eq!(e.event_kind, None);
    }

    #[test]
    fn deserialize_both_type_and_event_type_fields() {
        // Wire fact: real payloads carry BOTH `type` (TRADE) and
        // `event_type` (trade) — not aliases. The struct must read
        // each into a separate field.
        let raw = r#"{"type": "TRADE", "event_type": "trade"}"#;
        let e: UserChannelEvent = serde_json::from_str(raw).expect("dual-field parse");
        assert_eq!(e.event_type, "TRADE");
        assert_eq!(e.event_kind.as_deref(), Some("trade"));
    }

    // Plan C Task 2: liquidity_role is parsed from trader_side on TRADE events.
    // The Maker/Taker enum drives downstream rebate/fee accounting and the
    // active TAKER-rebalance gate (Plan C); this test pins the wire mapping.

    #[tokio::test]
    async fn maker_trader_side_yields_liquidity_role_maker() {
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            trader_side: Some("MAKER".into()),
            maker_orders: vec![maker(
                "0xord",
                "10",
                "0.50",
                "100",
                Some("1000"),
                Some(OUR_KEY),
            )],
            ..empty_event()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await;
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].liquidity_role, LiquidityRole::Maker);
    }

    #[tokio::test]
    async fn taker_trader_side_yields_liquidity_role_taker() {
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            size: Some("30".into()),
            price: Some("0.55".into()),
            fee_rate_bps: Some("1000".into()),
            trader_side: Some("TAKER".into()),
            taker_order_id: Some("0xtaker".into()),
            ..empty_event()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await;
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].liquidity_role, LiquidityRole::Taker);
    }

    #[tokio::test]
    async fn absent_trader_side_with_maker_orders_yields_liquidity_role_unknown() {
        // When trader_side is absent but maker_orders[] is populated, the MAKER
        // branch fires (`!e.maker_orders.is_empty()` fall-through). We can't
        // tell from the wire whether this was a maker or taker fill, so the
        // fill should land with LiquidityRole::Unknown — downstream code can
        // then fall back to its own heuristics or skip rebate attribution.
        let r = registry_with("100", "btc-updown-5m-1", Coin::Btc, Side::Up).await;
        let e = UserChannelEvent {
            status: Some("MINED".into()),
            asset_id: Some("100".into()),
            side: Some("BUY".into()),
            trader_side: None,
            maker_orders: vec![maker(
                "0xord",
                "10",
                "0.50",
                "100",
                Some("1000"),
                Some(OUR_KEY),
            )],
            ..empty_event()
        };
        let fills = event_to_fills(&e, &r, OUR_KEY, &ledger(), None).await;
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].liquidity_role, LiquidityRole::Unknown);
    }

    // ─── 2026-05-08 over-fill clamp (Fix A) ───────────────────────
    //
    // Background: 20:50 ETH-5m slot saw a -21 sh Up sell against 15.14
    // sh of held inventory. user_ws::record_fill clamped position to 0
    // (correct), but the broadcast-then-ledger-only path saw held=0 and
    // dropped the entire ledger record (runtime.rs:240). Net effect:
    // legitimate ~$3.79 of sale proceeds went uncredited. Fix is to
    // clamp the fill at the user_ws boundary BEFORE record_fill, so the
    // broadcast carries only the legit portion, and the downstream
    // phantom-sell guard never trips on a partial over-fill.

    fn fill(side: Side, shares: f64, price: f64, fee_bps: f64) -> Fill {
        let usd = (shares * price).abs();
        let fee_usd = (fee_bps / 10_000.0) * shares.abs() * price;
        Fill {
            slug: "eth-updown-5m-1778273400".into(),
            coin: Coin::Eth,
            side,
            token_id: "100".into(),
            id: RestingOrderId::from_string("0xfill"),
            shares,
            price,
            usd,
            fee_usd,
            ts_micros: 1_700_000_000_000_000,
            liquidity_role: LiquidityRole::Maker,
        }
    }

    #[test]
    fn clamp_fill_to_held_passes_through_buy_unchanged() {
        // BUYS are never clamped — the venue won't send us back more
        // shares than we asked for, and the position store only clamps
        // SELLS against held inventory.
        let f = fill(Side::Up, 10.0, 0.50, 0.0);
        let out =
            clamp_fill_to_held(f.clone(), /*held=*/ 0.0).expect("buy fills are never dropped");
        assert_eq!(out.shares, 10.0);
        assert_eq!(out.usd, 5.0);
        assert_eq!(out.fee_usd, 0.0);
    }

    #[test]
    fn clamp_fill_to_held_passes_through_sell_within_held() {
        // Sell of 10 sh against held 15 sh → unchanged (legit, no clamp).
        let f = fill(Side::Up, -10.0, 0.50, 1000.0); // fee = 1000bps × 10 × 0.5 = 0.50
        let out = clamp_fill_to_held(f.clone(), 15.0).expect("sell within held is legit");
        assert_eq!(out.shares, -10.0);
        assert_eq!(out.usd, 5.0);
        assert!((out.fee_usd - 0.50).abs() < 1e-9);
    }

    #[test]
    fn clamp_fill_to_held_drops_phantom_sell_when_no_inventory() {
        // The 20:50 slot's residual case — held=0 but a -21 sh sell
        // arrives. Legit portion is zero. Drop entirely.
        let f = fill(Side::Up, -21.0, 0.25, 1000.0);
        assert!(clamp_fill_to_held(f, 0.0).is_none());
    }

    #[test]
    fn clamp_fill_to_held_clamps_oversell_proportionally() {
        // The 20:50 slot's central case — held=15.14, raw sell=21 sh
        // @ $0.25. Fix must emit a single fill with shares=-15.14,
        // usd=15.14 × 0.25 = 3.785, fee_usd prorated by 15.14/21.
        // Phantom portion (5.86 sh) is silently dropped.
        let raw_fee = (1000.0 / 10_000.0) * 21.0 * 0.25; // 0.525
        let f = fill(Side::Up, -21.0, 0.25, 1000.0);
        assert!(
            (f.fee_usd - raw_fee).abs() < 1e-9,
            "test setup: fee = {}",
            f.fee_usd
        );

        let out =
            clamp_fill_to_held(f, 15.14).expect("partial over-sell yields clamped fill, not None");
        assert!(
            (out.shares - -15.14).abs() < 1e-9,
            "shares = {}",
            out.shares
        );
        assert!((out.usd - (15.14 * 0.25)).abs() < 1e-9, "usd = {}", out.usd);
        // Fee is prorated by held/want = 15.14/21.
        let expected_fee = raw_fee * (15.14 / 21.0);
        assert!(
            (out.fee_usd - expected_fee).abs() < 1e-9,
            "fee_usd = {}, expected ~{}",
            out.fee_usd,
            expected_fee,
        );
        assert_eq!(out.price, 0.25, "price unchanged");
    }

    #[test]
    fn clamp_fill_to_held_oversell_at_exact_held_no_clamp() {
        // Sell of exactly held shares → unchanged (boundary case; clamp
        // would be a no-op but we want to confirm the predicate uses >,
        // not >=, so that the fee-prorata branch only fires on real
        // over-fills).
        let f = fill(Side::Down, -15.0, 0.50, 1000.0);
        let raw_fee = f.fee_usd;
        let out = clamp_fill_to_held(f, 15.0).unwrap();
        assert_eq!(out.shares, -15.0);
        assert_eq!(out.usd, 7.5);
        assert!((out.fee_usd - raw_fee).abs() < 1e-9);
    }

    #[test]
    fn trade_fill_stamp_enables_auto_redeem_settle() {
        // Regression for a live self-termination (2026-07-18):
        // the live PositionStore is fed ONLY by record_fill, which never sets
        // condition_id. Without stamping it from the TRADE event's `market`,
        // settle_by_condition_id misses every auto_redeem → the AutoRedeem is
        // never forwarded → record_class(TradeCredit) never fires →
        // net_trade_pnl_24h_at is debit-only and trips the loss-kill on gross
        // spend (RUN1: -$61.32 = total buy-spend, the winning $96 redeem never
        // credited despite settling on-chain).
        let mut store = PositionStore::new();
        let f = fill(Side::Up, 96.0, 0.50, 0.0);
        let slug = f.slug.clone();
        let cond = "0xC0nDiTi0n";
        store.record_fill(&f);

        // Bug: record_fill alone leaves condition_id empty, so the venue's
        // auto_redeem (which joins by condition_id) cannot settle our position.
        assert!(
            store.settle_by_condition_id(cond, 96.0, 1_000).is_none(),
            "record_fill alone must NOT enable the redeem join (condition_id empty)"
        );

        // Fix: stamp the CTF condition_id from the TRADE event's `market`.
        stamp_condition_id_from_trade(&mut store, &slug, Some(cond));
        assert!(
            store.settle_by_condition_id(cond, 96.0, 2_000).is_some(),
            "after stamping, the auto_redeem joins the position -> TradeCredit can fire"
        );
    }

    #[test]
    fn stamp_condition_id_from_trade_is_idempotent() {
        // Only fills an EMPTY id — a later stamp (e.g. a second fill on the
        // same slug) must never clobber the first, so the redeem join stays
        // stable for the market's whole life.
        let mut store = PositionStore::new();
        let f = fill(Side::Up, 10.0, 0.50, 0.0);
        let slug = f.slug.clone();
        store.record_fill(&f);
        stamp_condition_id_from_trade(&mut store, &slug, Some("0xFIRST"));
        stamp_condition_id_from_trade(&mut store, &slug, Some("0xSECOND"));
        assert_eq!(store.get(&slug).unwrap().condition_id(), "0xFIRST");
    }

    #[test]
    fn stamp_condition_id_from_trade_ignores_absent_or_empty_market() {
        // A TRADE without a `market` field (or an empty one) must be a no-op —
        // never overwrite a good id with "", which would make settle miss.
        let mut store = PositionStore::new();
        let f = fill(Side::Up, 10.0, 0.50, 0.0);
        let slug = f.slug.clone();
        store.record_fill(&f);
        stamp_condition_id_from_trade(&mut store, &slug, None);
        stamp_condition_id_from_trade(&mut store, &slug, Some(""));
        assert!(store.get(&slug).unwrap().condition_id().is_empty());
    }

    #[test]
    fn stamp_condition_id_from_trade_noop_for_untracked_slug() {
        // Defensive: stamping a slug with no recorded position must not panic
        // or create a phantom position (record_fill always runs first in
        // run_once, but pin the guard so a reorder can't crash the hot loop).
        let mut store = PositionStore::new();
        stamp_condition_id_from_trade(&mut store, "btc-updown-5m-1", Some("0xAB"));
        assert!(store.get("btc-updown-5m-1").is_none());
    }
}
