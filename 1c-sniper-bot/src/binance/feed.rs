//! Binance spot WebSocket ingest.
//!
//! Subscribes to the combined `@bookTicker` streams for BTC/ETH/SOL/XRP, parses
//! updates, and publishes `SpotTick` events to both:
//! - `broadcast::Sender<SpotTick>` — consumed by the σ estimator, which needs
//!   every tick at 1-second sampling cadence.
//! - `PerCoin<watch::Sender<Option<SpotTick>>>` — latest-value state for the
//!   quoter's spot reads (lock-free).
//!
//! We use bookTicker's mid-price as the spot input. Reconnects on any error
//! with a fixed back-off.

use crate::types::{now_micros, Coin, PerCoin, SpotClose1s, SpotTick, SpotTrade};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::Connector;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_BACKOFF: Duration = Duration::from_secs(1);

/// Which Binance venue's combined `@bookTicker` stream to subscribe. Spot is the default hot path;
/// the USD-M perp is BTC's price-discovery leader (~50-150ms faster, 144h exchange-clock confirmed)
/// and carries the mid on the SAME `@bookTicker` frame, so selecting it is a `ws_base` swap — one
/// socket. Strategy-agnostic: no strategy is named here; a caller maps its own config onto this.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum BinanceVenue {
    #[default]
    Spot,
    UsdMPerp,
}

impl BinanceVenue {
    /// The default combined-stream `ws_base` for this venue.
    pub fn default_ws_base(self) -> &'static str {
        match self {
            BinanceVenue::Spot => "wss://stream.binance.com:9443",
            BinanceVenue::UsdMPerp => "wss://fstream.binance.com",
        }
    }
}

pub struct BinanceIngest {
    ws_base: String,
    coins: Vec<Coin>,
    /// Optional pre-built TLS connector. When `Some`, every
    /// `connect_async_tls_with_config` call reuses this `rustls::ClientConfig`
    /// instead of loading the OS native root-CA store synchronously on the
    /// tokio worker. Leaving it `None` falls back to `connect_async`, which
    /// re-loads native certs inside the runtime worker for every connect —
    /// fine for tests / single-shot probes, fatal during live hot paths (the
    /// macOS keychain walk can block a worker for tens of seconds and
    /// indirectly starve the quoter). See feat/clob-ws-reap-fix branch notes.
    tls_connector: Option<Connector>,
    agg_trade_coins: Vec<Coin>,
    agg_trade_tx: Option<broadcast::Sender<SpotTrade>>,
    kline_coins: Vec<Coin>,
    kline_tx: Option<broadcast::Sender<SpotClose1s>>,
}

impl BinanceIngest {
    pub fn new(ws_base: impl Into<String>, coins: Vec<Coin>) -> Self {
        Self {
            ws_base: ws_base.into(),
            coins,
            tls_connector: None,
            agg_trade_coins: Vec::new(),
            agg_trade_tx: None,
            kline_coins: Vec::new(),
            kline_tx: None,
        }
    }

    /// Additionally subscribe to `@aggTrade` for `coins`, publishing parsed
    /// prints on `tx`. The bookTicker streams are unaffected — σ/β consumers
    /// stay on mids; aggTrade is a separate transport for trade-driven
    /// triggers.
    pub fn with_agg_trades(mut self, coins: Vec<Coin>, tx: broadcast::Sender<SpotTrade>) -> Self {
        self.agg_trade_coins = coins;
        self.agg_trade_tx = Some(tx);
        self
    }

    /// Additionally subscribe to `@kline_1s` for `coins`, publishing parsed
    /// CLOSED klines on `tx`. Only closed klines (`k.x == true`) are emitted;
    /// `close_ts_ms` is the kline close time (Binance `k.T`, event time) —
    /// join on this, not local receive.
    pub fn with_kline_1s(mut self, coins: Vec<Coin>, tx: broadcast::Sender<SpotClose1s>) -> Self {
        self.kline_coins = coins;
        self.kline_tx = Some(tx);
        self
    }

    /// Inject a pre-built TLS connector. See `ClobWsIngest::with_tls_connector`
    /// for rationale; the identical argument applies to the Binance WS.
    pub fn with_tls_connector(mut self, connector: Connector) -> Self {
        self.tls_connector = Some(connector);
        self
    }

    /// Run the ingest loop forever. On WS error or clean close, logs and
    /// reconnects after a fixed back-off. Returns only on unrecoverable setup
    /// error (empty coins list).
    pub async fn run(
        self,
        broadcast_tx: broadcast::Sender<SpotTick>,
        watch_txs: PerCoin<watch::Sender<Option<SpotTick>>>,
    ) -> Result<()> {
        let url = self.build_url()?;
        tracing::info!(url = %url, "binance ingest connecting");
        loop {
            match self
                .connect_and_stream(&url, &broadcast_tx, &watch_txs)
                .await
            {
                Ok(()) => tracing::warn!("binance ws closed cleanly; reconnecting"),
                Err(e) => tracing::error!(?e, "binance ws error; reconnecting"),
            }
            tokio::time::sleep(RECONNECT_BACKOFF).await;
        }
    }

    pub fn build_url(&self) -> Result<String> {
        // The invariant is "at least one stream", not "at least one bookTicker
        // coin": a consumer that only wants @kline_1s passes
        // an empty `coins` so no bookTicker frames ride its socket at all.
        // Measured on the VPS 2026-07-26, 898 paired bars: carrying the
        // unused btcusdt@bookTicker (27.6 frames/s) delays the kline by a
        // systematic +3.6 ms mean / +3 ms p50, on 99.9% of bars.
        if self.coins.is_empty() && self.agg_trade_coins.is_empty() && self.kline_coins.is_empty() {
            anyhow::bail!("BinanceIngest: no streams requested (coins, aggTrade and kline all empty)");
        }
        let mut streams: Vec<String> = self
            .coins
            .iter()
            .map(|c| format!("{}@bookTicker", c.binance_symbol()))
            .collect();
        streams.extend(
            self.agg_trade_coins
                .iter()
                .map(|c| format!("{}@aggTrade", c.binance_symbol())),
        );
        streams.extend(
            self.kline_coins
                .iter()
                .map(|c| format!("{}@kline_1s", c.binance_symbol())),
        );
        Ok(format!(
            "{}/stream?streams={}",
            self.ws_base,
            streams.join("/")
        ))
    }

    async fn connect_and_stream(
        &self,
        url: &str,
        broadcast_tx: &broadcast::Sender<SpotTick>,
        watch_txs: &PerCoin<watch::Sender<Option<SpotTick>>>,
    ) -> Result<()> {
        // Prefer the shared connector when one was injected; falling back to
        // `connect_async` re-loads the OS native cert store synchronously
        // inside the async call, blocking the tokio worker for 100 ms — 28 s
        // depending on keychain contention. See
        // `crate::polymarket::clob_ws::build_shared_tls_connector`.
        let connect_fut = async {
            match &self.tls_connector {
                Some(conn) => {
                    tokio_tungstenite::connect_async_tls_with_config(
                        url,
                        None,
                        false,
                        Some(conn.clone()),
                    )
                    .await
                }
                None => tokio_tungstenite::connect_async(url).await,
            }
        };
        let (mut ws, _resp) = tokio::time::timeout(CONNECT_TIMEOUT, connect_fut)
            .await
            .context("binance ws connect timeout")?
            .context("binance ws connect")?;
        tracing::info!("binance ws connected");

        while let Some(msg) = ws.next().await {
            let msg = msg.context("binance ws read")?;
            match msg {
                Message::Text(t) => {
                    if let Some(tick) = parse_tick(t.as_str()) {
                        // broadcast::send returns err only when there are no
                        // receivers — tolerable in startup / during teardown.
                        let _ = broadcast_tx.send(tick);
                        // watch::send returns err only when receiver is dropped.
                        let _ = watch_txs.get(tick.coin).send(Some(tick));
                    } else {
                        // Not a bookTicker frame. aggTrade and kline_1s are
                        // independent optional streams on the same socket, so
                        // both get a chance — an `else if` chain here would
                        // let one configured-but-non-matching branch swallow
                        // frames meant for the other.
                        if let Some(tx) = &self.agg_trade_tx {
                            if let Some(trade) = parse_agg_trade(t.as_str()) {
                                let _ = tx.send(trade);
                            }
                        }
                        if let Some(tx) = &self.kline_tx {
                            if let Some(close) = parse_kline_1s_envelope(t.as_str()) {
                                let _ = tx.send(close);
                            }
                        }
                    }
                }
                Message::Ping(p) => {
                    ws.send(Message::Pong(p)).await.context("binance ws pong")?;
                }
                Message::Close(frame) => {
                    tracing::warn!(?frame, "binance ws received close frame");
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

pub type SpotWatchChannels = (
    PerCoin<watch::Sender<Option<SpotTick>>>,
    PerCoin<watch::Receiver<Option<SpotTick>>>,
);

/// Build matching `(senders, receivers)` for per-coin spot watch channels,
/// all starting at `None`.
pub fn build_spot_watch_channels() -> SpotWatchChannels {
    let (bs, br) = watch::channel(None);
    let (es, er) = watch::channel(None);
    let (ss, sr) = watch::channel(None);
    let (xs, xr) = watch::channel(None);
    (
        PerCoin {
            btc: bs,
            eth: es,
            sol: ss,
            xrp: xs,
        },
        PerCoin {
            btc: br,
            eth: er,
            sol: sr,
            xrp: xr,
        },
    )
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    #[allow(dead_code)]
    stream: &'a str,
    data: BookTicker,
}

#[derive(Deserialize)]
struct BookTicker {
    s: String, // symbol, e.g. "BTCUSDT"
    b: String, // best bid price (string-encoded decimal)
    a: String, // best ask price
}

fn parse_tick(s: &str) -> Option<SpotTick> {
    let env: Envelope = serde_json::from_str(s).ok()?;
    let coin = coin_from_symbol(&env.data.s)?;
    let bid: f64 = env.data.b.parse().ok()?;
    let ask: f64 = env.data.a.parse().ok()?;
    if !(bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0) {
        return None;
    }
    Some(SpotTick {
        coin,
        price: (bid + ask) / 2.0,
        ts_micros: now_micros(),
    })
}

#[derive(Deserialize)]
struct AggTradeEnvelope<'a> {
    #[serde(borrow)]
    #[allow(dead_code)]
    stream: &'a str,
    data: AggTrade,
}

#[derive(Deserialize)]
struct AggTrade {
    s: String, // symbol, e.g. "BTCUSDT"
    p: String, // trade price (string-encoded decimal)
    q: String, // trade quantity
    #[serde(rename = "T")]
    t: u64, // trade time, ms
    #[serde(rename = "m")]
    m: bool, // buyer is maker
}

fn parse_agg_trade(s: &str) -> Option<SpotTrade> {
    let env: AggTradeEnvelope = serde_json::from_str(s).ok()?;
    let coin = coin_from_symbol(&env.data.s)?;
    let price: f64 = env.data.p.parse().ok()?;
    let qty: f64 = env.data.q.parse().ok()?;
    if !(price.is_finite() && qty.is_finite() && price > 0.0 && qty >= 0.0) {
        return None;
    }
    Some(SpotTrade {
        coin,
        price,
        qty,
        buyer_is_maker: env.data.m,
        exch_ts_ms: env.data.t,
        recv_ts_us: now_micros(),
    })
}

#[derive(Deserialize)]
struct KlineFrame<'a> {
    #[serde(borrow)]
    k: KlineBody<'a>,
}
#[derive(Deserialize)]
struct KlineBody<'a> {
    #[serde(rename = "T")]
    close_ts_ms: u64,
    #[serde(rename = "c")]
    close: &'a str,
    #[serde(rename = "x")]
    closed: bool,
}

/// Parse one @kline_1s frame; `None` unless the kline is closed (`x == true`).
/// `raw` is the bare kline payload (`{"e":"kline",...,"k":{...}}}`), NOT the
/// combined-stream envelope — see `parse_kline_1s_envelope` for the dispatch
/// entry point that peels the `{"stream":...,"data":...}` wrapper first.
fn parse_kline_1s(raw: &str, coin: Coin) -> Option<SpotClose1s> {
    let f: KlineFrame = serde_json::from_str(raw).ok()?;
    if !f.k.closed {
        return None;
    }
    let close_px: f64 = f.k.close.parse().ok()?;
    if !(close_px.is_finite() && close_px > 0.0) {
        return None;
    }
    Some(SpotClose1s {
        coin,
        close_px,
        close_ts_ms: f.k.close_ts_ms,
    })
}

#[derive(Deserialize)]
struct KlineEnvelope {
    data: serde_json::Value,
}

/// Dispatch entry point for `@kline_1s`: peels the combined-stream envelope,
/// resolves the coin from the payload's own `s` symbol field (mirroring
/// `parse_tick`/`parse_agg_trade`), then hands the unwrapped payload to
/// `parse_kline_1s`. `KlineBody` doesn't carry a coin of its own, so the
/// symbol lookup has to happen out here before the tested leaf parser runs.
fn parse_kline_1s_envelope(raw: &str) -> Option<SpotClose1s> {
    let env: KlineEnvelope = serde_json::from_str(raw).ok()?;
    let coin = coin_from_symbol(env.data.get("s")?.as_str()?)?;
    let data_str = serde_json::to_string(&env.data).ok()?;
    parse_kline_1s(&data_str, coin)
}

fn coin_from_symbol(sym: &str) -> Option<Coin> {
    match sym {
        "BTCUSDT" => Some(Coin::Btc),
        "ETHUSDT" => Some(Coin::Eth),
        "SOLUSDT" => Some(Coin::Sol),
        "XRPUSDT" => Some(Coin::Xrp),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn builds_combined_stream_url() {
        let ing = BinanceIngest::new("wss://stream.binance.com:9443", Coin::ALL.to_vec());
        let url = ing.build_url().unwrap();
        assert_eq!(
            url,
            "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker/solusdt@bookTicker/xrpusdt@bookTicker",
        );
    }

    #[test]
    fn empty_coins_list_refused() {
        let ing = BinanceIngest::new("wss://", vec![]);
        assert!(ing.build_url().is_err());
    }

    /// An empty `coins` is NOT an error when another stream was requested —
    /// it is how a kline-only consumer keeps unused bookTicker frames off its
    /// socket (a measured +3.6 ms on the kline it actually waits for).
    #[test]
    fn kline_only_subscription_omits_bookticker() {
        let (tx, _rx) = broadcast::channel(4);
        let ing = BinanceIngest::new("wss://stream.binance.com:9443", vec![])
            .with_kline_1s(vec![Coin::Btc], tx);
        assert_eq!(
            ing.build_url().unwrap(),
            "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1s",
        );
    }

    #[test]
    fn parses_canonical_bookticker_envelope() {
        let s = r#"{
            "stream": "btcusdt@bookTicker",
            "data": {
                "u": 400900217,
                "s": "BTCUSDT",
                "b": "67123.45000000",
                "B": "1.00000000",
                "a": "67123.55000000",
                "A": "2.00000000"
            }
        }"#;
        let tick = parse_tick(s).unwrap();
        assert_eq!(tick.coin, Coin::Btc);
        assert_abs_diff_eq!(tick.price, (67123.45 + 67123.55) / 2.0, epsilon = 1e-6);
        assert!(tick.ts_micros > 0);
    }

    #[test]
    fn parses_each_coin_symbol() {
        for (sym, coin) in [
            ("BTCUSDT", Coin::Btc),
            ("ETHUSDT", Coin::Eth),
            ("SOLUSDT", Coin::Sol),
            ("XRPUSDT", Coin::Xrp),
        ] {
            let s = format!(
                r#"{{"stream":"x@bookTicker","data":{{"u":1,"s":"{sym}","b":"100","B":"1","a":"101","A":"1"}}}}"#
            );
            let tick = parse_tick(&s).expect("parse");
            assert_eq!(tick.coin, coin);
            assert_abs_diff_eq!(tick.price, 100.5, epsilon = 1e-9);
        }
    }

    #[test]
    fn rejects_unknown_symbol() {
        let s = r#"{"stream":"dogeusdt@bookTicker","data":{"u":1,"s":"DOGEUSDT","b":"1","B":"1","a":"2","A":"1"}}"#;
        assert!(parse_tick(s).is_none());
    }

    #[test]
    fn rejects_malformed_prices() {
        let s = r#"{"stream":"btcusdt@bookTicker","data":{"u":1,"s":"BTCUSDT","b":"nope","B":"1","a":"101","A":"1"}}"#;
        assert!(parse_tick(s).is_none());
    }

    #[test]
    fn rejects_zero_or_negative_prices() {
        let s = r#"{"stream":"btcusdt@bookTicker","data":{"u":1,"s":"BTCUSDT","b":"0","B":"1","a":"101","A":"1"}}"#;
        assert!(parse_tick(s).is_none());
    }

    #[test]
    fn rejects_non_json() {
        assert!(parse_tick("not json").is_none());
        assert!(parse_tick("").is_none());
        assert!(parse_tick("{}").is_none());
    }

    #[test]
    fn builds_combined_stream_url_with_agg_trades() {
        let (tx, _rx) = broadcast::channel(16);
        let ing = BinanceIngest::new("wss://stream.binance.com:9443", vec![Coin::Btc])
            .with_agg_trades(vec![Coin::Btc], tx);
        let url = ing.build_url().unwrap();
        assert_eq!(
            url,
            "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade",
        );
    }

    #[test]
    fn parses_canonical_aggtrade_envelope() {
        // Golden frame: Binance combined-stream @aggTrade payload shape
        // (docs: aggTrade event — p price, q qty, T trade time ms, m maker flag).
        let s = r#"{
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "E": 1748736001123,
                "s": "BTCUSDT",
                "a": 3268559919,
                "p": "104629.90000000",
                "q": "0.00214000",
                "f": 5081431256,
                "l": 5081431256,
                "T": 1748736001122,
                "m": true,
                "M": true
            }
        }"#;
        let tr = parse_agg_trade(s).expect("parse aggTrade");
        assert_eq!(tr.coin, Coin::Btc);
        assert_abs_diff_eq!(tr.price, 104629.9, epsilon = 1e-9);
        assert_abs_diff_eq!(tr.qty, 0.00214, epsilon = 1e-12);
        assert!(tr.buyer_is_maker, "m=true (the aggressor SOLD) must survive the parse");
        assert_eq!(tr.exch_ts_ms, 1748736001122);
        assert!(tr.recv_ts_us > 0);
    }

    #[test]
    fn aggtrade_rejects_malformed_price_and_unknown_symbol() {
        let bad_px = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"nope","q":"1","f":1,"l":1,"T":1,"m":false,"M":true}}"#;
        assert!(parse_agg_trade(bad_px).is_none());
        let unknown = r#"{"stream":"dogeusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"DOGEUSDT","a":1,"p":"1","q":"1","f":1,"l":1,"T":1,"m":false,"M":true}}"#;
        assert!(parse_agg_trade(unknown).is_none());
    }

    #[test]
    fn kline_1s_parses_closed_only() {
        // Binance spot @kline_1s frame. x=false => in-progress (skip); x=true => closed (emit).
        let open = r#"{"e":"kline","E":1700000000123,"s":"BTCUSDT","k":{"t":1700000000000,"T":1700000000999,"s":"BTCUSDT","i":"1s","o":"42000.0","c":"42010.5","h":"42011.0","l":"41999.0","v":"1.5","x":false}}"#;
        let closed = r#"{"e":"kline","E":1700000001010,"s":"BTCUSDT","k":{"t":1700000000000,"T":1700000000999,"s":"BTCUSDT","i":"1s","o":"42000.0","c":"42010.5","h":"42011.0","l":"41999.0","v":"1.5","x":true}}"#;
        assert!(
            parse_kline_1s(open, Coin::Btc).is_none(),
            "in-progress kline must be skipped"
        );
        let s = parse_kline_1s(closed, Coin::Btc).expect("closed kline parses");
        assert_eq!(s.close_px, 42010.5);
        assert_eq!(s.close_ts_ms, 1700000000999);
        assert_eq!(s.coin, Coin::Btc);
    }

    #[test]
    fn kline_1s_rejects_non_finite_or_non_positive_close() {
        for bad_c in ["NaN", "inf", "-1"] {
            let s = format!(
                r#"{{"e":"kline","E":1700000001010,"s":"BTCUSDT","k":{{"t":1700000000000,"T":1700000000999,"s":"BTCUSDT","i":"1s","o":"42000.0","c":"{bad_c}","h":"42011.0","l":"41999.0","v":"1.5","x":true}}}}"#
            );
            assert!(
                parse_kline_1s(&s, Coin::Btc).is_none(),
                "close={bad_c} must be rejected"
            );
        }
    }

    #[test]
    fn builds_combined_stream_url_with_kline_1s() {
        let (tx, _rx) = broadcast::channel(16);
        let ing = BinanceIngest::new("wss://stream.binance.com:9443", vec![Coin::Btc])
            .with_kline_1s(vec![Coin::Btc], tx);
        let url = ing.build_url().unwrap();
        assert_eq!(
            url,
            "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@kline_1s",
        );
    }

    #[test]
    fn parses_canonical_kline_envelope() {
        // Canonical combined-stream shape (`{"stream":...,"data":{"e":"kline",...}}`),
        // exercising the dispatch-facing bridge (parse_kline_1s_envelope), not just
        // the bare-payload parse_kline_1s tested above.
        let open = r#"{
            "stream": "btcusdt@kline_1s",
            "data": {"e":"kline","E":1700000000123,"s":"BTCUSDT","k":{"t":1700000000000,"T":1700000000999,"s":"BTCUSDT","i":"1s","o":"42000.0","c":"42010.5","h":"42011.0","l":"41999.0","v":"1.5","x":false}}
        }"#;
        let closed = r#"{
            "stream": "btcusdt@kline_1s",
            "data": {"e":"kline","E":1700000001010,"s":"BTCUSDT","k":{"t":1700000000000,"T":1700000000999,"s":"BTCUSDT","i":"1s","o":"42000.0","c":"42010.5","h":"42011.0","l":"41999.0","v":"1.5","x":true}}
        }"#;
        assert!(
            parse_kline_1s_envelope(open).is_none(),
            "in-progress kline must be skipped"
        );
        let s = parse_kline_1s_envelope(closed).expect("closed kline parses");
        assert_eq!(s.coin, Coin::Btc);
        assert_abs_diff_eq!(s.close_px, 42010.5, epsilon = 1e-9);
        assert_eq!(s.close_ts_ms, 1700000000999);
    }

    #[test]
    fn kline_envelope_rejects_unknown_symbol() {
        let s = r#"{"stream":"dogeusdt@kline_1s","data":{"e":"kline","E":1,"s":"DOGEUSDT","k":{"t":1,"T":1,"s":"DOGEUSDT","i":"1s","o":"1","c":"1","h":"1","l":"1","v":"1","x":true}}}"#;
        assert!(parse_kline_1s_envelope(s).is_none());
    }

    #[test]
    fn kline_envelope_rejects_non_finite_or_non_positive_close() {
        for bad_c in ["NaN", "inf", "-1"] {
            let s = format!(
                r#"{{"stream":"btcusdt@kline_1s","data":{{"e":"kline","E":1700000001010,"s":"BTCUSDT","k":{{"t":1700000000000,"T":1700000000999,"s":"BTCUSDT","i":"1s","o":"42000.0","c":"{bad_c}","h":"42011.0","l":"41999.0","v":"1.5","x":true}}}}}}"#
            );
            assert!(
                parse_kline_1s_envelope(&s).is_none(),
                "close={bad_c} must be rejected via envelope"
            );
        }
    }

    #[test]
    fn kline_envelope_rejects_bookticker_and_aggtrade_shaped_data() {
        let book = r#"{"stream":"btcusdt@bookTicker","data":{"u":1,"s":"BTCUSDT","b":"100","B":"1","a":"101","A":"1"}}"#;
        assert!(parse_kline_1s_envelope(book).is_none());
        let trade = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"100","q":"1","f":1,"l":1,"T":1,"m":false,"M":true}}"#;
        assert!(parse_kline_1s_envelope(trade).is_none());
    }

    #[test]
    fn bookticker_frame_is_not_an_aggtrade_and_vice_versa() {
        let book = r#"{"stream":"btcusdt@bookTicker","data":{"u":1,"s":"BTCUSDT","b":"100","B":"1","a":"101","A":"1"}}"#;
        assert!(parse_agg_trade(book).is_none());
        assert!(parse_tick(book).is_some());
        let trade = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"100","q":"1","f":1,"l":1,"T":1,"m":false,"M":true}}"#;
        assert!(parse_tick(trade).is_none());
        assert!(parse_agg_trade(trade).is_some());
    }

    #[test]
    fn coin_from_symbol_exact_match_only() {
        assert_eq!(coin_from_symbol("BTCUSDT"), Some(Coin::Btc));
        assert_eq!(coin_from_symbol("btcusdt"), None);
        assert_eq!(coin_from_symbol("BTC"), None);
        assert_eq!(coin_from_symbol("BTCUSDC"), None);
    }

    #[test]
    fn binance_venue_selects_spot_or_perp_base() {
        assert_eq!(BinanceVenue::Spot.default_ws_base(), "wss://stream.binance.com:9443");
        assert_eq!(BinanceVenue::UsdMPerp.default_ws_base(), "wss://fstream.binance.com");
        // perp builds the SAME combined @bookTicker URL shape on its base (one socket, ws_base swap):
        let ing = BinanceIngest::new(BinanceVenue::UsdMPerp.default_ws_base(), vec![Coin::Btc]);
        assert_eq!(
            ing.build_url().unwrap(),
            "wss://fstream.binance.com/stream?streams=btcusdt@bookTicker",
        );
    }

    #[test]
    fn build_spot_watch_channels_pairs_up() {
        let (s, r) = build_spot_watch_channels();
        let tick = SpotTick {
            coin: Coin::Eth,
            price: 1000.0,
            ts_micros: 1,
        };
        s.get(Coin::Eth).send(Some(tick)).unwrap();
        let seen = (*r.get(Coin::Eth).borrow()).unwrap();
        assert_eq!(seen.coin, Coin::Eth);
        assert_eq!(seen.price, 1000.0);
    }
}
