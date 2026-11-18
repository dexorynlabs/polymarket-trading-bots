//! End-to-end proof that the SHIPPED Rust reconstruction reproduces Binance's
//! own 1s close on live data.
//!
//! The unit tests in `boundary_close` pin the state machine against synthetic
//! input. They cannot catch the thing that would actually hurt: a mismatch
//! between our idea of a bar and Binance's — the `T = open + 999` convention,
//! trade-time bucketing, or the silent-bar carry-forward being subtly wrong on
//! real traffic. This runs the real `BoundaryClose` against the real
//! `@aggTrade` stream and scores it against the real `@kline_1s`, which is
//! exactly the check the offline Python probe made (90.6% bit-exact at
//! delay = 0, 97.8% at delay = 100 — deployed box, 1,799 bars).
//!
//! Ignored by default; needs live Binance WS. Run:
//!   cargo test -p taker-tickburst --release --test boundary_close_live -- --ignored --nocapture

use std::time::{Duration, Instant};

use sniperbot::types::{now_micros, Coin};
use sniperbot::binance::feed::{build_spot_watch_channels, BinanceIngest, BinanceVenue};
use sniperbot::strategy::boundary_close::{next_tick_ms, BoundaryClose, ShadowCompare};
use tokio::sync::broadcast;

const CAPTURE_SECS: u64 = 150;
/// The setting the example config recommends starting at.
const DELAY_MS: u64 = 100;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live Binance WS; run with --ignored --nocapture"]
async fn reconstruction_reproduces_the_real_kline_on_live_data() {
    let (ktx, mut krx) = broadcast::channel(4_096);
    let (ttx, mut trx) = broadcast::channel(4_096);
    let (spot_tx, _spot_rx) = broadcast::channel(1_024);
    let (watch_txs, _watch_rxs) = build_spot_watch_channels();

    let ingest = BinanceIngest::new(BinanceVenue::Spot.default_ws_base(), vec![])
        .with_kline_1s(vec![Coin::Btc], ktx)
        .with_agg_trades(vec![Coin::Btc], ttx);
    let feed = tokio::spawn(async move {
        let _ = ingest.run(spot_tx, watch_txs).await;
    });

    let mut bc = BoundaryClose::new(Coin::Btc, DELAY_MS);
    let mut shadow = ShadowCompare::default();
    let (mut paired, mut exact, mut worst, mut recon_bars) = (0u64, 0u64, 0f64, 0u64);
    let mut deltas: Vec<f64> = Vec::new();

    let deadline = Instant::now() + Duration::from_secs(CAPTURE_SECS);
    println!("  capturing {CAPTURE_SECS}s at delay={DELAY_MS}ms ...");
    while Instant::now() < deadline {
        let now = now_micros() / 1_000;
        let sleep_for = next_tick_ms(now, DELAY_MS).saturating_sub(now);
        tokio::select! {
            t = trx.recv() => {
                if let Ok(t) = t { bc.on_trade(t.exch_ts_ms, t.price); }
            }
            k = krx.recv() => {
                if let Ok(k) = k {
                    if let Some(s) = shadow.on_kline(k.close_ts_ms, k.close_px) {
                        paired += 1;
                        if s.exact { exact += 1; }
                        worst = worst.max(s.delta_usd.abs());
                        deltas.push(s.delta_usd.abs());
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(sleep_for)) => {
                for c in bc.on_tick(now_micros() / 1_000) {
                    recon_bars += 1;
                    if let Some(s) = shadow.on_recon(c.close_ts_ms, c.close_px) {
                        paired += 1;
                        if s.exact { exact += 1; }
                        worst = worst.max(s.delta_usd.abs());
                        deltas.push(s.delta_usd.abs());
                    }
                }
            }
        }
    }
    feed.abort();

    deltas.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = 100.0 * exact as f64 / paired.max(1) as f64;
    let p99 = deltas.get((0.99 * deltas.len() as f64) as usize).copied().unwrap_or(0.0);
    println!(
        "\n  reconstructed {recon_bars} bars, paired {paired} against the real kline\n  \
         bit-exact {exact}/{paired} = {pct:.1}%   |delta| p99 ${p99:.2}   max ${worst:.2}\n"
    );

    assert!(paired >= 60, "only {paired} bars paired in {CAPTURE_SECS}s — feed problem, not fidelity");
    // The offline probe measured 97.8% at this delay; allow real headroom for a
    // busy minute rather than pinning the exact figure.
    assert!(pct >= 85.0, "bit-exactness {pct:.1}% is far below the measured 97.8% at delay={DELAY_MS}");
    // The gate floor is $12 and the trigger burst is $18; a p99 anywhere near
    // that would make the reconstruction unusable regardless of hit rate.
    assert!(p99 < 2.0, "|delta| p99 ${p99:.2} is too large against a $12 gate floor");
}
