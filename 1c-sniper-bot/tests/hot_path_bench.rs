//! Bench the two pre-send hot-path costs the latency budget lists as
//! "unmeasured" (the latency-options probe of 2026-07-26 §7).
//!
//! `slot_driver::exec` runs, between the fire decision and `send_fak`:
//!   rails.check_fire -> actions_to_events(Fire) -> tele.emit
//! and `actions_to_events`'s Fire arm still clones the full ask ladder. (The
//! counterfactual dose `ladder_walk` it also used to run is gone with the
//! clip laws — POST-TWAP-SPEC §6.) The proposal is to move it after the send.
//! This says whether that is worth doing.
//!
//! Ignored by default (benchmark, not an assertion). Run:
//!   cargo test -p taker-tickburst --release --test hot_path_bench -- --ignored --nocapture

use std::time::Instant;

use onecentbot::types::Side;
use onecentbot::strategy::cards::CardId;
use onecentbot::strategy::engine::{Action, TickFire};
use onecentbot::strategy::features::SideFeatures;
use onecentbot::strategy::flow::FlowStats;
use onecentbot::bot::telemetry::actions_to_events;

fn pct(xs: &[f64], p: f64) -> f64 {
    let mut s = xs.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[((p / 100.0 * s.len() as f64) as usize).min(s.len() - 1)]
}

/// A realistic cheap-band ladder: 30 levels from 1c up, the shape the
/// budget walk actually sees late in a btc-5m slot.
fn deep_ladder() -> Vec<(f64, f64)> {
    (0..30)
        .map(|i| (0.01 + i as f64 * 0.01, 200.0 + (i as f64) * 37.0))
        .collect()
}

fn fire(asks: Vec<(f64, f64)>) -> Action {
    Action::Fire {
        t: 200,
        side: Side::Up,
        card: CardId::Burst,
        best_ask: 0.01,
        limit_px: 0.02,
        size_shares: 1_000.0,
        clip_usd: 10.0,
        feats: Some(SideFeatures { d: 30.0, v2: 7.0, v3: 4.0, sigma60: 0.006, z_d: 1.5, tau: 100.0 }),
        asks,
        tick: TickFire {
            venue_ts_ms: 1_786_098_800_137,
            vol_btc: 0.82,
            lean: 0.77,
            window_ms: 1_000,
            book_age_ms: 210,
        },
    }
}

#[test]
#[ignore = "benchmark; run with --ignored --nocapture"]
fn bench_pre_send_telemetry_mapping() {
    const WARMUP: usize = 500;
    const N: usize = 20_000;

    // Populated (not None) so the bench pays the full serialization cost the
    // live tick path pays.
    let flow = Some(FlowStats {
        tbuy_usd_1s: 187_000.0,
        notional_usd_1s: 188_000.0,
        vol_btc_1s: 1.88,
        trades_1s: 61,
        max_dose_20s_usd: 34.0,
    });

    for (label, asks) in [
        ("2-level ladder", vec![(0.02, 50.0), (0.18, 1000.0)]),
        ("30-level ladder", deep_ladder()),
    ] {
        let action = fire(asks);
        for _ in 0..WARMUP {
            std::hint::black_box(actions_to_events(std::slice::from_ref(&action), 1_784_000_000, flow));
        }
        let mut us = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            let ev = actions_to_events(std::slice::from_ref(&action), 1_784_000_000, flow);
            us.push(t.elapsed().as_secs_f64() * 1e6);
            std::hint::black_box(ev);
        }
        println!(
            "  actions_to_events(Fire), {label:<16} p50 {:>7.2}  p90 {:>7.2}  \
             p99 {:>7.2}  max {:>8.2}  (us)",
            pct(&us, 50.0),
            pct(&us, 90.0),
            pct(&us, 99.0),
            pct(&us, 100.0),
        );
    }
    println!(
        "\n  => compare against the ~560 ms pipeline and the >=50 ms target.\n"
    );
}

/// The trigger runs on EVERY `@aggTrade` — ~11/s on BTC steady-state and
/// several times that inside the bursts it exists to catch. This measures the
/// per-trade cost of a NON-firing print, which is >99.9% of them: a ring push
/// plus a prune plus the trailing-window sum per side.
#[test]
#[ignore = "benchmark; run with --ignored --nocapture"]
fn bench_per_trade_tick_cost() {
    use onecentbot::types::Side;
    use onecentbot::strategy::engine::{EngineCfg, Event, SlotEngine};
    use onecentbot::strategy::tick::TickCfg;

    const SLOT_OPEN: u64 = 1_786_098_600;
    const P0: f64 = 100_000.0;
    const WARMUP: usize = 2_000;
    const N: usize = 50_000;

    let mut e = SlotEngine::new(EngineCfg {
        ask_price_min: 0.009,
        ask_price_max: 0.011,
        limit_price: 0.02,
        clip_usd: 10.0,
        tick: Some(TickCfg {
            v_btc: 0.5,
            lean_min: 0.6,
            window_ms: 1_000,
            max_book_age_ms: 15_000,
        }),
    });
    e.on_event(Event::SlotOpen { slot_open: SLOT_OPEN, p0: P0 });
    for i in 0..=199 {
        e.on_event(Event::Kline { t: i, close_px: P0 - 45.0 + if i % 2 == 1 { 4.0 } else { 0.0 } });
    }
    e.on_event(Event::Book {
        ingest_ms: (SLOT_OPEN + 200) * 1000,
        side: Side::Up,
        asks: vec![(0.01, 1000.0)],
    });

    // Prints inside the band but mixed 50/50, so the gate is evaluated in full
    // every time and never clears the lean — the realistic steady state.
    let mut n = 0u64;
    let mut tick = |e: &mut SlotEngine| {
        n += 1;
        let venue_ts_ms = (SLOT_OPEN + 200) * 1000 + (n % 900);
        e.on_event(Event::Trade {
            venue_ts_ms,
            px: P0 - 30.0 - (n % 3) as f64 * 0.01,
            qty: 0.05,
            taker_buy: n.is_multiple_of(2),
            recv_ms: venue_ts_ms + 135,
        })
    };
    for _ in 0..WARMUP {
        std::hint::black_box(tick(&mut e));
    }
    let mut us = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        let out = tick(&mut e);
        us.push(t.elapsed().as_secs_f64() * 1e6);
        std::hint::black_box(out);
    }
    println!(
        "  SlotEngine::on_event(Trade), non-firing  p50 {:>7.3}  p90 {:>7.3}  \
         p99 {:>7.3}  max {:>8.3}  (us)",
        pct(&us, 50.0),
        pct(&us, 90.0),
        pct(&us, 99.0),
        pct(&us, 100.0),
    );
    println!("  => ~11 trades/s steady state; compare against the ~560 ms pipeline.\n");
}
