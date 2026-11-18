//! The 99c LEADER entry's behavioural contract — THE gating test of
//! `--live-readiness` (`bot/readiness.rs`).
//!
//! This is NOT a measured-edge proof. The 1c lineage gated on a census replay
//! (`posttwap_parity`); no census of 99c entries exists, so this file pins
//! behaviour only:
//!
//! - the mirror rule: the 1c bot's trigger (a burst toward the TRAILER) never
//!   buys the leader, and is logged as the placebo instead;
//! - both entry modes (`confirm`, `calm`);
//! - the two clocks (Binance's for the gate, ours for the fire stamp) and the
//!   held book's age measured against real feed latency.

use sniperbot::strategy::engine::{Action, EngineCfg, Event, ShotState, SlotEngine};
use sniperbot::strategy::tick::{TickCfg, TriggerMode};
use sniperbot::types::Side;

const SLOT_OPEN: u64 = 1_786_098_600; // divisible by 300
const BASE: u64 = SLOT_OPEN * 1000;
const P0: f64 = 100_000.0;
/// Binance tick -> our box, measured in the 1c lineage.
const FEED_LATENCY_MS: u64 = 135;

fn cfg_mode(mode: TriggerMode, max_book_age_ms: u64) -> EngineCfg {
    EngineCfg {
        ask_price_min: 0.97,
        ask_price_max: 0.99,
        limit_price: 0.99,
        clip_usd: 20.0,
        tick: Some(TickCfg {
            mode,
            v_btc: 0.5,
            lean_min: 0.6,
            adverse_max_btc: 0.3,
            window_ms: 1_000,
            max_book_age_ms,
        }),
    }
}

fn engine_mode(mode: TriggerMode, max_book_age_ms: u64) -> SlotEngine {
    let mut e = SlotEngine::new(cfg_mode(mode, max_book_age_ms));
    e.on_event(Event::SlotOpen { slot_open: SLOT_OPEN, p0: P0 });
    e
}

fn engine(max_book_age_ms: u64) -> SlotEngine {
    engine_mode(TriggerMode::Burst, max_book_age_ms)
}

/// A book on `side` with the given best ask, stamped `ingest_ms`.
fn book_at(e: &mut SlotEngine, ingest_ms: u64, side: Side, ask: f64) {
    e.on_event(Event::Book { ingest_ms, side, asks: vec![(ask, 5_000.0)] });
}

/// A 99c leader ask on `side`.
fn book(e: &mut SlotEngine, ingest_ms: u64, side: Side) {
    book_at(e, ingest_ms, side, 0.99);
}

/// Six 0.1 BTC prints at venue time `venue_ms + i*10`, all one aggressor
/// direction, reaching us `latency_ms` later.
fn prints(e: &mut SlotEngine, venue_ms: u64, latency_ms: u64, taker_buy: bool) -> Vec<Action> {
    let mut out = Vec::new();
    for i in 0..6u64 {
        let v = venue_ms + i * 10;
        out.extend(e.on_event(Event::Trade {
            venue_ts_ms: v,
            px: P0,
            qty: 0.1,
            taker_buy,
            recv_ms: v + latency_ms,
        }));
    }
    out
}

fn fires(actions: &[Action]) -> Vec<&Action> {
    actions.iter().filter(|a| matches!(a, Action::Fire { .. })).collect()
}

fn placebos(actions: &[Action]) -> Vec<&Action> {
    actions.iter().filter(|a| matches!(a, Action::Placebo { .. })).collect()
}

// ── The mirror rule ─────────────────────────────────────────────────────

/// The 1c bot buys the TRAILER on its recovery flow. With UP leading at 99c,
/// DOWN trails at ~1c and its recovery is taker SELLs. While the trailing
/// window carries that burst, the UP leader must NOT be bought — the moment
/// is logged as the placebo — in either mode.
///
/// Note the window is already loaded when the leader's book arrives: `calm`
/// evaluates the window at every print, so the FIRST prints of a building
/// burst (below `adverse_max_btc`) still read as calm. That is inherent to
/// the mode, not something this test hides — see the README.
#[test]
fn the_one_cent_bots_trigger_never_buys_the_leader() {
    for mode in [TriggerMode::Burst, TriggerMode::Calm] {
        let mut e = engine_mode(mode, 2_000);
        book_at(&mut e, BASE + 200_000, Side::Down, 0.01);
        let before = prints(&mut e, BASE + 200_100, FEED_LATENCY_MS, false);
        assert!(before.is_empty(), "{mode:?}: the 1c trailer is never a target");
        book(&mut e, BASE + 200_200, Side::Up);
        let acts = prints(&mut e, BASE + 200_300, FEED_LATENCY_MS, false);
        assert!(fires(&acts).is_empty(), "{mode:?}: a trailer-recovery burst must not buy the leader");
        assert_eq!(placebos(&acts).len(), 1, "{mode:?}: it is logged as the mirror");
        assert_eq!(e.shot(), ShotState::Ready, "{mode:?}: the mirror never spends the shot");
    }
}

/// The trailing 1c token is never the target, whatever the flow.
#[test]
fn only_the_leader_side_is_ever_bought() {
    let mut e = engine(2_000);
    book_at(&mut e, BASE + 200_000, Side::Down, 0.01);
    assert!(fires(&prints(&mut e, BASE + 200_100, FEED_LATENCY_MS, false)).is_empty());
    assert!(fires(&prints(&mut e, BASE + 200_300, FEED_LATENCY_MS, true)).is_empty());
}

// ── Confirm mode ────────────────────────────────────────────────────────

#[test]
fn confirm_buys_the_leader_on_a_burst_in_its_favour() {
    let mut e = engine(2_000);
    book(&mut e, BASE + 200_000, Side::Up);
    let acts = prints(&mut e, BASE + 200_100, FEED_LATENCY_MS, true);
    let f = fires(&acts);
    assert_eq!(f.len(), 1);
    let Action::Fire { side, best_ask, limit_px, size_shares, clip_usd, .. } = f[0] else {
        unreachable!()
    };
    assert_eq!(*side, Side::Up);
    assert_eq!(*best_ask, 0.99);
    assert_eq!(*limit_px, 0.99);
    assert!((size_shares * limit_px - clip_usd).abs() < 1e-9, "commits exactly the clip");
    assert_eq!(e.shot(), ShotState::Spent);
}

#[test]
fn confirm_buys_a_leading_down_on_taker_sells() {
    let mut e = engine(2_000);
    book(&mut e, BASE + 200_000, Side::Down);
    assert!(fires(&prints(&mut e, BASE + 200_100, FEED_LATENCY_MS, true)).is_empty());
    let mut e = engine(2_000);
    book(&mut e, BASE + 200_000, Side::Down);
    let acts = prints(&mut e, BASE + 200_100, FEED_LATENCY_MS, false);
    let f = fires(&acts);
    assert_eq!(f.len(), 1);
    let Action::Fire { side, .. } = f[0] else { unreachable!() };
    assert_eq!(*side, Side::Down);
}

#[test]
fn confirm_does_not_fire_on_thin_flow() {
    let mut e = engine(2_000);
    book(&mut e, BASE + 200_000, Side::Up);
    let acts = e.on_event(Event::Trade {
        venue_ts_ms: BASE + 200_100,
        px: P0,
        qty: 0.1,
        taker_buy: true,
        recv_ms: BASE + 200_100 + FEED_LATENCY_MS,
    });
    assert!(fires(&acts).is_empty(), "0.1 BTC is not a burst");
    assert_eq!(e.shot(), ShotState::Ready);
}

// ── Calm mode ───────────────────────────────────────────────────────────

#[test]
fn calm_buys_the_leader_on_the_first_quiet_print() {
    let mut e = engine_mode(TriggerMode::Calm, 2_000);
    book(&mut e, BASE + 200_000, Side::Up);
    let acts = e.on_event(Event::Trade {
        venue_ts_ms: BASE + 200_100,
        px: P0,
        qty: 0.01,
        taker_buy: false,
        recv_ms: BASE + 200_100 + FEED_LATENCY_MS,
    });
    assert_eq!(fires(&acts).len(), 1, "no burst against the leader = calm");
}

#[test]
fn calm_fires_once_per_slot() {
    let mut e = engine_mode(TriggerMode::Calm, 2_000);
    book(&mut e, BASE + 200_000, Side::Up);
    let first = prints(&mut e, BASE + 200_100, FEED_LATENCY_MS, true);
    assert_eq!(fires(&first).len(), 1);
    book(&mut e, BASE + 240_000, Side::Up);
    assert!(fires(&prints(&mut e, BASE + 240_100, FEED_LATENCY_MS, true)).is_empty());
}

// ── Clocks and book age ─────────────────────────────────────────────────

/// The GATE runs on Binance's clock and the FIRE on ours.
#[test]
fn the_gate_reads_binances_second_and_the_fire_is_stamped_with_ours() {
    let mut e = engine(2_000);
    book(&mut e, BASE + 200_900, Side::Up);
    // Venue second 200, 950ms in: +135ms of feed latency lands it in 201.
    let acts = prints(&mut e, BASE + 200_950, FEED_LATENCY_MS, true);
    let f = fires(&acts);
    assert_eq!(f.len(), 1);
    let Action::Fire { t, tick, .. } = f[0] else { unreachable!() };
    assert_eq!(*t, 201, "the order is stamped with the second it actually left in");
    assert_eq!(tick.venue_ts_ms / 1000 - SLOT_OPEN, 200);
}

/// The frame [150, 291) is enforced on the GATE's clock.
#[test]
fn the_frame_bound_is_binances_second_not_our_arrival_second() {
    let mut inside = engine(2_000);
    book(&mut inside, BASE + 290_500, Side::Up);
    assert_eq!(fires(&prints(&mut inside, BASE + 290_900, FEED_LATENCY_MS, true)).len(), 1);

    let mut outside = engine(2_000);
    book(&mut outside, BASE + 291_000, Side::Up);
    assert!(fires(&prints(&mut outside, BASE + 291_000, FEED_LATENCY_MS, true)).is_empty());
    assert_eq!(outside.shot(), ShotState::Ready);

    let mut early = engine(2_000);
    book(&mut early, BASE + 149_000, Side::Up);
    assert!(fires(&prints(&mut early, BASE + 149_100, FEED_LATENCY_MS, true)).is_empty());
}

/// Age = our arrival minus the snapshot's ingest stamp, so feed latency
/// counts against the bound.
#[test]
fn the_held_books_age_is_measured_from_arrival_so_feed_latency_counts() {
    let mut tight = engine(200);
    book(&mut tight, BASE + 200_000, Side::Up);
    assert!(fires(&prints(&mut tight, BASE + 200_100, FEED_LATENCY_MS, true)).is_empty());
    assert_eq!(tight.shot(), ShotState::Ready, "a stale-book skip must not spend the shot");

    let mut ok = engine(300);
    book(&mut ok, BASE + 200_000, Side::Up);
    assert_eq!(fires(&prints(&mut ok, BASE + 200_100, FEED_LATENCY_MS, true)).len(), 1);
}

/// The shipped 2s bound: a leader ask seen 1.5s ago is actionable, one seen
/// 2.5s ago is not (the 1c lineage's 15s would buy a long-gone 99c).
#[test]
fn the_shipped_bound_is_two_seconds() {
    let mut e = engine(2_000);
    book(&mut e, BASE + 200_000, Side::Up);
    assert_eq!(fires(&prints(&mut e, BASE + 201_300, FEED_LATENCY_MS, true)).len(), 1);

    let mut stale = engine(2_000);
    book(&mut stale, BASE + 200_000, Side::Up);
    assert!(fires(&prints(&mut stale, BASE + 202_400, FEED_LATENCY_MS, true)).is_empty());
}

/// A leader that slipped out of the band mid-burst is not bought: the gate
/// reads the LATEST ladder.
#[test]
fn a_leader_that_repriced_mid_burst_is_the_one_the_gate_reads() {
    let mut e = engine(2_000);
    book(&mut e, BASE + 200_000, Side::Up);
    for i in 0..3u64 {
        let v = BASE + 200_100 + i * 10;
        e.on_event(Event::Trade {
            venue_ts_ms: v,
            px: P0,
            qty: 0.1,
            taker_buy: true,
            recv_ms: v + FEED_LATENCY_MS,
        });
    }
    // The leader slipped to 90c.
    book_at(&mut e, BASE + 200_200, Side::Up, 0.90);
    assert!(fires(&prints(&mut e, BASE + 200_300, FEED_LATENCY_MS, true)).is_empty());
    assert_eq!(e.shot(), ShotState::Ready);
}
