//! The fire path's behavioural contract at the boundaries the pure unit tests
//! in `src/engine.rs` cannot reach: the TWO CLOCKS, and the held book's age
//! measured against real delivery latency.
//!
//! `src/engine.rs`'s own tests drive `recv_ms == venue_ts_ms` (no latency),
//! which is also what `tests/posttwap_parity.rs` does — the census is
//! fills-assumed at venue time. Live, the two differ by the ~135ms stream
//! latency plus the venue's ~250ms taker delay (POST-TWAP-SPEC §5), and that
//! gap is the ONE place a real network delay changes which second an order
//! lands in. This file is where that is pinned.
//!
//! The earlier version of this file pinned the dose gate inside a D-band
//! card (`dose_usd`, `zD`, per-card `tick_attempted`); all of it went with the
//! card machine (spec §6).

use onecentbot::types::Side;
use onecentbot::strategy::engine::{Action, EngineCfg, Event, ShotState, SlotEngine};
use onecentbot::strategy::tick::TickCfg;

const SLOT_OPEN: u64 = 1_786_098_600; // divisible by 300
const BASE: u64 = SLOT_OPEN * 1000;
const P0: f64 = 100_000.0;
/// The measured chain: Binance tick -> our box ~135ms -> software ~3ms ->
/// our box->venue ~10ms -> venue taker delay ~250ms (spec §5). What the ENGINE
/// sees is the feed half; the venue delay lands downstream of the send.
const FEED_LATENCY_MS: u64 = 135;

fn cfg(max_book_age_ms: u64) -> EngineCfg {
    EngineCfg {
        ask_price_min: 0.009,
        ask_price_max: 0.011,
        limit_price: 0.02,
        clip_usd: 10.0,
        tick: Some(TickCfg { v_btc: 0.5, lean_min: 0.6, window_ms: 1_000, max_book_age_ms }),
    }
}

/// A 1c ask delivered on `side`, stamped `ingest_ms`.
fn book(e: &mut SlotEngine, ingest_ms: u64, side: Side) {
    e.on_event(Event::Book {
        ingest_ms,
        side,
        asks: vec![(0.01, 5_000.0)],
    });
}

/// A recovery burst whose prints happen at venue time `venue_ms + i*10` and
/// reach us `latency_ms` later. Returns every action produced.
fn burst(e: &mut SlotEngine, venue_ms: u64, latency_ms: u64) -> Vec<Action> {
    let mut out = Vec::new();
    for i in 0..6u64 {
        let v = venue_ms + i * 10;
        out.extend(e.on_event(Event::Trade {
            venue_ts_ms: v,
            px: P0,
            qty: 0.1,
            taker_buy: true,
            recv_ms: v + latency_ms,
        }));
    }
    out
}

fn fires(actions: &[Action]) -> Vec<&Action> {
    actions.iter().filter(|a| matches!(a, Action::Fire { .. })).collect()
}

fn engine(max_book_age_ms: u64) -> SlotEngine {
    let mut e = SlotEngine::new(cfg(max_book_age_ms));
    e.on_event(Event::SlotOpen { slot_open: SLOT_OPEN, p0: P0 });
    e
}

/// The GATE runs on Binance's clock and the FIRE on ours. A tick in venue
/// second 200 that reaches us in second 201 is gated at 200 and STAMPED 201 —
/// the order really did leave in second 201, and the telemetry must say so,
/// while `tick.venue_ts_ms` keeps the instant the market moved.
#[test]
fn the_gate_reads_binances_second_and_the_fire_is_stamped_with_ours() {
    let mut e = engine(15_000);
    book(&mut e, BASE + 200_000, Side::Up);
    // Venue second 200, 950ms in: +135ms of feed latency lands it in 201.
    let acts = burst(&mut e, BASE + 200_950, FEED_LATENCY_MS);
    let f = fires(&acts);
    assert_eq!(f.len(), 1);
    let Action::Fire { t, tick, .. } = f[0] else { unreachable!() };
    assert_eq!(*t, 201, "the order is stamped with the second it actually left in");
    assert_eq!(
        tick.venue_ts_ms / 1000 - SLOT_OPEN,
        200,
        "the trigger keeps Binance's own instant, not ours"
    );
}

/// The frame (§3.3) is enforced on the GATE's clock. A tick at venue second
/// 290 is inside [150, 291) and fires even though it reaches us in 291; the
/// same latency applied to a venue-second-291 tick fires nothing.
#[test]
fn the_frame_bound_is_binances_second_not_our_arrival_second() {
    let mut inside = engine(15_000);
    book(&mut inside, BASE + 290_000, Side::Up);
    let acts = burst(&mut inside, BASE + 290_950, FEED_LATENCY_MS);
    assert_eq!(fires(&acts).len(), 1, "venue second 290 is inside the frame");

    let mut outside = engine(15_000);
    book(&mut outside, BASE + 291_000, Side::Up);
    let acts = burst(&mut outside, BASE + 291_000, FEED_LATENCY_MS);
    assert!(fires(&acts).is_empty(), "venue second 291 is past the frame");
    assert_eq!(outside.shot(), ShotState::Ready, "an out-of-frame tick spends nothing");
}

/// The held book's age is measured on OUR clock — arrival minus the snapshot's
/// own ingest stamp — because that is the interval over which the venue could
/// have moved the ladder out from under us. Feed latency therefore counts
/// against the bound, and a bound tighter than the latency can never fire.
#[test]
fn the_held_books_age_is_measured_from_arrival_so_feed_latency_counts() {
    // Book at 200.000, tick at venue 200.100 arriving 200.235: age 235ms.
    let mut tight = engine(200);
    book(&mut tight, BASE + 200_000, Side::Up);
    assert!(
        fires(&burst(&mut tight, BASE + 200_100, FEED_LATENCY_MS)).is_empty(),
        "a 200ms bound cannot be met once 135ms of feed latency is inside it"
    );
    assert_eq!(tight.shot(), ShotState::Ready, "a stale-book skip must not spend the shot");

    let mut ok = engine(300);
    book(&mut ok, BASE + 200_000, Side::Up);
    assert_eq!(fires(&burst(&mut ok, BASE + 200_100, FEED_LATENCY_MS)).len(), 1);
}

/// The census's staleness convention is 15s (`census_surface.py`'s
/// `STALE_MS`), and the shipped bound matches it because the target IS a quiet
/// 1c ask: spec §5's absorbed classes stand for 2.5s and 16.5s. A book last
/// seen 10s ago is still actionable; one last seen 16s ago is not.
#[test]
fn the_shipped_bound_still_buys_a_book_last_seen_ten_seconds_ago() {
    let mut e = engine(15_000);
    book(&mut e, BASE + 200_000, Side::Up);
    assert_eq!(fires(&burst(&mut e, BASE + 210_000, FEED_LATENCY_MS)).len(), 1);

    let mut stale = engine(15_000);
    book(&mut stale, BASE + 200_000, Side::Up);
    assert!(fires(&burst(&mut stale, BASE + 216_000, FEED_LATENCY_MS)).is_empty());
}

/// A fresher snapshot delivered mid-burst replaces the one the fire will buy
/// against — the held book is always the LATEST ladder, never the one that was
/// current when the burst began. Here the 1c ask is replaced by a 5c one
/// before the burst completes, so nothing fires.
#[test]
fn a_book_that_repriced_mid_burst_is_the_one_the_gate_reads() {
    let mut e = engine(15_000);
    book(&mut e, BASE + 200_000, Side::Up);
    // Three prints: not yet 0.5 BTC.
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
    // The cheap ask is gone.
    e.on_event(Event::Book {
        ingest_ms: BASE + 200_200,
        side: Side::Up,
        asks: vec![(0.05, 5_000.0)],
    });
    let acts = burst(&mut e, BASE + 200_300, FEED_LATENCY_MS);
    assert!(fires(&acts).is_empty(), "the burst must buy the ladder we now hold, not the old one");
    assert_eq!(e.shot(), ShotState::Ready);
}

/// The two sides are independent up to the slot's single shot: a burst that
/// recovers DOWN while only UP shows a 1c ask fires nothing, and the shot
/// survives for the side the market is actually moving toward.
#[test]
fn the_side_is_chosen_by_the_book_and_the_lean_must_match_it() {
    let mut e = engine(15_000);
    book(&mut e, BASE + 200_000, Side::Up);
    // Taker SELLs: they recover a DOWN token, but DOWN shows no cheap ask.
    let mut acts = Vec::new();
    for i in 0..6u64 {
        let v = BASE + 200_100 + i * 10;
        acts.extend(e.on_event(Event::Trade {
            venue_ts_ms: v,
            px: P0,
            qty: 0.1,
            taker_buy: false,
            recv_ms: v + FEED_LATENCY_MS,
        }));
    }
    assert!(fires(&acts).is_empty());
    assert_eq!(e.shot(), ShotState::Ready);

    // The same slot, seconds later: DOWN is the cheap side now, and the same
    // selling flow is its recovery.
    book(&mut e, BASE + 240_000, Side::Down);
    let mut acts = Vec::new();
    for i in 0..6u64 {
        let v = BASE + 240_100 + i * 10;
        acts.extend(e.on_event(Event::Trade {
            venue_ts_ms: v,
            px: P0,
            qty: 0.1,
            taker_buy: false,
            recv_ms: v + FEED_LATENCY_MS,
        }));
    }
    let f = fires(&acts);
    assert_eq!(f.len(), 1);
    let Action::Fire { side, .. } = f[0] else { unreachable!() };
    assert_eq!(*side, Side::Down);
}
