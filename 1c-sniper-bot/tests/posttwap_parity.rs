//! THE GATING TEST for the post-TWAP gate (`taker-tickburst/POST-TWAP-SPEC.md`).
//!
//! It replaces the two earlier parity harnesses (`replay_parity.rs`,
//! `tick_parity.rs`), both deleted with the card machine they pinned: the
//! D-band cards graded 0-for-48 on the TWAP era, so reproducing them exactly
//! would only prove we reproduce a dead strategy.
//!
//! `SlotEngine` is fed the RAW per-second book reads and `@aggTrade` prints of
//! the Aug 7-9 census window and must reproduce the graded census entry set
//! EXACTLY — slot, side, second, and the venue millisecond of the triggering
//! tick. The engine discovers its own entries from the raw stream; the census
//! (`data/tickburst_posttwap_census/census_surface.py`) computes them with
//! wholly separate numpy code, so agreement is a reimplementation check, not a
//! transcription check.
//!
//! The acceptance cell (HANDOFF §2.2) is the **98 entries / 5 wins** at a 1c
//! ask in `entries_A.json`; the fixture carries those rows verbatim so this
//! test asserts the operator's own number.
//!
//! Two conventions of the fixture, both declared rather than smuggled:
//!
//! - **No delivery latency.** The census is fills-assumed at venue time, so
//!   `recv_ms == venue_ts_ms` here. The gate/fire clock split is real live
//!   (`engine::on_trade`) and is pinned by `tests/tick_trigger.rs`, not here.
//! - **No depth.** The census is depth-blind and so is the v0 gate (spec §6:
//!   flat clip, `ladder_walk` unused), so every in-band second carries its ask
//!   at a nominal size. If depth ever becomes a gate input, this fixture is no
//!   longer adequate for it.
//!
//! Regenerate the fixture with:
//!   .venv/bin/python -u scripts/export_tickburst_posttwap_fixture.py

use onecentbot::types::Side;
use serde::Deserialize;
use std::collections::HashSet;
use onecentbot::strategy::engine::{Action, EngineCfg, Event, SlotEngine};
use onecentbot::strategy::tick::TickCfg;

#[derive(Deserialize)]
struct Fixture {
    schema_v: u32,
    frame_lo: usize,
    frame_hi: usize,
    v_btc: f64,
    lean_min: f64,
    window_ms: u64,
    ask_price: f64,
    ladder_size: f64,
    /// The graded census's own ask==1c entries — the acceptance cell.
    census_1c_cell: Vec<CensusEntry>,
    /// The census's entries at a NON-1c ask: the slots where it burned its one
    /// shot somewhere spec §3.4 forbids us from entering.
    census_non_1c_entries: Vec<CensusOther>,
    slots: Vec<SlotFixture>,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct CensusEntry {
    slot_open: u64,
    side: String,
    t: usize,
    tick_ms: u64,
    won: bool,
}

#[derive(Deserialize, Debug, Clone)]
struct CensusOther {
    slot_open: u64,
    t: usize,
    ask: f64,
}

#[derive(Deserialize)]
struct SlotFixture {
    slot_open: u64,
    /// `"up"` / `"dn"` / `null` — the settled winner.
    winner: Option<String>,
    /// Best ask at each second of `[frame_lo, frame_hi)`, read the census's
    /// way; `null` where no snapshot inside the staleness bound exists.
    up_ask: Vec<Option<f64>>,
    dn_ask: Vec<Option<f64>>,
    /// Offsets from `slot_open * 1000`, ascending.
    tick_ms: Vec<u64>,
    tick_px: Vec<f64>,
    tick_qty: Vec<f64>,
    /// 1 = Binance `m` = the aggressor SOLD.
    tick_sell: Vec<u8>,
    /// The reference detector's single entry for this slot, or `null`.
    expected: Option<Entry>,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct Entry {
    side: String,
    t: usize,
    tick_ms: u64,
}

fn load() -> Fixture {
    let path =
        concat!(env!("CARGO_MANIFEST_DIR"), "/data/tickburst/posttwap_parity_fixture.json");
    let raw = std::fs::read_to_string(path).unwrap_or_else(|e| {
        panic!(
            "missing post-TWAP parity fixture at {path}: {e}\n\
             regenerate: .venv/bin/python -u scripts/export_tickburst_posttwap_fixture.py"
        )
    });
    let f: Fixture = serde_json::from_str(&raw).expect("fixture parses");
    assert_eq!(f.schema_v, 1, "fixture schema drifted");
    // The frozen gate (spec §3). A fixture regenerated against different
    // numbers is grading a different strategy.
    assert_eq!((f.frame_lo, f.frame_hi), (150, 291));
    assert_eq!(f.v_btc, 0.5);
    assert_eq!(f.lean_min, 0.6);
    assert_eq!(f.window_ms, 1_000);
    assert_eq!(f.ask_price, 0.01);
    f
}

/// The engine under a given entry band. The GRADED gate is the 1c equality
/// (`f.ask_price` both ends) — that is what the census measured and what the
/// first test below proves. The SHIPPED band (0.9c-1.1c, operator widening
/// 2026-08-09) is a separate cell, measured by the second test.
fn cfg_band(f: &Fixture, ask_price_min: f64, ask_price_max: f64) -> EngineCfg {
    EngineCfg {
        ask_price_min,
        ask_price_max,
        limit_price: 0.02,
        clip_usd: 10.0,
        tick: Some(TickCfg {
            v_btc: f.v_btc,
            lean_min: f.lean_min,
            window_ms: f.window_ms,
            // The fixture delivers a book event every second, so the held book
            // is never more than 999ms old here — this bound is exercised by
            // tests/tick_trigger.rs, not by parity.
            max_book_age_ms: 15_000,
        }),
    }
}

fn side_str(side: Side) -> String {
    match side {
        Side::Up => "up".into(),
        Side::Down => "down".into(),
    }
}

/// Ordering of the merged delivery stream: within one millisecond the book
/// snapshot lands before the tick that may fire against it — a tick fires
/// against the HELD ladder, and a snapshot delivered in the same instant is
/// strictly the better view to act on (`slot_driver`'s `biased` select mirrors
/// this live).
const KIND_BOOK: u8 = 0;
const KIND_TICK: u8 = 1;

/// One fire, reduced to what the census records.
#[derive(Debug, Clone, PartialEq)]
struct Fired {
    entry: Entry,
    best_ask: f64,
    limit_px: f64,
    size_shares: f64,
}

fn cfg(f: &Fixture) -> EngineCfg {
    cfg_band(f, f.ask_price, f.ask_price)
}

fn replay_with(f: &Fixture, s: &SlotFixture, cfg: EngineCfg) -> Vec<Fired> {
    let mut e = SlotEngine::new(cfg);
    // `p0` is a TELEMETRY input under this spec — no gate reads it (spec §3,
    // §4.1: D/zD are telemetry only) — so the fixture carries no klines and
    // this value is arbitrary.
    e.on_event(Event::SlotOpen { slot_open: s.slot_open, p0: 100_000.0 });

    let base = s.slot_open * 1000;
    let n_secs = f.frame_hi - f.frame_lo;
    // (deliver_ms, kind, index). Book indices run `up` seconds then `dn`.
    let mut stream: Vec<(u64, u8, usize)> =
        Vec::with_capacity(2 * n_secs + s.tick_ms.len());
    for k in 0..n_secs {
        let at = base + ((f.frame_lo + k) as u64) * 1000;
        stream.push((at, KIND_BOOK, k));
        stream.push((at, KIND_BOOK, n_secs + k));
    }
    for (i, off) in s.tick_ms.iter().enumerate() {
        stream.push((base + off, KIND_TICK, i));
    }
    stream.sort_by_key(|(ms, kind, i)| (*ms, *kind, *i));

    let mut got = Vec::new();
    for (ms, kind, i) in stream {
        let actions = if kind == KIND_BOOK {
            let (side, ask) = if i < n_secs {
                (Side::Up, s.up_ask[i])
            } else {
                (Side::Down, s.dn_ask[i - n_secs])
            };
            e.on_event(Event::Book {
                ingest_ms: ms,
                side,
                // A second with no fresh snapshot delivers an EMPTY ladder,
                // which is exactly what the census does with it: `None` from
                // `ask_at_second_start` means that second is not a candidate.
                asks: ask.map(|a| vec![(a, f.ladder_size)]).unwrap_or_default(),
            })
        } else {
            e.on_event(Event::Trade {
                venue_ts_ms: base + s.tick_ms[i],
                px: s.tick_px[i],
                qty: s.tick_qty[i],
                taker_buy: s.tick_sell[i] == 0,
                recv_ms: base + s.tick_ms[i],
            })
        };
        for a in actions {
            if let Action::Fire { t, side, best_ask, limit_px, size_shares, tick, .. } = a {
                got.push(Fired {
                    entry: Entry { side: side_str(side), t, tick_ms: tick.venue_ts_ms },
                    best_ask,
                    limit_px,
                    size_shares,
                });
            }
        }
    }
    got
}

fn replay(f: &Fixture, s: &SlotFixture) -> Vec<Fired> {
    replay_with(f, s, cfg(f))
}

fn won(winner: &Option<String>, side: &str) -> bool {
    let w = if side == "up" { "up" } else { "dn" };
    winner.as_deref() == Some(w)
}

#[test]
fn the_engine_reproduces_the_graded_post_twap_census_exactly() {
    let f = load();
    assert!(f.slots.len() >= 600, "fixture too small: {} slots", f.slots.len());

    let mut mismatches = Vec::new();
    let mut entries: Vec<(u64, Entry)> = Vec::new();
    let mut wins = 0usize;
    for s in &f.slots {
        let got = replay(&f, s);
        assert!(
            got.len() <= 1,
            "slot {}: {} fires — spec §3.6 is ONE shot per slot: {got:?}",
            s.slot_open,
            got.len()
        );
        let got_entry = got.first().map(|g| g.entry.clone());
        if got_entry != s.expected {
            mismatches.push(format!(
                "slot {}:\n  got  {got_entry:?}\n  want {:?}",
                s.slot_open, s.expected
            ));
            continue;
        }
        if let Some(g) = got.first() {
            // Spec §3.4 / §3.7: entry at the standing 1c ask, FAK limit 2c,
            // flat $10 clip = 1000 shares.
            assert!(
                (g.best_ask - 0.01).abs() < 1e-9,
                "slot {}: entered at ask {} — spec §3.4 permits 1c only",
                s.slot_open,
                g.best_ask
            );
            assert!((g.limit_px - 0.02).abs() < 1e-9, "slot {}: limit {}", s.slot_open, g.limit_px);
            // The DOLLAR assertion, added 2026-08-10: this fixture pins
            // COUNTS, so it passed unchanged through a sizing bug that
            // over-committed ~2x the clip. A fire must commit exactly the clip.
            let commit = g.size_shares * g.limit_px;
            assert!(
                (commit - 10.0).abs() < 1e-9,
                "slot {}: {} sh x {} = ${commit} committed, want the $10 clip",
                s.slot_open,
                g.size_shares,
                g.limit_px
            );
            // ...and that commitment buys the graded 1,000 shares at the 1c
            // ask it qualified on, because price improvement pays in shares.
            assert!(
                (commit / g.best_ask - 1000.0).abs() < 1e-6,
                "slot {}: ${commit} at ask {} is {} shares, want $10 / 1c",
                s.slot_open,
                g.best_ask,
                commit / g.best_ask
            );
            entries.push((s.slot_open, g.entry.clone()));
            if won(&s.winner, &g.entry.side) {
                wins += 1;
            }
        }
    }

    assert!(
        mismatches.is_empty(),
        "{} parity mismatches (first 5):\n{}",
        mismatches.len(),
        mismatches.iter().take(5).cloned().collect::<Vec<_>>().join("\n")
    );

    // ---- the acceptance cell: 98 entries / 5 wins (HANDOFF §2.2) ----------
    let got: HashSet<(u64, String, usize, u64)> =
        entries.iter().map(|(ep, e)| (*ep, e.side.clone(), e.t, e.tick_ms)).collect();
    let cell: HashSet<(u64, String, usize, u64)> =
        f.census_1c_cell.iter().map(|c| (c.slot_open, c.side.clone(), c.t, c.tick_ms)).collect();
    assert_eq!(f.census_1c_cell.len(), 98, "the graded cell is 98 entries");
    assert_eq!(
        f.census_1c_cell.iter().filter(|c| c.won).count(),
        5,
        "the graded cell carries 5 wins"
    );
    let missing: Vec<_> = cell.difference(&got).collect();
    assert!(
        missing.is_empty(),
        "{} of the 98 graded 1c entries were NOT reproduced (first 5): {:?}",
        missing.len(),
        missing.iter().take(5).collect::<Vec<_>>()
    );

    // The engine is a strict SUPERSET of the census's 1c cell, by spec §3.4:
    // the census burns its one shot wherever the trigger first fires, 2c asks
    // included; we never enter at 2c, so those slots stay live for a later 1c
    // second.
    let extra: Vec<_> = got.difference(&cell).collect();

    // Pin the MECHANISM, not just the count: every extra entry must land on a
    // slot where the census itself fired at a NON-1c ask, at a second no later
    // than ours. Anything else would be a genuinely new entry — an engine that
    // fires where the census found nothing at all — which is a different claim
    // from "the 2c rule leaves the shot unspent", and would be a real bug.
    let burned: std::collections::HashMap<u64, (usize, f64)> = f
        .census_non_1c_entries
        .iter()
        .map(|c| (c.slot_open, (c.t, c.ask)))
        .collect();
    for (ep, side, t, tick_ms) in &extra {
        match burned.get(ep) {
            Some((census_t, ask)) => assert!(
                census_t <= t,
                "slot {ep}: our entry at t={t} precedes the census's own fire at t={census_t}                  (ask {ask}) — this is NOT the 2c rule leaving a shot unspent"
            ),
            None => panic!(
                "slot {ep} ({side} t={t} tick={tick_ms}) is an entry the census never made at                  ANY ask — spec §3.4 explains extra entries only where the census burned its                  shot above 1c"
            ),
        }
    }

    // In-sample the 2c rule adds 22 entries and ZERO wins — the measured cost,
    // pinned so a regeneration cannot move it silently.
    assert_eq!(extra.len(), 22, "extra-over-cell entries: {extra:?}");
    assert_eq!(entries.len(), 120, "total entries");
    assert_eq!(wins, 5, "wins — every win of the graded cell, and no new one");

    eprintln!(
        "post-TWAP parity: {} slots, {} entries ({} graded cell + {} added by the 2c rule), \
         {wins} wins",
        f.slots.len(),
        entries.len(),
        cell.len(),
        extra.len()
    );
}

/// The SHIPPED band is 0.9c-1.1c (operator widening, 2026-08-09) — wider than
/// the 1c equality the census graded, so it needs its own measured cell rather
/// than riding on the parity claim above.
///
/// Measured over the same Aug 7-9 window, the widening is very nearly a no-op,
/// and every number below is a fact about the data, not a target:
/// - the entry COUNT and the WIN count are unchanged;
/// - exactly ONE entry moves, to an earlier and cheaper second, and it is a
///   loss under both configs;
/// - no 1.1c ask qualifies anywhere, so the band's upper half is inert here.
///
/// If a regeneration moves any of this, the widening has started doing
/// something the operator has not seen — which is the whole point of pinning
/// it separately.
#[test]
fn the_shipped_band_is_measured_as_its_own_cell() {
    let f = load();
    let graded = cfg(&f);
    let shipped = cfg_band(&f, 0.009, 0.011);

    let (mut n_graded, mut n_shipped) = (0usize, 0usize);
    let (mut w_graded, mut w_shipped) = (0usize, 0usize);
    let (mut moved, mut added, mut lost) = (Vec::new(), Vec::new(), Vec::new());
    let mut asks: std::collections::BTreeMap<String, usize> = Default::default();

    for s in &f.slots {
        let g = replay_with(&f, s, graded);
        let p = replay_with(&f, s, shipped);
        assert!(p.len() <= 1, "slot {}: one shot per slot", s.slot_open);
        n_graded += g.len();
        n_shipped += p.len();
        if let Some(x) = g.first() {
            if won(&s.winner, &x.entry.side) {
                w_graded += 1;
            }
        }
        if let Some(x) = p.first() {
            if won(&s.winner, &x.entry.side) {
                w_shipped += 1;
            }
            *asks.entry(format!("{:.4}", x.best_ask)).or_default() += 1;
            // The clip is a DOLLAR commitment, flat across the band: the ask
            // moves what the money BUYS, not what it SPENDS.
            let commit = x.size_shares * x.limit_px;
            assert!(
                (commit - 10.0).abs() < 1e-9,
                "slot {}: ${commit} committed at ask {} — the clip is $10 everywhere in the band",
                s.slot_open,
                x.best_ask
            );
            let want = 10.0 / x.best_ask;
            assert!(
                (commit / x.best_ask - want).abs() < 1e-6,
                "slot {}: ${commit} at ask {} — the $10 clip must buy {want}",
                s.slot_open,
                x.best_ask
            );
        }
        match (g.first(), p.first()) {
            (Some(a), Some(b)) if a.entry != b.entry => {
                moved.push((s.slot_open, a.entry.clone(), b.entry.clone(), a.best_ask, b.best_ask))
            }
            (None, Some(b)) => added.push((s.slot_open, b.entry.clone(), b.best_ask)),
            (Some(a), None) => lost.push((s.slot_open, a.entry.clone())),
            _ => {}
        }
    }

    assert_eq!(n_shipped, n_graded, "the widening must not change the entry COUNT here");
    assert_eq!(w_shipped, w_graded, "nor the win count");
    assert_eq!(w_shipped, 5, "and the wins are the graded five");
    assert!(added.is_empty(), "no slot becomes newly reachable: {added:?}");
    assert!(lost.is_empty(), "and none is lost: {lost:?}");
    assert_eq!(moved.len(), 1, "exactly one entry moves: {moved:?}");
    let (ep, from, to, from_ask, to_ask) = &moved[0];
    assert_eq!(*ep, 1_786_114_200);
    assert!(to.t < from.t, "it moves EARLIER: t+{} -> t+{}", from.t, to.t);
    assert!(to_ask < from_ask, "and CHEAPER: {from_ask} -> {to_ask}");
    assert!(
        !won(&f.slots.iter().find(|s| s.slot_open == *ep).unwrap().winner, &to.side),
        "the moved entry is a loss under the shipped band too — the widening buys no win here"
    );
    assert!(
        !asks.contains_key("0.0110"),
        "no 1.1c ask qualified in this window; the band's upper half is inert: {asks:?}"
    );

    eprintln!(
        "shipped band 0.9c-1.1c: {n_shipped} entries, {w_shipped} wins, asks={asks:?}, \
         moved={} added=0 lost=0",
        moved.len()
    );
}
