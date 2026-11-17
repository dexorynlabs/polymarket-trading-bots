//! Risk rails (design §5) — the six vetoes that can suppress a `Fire`.
//!
//! **Governors vs latches.** `DailySpend`, `ConcurrentExposure`,
//! `PerOrderCommit`, and `FeedUnhealthy` are governors: they bound a budget or
//! track a health signal, so they suppress only the CURRENT fire and clear on
//! their own once the underlying quantity changes (spend rolls off, exposure
//! is released, the feed recovers). `RealizedLossKill` and `SelfFireRate` are
//! latches: they detect a fault — a real trading loss, or a state-machine bug
//! spraying sends — so once tripped they are STICKY for the life of the
//! process. `halted()` reports the tripped latch; recovery is an operator
//! restart, not a timer.
//!
//! **`SelfFireRate` is a latch, not a rate limiter.** Slots are sequential,
//! and POST-TWAP-SPEC §3.6 gives each slot ONE shot — so the decision core
//! bounds fires structurally at 1 order per 300s slot, i.e. no 60s window can
//! hold more than one send at all. That is ~100x below the 20/min default
//! threshold. (The earlier bound was 3 cards x 2 sides = 6 orders/slot;
//! the re-gating made it strictly tighter, so the shipped threshold has even
//! more headroom than it was set with.) No market condition reaches that rate;
//! a trip is proof of a state-machine fault (e.g. the duplicate-`SlotOpen`
//! shot-resurrection class this rail was designed to catch). Faults don't heal
//! when a 60s window rolls over: an auto-recovering rail would let a
//! resurrection bug duty-cycle (trip, wait out the window, trip again),
//! spraying clips until the daily spend cap finally caught it — turning a
//! "halt + alarm" rail into a "bleed slower" rail. So this
//! module never re-arms a tripped latch. It only halts the MONEY path:
//! `check_fire` starts vetoing once a latch trips, but nothing here exits the
//! process — settlement, telemetry, and the placebo lane are untouched.
//!
//! **Units.** This module's public API is milliseconds (`now_ms`) throughout,
//! matching the rest of the crate. `crate::execution::ledger::
//! SpendLedger` is MICROSECONDS — every call site here converts (`now_ms *
//! 1_000`). Passing `now_ms` straight through would silently widen the
//! rolling 24h spend window to ~1000 days (the bug
//! `daily_spend_ledger_converts_ms_to_micros_for_the_rolling_window` pins).
//!
//! **No telemetry dependency, by design.** `check_fire` sees only
//! `commit_usd` and `now_ms` — no `t`, `side`, `cards`, or `slot_open` — so it
//! cannot itself emit an attributed `RailTrip` row. See the doc comment on
//! `check_fire` for the Task 13 obligation this creates.
//!
//! **`RailTripInfo` (Task 13 review D).** `check_fire`'s `Err` carries not
//! just the tripped veto but the `value`/`threshold` it was compared
//! against — reported by the rail itself, not reconstructed by the caller
//! from getters. A getter-based design would force the driver to
//! recompute what the rail compared (e.g. `concurrent_usd + commit`),
//! which is a second source of truth for rail internals — this module
//! stays the only place that knows its own arithmetic, and stays free of
//! any telemetry dependency (the returned numbers are plain `f64`s).

use crate::config::StrategySection;
use crate::execution::ledger::SpendLedger;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

const MS_PER_DAY: u64 = 24 * 3_600 * 1_000;
const SELF_FIRE_WINDOW_MS: u64 = 60_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RailVeto {
    DailySpend,
    ConcurrentExposure,
    RealizedLossKill,
    SelfFireRate,
    FeedUnhealthy,
    PerOrderCommit,
}

impl RailVeto {
    pub fn as_str(self) -> &'static str {
        match self {
            RailVeto::DailySpend => "daily_spend",
            RailVeto::ConcurrentExposure => "concurrent_exposure",
            RailVeto::RealizedLossKill => "realized_loss_kill",
            RailVeto::SelfFireRate => "self_fire_rate",
            RailVeto::FeedUnhealthy => "feed_unhealthy",
            RailVeto::PerOrderCommit => "per_order_commit",
        }
    }
}

/// `check_fire`'s `Err`: the tripped veto plus the value/threshold the rail
/// itself compared (Task 13 review D). Per-variant contents:
/// - `PerOrderCommit` → `value = commit_usd`, `threshold = per_order_cap_usd`.
/// - `ConcurrentExposure` → `value = concurrent_usd + commit_usd` (what this
///   module actually compares), `threshold = concurrent_cap_usd`.
/// - `DailySpend` → `value = rolling-24h spend + commit`, `threshold =
///   daily_spend_cap_usd`.
/// - `SelfFireRate` → `value = sends in the trailing 60s window`, `threshold
///   = self_fire_rate_per_min`.
/// - `RealizedLossKill` → `value = today's accumulated realized PnL`,
///   `threshold = -realized_loss_kill_usd`.
/// - `FeedUnhealthy` → both `f64::NAN`, legitimately: nothing numeric was
///   compared.
///
/// For the two LATCHED vetoes (`SelfFireRate`, `RealizedLossKill`) `value`
/// is the snapshot taken at the moment the latch tripped, not a live
/// re-read (Task 13 re-review Minor 3) — see `Rails::trip_latch`'s doc for
/// why a live re-read is actively misleading here.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RailTripInfo {
    pub veto: RailVeto,
    pub value: f64,
    pub threshold: f64,
}

/// Running realized-PnL accumulator for the current UTC calendar day, plus
/// the day index it belongs to. A single running sum reset at UTC midnight,
/// rather than a `HashMap<i64, f64>` keyed by day — v0 only ever needs
/// "today's" total, so a map that grows one entry per day of process uptime
/// buys nothing a scalar-plus-day-index doesn't already give.
struct RealizedDayState {
    day: i64,
    pnl_usd: f64,
}

pub struct Rails {
    daily: Mutex<SpendLedger>,
    concurrent_cap_usd: f64,
    concurrent_usd: Mutex<f64>,
    per_order_cap_usd: f64,
    realized_loss_kill_usd: f64,
    realized: Mutex<RealizedDayState>,
    self_fire_rate_per_min: u32,
    sends: Mutex<VecDeque<u64>>,
    feed_healthy: AtomicBool,
    /// The full trip snapshot (Task 13 re-review Minor 3), not just the
    /// veto — see `trip_latch`'s doc.
    halted: Mutex<Option<RailTripInfo>>,
}

impl Rails {
    pub fn new(cfg: &StrategySection) -> Self {
        Self {
            daily: Mutex::new(SpendLedger::new(cfg.daily_spend_cap_usd)),
            concurrent_cap_usd: cfg.concurrent_exposure_cap_usd,
            concurrent_usd: Mutex::new(0.0),
            // design §5.0 REV 2: its OWN config field, no longer
            // `concurrent/2`. The `/2` "can't starve the slot" rationale is
            // deliberately abandoned — the deep-walk cheap orders are the
            // LARGEST commits (p99 $52, max $89 under the 3x bound) AND the
            // best-performing cell, so a cap/2 bound ($30 here) vetoed 17% of
            // orders carrying 61% of PnL. Concurrent overlap is rare (measured
            // p50 $5.26), so a single large order starving the slot is not the
            // binding risk; censoring the deep walk is.
            per_order_cap_usd: cfg.per_order_cap_usd,
            realized_loss_kill_usd: cfg.realized_loss_kill_usd,
            realized: Mutex::new(RealizedDayState { day: 0, pnl_usd: 0.0 }),
            self_fire_rate_per_min: cfg.self_fire_rate_per_min,
            sends: Mutex::new(VecDeque::new()),
            feed_healthy: AtomicBool::new(true),
            halted: Mutex::new(None),
        }
    }

    /// Vetoes a fire, or clears it to proceed. Checks a tripped latch first
    /// (sticky, overrides everything below), then the four governors in
    /// cheapest-and-most-diagnostic-first order: per-order shape bound
    /// (pure arithmetic, no lock), daily spend, concurrent exposure, feed
    /// health. This is NOT the design §5 table's order — there, per-order
    /// commit is row 5, after realized loss and self-fire-rate (which are
    /// latches checked above, via `halted()`, not governors in this list).
    ///
    /// **Check-then-act invariant (not enforced by locking):** this
    /// function reads `daily`'s ledger and `concurrent_usd`, then drops
    /// both locks, before `record_send` re-acquires them to book the fire
    /// — the same shape as `can_spend` (see `SpendLedger`) followed later
    /// by `record`. For a given `Rails`, at most one fire may sit between
    /// `check_fire` and its matching `record_send` at any instant: the
    /// fire path must run on a single task with no `.await` between the
    /// check and the booking, and must never `tokio::spawn` / `join_all`
    /// fan-out over concurrent slots. Violate this and each interleaved
    /// caller admits up to `per_order_cap_usd` past a stale read, so exposure
    /// can reach `concurrent_cap + (N-1)*per_order_cap` — with N concurrent
    /// drivers and the shipped defaults that is $150 + (N-1)*$100 against a
    /// $150 rail, with daily spend degrading the same way — and no governor
    /// raises an alarm, because nothing here ever observes the concurrent
    /// send. One shot per slot bounds N at the number of slots driven at
    /// once, which the runtime holds at ONE. This
    /// module is safe today only because the design mandates a single
    /// driver (`slot_driver.rs` awaits `post_buy` sequentially in one
    /// task) — do
    /// not add concurrency here without re-deriving this invariant. It is
    /// a tempting "optimization" precisely because this strategy fires on
    /// the first second a qualifying ask exists, so a fan-out across cards
    /// looks like free latency — which is exactly why the constraint has
    /// to be written down rather than left implicit.
    ///
    /// **Task 13 obligation:** this function cannot emit `RailTrip` itself —
    /// it never sees `t`, `side`, `cards`, or `slot_open`. The caller MUST
    /// emit `TelemetryEvent::RailTrip { slot_open, rail: veto.as_str().into(),
    /// value, threshold, t, side, cards, commit_usd }` on every `Err`
    /// returned here (schema_v 2 requires `slot_open`), using THIS error's
    /// `value`/`threshold` — not values reconstructed independently. That
    /// row is the sole input to the promotion gate's rail-suppression-rate
    /// check (< 0.1% of fires, design §5.0) — a rail that binds silently is
    /// a strategy change nobody approved.
    pub fn check_fire(&self, commit_usd: f64, now_ms: u64) -> Result<(), RailTripInfo> {
        // Task 13 re-review Minor 3: return the snapshot taken AT TRIP TIME
        // verbatim — do NOT recompute `value` from current state here. A
        // live re-read is wrong for a sticky latch: `SelfFireRate`'s
        // trailing-60s window rolls over long before the halt does (the
        // latch never clears), so a live re-read would report 0 sends long
        // after the trip; `RealizedLossKill`'s live `pnl_usd` resets on the
        // next UTC day rollover while the halt persists. Both would make
        // the promotion-gate's `RailTrip` row lie about what actually
        // tripped it.
        if let Some(trip) = *self.halted.lock().expect("halted lock") {
            return Err(trip);
        }
        if commit_usd > self.per_order_cap_usd {
            return Err(RailTripInfo {
                veto: RailVeto::PerOrderCommit,
                value: commit_usd,
                threshold: self.per_order_cap_usd,
            });
        }
        {
            let mut daily = self.daily.lock().expect("daily ledger lock");
            if !daily.can_spend(commit_usd, now_ms * 1_000) {
                return Err(RailTripInfo {
                    veto: RailVeto::DailySpend,
                    value: daily.total_24h(now_ms * 1_000) + commit_usd,
                    threshold: daily.cap_usd(),
                });
            }
        }
        let concurrent = *self.concurrent_usd.lock().expect("concurrent lock");
        if concurrent + commit_usd > self.concurrent_cap_usd {
            return Err(RailTripInfo {
                veto: RailVeto::ConcurrentExposure,
                value: concurrent + commit_usd,
                threshold: self.concurrent_cap_usd,
            });
        }
        if !self.feed_healthy.load(Ordering::Relaxed) {
            return Err(RailTripInfo { veto: RailVeto::FeedUnhealthy, value: f64::NAN, threshold: f64::NAN });
        }
        Ok(())
    }

    /// Books a sent order against the daily spend ledger, the concurrent
    /// exposure budget, and the self-fire-rate window. `commit_usd` is always
    /// `size_shares * limit_px` — the max possible spend — never
    /// `walk_spend_usd`; matches `check_fire`'s `ConcurrentExposure` rail.
    pub fn record_send(&self, commit_usd: f64, now_ms: u64) {
        self.daily.lock().expect("daily ledger lock").record(commit_usd, now_ms * 1_000);
        *self.concurrent_usd.lock().expect("concurrent lock") += commit_usd;

        let cutoff = now_ms.saturating_sub(SELF_FIRE_WINDOW_MS);
        let send_count = {
            let mut sends = self.sends.lock().expect("sends lock");
            sends.push_back(now_ms);
            while let Some(&front) = sends.front() {
                if front < cutoff {
                    sends.pop_front();
                } else {
                    break;
                }
            }
            sends.len()
        };
        if send_count >= self.self_fire_rate_per_min as usize {
            // Task 13 re-review Minor 3: snapshot `send_count` NOW, while it
            // is still the trailing-60s count that actually tripped the
            // latch — by the time anything reads `halted()` the window may
            // have long since rolled over to 0.
            self.trip_latch(RailTripInfo {
                veto: RailVeto::SelfFireRate,
                value: send_count as f64,
                threshold: self.self_fire_rate_per_min as f64,
            });
        }
    }

    /// Step 2 of the three-step concurrent-exposure lifecycle (design
    /// §5.1a):
    /// 1. `record_send(commit_usd)` books the max-possible spend
    ///    (`size_shares * limit_px`) against `concurrent_usd`.
    /// 2. `reconcile(committed_usd, actual_usd)` — THIS function — releases
    ///    the overshoot (`committed_usd - actual_usd`) once the fill or
    ///    zero-fill is known, collapsing the commit down to what actually
    ///    happened. Design §5.1a: commits run ~2x actual spend until this
    ///    sub-second FAK reconciliation lands.
    /// 3. **Settlement releases the remainder.** `reconcile` does NOT
    ///    release the position's exposure — only the commit-vs-actual
    ///    overshoot. `actual_usd` stays booked against `concurrent_usd`
    ///    until the position resolves, because design §6 holds every
    ///    position to resolution (no exits, no stop-loss) — this rail
    ///    tracks actual money in unresolved positions, not merely
    ///    in-flight orders.
    ///
    /// **Task 12/13 obligation:** the caller MUST call
    /// `release_exposure(actual_usd)` on settlement — that is the ONLY
    /// release of step 3, and nothing in this module calls it. Omit it and
    /// `concurrent_usd` ratchets monotonically to the cap and vetoes every
    /// subsequent fire, forever, with no latch and no alarm — precisely
    /// the "rail binds silently" failure design §5.0 exists to prevent.
    /// Because the deep-walk population produces the largest commits, this
    /// would censor selectively from the best-performing cell.
    ///
    /// **Backstop:** hold-to-resolution (design §6) guarantees every
    /// position *resolves* — it does NOT guarantee `settle.rs` ever
    /// *observes* that resolution (a wedged redeem poll, a fill the
    /// user-WS never reports). "Settlement calls `release_exposure`" is
    /// therefore not sufficient on its own: every booked `actual_usd` MUST
    /// be released by settlement OR by a slot-close/expiry sweep,
    /// whichever comes first, so an unobserved settlement can never hold
    /// exposure indefinitely. `actual_usd` here means the same value
    /// passed into THIS call — the actual fill spend — never the
    /// settlement proceeds; for a winning position proceeds are `shares *
    /// $1`, and releasing proceeds instead of cost would corrupt the
    /// budget. A zero-fill (`actual_usd = 0`) needs no separate step-3
    /// call: step 2 above already released the full commit, so step 3 is
    /// correctly a no-op there, not a double-release to "fix".
    pub fn reconcile(&self, committed_usd: f64, actual_usd: f64) {
        self.release_exposure(committed_usd - actual_usd);
    }

    /// Only source for the `RealizedLossKill` latch. Never fed by spend — the
    /// earlier maker bug (that kill tripped on gross spend and was inert).
    /// Wins offset losses within the same UTC day.
    ///
    /// `now_ms` MUST be observation time (when this call happens), not
    /// slot-close time — settlement observation can lag (data-api REDEEM
    /// rows lag hours) and settles for adjacent slots can be observed out
    /// of order around UTC midnight. The day rollover is therefore
    /// FORWARD-ONLY: the accumulator resets only when `now_ms`'s day index
    /// is strictly greater than the last-seen day. A settlement carrying
    /// an earlier day index adds to the current day's total instead of
    /// wiping it — resetting on a backwards day change would under-count
    /// the newer day's losses and let the kill fail to trip, the same
    /// "kill is inert" class this rail exists to fix.
    pub fn record_settled_pnl(&self, pnl_usd: f64, now_ms: u64) {
        let day = (now_ms / MS_PER_DAY) as i64;
        let (tripped, trip_pnl) = {
            let mut state = self.realized.lock().expect("realized lock");
            if day > state.day {
                state.day = day;
                state.pnl_usd = 0.0;
            }
            state.pnl_usd += pnl_usd;
            // Positive magnitude threshold; trips when cumulative loss
            // REACHES -kill, not merely exceeds it.
            (state.pnl_usd <= -self.realized_loss_kill_usd, state.pnl_usd)
        };
        if tripped {
            // Task 13 re-review Minor 3: snapshot today's PnL NOW — a later
            // UTC day rollover resets the LIVE `pnl_usd` accumulator while
            // this halt stays latched, so a live re-read would eventually
            // report a value that doesn't breach the threshold at all.
            self.trip_latch(RailTripInfo {
                veto: RailVeto::RealizedLossKill,
                value: trip_pnl,
                threshold: -self.realized_loss_kill_usd,
            });
        }
    }

    pub fn release_exposure(&self, usd: f64) {
        let mut c = self.concurrent_usd.lock().expect("concurrent lock");
        // `.max(0.0)` is a floor against a caller bug (e.g. a double
        // release), not a normal path — correct accounting never drives
        // this negative, so a double-release is indistinguishable from
        // correct behavior here rather than surfaced as an error.
        *c = (*c - usd).max(0.0);
    }

    pub fn set_feed_healthy(&self, healthy: bool) {
        self.feed_healthy.store(healthy, Ordering::Relaxed);
    }

    /// `Some` once a latch (`RealizedLossKill` or `SelfFireRate`) has
    /// tripped. Sticky — never clears itself. First latch to trip wins
    /// permanently; a second, different latch tripping later does not
    /// overwrite it (a second trip implies a caller kept firing after
    /// ignoring the first veto — itself the fault class `SelfFireRate`
    /// exists to catch — and the operator is paged either way).
    pub fn halted(&self) -> Option<RailVeto> {
        self.halted.lock().expect("halted lock").map(|trip| trip.veto)
    }

    /// Latches `trip` — sticky, first-wins (see `halted`'s doc). Stores the
    /// FULL `RailTripInfo`, not just the veto (Task 13 re-review Minor 3):
    /// the caller passes in the `value`/`threshold` it observed at the
    /// exact moment the latch condition became true, and `check_fire`
    /// replays that snapshot verbatim on every later call rather than
    /// recomputing a live value that may no longer reflect why this
    /// latched at all (a rolled-over self-fire window, a day-rollover-reset
    /// realized PnL). That is the whole point of a STICKY veto reporting
    /// what actually tripped it.
    fn trip_latch(&self, trip: RailTripInfo) {
        let mut h = self.halted.lock().expect("halted lock");
        if h.is_none() {
            *h = Some(trip);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StrategySection;

    fn rails() -> Rails {
        Rails::new(&StrategySection::default())
    }

    /// For tests that isolate ONE rail by hammering `record_send` far faster
    /// than any real slot could (e.g. 50 sends in 50ms, to walk the daily
    /// cap up to its boundary): at the default `self_fire_rate_per_min`
    /// (20), that call pattern trips the self-fire-rate latch as an
    /// unrelated side effect, since 20+ sends land in its 60s window well
    /// before the loop finishes. Raising the rate ceiling out of the way
    /// keeps the test measuring the rail it names.
    fn rails_with_self_fire_rate(per_min: u32) -> Rails {
        Rails::new(&StrategySection { self_fire_rate_per_min: per_min, ..StrategySection::default() })
    }

    /// Task 13 review D: `check_fire` now returns `RailTripInfo`, not a bare
    /// `RailVeto`. Kept for tests that only care about which rail tripped
    /// (the `value`/`threshold` shape is exercised directly where a test's
    /// whole point is that shape).
    fn assert_veto(result: Result<(), RailTripInfo>, expected: RailVeto) {
        assert_eq!(result.unwrap_err().veto, expected);
    }

    #[test]
    fn daily_spend_cap_vetoes_once_exhausted() {
        // Explicit $250 cap (not the REV 2 $1200 default) so the boundary is
        // reachable in 50 x $5 sends and this test measures ONLY the spend cap.
        let r = Rails::new(&StrategySection {
            daily_spend_cap_usd: 250.0,
            self_fire_rate_per_min: 1_000,
            ..StrategySection::default()
        });
        for i in 0..50 {
            assert!(r.check_fire(5.0, 1000 + i).is_ok(), "fire {i} vetoed early");
            r.record_send(5.0, 1000 + i);
            r.release_exposure(5.0);
        }
        let trip = r.check_fire(5.0, 2000).unwrap_err();
        assert_eq!(trip.veto, RailVeto::DailySpend);
        // 50 sends x $5 = $250 rolling spend + this $5 commit = $255.
        assert!((trip.value - 255.0).abs() < 1e-9, "value = rolling spend + commit, got {}", trip.value);
        assert_eq!(trip.threshold, 250.0);
    }

    #[test]
    fn concurrent_exposure_commits_the_max_possible_spend() {
        // Explicit $60 cap (not the REV 2 $150 default) => 12 open $5 commits.
        let r = Rails::new(&StrategySection {
            concurrent_exposure_cap_usd: 60.0,
            ..StrategySection::default()
        });
        for i in 0..12 {
            assert!(r.check_fire(5.0, 1000 + i).is_ok());
            r.record_send(5.0, 1000 + i);
        }
        let trip = r.check_fire(5.0, 2000).unwrap_err();
        assert_eq!(trip.veto, RailVeto::ConcurrentExposure);
        assert!((trip.value - 65.0).abs() < 1e-9, "value = concurrent_usd + commit_usd, got {}", trip.value);
        assert_eq!(trip.threshold, 60.0);
        r.release_exposure(5.0);
        assert!(r.check_fire(5.0, 2000).is_ok());
    }

    /// Explicit -$150 loss kill (not the REV 2 -$400 default) so the boundary
    /// is reachable with small settlements and the test measures the kill.
    fn rails_with_loss_kill(kill_usd: f64) -> Rails {
        Rails::new(&StrategySection {
            realized_loss_kill_usd: kill_usd,
            self_fire_rate_per_min: 1_000,
            ..StrategySection::default()
        })
    }

    #[test]
    fn realized_loss_kill_reads_settled_pnl_only_and_is_a_halt() {
        let r = rails_with_loss_kill(150.0);
        // Spending money is NOT loss: the maker-lineage bug was tripping on
        // gross spend. 40 sends x $5 = $200 spent, zero settled: no halt.
        for i in 0..40 {
            r.record_send(5.0, 1000 + i);
            r.release_exposure(5.0);
        }
        assert!(r.halted().is_none(), "spend must never trip the LOSS kill");
        r.record_settled_pnl(-149.0, 5000);
        assert!(r.halted().is_none());
        r.record_settled_pnl(-2.0, 5001);
        assert_eq!(r.halted(), Some(RailVeto::RealizedLossKill));
        let trip = r.check_fire(5.0, 6000).unwrap_err();
        assert_eq!(trip.veto, RailVeto::RealizedLossKill);
        assert!((trip.value - (-151.0)).abs() < 1e-9, "value = today's realized PnL, got {}", trip.value);
        assert_eq!(trip.threshold, -150.0, "threshold = -realized_loss_kill_usd");
    }

    #[test]
    fn realized_wins_offset_losses() {
        let r = rails_with_loss_kill(150.0);
        r.record_settled_pnl(-100.0, 1000);
        r.record_settled_pnl(60.0, 1001);
        r.record_settled_pnl(-100.0, 1002);
        assert!(r.halted().is_none(), "net -140 is inside the -150 kill");
    }

    /// Rewritten per operator resolution 2026-07-22: the brief's original
    /// test asserted the halt cleared after a minute ("the window has
    /// rolled"), contradicting design §5's "halt + alarm" and its own prose
    /// ("a halt is sticky"). SelfFireRate is a LATCH: once tripped it stays
    /// tripped for the life of the process. See the module doc for why.
    #[test]
    fn self_fire_rate_kill_trips_on_our_own_sends_and_latches() {
        let r = rails();
        for i in 0..20 {
            r.record_send(0.0, 60_000 + i);
        }
        let trip = r.check_fire(5.0, 60_100).unwrap_err();
        assert_eq!(trip.veto, RailVeto::SelfFireRate);
        assert_eq!(trip.value, 20.0, "value = sends in the trailing 60s window");
        assert_eq!(trip.threshold, 20.0, "threshold = self_fire_rate_per_min (default 20)");
        assert_eq!(r.halted(), Some(RailVeto::SelfFireRate));
        // A minute later the 60s send-count window has rolled, but the latch
        // has NOT: still vetoed, forever, until the process restarts.
        assert_veto(r.check_fire(5.0, 121_000), RailVeto::SelfFireRate);
    }

    /// Task 13 re-review Minor 3: a latched `RailTrip` row must report the
    /// value AS OF THE TRIP, not a live re-read. Without the fix, this probe
    /// at t=121_000 (well past the 60s self-fire window that tripped the
    /// latch at t=60_100-ish) would see `sends_in_window` recompute to 0 —
    /// exactly the misleading `value: 0.0, threshold: 20.0` row the finding
    /// describes.
    #[test]
    fn self_fire_rate_latched_trip_reports_the_trip_time_value_not_a_live_reread() {
        let r = rails();
        for i in 0..20 {
            r.record_send(0.0, 60_000 + i);
        }
        assert_eq!(r.halted(), Some(RailVeto::SelfFireRate));
        let trip = r.check_fire(5.0, 121_000).unwrap_err();
        assert_eq!(trip.veto, RailVeto::SelfFireRate);
        assert_eq!(
            trip.value, 20.0,
            "value must stay the trip-time send count; a live re-read would see 0 sends \
             in the now-rolled-over 60s window"
        );
        assert_eq!(trip.threshold, 20.0);
    }

    /// Task 13 re-review Minor 3, the `RealizedLossKill` half: a forward day
    /// rollover resets the LIVE `pnl_usd` accumulator (by design — see
    /// `record_settled_pnl`'s doc) while the halt itself stays latched. A
    /// `RailTrip` row read after that rollover must still report the PnL
    /// that actually tripped the kill, not the post-rollover accumulator
    /// (which wouldn't even breach the threshold on its own).
    #[test]
    fn realized_loss_kill_latched_trip_survives_a_day_rollover_reset() {
        let r = rails_with_loss_kill(150.0);
        r.record_settled_pnl(-150.0, 1000); // day 0: trips at exactly -150.
        assert_eq!(r.halted(), Some(RailVeto::RealizedLossKill));
        // Forward day rollover: resets the live accumulator to 0, then adds
        // a loss far too small to trip the kill on its own.
        r.record_settled_pnl(-1.0, 1000 + MS_PER_DAY);
        assert!(
            r.halted() == Some(RailVeto::RealizedLossKill),
            "the day rollover must not clear an already-latched kill"
        );
        let trip = r.check_fire(5.0, 2000 + MS_PER_DAY).unwrap_err();
        assert_eq!(trip.veto, RailVeto::RealizedLossKill);
        assert_eq!(
            trip.value, -150.0,
            "value must stay the trip-time PnL; a live re-read would see the \
             post-rollover accumulator (-1.0), which doesn't even breach the threshold"
        );
        assert_eq!(trip.threshold, -150.0);
    }

    /// Pins the rail's calibration to the decision core's structural bound:
    /// ONE order per 300s slot (POST-TWAP-SPEC §3.6), so a 60s window can
    /// never hold more than one send. Many consecutive slots at that maximum
    /// must never trip the latch — without this test, nothing catches a future
    /// threshold change that drops the limit below what normal operation
    /// produces. The old 6-per-slot bound is kept as a second, looser
    /// probe: the shipped threshold must still clear the bound it was
    /// originally SET with, or lowering it later would look safe here while
    /// being a real change.
    #[test]
    fn self_fire_rate_never_trips_at_the_decision_cores_structural_max() {
        for sends_per_slot in [1, 6] {
            let r = rails();
            let mut t = 0u64;
            for _slot in 0..200 {
                for _send in 0..sends_per_slot {
                    r.record_send(0.0, t);
                    t += 1;
                }
                assert!(
                    r.halted().is_none(),
                    "{sends_per_slot} sends/slot must never latch"
                );
                t += 300_000 - sends_per_slot; // advance to the next 5-minute slot
            }
            assert!(r.check_fire(5.0, t).is_ok());
        }
    }

    #[test]
    fn feed_unhealthy_vetoes_and_recovers() {
        let r = rails();
        r.set_feed_healthy(false);
        let trip = r.check_fire(5.0, 1000).unwrap_err();
        assert_eq!(trip.veto, RailVeto::FeedUnhealthy);
        assert!(trip.value.is_nan(), "FeedUnhealthy compares no numeric quantity");
        assert!(trip.threshold.is_nan());
        r.set_feed_healthy(true);
        assert!(r.check_fire(5.0, 1000).is_ok());
    }

    /// Design §5.0 REV 2: `per_order_cap_usd` is its OWN config field, no
    /// longer `concurrent/2`. It bounds one fire's committed exposure
    /// (`shares * fak_limit_px`). Default $100 sits above the measured max
    /// single-order commit of $89 under the 3x slippage bound, so it should
    /// not bind in v0 — but a fire past it is vetoed with its own value.
    #[test]
    fn per_order_commit_bound_is_its_own_config_field() {
        // Concurrent opened wide ($1000) so the PER-ORDER rail is what trips,
        // proving the two are decoupled (a $100.01 commit is far under $1000).
        let r = Rails::new(&StrategySection {
            per_order_cap_usd: 100.0,
            concurrent_exposure_cap_usd: 1_000.0,
            ..StrategySection::default()
        });
        assert!(r.check_fire(100.0, 1000).is_ok(), "exactly at the bound must clear");
        let trip = r.check_fire(100.01, 1000).unwrap_err();
        assert_eq!(trip.veto, RailVeto::PerOrderCommit);
        assert_eq!(trip.value, 100.01);
        assert_eq!(trip.threshold, 100.0);
    }

    /// Pins the ms->micros conversion at every `SpendLedger` call site.
    /// `SpendLedger` takes MICROSECONDS; `Rails`'s public API is
    /// milliseconds. Passing `now_ms` straight through (the bug) would widen
    /// the rolling 24h window to ~1000 days — every other test here still
    /// passes under that bug, because none of their timestamps span 24h of
    /// real time in either unit. This one does.
    ///
    /// Uses a dedicated small `daily_spend_cap_usd` (with the concurrent and
    /// self-fire-rate rails opened wide) rather than the $60/$250 defaults,
    /// so the boundary commit stays under the per-order bound
    /// (`concurrent_exposure_cap_usd / 2`) and this test measures only the
    /// spend-cap rollover.
    #[test]
    fn daily_spend_ledger_converts_ms_to_micros_for_the_rolling_window() {
        let cfg = StrategySection {
            daily_spend_cap_usd: 10.0,
            concurrent_exposure_cap_usd: 1_000.0,
            self_fire_rate_per_min: 1_000,
            ..StrategySection::default()
        };
        let r = Rails::new(&cfg);
        r.record_send(5.0, 1_000);
        let day_ms = 24 * 3_600 * 1_000;
        // Just before 24h real time has elapsed: the $5 entry is still
        // inside the rolling window, so only $5 of headroom remains.
        assert_veto(r.check_fire(6.0, 1_000 + day_ms), RailVeto::DailySpend);
        // Just after 24h real time: the $5 entry has rolled off, and the
        // same $6 fire clears. Under the ms-straight-through bug the window
        // would be ~1000x too wide and this would still be vetoed.
        assert!(r.check_fire(6.0, 1_000 + day_ms + 1).is_ok());
    }

    /// Pins `reconcile`'s lifecycle contract (design §5.1a, step 2 of 3):
    /// it releases ONLY `committed_usd - actual_usd`, the overshoot between
    /// the max-possible commit and what actually happened — `actual_usd`
    /// itself stays booked until settlement calls `release_exposure`
    /// separately (step 3, a Task 12/13 obligation, not exercised here).
    /// Must fail if `reconcile`'s arguments are swapped (releasing
    /// `actual_usd` instead) or if the subtraction is dropped.
    #[test]
    fn reconcile_releases_only_the_commit_minus_actual_overshoot() {
        let cfg = StrategySection { concurrent_exposure_cap_usd: 20.0, ..StrategySection::default() };
        let r = Rails::new(&cfg);
        // per_order_cap_usd is its own field (default $100, REV 2) and never
        // binds here — every commit below is under it, so this test isolates
        // the concurrent-exposure reconcile lifecycle.
        r.record_send(10.0, 1000);
        // Only $3 actually spent: reconcile must release the $7 overshoot
        // and leave $3 booked against concurrent_usd.
        r.reconcile(10.0, 3.0);
        r.record_send(9.0, 1001);
        // concurrent_usd is now 3.0 + 9.0 = 12.0; cap is 20.0, so headroom
        // is exactly 8.0. Probe both sides of that boundary.
        assert!(r.check_fire(8.0, 1002).is_ok(), "at exactly the boundary (cap - committed) must clear");
        assert_veto(r.check_fire(8.01, 1002), RailVeto::ConcurrentExposure);
    }

    /// No current test exercises a day change at all (every other test sits
    /// in day 0), so deleting the day-rollover reset entirely still passes
    /// the suite without this. Forward day change (day 0 -> day 1) must
    /// reset the accumulator: -60 alone must not trip, even though
    /// -100 + -60 = -160 would.
    #[test]
    fn record_settled_pnl_forward_day_change_resets_the_accumulator() {
        let r = rails();
        r.record_settled_pnl(-100.0, 0);
        assert!(r.halted().is_none());
        r.record_settled_pnl(-60.0, MS_PER_DAY);
        assert!(r.halted().is_none(), "forward day change must reset the prior day's accumulated loss");
    }

    /// An out-of-order settlement stamped with an EARLIER day index than
    /// the last-seen day (settlement observation lags — data-api REDEEM
    /// rows lag hours — and settles can arrive out of order around UTC
    /// midnight) must NOT reset the current day's accumulator back to
    /// zero. Only a forward day change resets; this pins the `day >
    /// state.day` condition against a regression to `day != state.day`.
    #[test]
    fn record_settled_pnl_backward_day_index_does_not_reset_the_accumulator() {
        let r = rails_with_loss_kill(150.0); // explicit -150 so -155 trips
        r.record_settled_pnl(-100.0, 0); // day 0: -100
        r.record_settled_pnl(-60.0, MS_PER_DAY); // day 1: resets, then -60
        assert!(r.halted().is_none());
        r.record_settled_pnl(-95.0, 0); // backwards day index: day 0
        assert_eq!(
            r.halted(),
            Some(RailVeto::RealizedLossKill),
            "backward day index must not wipe day 1's accumulated -60.0 (-60 + -95 = -155 <= -150)"
        );
    }

    #[test]
    fn realized_loss_kill_trips_exactly_at_the_boundary() {
        // Explicit -$150 kill; the boundary must trip on <=, not strict <, so
        // a loss reaching EXACTLY -150.0 halts.
        let r = rails_with_loss_kill(150.0);
        r.record_settled_pnl(-150.0, 1000);
        assert_eq!(r.halted(), Some(RailVeto::RealizedLossKill));
    }

    #[test]
    fn rail_veto_as_str_maps_each_variant() {
        assert_eq!(RailVeto::DailySpend.as_str(), "daily_spend");
        assert_eq!(RailVeto::ConcurrentExposure.as_str(), "concurrent_exposure");
        assert_eq!(RailVeto::RealizedLossKill.as_str(), "realized_loss_kill");
        assert_eq!(RailVeto::SelfFireRate.as_str(), "self_fire_rate");
        assert_eq!(RailVeto::FeedUnhealthy.as_str(), "feed_unhealthy");
        assert_eq!(RailVeto::PerOrderCommit.as_str(), "per_order_commit");
    }

    /// Pins the first-latch-wins-permanently semantics: `RealizedLossKill`
    /// trips first here, then enough sends to cross `SelfFireRate`'s
    /// threshold would also trip it — but `halted()` must still report the
    /// FIRST latch, not the second.
    ///
    /// Uses `rails_with_self_fire_rate(5)` — not the `rails()` default —
    /// with a send count (20) that provably exceeds that threshold BY
    /// CONSTRUCTION, so this test doesn't silently start asserting the
    /// wrong thing if `StrategySection::default().self_fire_rate_per_min`
    /// (currently 20) is ever raised. Before checking which latch wins, a
    /// control run on a fresh, unlatched `Rails` proves the send pattern
    /// really does trip `SelfFireRate` on its own — without that, "first
    /// latch wins over a second trip" would be unfalsifiable: a config
    /// drift that stopped the second trip from happening at all would
    /// still leave `RealizedLossKill` as the (only) latch and this test
    /// would keep passing for the wrong reason, the failure mode
    /// `self_fire_rate_kill_trips_on_our_own_sends_and_latches` avoids by
    /// asserting `Err(SelfFireRate)` directly.
    #[test]
    fn halted_first_latch_wins_permanently() {
        let per_min = 5;
        let sends = 20; // >> per_min, not merely >=, so the margin survives tuning.

        let control = rails_with_self_fire_rate(per_min);
        for i in 0..sends {
            control.record_send(0.0, 60_000 + i);
        }
        assert_eq!(
            control.halted(),
            Some(RailVeto::SelfFireRate),
            "test setup bug: this send pattern must trip SelfFireRate on an unlatched Rails"
        );

        // `r` needs BOTH an explicit -150 loss kill (so -150 trips it) AND
        // self_fire = per_min (so the later sends would also trip that latch).
        let r = Rails::new(&StrategySection {
            self_fire_rate_per_min: per_min,
            realized_loss_kill_usd: 150.0,
            ..StrategySection::default()
        });
        r.record_settled_pnl(-150.0, 1000);
        assert_eq!(r.halted(), Some(RailVeto::RealizedLossKill));
        for i in 0..sends {
            r.record_send(0.0, 60_000 + i); // would also trip SelfFireRate
        }
        assert_eq!(
            r.halted(),
            Some(RailVeto::RealizedLossKill),
            "first latch wins permanently; a later different latch must not overwrite it"
        );
    }
}
