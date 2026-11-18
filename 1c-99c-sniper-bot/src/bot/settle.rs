//! Settlement is the ONLY source of realized PnL in this crate.
//!
//! Truth is the CLOB: `GET /markets/<conditionId>` -> `tokens[].winner`.
//! Explicitly NOT used, and none of these may ever be wired to the loss kill:
//!   * data-api REDEEM rows — lag HOURS, producing phantom losses;
//!   * /activity — misses REDEEM rows entirely;
//!   * kline sign — INVALID for this lane; winners cluster at near-ties,
//!     where Binance and Chainlink disagree 27% of the time within $4.
//!
//! Positions are held to resolution — no exits, no stop-loss, no stop-win.
//! Auto-redeem fires ~30s after close; this module's sole grace period is
//! `SETTLE_ELIGIBLE_AFTER_SECS` below (300s slot + 30s). The 120s
//! manual-claim-fallback grace some other crates keep does not apply here —
//! this lane has no manual-claim fallback of its own.
//!
//! **Exposure-release contract (rails.rs step 3 of 3, design §5.1a).** This
//! module owns the ONLY release of a position's `cost_usd` from
//! `Rails::concurrent_usd`. Every exit path — resolved-and-settled, or
//! bounded-retry give-up — MUST call `rails.release_exposure(pos.cost_usd)`
//! exactly once. Drop a position without releasing and `concurrent_usd`
//! ratchets monotonically to the cap and vetoes every subsequent fire,
//! forever, with no latch and no alarm (see `rails.rs::reconcile`'s doc for
//! why). `release_exposure` always takes `pos.cost_usd` — the actual fill
//! spend, the same value passed to `reconcile` — NEVER the settlement
//! proceeds (a winner's proceeds are `shares * $1`, a different number;
//! releasing proceeds instead of cost would corrupt the budget).
//!
//! On the give-up path specifically: `rails.record_settled_pnl` must NOT be
//! called. The outcome is unknown, and inventing a PnL would poison the
//! realized-loss kill.

use crate::risk::rails::Rails;
use crate::bot::telemetry::{TelemetryEvent, Writer};
use crate::types::{now_micros, Side};
use crate::polymarket::clob_http::{ClobHttpClient, MarketFetch};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Empirical taker fee constant (workspace-wide: NOT the venue-advertised
/// 0.072 — see `feedback_polymarket_fee_formula_007.md`).
const TAKER_FEE_C: f64 = 0.07;

const POLL_INTERVAL_SECS: u64 = 15;
/// btc-updown-5m slot duration (300s) + the 30s auto-redeem / resolution
/// grace the existing crates use before a position is even worth polling.
const SETTLE_ELIGIBLE_AFTER_SECS: u64 = 300 + 30;
/// ~10 min at the 15s poll interval (the design's bounded-retry backstop).
const MAX_POLL_ATTEMPTS: u32 = 40;

#[derive(Clone, Debug)]
pub struct OpenPosition {
    pub slug: String,
    pub slot_open: u64,
    pub side: Side,
    pub condition_id: String,
    pub shares: f64,
    pub cost_usd: f64,
    pub fee_usd: f64,
}

struct Tracked {
    pos: OpenPosition,
    attempts: u32,
}

/// Generic over `MarketFetch` (`ClobHttpClient` in production, a stub in
/// tests) rather than a hardcoded base-URL string — `run()` performs the
/// exposure release, the single path whose failure silently disables all
/// trading, and that path must be exercisable without live HTTP.
///
/// All fields are `Arc`-wrapped internally, so `SettleLane` is cheap to
/// `Clone`: the production shape is one live clone consumed by `run()` in a
/// background task while the original (or further clones) keep calling
/// `track()` as new fires land.
pub struct SettleLane<F: MarketFetch> {
    fetch: Arc<F>,
    rails: Arc<Rails>,
    tele: Arc<Writer>,
    positions: Arc<Mutex<Vec<Tracked>>>,
}

// Manual impl: `#[derive(Clone)]` on a generic struct adds an `F: Clone`
// bound even though `F` only ever appears behind an `Arc` here — that bound
// isn't needed and would leak into every caller's generic parameter.
impl<F: MarketFetch> Clone for SettleLane<F> {
    fn clone(&self) -> Self {
        Self {
            fetch: self.fetch.clone(),
            rails: self.rails.clone(),
            tele: self.tele.clone(),
            positions: self.positions.clone(),
        }
    }
}

impl<F: MarketFetch> SettleLane<F> {
    pub fn new(fetch: F, rails: Arc<Rails>, tele: Writer) -> Self {
        Self {
            fetch: Arc::new(fetch),
            rails,
            tele: Arc::new(tele),
            positions: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Starts tracking a filled position for settlement.
    pub fn track(&self, pos: OpenPosition) {
        self.positions.lock().expect("settle positions lock").push(Tracked { pos, attempts: 0 });
    }

    /// Polls every `POLL_INTERVAL_SECS` until `shutdown` fires. See the
    /// module doc's exposure-release contract — every tracked position must
    /// exit via exactly one of `settle` or the give-up path in
    /// `requeue_or_give_up`, both of which release exposure.
    pub async fn run(self, shutdown: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_secs(POLL_INTERVAL_SECS));
        // Default `Burst` behavior would let a slow poll round (fetch_resolved
        // taking longer than POLL_INTERVAL_SECS) fire missed ticks back-to-back,
        // turning MAX_POLL_ATTEMPTS's documented "~10 min" give-up bound into
        // `40 * max(15s, actual_round_time)`. `Delay` keeps ticks spaced from
        // the end of the last poll, so the bound holds under real latency.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            self.poll_once().await;
        }
    }

    async fn poll_once(&self) {
        let now_secs = now_micros() / 1_000_000;
        let due: Vec<Tracked> = {
            let mut positions = self.positions.lock().expect("settle positions lock");
            let mut still_pending = Vec::with_capacity(positions.len());
            let mut due = Vec::new();
            for t in positions.drain(..) {
                if now_secs >= t.pos.slot_open + SETTLE_ELIGIBLE_AFTER_SECS {
                    due.push(t);
                } else {
                    still_pending.push(t);
                }
            }
            *positions = still_pending;
            due
        };

        for t in due {
            match self.fetch.fetch_resolved(&t.pos.condition_id).await {
                Ok(Some(winner)) => self.settle(&t.pos, winner),
                Ok(None) => self.requeue_or_give_up(t),
                Err(e) => {
                    tracing::warn!(
                        slug = %t.pos.slug, condition_id = %t.pos.condition_id, ?e,
                        "settle: fetch_resolved error; will retry"
                    );
                    self.requeue_or_give_up(t);
                }
            }
        }
    }

    /// The bounded-retry give-up backstop: releases exposure but MUST NOT
    /// record a PnL — see the module doc.
    fn requeue_or_give_up(&self, mut t: Tracked) {
        t.attempts += 1;
        if t.attempts >= MAX_POLL_ATTEMPTS {
            tracing::error!(
                slug = %t.pos.slug, condition_id = %t.pos.condition_id,
                attempts = t.attempts, cost_usd = t.pos.cost_usd,
                "settle: giving up after max_poll_attempts without a resolved winner \
                 — releasing exposure WITHOUT crediting PnL (RailTrip-style alarm)"
            );
            self.rails.release_exposure(t.pos.cost_usd);
        } else {
            self.positions.lock().expect("settle positions lock").push(t);
        }
    }

    fn settle(&self, pos: &OpenPosition, winner: Side) {
        let pnl = realized_pnl(pos, winner);
        // Observation time, in MS — never slot_open (SECONDS); see the
        // module doc and `rails.rs::record_settled_pnl`'s doc.
        self.rails.record_settled_pnl(pnl, now_ms());
        self.rails.release_exposure(pos.cost_usd);
        self.tele.emit(TelemetryEvent::Settle {
            slot_open: pos.slot_open,
            slug: pos.slug.clone(),
            side: side_str(pos.side),
            winner: side_str(winner),
            shares: pos.shares,
            cost_usd: pos.cost_usd,
            fee_usd: pos.fee_usd,
            pnl_usd: pnl,
        });
    }
}

impl SettleLane<ClobHttpClient> {
    /// Convenience constructor for production use: builds a `ClobHttpClient`
    /// from a base URL. `new` (generic over `MarketFetch`) is the primary,
    /// testable form — see the module doc.
    pub fn new_live(
        clob_base_url: impl Into<String>,
        rails: Arc<Rails>,
        tele: Writer,
    ) -> anyhow::Result<Self> {
        Ok(Self::new(ClobHttpClient::new(clob_base_url)?, rails, tele))
    }
}

fn side_str(side: Side) -> String {
    match side {
        Side::Up => "up",
        Side::Down => "down",
    }
    .to_string()
}

/// Observation-time in MILLISECONDS. `Rails::record_settled_pnl` takes ms
/// (it derives a UTC day index from it); `slot_open` throughout this crate
/// is epoch SECONDS. Passing `slot_open` (or any seconds value) straight
/// through here would compress every realistic settlement into the same day
/// index, since the ms-vs-seconds factor-of-1000 error dwarfs the minutes-to-
/// hours gap between slots — the daily loss accumulator would then never
/// roll over. This is the sole conversion site; `settle()` must call this,
/// never a raw `slot_open`.
fn now_ms() -> u64 {
    now_micros() / 1_000
}

pub fn taker_fee_usd(price: f64, shares: f64) -> f64 {
    shares * TAKER_FEE_C * price * (1.0 - price)
}

pub fn realized_pnl(pos: &OpenPosition, winner: Side) -> f64 {
    if winner == pos.side {
        pos.shares - pos.cost_usd - pos.fee_usd
    } else {
        -(pos.cost_usd + pos.fee_usd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StrategySection;
    use crate::risk::rails::RailVeto;
    use std::future::Future;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn fee_uses_the_empirical_c_of_007() {
        // fee = 0.07 * p * (1-p) per share. NOT 0.072.
        let f = taker_fee_usd(0.03, 100.0);
        assert!((f - 100.0 * 0.07 * 0.03 * 0.97).abs() < 1e-12, "got {f}");
        // Symmetric in p, maximal at 0.5.
        assert!(taker_fee_usd(0.5, 1.0) > taker_fee_usd(0.03, 1.0));
    }

    #[test]
    fn winning_position_pays_one_dollar_per_share_minus_cost_and_fee() {
        let p = OpenPosition {
            slug: "s".into(), slot_open: 1, side: Side::Up,
            condition_id: "c".into(), shares: 200.0, cost_usd: 5.0, fee_usd: 0.04,
        };
        assert!((realized_pnl(&p, Side::Up) - (200.0 - 5.0 - 0.04)).abs() < 1e-9);
    }

    #[test]
    fn losing_position_loses_exactly_cost_plus_fee() {
        let p = OpenPosition {
            slug: "s".into(), slot_open: 1, side: Side::Up,
            condition_id: "c".into(), shares: 200.0, cost_usd: 5.0, fee_usd: 0.04,
        };
        assert!((realized_pnl(&p, Side::Down) - (-5.04)).abs() < 1e-9);
    }

    /// Pins Resolution 3's unit trap at its source: `now_ms()` must return a
    /// MILLISECOND timestamp. A real epoch-seconds value today is ~1.8e9;
    /// the millisecond value must be ~1000x that (~1.8e12). If `now_ms()`
    /// ever regressed to returning `now_micros() / 1_000_000` (seconds), or
    /// a raw `slot_open`, this catches it directly rather than relying on
    /// the day-rollover symptom rails.rs describes.
    #[test]
    fn now_ms_returns_milliseconds_not_seconds() {
        let ms = now_ms();
        let plausible_epoch_seconds = 1_700_000_000u64;
        assert!(
            ms > plausible_epoch_seconds * 1000,
            "now_ms() = {ms} looks like seconds, not milliseconds (expected > {})",
            plausible_epoch_seconds * 1000
        );
    }

    fn rails_with(realized_loss_kill_usd: f64, concurrent_exposure_cap_usd: f64) -> Arc<Rails> {
        Arc::new(Rails::new(&StrategySection {
            realized_loss_kill_usd,
            concurrent_exposure_cap_usd,
            daily_spend_cap_usd: 1_000.0,
            self_fire_rate_per_min: 1_000,
            ..StrategySection::default()
        }))
    }

    /// Returns a live `Writer` plus the `TempDir` guard that must outlive it
    /// (the writer task holds an open file handle into that directory).
    fn writer() -> (Writer, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("telemetry.jsonl");
        (Writer::new(&path).unwrap(), dir)
    }

    fn pos(slot_open: u64, side: Side, cost_usd: f64) -> OpenPosition {
        OpenPosition {
            slug: format!("btc-updown-5m-{slot_open}"),
            slot_open,
            side,
            condition_id: "0xcond".into(),
            shares: 20.0,
            cost_usd,
            fee_usd: 0.0,
        }
    }

    /// Always-resolved stub: every `fetch_resolved` call returns the same
    /// configured winner immediately.
    struct AlwaysResolved {
        winner: Side,
        calls: Arc<AtomicU32>,
    }

    impl MarketFetch for AlwaysResolved {
        fn fetch_resolved(
            &self,
            _condition_id: &str,
        ) -> impl Future<Output = anyhow::Result<Option<Side>>> + Send {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let winner = self.winner;
            async move { Ok(Some(winner)) }
        }
    }

    /// Never-resolved stub: every `fetch_resolved` call returns `Ok(None)`
    /// (market not yet closed), forcing the bounded-retry give-up path.
    struct NeverResolved {
        calls: Arc<AtomicU32>,
    }

    impl MarketFetch for NeverResolved {
        fn fetch_resolved(
            &self,
            _condition_id: &str,
        ) -> impl Future<Output = anyhow::Result<Option<Side>>> + Send {
            self.calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(None) }
        }
    }

    /// A resolved WINNING position: `run()` must release exposure — and
    /// specifically `pos.cost_usd` ($10), never the settlement proceeds
    /// (`pos.shares` = $20, since `pos()` fixes `shares: 20.0`). A single
    /// `record_send` equal to `cost_usd` can't catch that swap:
    /// `release_exposure`'s `.max(0.0)` floor sends BOTH a correct $10
    /// release and a buggy $20 release to the same `concurrent_usd = 0.0`,
    /// so nothing downstream can tell them apart (this is what the prior
    /// version of this test missed). Booking an extra, untouched $100 of
    /// "other open position" exposure keeps `concurrent_usd` away from that
    /// floor after release, so the two outcomes land on different nonzero
    /// values ($100 vs $90) and become distinguishable via `check_fire`'s
    /// headroom.
    ///
    /// `slot_open = 1` is always eligible (real wall-clock is far past
    /// `1 + 330`), and `tokio::time::interval`'s first tick fires
    /// immediately, so the very first poll settles it — no time advance
    /// needed beyond letting the spawned task run.
    #[tokio::test(start_paused = true)]
    async fn resolved_winning_position_releases_exposure() {
        let rails = rails_with(150.0, 150.0); // per_order_cap_usd = 75.0
        let p = pos(1, Side::Up, 10.0); // shares = 20.0 (fixed by pos()), so proceeds = $20
        rails.record_send(10.0, 1_000); // this position's own commit
        rails.record_send(100.0, 1_001); // another still-open position, never released here
        assert_eq!(
            rails.check_fire(41.0, 2_000).map_err(|e| e.veto),
            Err(RailVeto::ConcurrentExposure),
            "test setup: $110 booked against a $150 cap leaves exactly $40 headroom"
        );

        let calls = Arc::new(AtomicU32::new(0));
        let fetch = AlwaysResolved { winner: Side::Up, calls: calls.clone() };
        let (tele, _dir) = writer();
        let lane = SettleLane::new(fetch, rails.clone(), tele);
        lane.track(p);

        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(lane.run(shutdown.clone()));
        // `sleep` (not raw `time::advance`) cooperates with paused-clock
        // auto-advance to actually drive the spawned task's timer through
        // its first tick before returning — a bare `advance()` + one
        // `yield_now()` races the spawned task's first poll.
        tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
        shutdown.cancel();
        handle.await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1, "must have polled exactly once");
        // Correct release (cost_usd = $10): concurrent_usd = 100.0 -> $50 headroom.
        assert!(
            rails.check_fire(50.0, 3_000).is_ok(),
            "an amount fitting the correctly-restored $50 headroom (cap $150 - other $100 \
             - this position released to $0) must clear"
        );
        // A buggy release of proceeds ($20) would land concurrent_usd at 90.0
        // -> $60 headroom, which would ALSO admit $55. Only a release bounded
        // to cost_usd ($10) leaves $55 vetoed.
        assert_eq!(
            rails.check_fire(55.0, 3_001).map_err(|e| e.veto),
            Err(RailVeto::ConcurrentExposure),
            "an amount that only fits if MORE than cost_usd ($10) had been released must \
             still be vetoed — this is what catches releasing settlement proceeds \
             (shares = $20) instead of cost_usd"
        );
        assert_eq!(rails.halted(), None, "a win must never trip the loss kill");
    }

    /// A resolved LOSING position: `run()` must call `record_settled_pnl`
    /// with the real loss (`-(cost_usd + fee_usd)`), proven here by a
    /// deliberately tiny `realized_loss_kill_usd` that only a genuine
    /// `record_settled_pnl` call — not a silently-skipped one — would trip.
    #[tokio::test(start_paused = true)]
    async fn resolved_losing_position_records_pnl_and_trips_the_configured_kill() {
        let rails = rails_with(1.0, 1_000.0); // trips on any loss >= $1
        let p = pos(1, Side::Up, 5.0); // side Up loses if winner is Down
        let calls = Arc::new(AtomicU32::new(0));
        let fetch = AlwaysResolved { winner: Side::Down, calls: calls.clone() };
        let (tele, _dir) = writer();
        let lane = SettleLane::new(fetch, rails.clone(), tele);
        lane.track(p);

        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(lane.run(shutdown.clone()));
        tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
        shutdown.cancel();
        handle.await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            rails.halted(),
            Some(RailVeto::RealizedLossKill),
            "a -$5.00 realized loss against a -$1.00 kill threshold must trip the latch \
             — this only happens if record_settled_pnl was actually called"
        );
    }

    /// Pins Resolution 3's unit trap at the actual `settle()` call site, not
    /// just the `now_ms()` helper: `now_ms_returns_milliseconds_not_seconds`
    /// alone does not catch passing `pos.slot_open` (epoch SECONDS) as
    /// `record_settled_pnl`'s second argument instead of calling `now_ms()`.
    /// With `slot_open` small, its day index is 0 — the same as
    /// `RealizedDayState`'s initial `day: 0` — so `rails.rs`'s forward-only
    /// rollover guard (`day > state.day`) reads `0 > 0` == false: a
    /// pre-seeded prior-day loss never resets and silently adds into
    /// today's total.
    ///
    /// Seeds a day-0 loss of -$0.60 (below the $1.00 kill on its own), then
    /// settles a position with a real loss of -$0.50. Correct code observes
    /// settlement at the real wall-clock day (`now_ms()` reads `SystemTime`,
    /// unaffected by this test's paused Tokio clock, so its day index is far
    /// past 0), resets the accumulator, and lands at -0.50 — inside the
    /// kill, no halt. Under the `slot_open` mutation the day index never
    /// advances past 0, the totals add instead of resetting
    /// (-0.60 + -0.50 = -1.10), and the kill trips — this assertion then
    /// fails.
    #[tokio::test(start_paused = true)]
    async fn settle_day_index_comes_from_now_ms_not_slot_open() {
        let rails = rails_with(1.0, 1_000.0); // trips on any cumulative loss >= $1
        rails.record_settled_pnl(-0.6, 0); // day 0: below the kill alone

        let p = pos(1, Side::Up, 0.5); // side Up loses if winner is Down; -$0.50 realized
        let calls = Arc::new(AtomicU32::new(0));
        let fetch = AlwaysResolved { winner: Side::Down, calls: calls.clone() };
        let (tele, _dir) = writer();
        let lane = SettleLane::new(fetch, rails.clone(), tele);
        lane.track(p);

        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(lane.run(shutdown.clone()));
        tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
        shutdown.cancel();
        handle.await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            rails.halted().is_none(),
            "settlement must be stamped with real observation time so the day-0 seed \
             (-$0.60) rolls over and today's -$0.50 loss alone stays inside the -$1.00 kill"
        );
    }

    /// The bounded-retry give-up backstop: after `MAX_POLL_ATTEMPTS` polls
    /// all returning `Ok(None)`, `run()` must release exposure but must NEVER
    /// call `record_settled_pnl` — proven by a tiny loss-kill threshold that
    /// stays untripped despite the position's cost being large enough to
    /// trip it if (incorrectly) credited as a loss.
    ///
    /// Same blind spot as `resolved_winning_position_releases_exposure`:
    /// with only the position's own $5 booked, `release_exposure`'s
    /// `.max(0.0)` floor makes a correct $5 release and any over-release
    /// indistinguishable (both land on `concurrent_usd = 0.0`). An extra,
    /// untouched $100 "other open position" keeps the floor out of reach so
    /// a wrong release amount shows up as different headroom.
    #[tokio::test(start_paused = true)]
    async fn give_up_after_max_attempts_releases_exposure_without_recording_pnl() {
        let rails = rails_with(1.0, 150.0); // would trip on ANY credited loss >= $1; per_order_cap_usd = 75.0
        let p = pos(1, Side::Up, 5.0);
        rails.record_send(5.0, 1_000); // this position's own commit
        rails.record_send(100.0, 1_001); // another still-open position, never released here

        let calls = Arc::new(AtomicU32::new(0));
        let fetch = NeverResolved { calls: calls.clone() };
        let (tele, _dir) = writer();
        let lane = SettleLane::new(fetch, rails.clone(), tele);
        lane.track(p);

        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(lane.run(shutdown.clone()));
        // A single `sleep` past `MAX_POLL_ATTEMPTS` ticks: paused-clock
        // auto-advance processes every intervening timer (each interval
        // tick) in order while this task waits, which a raw `advance()` +
        // one `yield_now()` does not reliably do.
        tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS * (MAX_POLL_ATTEMPTS as u64 + 5))).await;
        shutdown.cancel();
        handle.await.unwrap();

        assert_eq!(
            calls.load(Ordering::SeqCst),
            MAX_POLL_ATTEMPTS,
            "must give up at exactly max_poll_attempts, not poll forever nor stop early"
        );
        // Correct release (cost_usd = $5): concurrent_usd = 100.0 -> $50 headroom.
        assert!(
            rails.check_fire(50.0, 3_000).is_ok(),
            "an amount fitting the correctly-restored $50 headroom (cap $150 - other $100 \
             - this position released to $0) must clear on give-up even though the outcome \
             is unknown"
        );
        // A release of more than cost_usd (e.g. the $20 `pos.shares` value —
        // meaningless here, but the same copy-paste mistake as the settle()
        // path) would land concurrent_usd at 85.0 -> $65 headroom, which
        // would ALSO admit $55. Only a release bounded to cost_usd ($5)
        // leaves $55 vetoed.
        assert_eq!(
            rails.check_fire(55.0, 3_001).map_err(|e| e.veto),
            Err(RailVeto::ConcurrentExposure),
            "an amount that only fits if MORE than cost_usd ($5) had been released must \
             still be vetoed"
        );
        assert_eq!(
            rails.halted(),
            None,
            "give-up must NEVER record a PnL — inventing one would poison the loss kill"
        );
    }
}
