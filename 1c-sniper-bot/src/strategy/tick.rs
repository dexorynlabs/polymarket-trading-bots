//! PURE tick-time trigger — under POST-TWAP-SPEC §3.5 the ONLY fire path.
//!
//! The earlier gate this file used to hold (a trailing DISPLACEMENT
//! `dose_usd` inside a D-band card) is retired: the whole card machine graded
//! 4/4 arms KILLED on the TWAP era — base+B1 0-for-48, −100% ROS.
//! What replaces it is the micro-burst trigger
//! extracted from the 696-slot census
//! (`data/tickburst_posttwap_census/`), the ONLY entry the census found +EV:
//!
//!   trailing-1s Binance-spot volume >= 0.5 BTC, with >= 60% of that volume
//!   in the trailing side's RECOVERY direction (taker SELLs for a trailing-
//!   Down entry, taker BUYs for trailing-Up).
//!
//! Everything else about the entry — the 1c standing ask, the [150, 291)
//! frame, one shot per slot — is in `cards.rs` and `engine.rs`. This module is
//! only the burst predicate.
//!
//! Three things the gate deliberately does NOT read, each measured and graded
//! off (spec §4 — do not resurrect any of them without new OOS evidence):
//! - **the deficit D** in either direction. The old D∈[12,60] band went
//!   0-for-48; its near-tie inversion (D<=10) is ALSO refuted — 202 one-cent
//!   episodes at D<=10 contained ONE winner, and winners' D at entry spans
//!   −7…+35, indistinguishable from losers'. `features.rs` keeps computing
//!   D/zD/σ60 for TELEMETRY only.
//! - **any second venue.** Perp early-fire led 0/3 races in-frame at +11.6
//!   fake entries/day; OKX never led; bybit stamps ~28ms late. Binance spot
//!   `@aggTrade` is the trigger and nothing leads it.
//! - **book depth / precursors.** Race books sat at the 83rd–95th percentile
//!   of size-per-level (FATTER than normal, no pull).

/// Operator-settable trigger parameters (`[strategy.tick]`). The shipped
/// values ARE the frozen gate (spec §3.5): 0.5 BTC over 1000ms with a 60%
/// recovery lean. The volume threshold is a verified plateau over 0.5–1.0
/// (V=1.0 equal-or-better in-sample), which is why it is a knob at all —
/// spec §7 still says do not tune it while grading it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TickCfg {
    /// Trailing-window Binance-spot volume, in BTC, required to fire.
    pub v_btc: f64,
    /// Minimum share of that volume in the recovery direction, in [0, 1].
    pub lean_min: f64,
    /// Width of the trailing window, in ms.
    pub window_ms: u64,
    /// A held book older than this is not acted on. Sized to the census's own
    /// staleness bound (`STALE_MS = 15_000` in `census_surface.py`): the
    /// target IS a quiet 1c ask that can stand for 16s (spec §5's absorbed
    /// class), so the earlier 500ms bound would suppress nearly every
    /// entry the census counted.
    pub max_book_age_ms: u64,
}

/// The §3.5 burst predicate. `vol_btc` is the trailing-window total and
/// `dir_btc` the part of it in the direction under test — the RECOVERY
/// direction for the real gate, the adverse one for the placebo mirror
/// (`cards::placebo_burst_pass`).
///
/// Every comparison is written so NaN fails closed, and a zero-volume window
/// can never pass (the lean would be 0/0).
pub fn burst_pass(vol_btc: f64, dir_btc: f64, cfg: &TickCfg) -> bool {
    if vol_btc.is_nan() || dir_btc.is_nan() || vol_btc <= 0.0 {
        return false;
    }
    if vol_btc < cfg.v_btc {
        return false;
    }
    let lean = dir_btc / vol_btc;
    // Explicit NaN check rather than `!(lean >= ..)` — this crate carries no
    // `#[allow(clippy::neg_cmp_op_on_partial_ord)]` anywhere.
    !lean.is_nan() && lean >= cfg.lean_min
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> TickCfg {
        TickCfg { v_btc: 0.5, lean_min: 0.6, window_ms: 1_000, max_book_age_ms: 15_000 }
    }

    #[test]
    fn the_volume_floor_is_inclusive() {
        assert!(burst_pass(0.5, 0.5, &cfg()), "exactly 0.5 BTC must fire");
        assert!(!burst_pass(0.4999, 0.4999, &cfg()));
    }

    #[test]
    fn the_lean_floor_is_inclusive_and_measured_against_the_window_total() {
        // 1.0 BTC with 0.6 recovering = exactly 60%.
        assert!(burst_pass(1.0, 0.6, &cfg()));
        assert!(!burst_pass(1.0, 0.59, &cfg()));
        // Volume alone never fires: a big MIXED window is churn, not a burst.
        assert!(!burst_pass(10.0, 5.0, &cfg()), "50% lean must not fire at any volume");
    }

    #[test]
    fn a_pure_one_sided_burst_under_the_volume_floor_never_fires() {
        assert!(!burst_pass(0.2, 0.2, &cfg()), "100% lean does not substitute for volume");
    }

    #[test]
    fn an_empty_or_negative_window_fails_closed() {
        assert!(!burst_pass(0.0, 0.0, &cfg()));
        assert!(!burst_pass(-1.0, -1.0, &cfg()));
    }

    #[test]
    fn nan_anywhere_fails_closed() {
        assert!(!burst_pass(f64::NAN, 1.0, &cfg()));
        assert!(!burst_pass(1.0, f64::NAN, &cfg()));
        assert!(!burst_pass(f64::INFINITY, f64::INFINITY, &cfg()), "inf/inf is NaN lean");
    }
}
