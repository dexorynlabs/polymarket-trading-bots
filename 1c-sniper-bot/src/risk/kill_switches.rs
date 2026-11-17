//! Kill switch trigger evaluators (Strategy §13).
//!
//! Each `check_*` function is a pure predicate on (cfg, current state).
//! Returns `Some(KillReason)` when threshold is crossed; `None` otherwise.
//! Emission is the caller's responsibility.

use serde::{Deserialize, Serialize};

/// Per strategy §13, in priority order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillReason {
    /// Daily P&L floor (-$10 cumulative trade-leg). Global halt.
    DailyPnl,
    /// Per-slot mid-jump ≥ 3¢ in 1s.
    MidJump,
    /// Per-slot trade rate > 1.2/s sustained 5s (or > 0.9/s in shock-risk).
    TradeRate,
    /// Per-slot depth-within-v < 50% of 60s MA.
    DepthCollapse,
    /// Per-slot book flip (best_bid ≥ best_ask).
    BookFlip,
    /// WS staleness: no event in > 5s (spec §1.6).
    WsStaleness,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct KillSwitchCfg {
    #[serde(default = "default_mid_jump_cents")]
    pub mid_jump_1s_cents: f64,
    #[serde(default = "default_trade_rate")]
    pub trade_rate_5s_per_sec: f64,
    #[serde(default = "default_trade_rate_shock")]
    pub trade_rate_5s_per_sec_shock: f64,
    #[serde(default = "default_depth_collapse_pct")]
    pub depth_collapse_pct: f64,
    #[serde(default = "default_ws_stale_ms")]
    pub ws_stale_threshold_ms: u64,
    /// Minimum continuous duration (seconds) the trade rate must be above
    /// threshold before a TradeRate kill is honored. `0` reproduces the
    /// pre-2026-05-11 behavior (fire on any single-poll breach). The
    /// default value `10` absorbs typical slot-open trade bursts on the
    /// eth-updown-5m capture (1.4/s peak, ~5 s duration → never reaches
    /// 10 s of sustained breach), while still catching genuine sustained
    /// high-flow conditions. See docs/v1-paper-validation-tuning-sweep-2026-05-11.md
    /// for the sweep that produced this calibration.
    #[serde(default = "default_trade_rate_breach_secs")]
    pub trade_rate_breach_secs: u64,
    /// Minimum continuous duration (seconds) the depth-within-v must be
    /// below `depth_collapse_pct` of its 60s MA before a DepthCollapse kill
    /// is honored. `0` reproduces single-poll behavior (the historical
    /// default — preserved here so existing call sites that use
    /// `check_depth_collapse` unchanged keep their semantics). Set to a
    /// positive value AND call [`check_depth_collapse_sustained`] to opt
    /// into the duration-gated variant.
    ///
    /// **Why this exists**: a sol-updown-5m smoke session on
    /// 2026-05-22 recorded 149 DepthCollapse kill_set events in 14 min,
    /// median hold time 3 s, 41% with hold time <2 s and ZERO held to the
    /// 60 s TTL. Root cause is the orchestrator's
    /// `iter_recent(now-1s).last().unwrap_or(0.0)` read returning 0.0
    /// whenever the Down-side book is quiet for >=1s between 1Hz ticks,
    /// yielding 0% of MA → spurious fire. The sustained gate gives
    /// operators a way to absorb these blips while still detecting genuine
    /// sustained depth collapse.
    #[serde(default = "default_depth_collapse_breach_secs")]
    pub depth_collapse_breach_secs: u64,
}

fn default_mid_jump_cents() -> f64 {
    3.0
}
fn default_trade_rate() -> f64 {
    1.2
}
fn default_trade_rate_shock() -> f64 {
    0.9
}
fn default_depth_collapse_pct() -> f64 {
    50.0
}
fn default_ws_stale_ms() -> u64 {
    5_000 // spec §1.6 "kill if WS silent > 5s"
}
fn default_trade_rate_breach_secs() -> u64 {
    10
}
fn default_depth_collapse_breach_secs() -> u64 {
    5
}

impl Default for KillSwitchCfg {
    fn default() -> Self {
        Self {
            mid_jump_1s_cents: 3.0,
            trade_rate_5s_per_sec: 1.2,
            trade_rate_5s_per_sec_shock: 0.9,
            depth_collapse_pct: 50.0,
            ws_stale_threshold_ms: 5_000, // spec §1.6 "kill if WS silent > 5s"
            trade_rate_breach_secs: 10,
            depth_collapse_breach_secs: 5,
        }
    }
}

/// Per-slot kill-switch state. Tracks duration-gated breach timers for
/// `TradeRate` and `DepthCollapse`; other kills (`MidJump`, `BookFlip`,
/// `WsStaleness`) are stateless and remain so.
#[derive(Default, Debug)]
pub struct KillSwitchState {
    /// Set when a kill triggers; subsequent checks are no-ops.
    pub fired: Option<KillReason>,
    /// Wall-clock micros when the trade rate first crossed threshold for
    /// the current phase, and has stayed above since. `None` means no
    /// active breach. Used by [`check_trade_rate_sustained`] to gate the
    /// kill on continuous-duration breach rather than single-poll breach.
    pub trade_rate_breach_started_us: Option<u64>,
    /// Wall-clock micros when the depth-within-v first dropped below
    /// `cfg.depth_collapse_pct` of its 60s MA, and has stayed below since.
    /// `None` means no active breach. Used by
    /// [`check_depth_collapse_sustained`] to gate the kill on
    /// continuous-duration breach rather than single-poll breach.
    pub depth_collapse_breach_started_us: Option<u64>,
}

/// Returns Some(MidJump) iff the 1-second mid jump exceeds the cents threshold.
pub fn check_mid_jump(cfg: &KillSwitchCfg, mid_jump_pp: f64) -> Option<KillReason> {
    let mid_jump_cents = mid_jump_pp * 100.0;
    if mid_jump_cents >= cfg.mid_jump_1s_cents {
        Some(KillReason::MidJump)
    } else {
        None
    }
}

/// Returns Some(TradeRate) iff trade rate exceeds threshold for current phase.
///
/// Pure single-poll predicate — fires on any breach. For the production
/// duration-gated variant used inside the quoter, see
/// [`check_trade_rate_sustained`].
pub fn check_trade_rate(
    cfg: &KillSwitchCfg,
    rate_per_sec: f64,
    in_shock: bool,
) -> Option<KillReason> {
    let threshold = if in_shock {
        cfg.trade_rate_5s_per_sec_shock
    } else {
        cfg.trade_rate_5s_per_sec
    };
    if rate_per_sec > threshold {
        Some(KillReason::TradeRate)
    } else {
        None
    }
}

/// Duration-gated TradeRate kill (the production variant).
///
/// Tracks `state.trade_rate_breach_started_us` across calls. The kill
/// fires only when the rate has been continuously above threshold for at
/// least `cfg.trade_rate_breach_secs` seconds. Any single-poll dip below
/// threshold resets the breach timer.
///
/// **Why duration-gated:** the Phase 7 v3 capture exposes a slot-open
/// trade burst (1.4/s for ~5 s) that the single-poll predicate would
/// kill on at T+1 s — costing the bot 50+ ticks of LIP accrual before
/// the natural pre-shock MidJump pull at T+118 s. The 10 s default gate
/// is calibrated to absorb that burst (too brief to sustain 10 s)
/// without sacrificing detection of genuinely sustained high-flow
/// regimes (those will trip ~10 s after onset). See
/// docs/v1-paper-validation-tuning-sweep-2026-05-11.md.
///
/// `cfg.trade_rate_breach_secs = 0` reproduces single-poll behavior.
pub fn check_trade_rate_sustained(
    cfg: &KillSwitchCfg,
    state: &mut KillSwitchState,
    rate_per_sec: f64,
    in_shock: bool,
    now_micros: u64,
) -> Option<KillReason> {
    let threshold = if in_shock {
        cfg.trade_rate_5s_per_sec_shock
    } else {
        cfg.trade_rate_5s_per_sec
    };
    let breach_now = rate_per_sec > threshold;
    if !breach_now {
        state.trade_rate_breach_started_us = None;
        return None;
    }
    // Gate of 0 → first-poll fire (legacy single-poll semantics).
    if cfg.trade_rate_breach_secs == 0 {
        return Some(KillReason::TradeRate);
    }
    let started = *state.trade_rate_breach_started_us.get_or_insert(now_micros);
    let elapsed_us = now_micros.saturating_sub(started);
    let gate_us = cfg.trade_rate_breach_secs.saturating_mul(1_000_000);
    if elapsed_us >= gate_us {
        Some(KillReason::TradeRate)
    } else {
        None
    }
}

/// Returns Some(DepthCollapse) iff current depth is below the configured % of 60s MA.
///
/// Pure single-poll predicate — fires on any breach. For the duration-gated
/// variant that absorbs transient "current=0" reads caused by upstream
/// 1s-cutoff windows on a quiet book, see [`check_depth_collapse_sustained`].
pub fn check_depth_collapse(cfg: &KillSwitchCfg, current_pct_of_ma: f64) -> Option<KillReason> {
    if current_pct_of_ma < cfg.depth_collapse_pct {
        Some(KillReason::DepthCollapse)
    } else {
        None
    }
}

/// Duration-gated DepthCollapse kill (the post-smoke variant).
///
/// Tracks `state.depth_collapse_breach_started_us` across calls. The kill
/// fires only when `current_pct_of_ma < cfg.depth_collapse_pct` for at
/// least `cfg.depth_collapse_breach_secs` continuous seconds. Any
/// single-poll recovery to >= threshold resets the breach timer.
///
/// **Why duration-gated:** a sol-updown-5m smoke session on
/// 2026-05-22 recorded 149 DepthCollapse kill_set events over 14 min on a
/// 1Hz orchestrator that reads
/// `iter_recent(now - 1s).last().unwrap_or(0.0)` against the
/// `depth_within_v_ask` rolling window. The fallback to 0.0 (when the
/// Down book is quiet for >=1s) yields a 0% pct of MA → fires the
/// single-poll predicate spuriously. Median observed hold time was 3 s,
/// 41% of fires held < 2 s, and **zero** held to the 60 s TTL. The
/// duration gate at e.g. 5 s absorbs the false-negative blip while still
/// catching genuine sustained collapse.
///
/// `cfg.depth_collapse_breach_secs = 0` reproduces single-poll behavior
/// — this is the default to preserve the existing `check_depth_collapse`
/// call-site semantics; operators opt in by setting non-zero.
pub fn check_depth_collapse_sustained(
    cfg: &KillSwitchCfg,
    state: &mut KillSwitchState,
    current_pct_of_ma: f64,
    now_micros: u64,
) -> Option<KillReason> {
    let breach_now = current_pct_of_ma < cfg.depth_collapse_pct;
    if !breach_now {
        state.depth_collapse_breach_started_us = None;
        return None;
    }
    // Gate of 0 → first-poll fire (legacy single-poll semantics).
    if cfg.depth_collapse_breach_secs == 0 {
        return Some(KillReason::DepthCollapse);
    }
    let started = *state
        .depth_collapse_breach_started_us
        .get_or_insert(now_micros);
    let elapsed_us = now_micros.saturating_sub(started);
    let gate_us = cfg.depth_collapse_breach_secs.saturating_mul(1_000_000);
    if elapsed_us >= gate_us {
        Some(KillReason::DepthCollapse)
    } else {
        None
    }
}

/// Returns Some(BookFlip) iff best_bid >= best_ask.
pub fn check_book_flip(_cfg: &KillSwitchCfg, best_bid: f64, best_ask: f64) -> Option<KillReason> {
    if best_bid >= best_ask {
        Some(KillReason::BookFlip)
    } else {
        None
    }
}

/// Returns Some(WsStaleness) iff WS hasn't emitted in > threshold ms.
pub fn check_ws_staleness(cfg: &KillSwitchCfg, ms_since_last_event: u64) -> Option<KillReason> {
    if ms_since_last_event > cfg.ws_stale_threshold_ms {
        Some(KillReason::WsStaleness)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mid_jump_threshold_fires_on_3c_in_1s() {
        let cfg = KillSwitchCfg::default();
        let _state = KillSwitchState::default();
        // Synthetic 1s-old mid jump of 0.04 (4¢) — should fire
        let reason = check_mid_jump(&cfg, /* mid_jump = */ 0.04);
        assert_eq!(reason, Some(KillReason::MidJump));
    }

    #[test]
    fn mid_jump_does_not_fire_below_threshold() {
        let cfg = KillSwitchCfg::default();
        assert_eq!(check_mid_jump(&cfg, 0.02), None);
        assert_eq!(check_mid_jump(&cfg, 0.029), None);
    }

    #[test]
    fn trade_rate_fires_above_threshold() {
        let cfg = KillSwitchCfg::default();
        // 8 trades in 5s = 1.6/s, above default 1.2/s
        assert_eq!(
            check_trade_rate(&cfg, 1.6, false),
            Some(KillReason::TradeRate)
        );
        assert_eq!(check_trade_rate(&cfg, 1.0, false), None);
    }

    #[test]
    fn trade_rate_in_shock_phase_uses_tighter_threshold() {
        let cfg = KillSwitchCfg::default();
        // 1.0/s in shock phase = above 0.9/s threshold
        assert_eq!(
            check_trade_rate(&cfg, 1.0, true),
            Some(KillReason::TradeRate)
        );
    }

    #[test]
    fn depth_collapse_fires_below_50pct() {
        let cfg = KillSwitchCfg::default();
        // current depth = 30sh, MA = 100sh → 30% of MA, well below 50% → fire
        assert_eq!(
            check_depth_collapse(&cfg, /* pct */ 30.0),
            Some(KillReason::DepthCollapse)
        );
        assert_eq!(check_depth_collapse(&cfg, 60.0), None);
    }

    #[test]
    fn book_flip_fires_when_bid_ge_ask() {
        let cfg = KillSwitchCfg::default();
        // Args are (best_bid, best_ask). Predicate fires when bid >= ask.
        // bid=0.55, ask=0.55 → equal → fire
        assert_eq!(
            check_book_flip(&cfg, 0.55, 0.55),
            Some(KillReason::BookFlip)
        );
        // bid=0.56, ask=0.55 → bid > ask → fire (deviation from plan: arg order
        // in plan test was (0.55, 0.56) which would not flip given the docstring
        // predicate "best_bid >= best_ask"; swapped to match the documented spec)
        assert_eq!(
            check_book_flip(&cfg, 0.56, 0.55),
            Some(KillReason::BookFlip)
        );
        // bid=0.54, ask=0.55 → bid < ask → no fire
        assert_eq!(check_book_flip(&cfg, 0.54, 0.55), None);
    }

    #[test]
    fn ws_staleness_fires_after_threshold() {
        let cfg = KillSwitchCfg::default();
        // last event 12s ago, threshold = 5s (spec §1.6) → fire
        assert_eq!(
            check_ws_staleness(&cfg, 12_000),
            Some(KillReason::WsStaleness)
        );
        // exactly at threshold (5_000 ms) → no fire (strict >)
        assert_eq!(check_ws_staleness(&cfg, 5_000), None);
    }

    /// Task 3.3: pin the strict `>` boundary in `check_ws_staleness`.
    /// At exactly threshold, no fire; one ms above, fire. A regression
    /// to `>=` would slip the boundary; this is the only test that
    /// exercises the predicate at the edge.
    ///
    /// Updated in Task 5 (2026-05-23): default threshold lowered from 10_000 to
    /// 5_000 (spec §1.6). Only the assertion values changed; the `>` boundary
    /// semantics are unchanged.
    #[test]
    fn ws_staleness_check_uses_correct_threshold() {
        let cfg = KillSwitchCfg::default();
        assert_eq!(check_ws_staleness(&cfg, 4_999), None);
        assert_eq!(
            check_ws_staleness(&cfg, 5_001),
            Some(KillReason::WsStaleness)
        );
    }

    #[test]
    fn default_breach_secs_is_ten() {
        let cfg = KillSwitchCfg::default();
        assert_eq!(cfg.trade_rate_breach_secs, 10);
    }

    #[test]
    fn sustained_does_not_fire_on_first_breach_with_default_gate() {
        let cfg = KillSwitchCfg::default();
        let mut state = KillSwitchState::default();
        // First poll above threshold → no fire (gate=10s, elapsed=0).
        assert_eq!(
            check_trade_rate_sustained(&cfg, &mut state, 1.5, false, 1_000_000),
            None,
        );
        assert_eq!(state.trade_rate_breach_started_us, Some(1_000_000));
    }

    #[test]
    fn sustained_fires_after_breach_secs_elapsed() {
        let cfg = KillSwitchCfg::default();
        let mut state = KillSwitchState::default();
        // Start the breach.
        check_trade_rate_sustained(&cfg, &mut state, 1.5, false, 1_000_000);
        // 9 s later, still no fire (gate=10s).
        assert_eq!(
            check_trade_rate_sustained(&cfg, &mut state, 1.5, false, 10_000_000),
            None,
        );
        // 10 s later, fire.
        assert_eq!(
            check_trade_rate_sustained(&cfg, &mut state, 1.5, false, 11_000_000),
            Some(KillReason::TradeRate),
        );
    }

    #[test]
    fn sustained_resets_breach_on_dip_below_threshold() {
        let cfg = KillSwitchCfg::default();
        let mut state = KillSwitchState::default();
        check_trade_rate_sustained(&cfg, &mut state, 1.5, false, 1_000_000);
        assert!(state.trade_rate_breach_started_us.is_some());
        // Rate dips below threshold — breach timer resets.
        assert_eq!(
            check_trade_rate_sustained(&cfg, &mut state, 1.0, false, 2_000_000),
            None,
        );
        assert_eq!(state.trade_rate_breach_started_us, None);
        // New breach starts fresh — won't fire for another 10 s.
        assert_eq!(
            check_trade_rate_sustained(&cfg, &mut state, 1.5, false, 3_000_000),
            None,
        );
        assert_eq!(state.trade_rate_breach_started_us, Some(3_000_000));
    }

    #[test]
    fn sustained_gate_zero_reproduces_single_poll_fire() {
        let cfg = KillSwitchCfg {
            trade_rate_breach_secs: 0,
            ..KillSwitchCfg::default()
        };
        let mut state = KillSwitchState::default();
        // gate=0 → fire on first breach poll, no state tracking needed.
        assert_eq!(
            check_trade_rate_sustained(&cfg, &mut state, 1.5, false, 1_000_000),
            Some(KillReason::TradeRate),
        );
    }

    #[test]
    fn sustained_uses_shock_threshold_in_shock_phase() {
        let cfg = KillSwitchCfg::default();
        let mut state = KillSwitchState::default();
        // 1.0/s with in_shock=true → above 0.9 → arms breach.
        // 1.0/s with in_shock=false → below 1.2 → doesn't arm.
        assert_eq!(
            check_trade_rate_sustained(&cfg, &mut state, 1.0, false, 1_000_000),
            None,
        );
        assert_eq!(state.trade_rate_breach_started_us, None);
        assert_eq!(
            check_trade_rate_sustained(&cfg, &mut state, 1.0, true, 1_000_000),
            None,
        );
        assert_eq!(state.trade_rate_breach_started_us, Some(1_000_000));
    }

    // -- depth_collapse_sustained tests (post SOL smoke 2026-05-22). --
    //
    // Investigation finding: the orchestrator's depth-collapse evaluator uses
    // `iter_recent(now - 1s).last().unwrap_or(0.0)` to read "current depth"
    // and divides into the 60s MA. When the Down book is quiet for ≥1s
    // between consecutive orchestrator ticks (1Hz), the read returns 0.0,
    // yielding 0% of MA, which is below the 50% threshold and fires
    // DepthCollapse spuriously. The 14-min sol-updown-5m smoke session
    // (1779401365) recorded 149 DepthCollapse kill_set events across 4 slugs
    // — median hold-time 3s, 41% < 2s, ZERO held to the 60s TTL.
    //
    // The sustained variant gates on continuous-duration breach analogous to
    // `check_trade_rate_sustained`, so a single-tick quiet-book false negative
    // does not fire. v0.5 defaults to 5s (flap absorption — median DepthCollapse
    // hold time was 3s in the v0 smoke); operators opt to legacy single-poll
    // fire by setting `depth_collapse_breach_secs = 0` on the cfg.

    #[test]
    fn default_depth_collapse_breach_secs_is_five() {
        // v0.5 default flip 0→5: absorbs flap on paper-trading
        // (median DepthCollapse hold time empirically ~3s in v0 smoke v2).
        let cfg = KillSwitchCfg::default();
        assert_eq!(cfg.depth_collapse_breach_secs, 5);
    }

    #[test]
    fn depth_collapse_sustained_gate_zero_reproduces_single_poll_fire() {
        let cfg = KillSwitchCfg {
            depth_collapse_breach_secs: 0,
            ..KillSwitchCfg::default()
        };
        let mut state = KillSwitchState::default();
        // gate=0 → fire on first breach poll (current pct < 50% of MA).
        assert_eq!(
            check_depth_collapse_sustained(&cfg, &mut state, 30.0, 1_000_000),
            Some(KillReason::DepthCollapse),
        );
    }

    #[test]
    fn depth_collapse_sustained_does_not_fire_on_first_breach_with_nonzero_gate() {
        let cfg = KillSwitchCfg {
            depth_collapse_breach_secs: 5,
            ..KillSwitchCfg::default()
        };
        let mut state = KillSwitchState::default();
        // First poll below threshold → no fire (gate=5s, elapsed=0).
        assert_eq!(
            check_depth_collapse_sustained(&cfg, &mut state, 30.0, 1_000_000),
            None,
        );
        assert_eq!(state.depth_collapse_breach_started_us, Some(1_000_000));
    }

    #[test]
    fn depth_collapse_sustained_fires_after_breach_secs_elapsed() {
        let cfg = KillSwitchCfg {
            depth_collapse_breach_secs: 5,
            ..KillSwitchCfg::default()
        };
        let mut state = KillSwitchState::default();
        // Start the breach at t=1s.
        check_depth_collapse_sustained(&cfg, &mut state, 30.0, 1_000_000);
        // 4s later, still no fire (gate=5s).
        assert_eq!(
            check_depth_collapse_sustained(&cfg, &mut state, 30.0, 5_000_000),
            None,
        );
        // 5s after start, fire.
        assert_eq!(
            check_depth_collapse_sustained(&cfg, &mut state, 30.0, 6_000_000),
            Some(KillReason::DepthCollapse),
        );
    }

    #[test]
    fn depth_collapse_sustained_resets_breach_on_recovery_above_threshold() {
        let cfg = KillSwitchCfg {
            depth_collapse_breach_secs: 5,
            ..KillSwitchCfg::default()
        };
        let mut state = KillSwitchState::default();
        // Start the breach at t=1s.
        check_depth_collapse_sustained(&cfg, &mut state, 30.0, 1_000_000);
        assert!(state.depth_collapse_breach_started_us.is_some());
        // Pct recovers above 50% — breach timer resets.
        assert_eq!(
            check_depth_collapse_sustained(&cfg, &mut state, 80.0, 2_000_000),
            None,
        );
        assert_eq!(state.depth_collapse_breach_started_us, None);
        // New breach starts fresh — won't fire for another 5s.
        assert_eq!(
            check_depth_collapse_sustained(&cfg, &mut state, 30.0, 3_000_000),
            None,
        );
        assert_eq!(state.depth_collapse_breach_started_us, Some(3_000_000));
    }

    /// Pin: a 2-second false-negative blip (quiet Down book → current=0) must
    /// NOT fire with `breach_secs=5`. This is the exact scenario the smoke
    /// session exposed: the orchestrator reads current=0 for a tick or two,
    /// then the next book update restores depth. Sustained gate absorbs it.
    #[test]
    fn depth_collapse_sustained_absorbs_two_second_quiet_book_blip() {
        let cfg = KillSwitchCfg {
            depth_collapse_breach_secs: 5,
            ..KillSwitchCfg::default()
        };
        let mut state = KillSwitchState::default();
        // t=1s: pct=0 (quiet book → unwrap_or(0.0) in orchestrator).
        assert_eq!(
            check_depth_collapse_sustained(&cfg, &mut state, 0.0, 1_000_000),
            None,
        );
        // t=2s: still pct=0 (still quiet).
        assert_eq!(
            check_depth_collapse_sustained(&cfg, &mut state, 0.0, 2_000_000),
            None,
        );
        // t=3s: book ticked, pct=85 (healthy) → reset breach.
        assert_eq!(
            check_depth_collapse_sustained(&cfg, &mut state, 85.0, 3_000_000),
            None,
        );
        assert_eq!(state.depth_collapse_breach_started_us, None);
        // The 2-second blip never fired.
    }
}
