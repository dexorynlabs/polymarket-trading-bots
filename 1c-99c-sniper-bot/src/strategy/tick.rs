//! PURE tick-time trigger — the ONLY fire path, shared by both modes.
//!
//! Every decision is made from the point of view of the token we would BUY
//! (the trailer in `1c` mode, the leader in `99c` mode). "Toward" is the
//! aggressor direction that moves BTC that token's way: taker BUYs for an
//! `up` token, taker SELLs for a `down` token. "Adverse" is the other way.
//!
//! - [`TriggerMode::Burst`] — trailing-window volume `>= v_btc` with
//!   `>= lean_min` of it TOWARD the token. In `1c` mode this is the trailer's
//!   recovery burst (the entry the 1c census found +EV). In `99c` mode it is a
//!   burst confirming the leader.
//! - [`TriggerMode::Calm`] (`99c` only) — fire on any print while the
//!   trailing-window ADVERSE volume is `< adverse_max_btc`. Rejected in `1c`
//!   mode: entering a standing 1c ask with no burst graded −$563 to −$2,954.
//!
//! The PLACEBO mirror (log only, never an order) is the same burst predicate
//! pointed the ADVERSE way. Its cumulative PnL should stay negative.

/// Which entry predicate the slot automaton uses. See the module doc.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TriggerMode {
    /// A burst toward the token we buy.
    #[default]
    Burst,
    /// No burst against the token we buy (`99c` mode only).
    Calm,
}

impl TriggerMode {
    pub fn as_str(self) -> &'static str {
        match self {
            TriggerMode::Burst => "burst",
            TriggerMode::Calm => "calm",
        }
    }

    /// Parses `burst` / `calm` (case-insensitive). `confirm` is accepted as
    /// an alias of `burst`, its old 99c name.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "burst" | "confirm" => Some(TriggerMode::Burst),
            "calm" => Some(TriggerMode::Calm),
            _ => None,
        }
    }
}

/// Trigger parameters, resolved from the mode preset + `SNIPER_*` env vars.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TickCfg {
    /// Entry predicate.
    pub mode: TriggerMode,
    /// Trailing-window Binance-spot volume, in BTC, that counts as a BURST —
    /// the `Burst` entry floor, and the placebo mirror's floor in both modes.
    pub v_btc: f64,
    /// Minimum share of a burst's volume in the direction under test, in
    /// (0.5, 1].
    pub lean_min: f64,
    /// `Calm` only: the entry is vetoed while the trailing-window volume
    /// AGAINST the token is at or above this many BTC.
    pub adverse_max_btc: f64,
    /// Width of the trailing window, in ms.
    pub window_ms: u64,
    /// A held book older than this is not acted on.
    pub max_book_age_ms: u64,
}

/// The burst predicate. `vol_btc` is the trailing-window total and `dir_btc`
/// the part of it in the direction under test.
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
    !lean.is_nan() && lean >= cfg.lean_min
}

/// The `Calm` predicate: the trailing window carries LESS than
/// `adverse_max_btc` of volume against the token. An empty-but-delivered
/// window (zero volume) is calm. NaN and negative inputs fail closed.
pub fn calm_pass(adverse_btc: f64, cfg: &TickCfg) -> bool {
    !adverse_btc.is_nan()
        && adverse_btc >= 0.0
        && !cfg.adverse_max_btc.is_nan()
        && adverse_btc < cfg.adverse_max_btc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> TickCfg {
        TickCfg {
            mode: TriggerMode::Burst,
            v_btc: 0.5,
            lean_min: 0.6,
            adverse_max_btc: 0.3,
            window_ms: 1_000,
            max_book_age_ms: 15_000,
        }
    }

    #[test]
    fn the_volume_floor_is_inclusive() {
        assert!(burst_pass(0.5, 0.5, &cfg()), "exactly 0.5 BTC must fire");
        assert!(!burst_pass(0.4999, 0.4999, &cfg()));
    }

    #[test]
    fn the_lean_floor_is_inclusive_and_measured_against_the_window_total() {
        assert!(burst_pass(1.0, 0.6, &cfg()));
        assert!(!burst_pass(1.0, 0.59, &cfg()));
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
        assert!(!calm_pass(f64::NAN, &cfg()));
    }

    #[test]
    fn calm_is_strictly_below_the_adverse_ceiling() {
        assert!(calm_pass(0.0, &cfg()), "a quiet window is calm");
        assert!(calm_pass(0.2999, &cfg()));
        assert!(!calm_pass(0.3, &cfg()), "the ceiling itself vetoes");
        assert!(!calm_pass(-0.1, &cfg()), "negative volume is a defect, not calm");
    }

    #[test]
    fn trigger_names_parse_case_insensitively() {
        assert_eq!(TriggerMode::parse("burst"), Some(TriggerMode::Burst));
        assert_eq!(TriggerMode::parse(" Calm "), Some(TriggerMode::Calm));
        assert_eq!(TriggerMode::parse("confirm"), Some(TriggerMode::Burst));
        assert_eq!(TriggerMode::parse("recovery"), None);
        assert_eq!(TriggerMode::parse(""), None);
    }
}
