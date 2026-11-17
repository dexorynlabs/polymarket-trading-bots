//! The ONE entry card, verbatim from POST-TWAP-SPEC §3. DATA, not code paths.
//!
//! **The three D-band cards (Cap5/Cap10/Cap20) are RETIRED** (spec §6). They
//! were the earlier machine: a deficit band, a velocity form and a zD
//! ceiling per card, three shots per slot-side. On the TWAP era that machine
//! graded 4/4 arms KILLED (base+B1 0-for-48, −100% ROS), and its inversion
//! (near-tie D<=10) is refuted too. What survives of
//! it is `features.rs`, computing D/zD/σ60 as TELEMETRY (spec §4.1).
//!
//! What replaces them is one card carrying the frozen frame and the price it
//! is allowed to pay, with the burst predicate in `tick.rs`:
//!
//!   §3.3  decision `t` in [150, 291)
//!   §3.4  the standing best ask IS the entry price (1c), FAK limit 2c
//!   §3.5  trailing-1s >= 0.5 BTC with >= 60% recovery lean
//!   §3.6  one shot per SLOT — not per card, not per side
//!
//! Architectural rule, unchanged from the earlier era and re-confirmed by the
//! census: the signals stay separate — the burst TRIGGERS, the standing 1c ask
//! is the PERMISSION. Never fold them into a blended score; the quiet
//! availability channel (enter a standing 1c window with no burst) grades
//! −$563 to −$2,954 and NO static gate rescues it (spec §4.2).

use crate::strategy::flow::Burst;
use crate::strategy::tick::{burst_pass, TickCfg};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CardId {
    /// The §3 micro-burst gate. The only card there is.
    Burst,
}

impl CardId {
    pub fn as_str(self) -> &'static str {
        match self {
            CardId::Burst => "burst",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Card {
    pub id: CardId,
    /// Entry window, half-open: `t_lo <= t < t_hi` (spec §3.3).
    pub t_lo: usize,
    pub t_hi: usize,
}

/// Spec §3, frozen. Changing a number here changes the strategy.
pub const CARDS: [Card; 1] = [Card { id: CardId::Burst, t_lo: 150, t_hi: 291 }];

/// The card, for call sites that want it by name rather than by index.
pub const BURST: Card = CARDS[0];

/// Price-vs-entry tolerance — a float-representation slack, NOT a band.
/// Mirrors `sizing::CAP_EPS`.
pub const ASK_EPS: f64 = 1e-4;

impl Card {
    /// Spec §3.3, half-open.
    pub fn frame_contains(&self, t: usize) -> bool {
        t >= self.t_lo && t < self.t_hi
    }
}

/// The entry BAND: the standing best ask must sit in
/// `[ask_price_min, ask_price_max]`. Written so NaN fails closed.
///
/// **Spec §3.4 is an equality at 1c; the shipped band 0.9c–1.1c is an
/// operator widening on top of it** (2026-08-09), and the two are kept
/// separable on purpose: `tests/posttwap_parity.rs` pins the ENGINE against
/// the graded census with `min = max = 0.01`, and a second cell in that file
/// measures what the shipped band does to the same window. Measured effect of
/// the widening over Aug 7-9: ONE entry moves (slot 1786114200, t+269@1c ->
/// t+251@0.9c, a loss either way), nothing added, nothing lost, the same 5
/// wins — and no 1.1c ask qualified at all, so the upper half is inert
/// in-sample.
///
/// What is still excluded, and why:
/// - **2c.** 57 first-moments at a 2c standing ask contained 0 winners, and
///   the 2c FAK limit is race-tail optionality only, never permission.
/// - **Below 0.9c.** The census's own band floor is 0.95c
///   (`census_surface.py`'s `BAND_LO`), so every sub-1c moment is UNMEASURED
///   — and only 4 of 2,065 sub-1c slot-seconds in the window carry a
///   qualifying burst at all (0.19%), so the pocket is nearly empty.
///
/// `ASK_EPS` is float-representation slack only. Venue prices sit on a 0.001
/// grid, so a 1e-4 tolerance can never reach an adjacent grid price.
pub fn ask_qualifies(best_ask: f64, ask_price_min: f64, ask_price_max: f64) -> bool {
    !best_ask.is_nan()
        && best_ask >= ask_price_min - ASK_EPS
        && best_ask <= ask_price_max + ASK_EPS
}

/// The §3.5 trigger for a side whose recovery direction is taker-BUY (`up`)
/// or taker-SELL (`down`).
pub fn burst_gates_pass(burst: &Burst, recovery_is_buy: bool, cfg: &TickCfg) -> bool {
    let dir = if recovery_is_buy { burst.buy_btc } else { burst.sell_btc };
    burst_pass(burst.vol_btc, dir, cfg)
}

/// The placebo mirror — inherited from the earlier design's §11.5 and
/// RE-POINTED at the new gate by POST-TWAP-SPEC §6: the same volume threshold
/// with the lean measured in the ADVERSE
/// direction — a burst pushing the trailing side further behind, where the
/// census found no edge. Its cumulative PnL must stay NEGATIVE; that is the
/// regime-change alarm. LOG ONLY. This must never reach an order path.
pub fn placebo_gates_pass(burst: &Burst, recovery_is_buy: bool, cfg: &TickCfg) -> bool {
    burst_gates_pass(burst, !recovery_is_buy, cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> TickCfg {
        TickCfg { v_btc: 0.5, lean_min: 0.6, window_ms: 1_000, max_book_age_ms: 15_000 }
    }

    fn burst(vol: f64, buy: f64) -> Burst {
        Burst { vol_btc: vol, buy_btc: buy, sell_btc: vol - buy }
    }

    #[test]
    fn the_card_table_matches_spec_section_3_verbatim() {
        assert_eq!(CARDS.len(), 1, "the three D-band cards are RETIRED (spec §6)");
        assert_eq!((BURST.t_lo, BURST.t_hi), (150, 291));
        assert_eq!(BURST.id.as_str(), "burst");
    }

    #[test]
    fn the_frame_is_half_open_lower_inclusive_upper_exclusive() {
        assert!(!BURST.frame_contains(149));
        assert!(BURST.frame_contains(150));
        assert!(BURST.frame_contains(290));
        assert!(!BURST.frame_contains(291));
    }

    /// The GRADED gate: `min == max == 0.01` is spec §3.4's equality, and it
    /// is what `tests/posttwap_parity.rs` proves the engine against.
    #[test]
    fn the_graded_configuration_is_a_one_cent_equality() {
        assert!(ask_qualifies(0.01, 0.01, 0.01));
        assert!(!ask_qualifies(0.009, 0.01, 0.01));
        assert!(!ask_qualifies(0.011, 0.01, 0.01));
        assert!(!ask_qualifies(0.02, 0.01, 0.01), "never enter at 2c");
    }

    /// The SHIPPED band (operator widening, 2026-08-09).
    #[test]
    fn the_shipped_band_admits_nine_to_eleven_tenths_of_a_cent() {
        let (lo, hi) = (0.009, 0.011);
        for a in [0.009, 0.010, 0.011] {
            assert!(ask_qualifies(a, lo, hi), "{a} must qualify in the shipped band");
        }
        // The grid steps by 0.001, so the neighbours outside the band must
        // stay out — the eps is representation slack, not a widening.
        assert!(!ask_qualifies(0.008, lo, hi));
        assert!(!ask_qualifies(0.012, lo, hi));
        assert!(!ask_qualifies(0.02, lo, hi), "2c stays excluded at any band");
    }

    /// Deep sub-cent asks stay out at the shipped band too: the census never
    /// measured one, and only 0.19% of such seconds carry a burst anyway.
    #[test]
    fn deep_sub_cent_asks_are_excluded_as_unmeasured() {
        for a in [0.001, 0.004, 0.005] {
            assert!(!ask_qualifies(a, 0.009, 0.011), "{a} must not qualify");
        }
    }

    #[test]
    fn nan_ask_never_qualifies() {
        assert!(!ask_qualifies(f64::NAN, 0.009, 0.011));
    }

    #[test]
    fn recovery_direction_is_taker_buys_for_up_and_taker_sells_for_down() {
        // 1.0 BTC, 80% BUYING: recovers an UP entry, not a DOWN one.
        let b = burst(1.0, 0.8);
        assert!(burst_gates_pass(&b, true, &cfg()), "up recovers on taker BUYs");
        assert!(!burst_gates_pass(&b, false, &cfg()), "down does not");
        // ...and the mirror image.
        let s = burst(1.0, 0.2);
        assert!(burst_gates_pass(&s, false, &cfg()), "down recovers on taker SELLs");
        assert!(!burst_gates_pass(&s, true, &cfg()));
    }

    #[test]
    fn the_placebo_mirror_flips_only_the_lean_direction() {
        let b = burst(1.0, 0.8);
        // The deficit-widening mirror of an UP entry: the real gate fires, the
        // mirror does not, and vice-versa on the same print.
        assert!(burst_gates_pass(&b, true, &cfg()));
        assert!(!placebo_gates_pass(&b, true, &cfg()));
        assert!(placebo_gates_pass(&b, false, &cfg()));
        // ...and the mirror respects the volume floor exactly as the gate does.
        let thin = burst(0.2, 0.2);
        assert!(!placebo_gates_pass(&thin, false, &cfg()));
    }
}
