//! The ONE entry card, shared by both modes. DATA, not code paths.
//!
//!   frame       decision `t` in [150, 291) of the 300s slot
//!   permission  the token's STANDING best ask sits in the mode's band
//!               (`1c`: 0.9c–1.1c, the trailer · `99c`: 97c–99c, the leader)
//!   trigger     `tick.rs` — `burst` (flow toward the token) or `calm`
//!               (no flow against it; `99c` only)
//!   shot        one per SLOT — not per side
//!
//! Architectural rule: the signals stay separate — the Binance flow
//! TRIGGERS, the standing ask is the PERMISSION. Never fold them into a
//! blended score.
//!
//! The PLACEBO mirror is a burst AGAINST the token. It is LOG ONLY, and its
//! cumulative PnL should stay negative; if it turns positive, the regime has
//! changed.

use crate::strategy::flow::Burst;
use crate::strategy::tick::{burst_pass, calm_pass, TickCfg, TriggerMode};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CardId {
    /// The only card there is.
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
    /// Entry window, half-open: `t_lo <= t < t_hi`.
    pub t_lo: usize,
    pub t_hi: usize,
}

/// Frozen. Changing a number here changes the strategy (and `slot_driver`'s
/// grace-slack check is derived from `t_hi`).
pub const CARDS: [Card; 1] = [Card { id: CardId::Burst, t_lo: 150, t_hi: 291 }];

/// The card, for call sites that want it by name rather than by index.
pub const BURST: Card = CARDS[0];

/// Price-vs-entry tolerance — a float-representation slack, NOT a band.
/// Mirrors `sizing::CAP_EPS`.
pub const ASK_EPS: f64 = 1e-4;

impl Card {
    /// Half-open.
    pub fn frame_contains(&self, t: usize) -> bool {
        t >= self.t_lo && t < self.t_hi
    }
}

/// The entry BAND: the standing best ask must sit in
/// `[ask_price_min, ask_price_max]`. Written so NaN fails closed.
///
/// The two tokens of one market can never sit in the same mode's band at
/// once, so the band alone decides which token is bought.
///
/// `ASK_EPS` is float-representation slack only. Venue prices near the
/// extremes sit on a 0.001 grid, so a 1e-4 tolerance can never reach an
/// adjacent grid price.
pub fn ask_qualifies(best_ask: f64, ask_price_min: f64, ask_price_max: f64) -> bool {
    !best_ask.is_nan()
        && best_ask >= ask_price_min - ASK_EPS
        && best_ask <= ask_price_max + ASK_EPS
}

/// The flow toward the token and against it, for a side whose "toward"
/// aggressor is taker-BUY (`up`) or taker-SELL (`down`).
fn split(burst: &Burst, toward_is_buy: bool) -> (f64, f64) {
    if toward_is_buy {
        (burst.buy_btc, burst.sell_btc)
    } else {
        (burst.sell_btc, burst.buy_btc)
    }
}

/// The entry trigger for the token we would buy, dispatched on `cfg.mode`.
pub fn entry_gates_pass(burst: &Burst, toward_is_buy: bool, cfg: &TickCfg) -> bool {
    let (toward, adverse) = split(burst, toward_is_buy);
    match cfg.mode {
        TriggerMode::Burst => burst_pass(burst.vol_btc, toward, cfg),
        TriggerMode::Calm => calm_pass(adverse, cfg),
    }
}

/// The placebo mirror: a BURST against the token (volume `>= v_btc`, lean
/// `>= lean_min` adverse). Same predicate for every trigger. LOG ONLY; must
/// never reach an order path.
pub fn placebo_gates_pass(burst: &Burst, toward_is_buy: bool, cfg: &TickCfg) -> bool {
    let (_, adverse) = split(burst, toward_is_buy);
    burst_pass(burst.vol_btc, adverse, cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(mode: TriggerMode) -> TickCfg {
        TickCfg {
            mode,
            v_btc: 0.5,
            lean_min: 0.6,
            adverse_max_btc: 0.3,
            window_ms: 1_000,
            max_book_age_ms: 15_000,
        }
    }

    fn burst(vol: f64, buy: f64) -> Burst {
        Burst { vol_btc: vol, buy_btc: buy, sell_btc: vol - buy }
    }

    #[test]
    fn the_card_table_is_frozen() {
        assert_eq!(CARDS.len(), 1);
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

    #[test]
    fn the_1c_band_admits_nine_to_eleven_tenths_of_a_cent() {
        let (lo, hi) = (0.009, 0.011);
        for a in [0.009, 0.010, 0.011] {
            assert!(ask_qualifies(a, lo, hi), "{a} must qualify");
        }
        assert!(!ask_qualifies(0.008, lo, hi));
        assert!(!ask_qualifies(0.012, lo, hi));
        assert!(!ask_qualifies(0.02, lo, hi), "2c stays excluded");
        assert!(!ask_qualifies(0.99, lo, hi), "the 99c leader never qualifies");
    }

    #[test]
    fn the_99c_band_admits_97_to_99_cents_only() {
        let (lo, hi) = (0.97, 0.99);
        for a in [0.97, 0.975, 0.98, 0.99] {
            assert!(ask_qualifies(a, lo, hi), "{a} must qualify");
        }
        assert!(!ask_qualifies(0.969, lo, hi));
        assert!(!ask_qualifies(0.991, lo, hi), "the 0.001 grid neighbour stays out");
        assert!(!ask_qualifies(0.01, lo, hi), "the 1c trailer never qualifies");
        assert!(!ask_qualifies(f64::NAN, lo, hi));
    }

    #[test]
    fn toward_is_taker_buys_for_up_and_taker_sells_for_down() {
        let c = cfg(TriggerMode::Burst);
        let b = burst(1.0, 0.8);
        assert!(entry_gates_pass(&b, true, &c), "BUYs move BTC toward UP");
        assert!(!entry_gates_pass(&b, false, &c), "...and against DOWN");
        let s = burst(1.0, 0.2);
        assert!(entry_gates_pass(&s, false, &c), "SELLs move BTC toward DOWN");
        assert!(!entry_gates_pass(&s, true, &c));
    }

    #[test]
    fn burst_needs_volume_not_just_direction() {
        let c = cfg(TriggerMode::Burst);
        assert!(!entry_gates_pass(&burst(0.2, 0.2), true, &c));
    }

    #[test]
    fn calm_vetoes_on_adverse_volume_only() {
        let c = cfg(TriggerMode::Calm);
        assert!(entry_gates_pass(&burst(0.05, 0.05), true, &c), "a thin window is calm");
        assert!(entry_gates_pass(&burst(5.0, 4.8), true, &c), "big FAVOURABLE flow is still calm");
        assert!(!entry_gates_pass(&burst(1.0, 0.6), true, &c), "0.4 BTC of SELLs against UP");
        assert!(entry_gates_pass(&burst(1.0, 0.75), true, &c), "0.25 adverse < 0.3");
    }

    #[test]
    fn the_placebo_is_a_burst_against_the_token_for_every_trigger() {
        for mode in [TriggerMode::Burst, TriggerMode::Calm] {
            let c = cfg(mode);
            let against_up = burst(1.0, 0.2);
            assert!(placebo_gates_pass(&against_up, true, &c), "{mode:?}");
            assert!(!entry_gates_pass(&against_up, true, &c), "{mode:?}: never both on one print");
            assert!(!placebo_gates_pass(&burst(0.2, 0.0), true, &c), "volume floor applies");
        }
    }
}
