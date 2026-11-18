//! `SNIPER_MODE` — which side of the binary market this process buys.
//!
//! In a `btc-updown-5m` market the two tokens' prices sum to about $1, so a
//! token whose ask is ~1c has an opposite token worth ~99c. One engine serves
//! both strategies; the mode only picks the PRESET (price band, FAK limit,
//! clip, loss kill, book-age bound) and the validation rules:
//!
//! | mode  | buys          | band         | limit | win / loss per share |
//! |-------|---------------|--------------|-------|----------------------|
//! | `1c`  | the trailer   | 0.9c – 1.1c  | 2c    | ~+99c / ~−1c         |
//! | `99c` | the leader    | 97c – 99c    | 99c   | ~+1c / ~−99c         |
//!
//! Both tokens can never sit in the same band at once, so the band alone
//! decides which token is bought — the engine needs no other mode switch.

use crate::strategy::tick::TriggerMode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SniperMode {
    /// Buy the TRAILING token at ~1c.
    OneCent,
    /// Buy the LEADING token at ~99c.
    NinetyNineCent,
}

/// Every strategy value a mode supplies. Each one can be overridden by its
/// `SNIPER_*` env var (`config::env`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Preset {
    pub ask_price_min: f64,
    pub ask_price_max: f64,
    pub limit_price: f64,
    pub clip_usd: f64,
    pub realized_loss_kill_usd: f64,
    pub trigger: TriggerMode,
    pub v_btc: f64,
    pub lean_min: f64,
    pub adverse_max_btc: f64,
    pub window_ms: u64,
    pub max_book_age_ms: u64,
}

impl SniperMode {
    /// Parses `1c` / `99c` (case-insensitive; `1`, `99`, `1cent`, `99cent`
    /// are accepted too).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "1c" | "1" | "1cent" | "one-cent" => Some(SniperMode::OneCent),
            "99c" | "99" | "99cent" | "ninety-nine-cent" => Some(SniperMode::NinetyNineCent),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SniperMode::OneCent => "1c",
            SniperMode::NinetyNineCent => "99c",
        }
    }

    pub fn preset(self) -> Preset {
        match self {
            // The 1c census entry: 0.9c–1.1c band, 2c FAK limit, $10 clip
            // (1,000 shares at 1c; a win pays ~+$989 net of fees). Break-even
            // is a ~1.07% win rate vs a measured 5/98 in-band.
            SniperMode::OneCent => Preset {
                ask_price_min: 0.009,
                ask_price_max: 0.011,
                limit_price: 0.02,
                clip_usd: 10.0,
                realized_loss_kill_usd: 400.0,
                trigger: TriggerMode::Burst,
                v_btc: 0.5,
                lean_min: 0.6,
                adverse_max_btc: 0.3,
                window_ms: 1_000,
                // The 1c target is a QUIET ask that can stand for ~16s.
                max_book_age_ms: 15_000,
            },
            // UNMEASURED. A $20 clip at 99c: a win pays ~+$0.19 net, a loss
            // costs ~$20. Break-even is a ~99.07% win rate.
            SniperMode::NinetyNineCent => Preset {
                ask_price_min: 0.97,
                ask_price_max: 0.99,
                limit_price: 0.99,
                clip_usd: 20.0,
                // ~5 losing fills.
                realized_loss_kill_usd: 100.0,
                trigger: TriggerMode::Burst,
                v_btc: 0.5,
                lean_min: 0.6,
                adverse_max_btc: 0.3,
                window_ms: 1_000,
                // A 99c ask seen 15s ago may belong to a leader that is gone.
                max_book_age_ms: 2_000,
            },
        }
    }
}

impl std::fmt::Display for SniperMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_parse() {
        assert_eq!(SniperMode::parse("1c"), Some(SniperMode::OneCent));
        assert_eq!(SniperMode::parse(" 99C "), Some(SniperMode::NinetyNineCent));
        assert_eq!(SniperMode::parse("99"), Some(SniperMode::NinetyNineCent));
        assert_eq!(SniperMode::parse("50c"), None);
        assert_eq!(SniperMode::parse(""), None);
    }

    #[test]
    fn the_two_bands_never_overlap() {
        let a = SniperMode::OneCent.preset();
        let b = SniperMode::NinetyNineCent.preset();
        assert!(a.ask_price_max < b.ask_price_min);
        // ...and no single market can put both tokens in one band: two asks
        // in the 99c band would sum to > $1.
        assert!(2.0 * b.ask_price_min > 1.0);
    }
}
