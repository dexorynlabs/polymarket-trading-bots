//! PURE sizing. Under POST-TWAP-SPEC §3.7 this is one division.
//!
//! **v0 is depth-blind, deliberately.** A fire buys `clip_usd / ask_price`
//! shares at the configured entry price regardless of what the ladder
//! displays: the census that graded this gate never modelled depth (it is a
//! fills-assumed count of 1c moments), a partial fill is free, and a total
//! miss costs $0 — so sizing off displayed depth would make live behaviour
//! diverge from the only measurement there is, in exchange for nothing.
//!
//! `ladder_walk` survives from the earlier inheritance as the budget walk
//! spec §6 records as **unused at v0**. Its REV 2 companions — the bounded
//! cap-limit price and its commit — are GONE with `slip_mult`: the FAK's
//! limit is now a flat configured price (§3.4), not a multiple of a walk
//! stop, so the commit is simply `shares * limit_price`. So are the tiered
//! and dose clip laws, with `clip_mode`.

/// Price-vs-cap tolerance. Mirrors `p <= cap + 1e-4` in the harness.
pub const CAP_EPS: f64 = 1e-4;

/// Budget-exhausted threshold.
const BUDGET_EPS: f64 = 0.01;

/// Minimum displayed notional for a tradeable entry — the venue's $1
/// marketable floor.
const MIN_SPEND_USD: f64 = 1.0;

/// Shares a dollar clip buys at `price` — spec §3.7's flat clip, applied to
/// whatever price the dollars actually transact at. $10 is 1,111 shares at
/// 0.9c, 1,000 at 1c, 909 at 1.1c.
///
/// This is a REPORTING quantity: it answers "what will the clip come back as
/// if it fills here". It is NOT the number to put on the order — see
/// [`fak_shares`], and do not confuse the two again.
pub fn flat_shares(clip_usd: f64, price: f64) -> f64 {
    clip_usd / price
}

/// Shares to SEND on the FAK, so that a fire commits exactly `clip_usd`.
///
/// **A Polymarket BUY is a dollar commitment.** The signed order carries
/// `makerAmount = size_shares * limit_px` and that is what the venue spends;
/// price improvement is returned as EXTRA SHARES, not as change. Sizing the
/// order off the ASK therefore over-commits by `limit_px / ask` — a 2x
/// over-spend against the shipped 1c-ask / 2c-limit pair.
///
/// Worked example: a $10 clip sent as `clip/ask` shares at a 1c ask with a
/// 2c limit is 1,000 sh x 0.02 = $20 committed, returned as 20/0.01 = 2,000
/// shares, where the order asked for 1,000.
///
/// Sizing off the LIMIT makes the commitment exactly `clip_usd` at every ask
/// in the band, while the shares it returns still track the fill price
/// (`clip_usd / fill_px`) — which is the quantity the census graded: $10
/// filling at 1c is 1,000 shares, as it always should have been.
pub fn fak_shares(clip_usd: f64, limit_px: f64) -> f64 {
    clip_usd / limit_px
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Walk {
    pub shares: f64,
    pub spend_usd: f64,
    /// Price of the DEEPEST level the walk consumed. Always <= cap.
    pub limit_px: f64,
}

/// Walk displayed asks at or below `cap` cheapest-first, spending up to
/// `budget_usd`. `None` when under $1 of notional is reachable.
///
/// UNUSED at v0 (module doc) — kept because a depth-aware clip is the first
/// refinement anyone will reach for, and it must then be a separate, labelled
/// cell (spec §7), not a quiet change to the shipped one.
pub fn ladder_walk(asks: &[(f64, f64)], cap: f64, budget_usd: f64) -> Option<Walk> {
    let mut levels: Vec<(f64, f64)> =
        asks.iter().copied().filter(|(p, _)| *p <= cap + CAP_EPS).collect();
    levels.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    let (mut shares, mut spend, mut limit_px) = (0.0f64, 0.0f64, 0.0f64);
    for (p, s) in levels {
        if budget_usd - spend <= BUDGET_EPS {
            break;
        }
        if p.is_nan() || p <= 0.0 {
            continue;
        }
        let level_take_usd = p * s;
        // Explicit NaN check (not `!(level_take_usd > 0.0)`) avoids
        // clippy::neg_cmp_op_on_partial_ord while keeping NaN fail-closed.
        if level_take_usd.is_nan() || level_take_usd <= 0.0 {
            continue;
        }
        let take_usd = (budget_usd - spend).min(level_take_usd);
        shares += take_usd / p;
        spend += take_usd;
        limit_px = p;
    }
    if spend < MIN_SPEND_USD {
        return None;
    }
    Some(Walk { shares, spend_usd: spend, limit_px })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_flat_clip_is_the_budget_at_the_entry_price() {
        assert_eq!(flat_shares(10.0, 0.01), 1_000.0);
        // ...and it does not read the book: the same clip at the same entry
        // price is the same size whatever is displayed (module doc).
        assert_eq!(flat_shares(10.0, 0.01), flat_shares(10.0, 0.01));
    }

    /// THE sizing invariant (2026-08-10). A BUY commits `size_shares *
    /// limit_px` dollars whatever the ask is doing, so the ORDER must be sized
    /// off the LIMIT. Sized off the ask instead, a $10 clip commits ~2x the
    /// clip at a 0.9c ask.
    #[test]
    fn a_fire_commits_exactly_the_clip_at_every_ask_in_the_band() {
        for _ask in [0.009, 0.01, 0.011] {
            let commit = fak_shares(10.0, 0.02) * 0.02;
            assert!((commit - 10.0).abs() < 1e-9, "commit {commit}");
        }
    }

    /// ...and the shares that commitment RETURNS still track the fill price,
    /// because price improvement pays in shares, not change. This is the
    /// quantity the census graded: $10 filling at 1c is 1,000 shares.
    #[test]
    fn the_commitment_still_buys_the_graded_share_count_at_the_fill_price() {
        let commit = fak_shares(10.0, 0.02) * 0.02;
        assert!((commit / 0.01 - 1_000.0).abs() < 1e-9);
        assert!((commit / 0.009 - 10.0 / 0.009).abs() < 1e-9);
    }

    /// The venue's LIMIT minimum is 5 SHARES, not $5 of notional. The count
    /// that must clear it is the one we SEND (`clip / limit`), which is the
    /// SMALLER of the two — `config::validate_strategy` checks that one.
    #[test]
    fn the_shipped_clip_clears_the_five_share_venue_minimum() {
        assert!(fak_shares(10.0, 0.02) >= 5.0);
        assert!(fak_shares(10.0, 0.02) < flat_shares(10.0, 0.011), "the sent count is the smaller");
    }

    #[test]
    fn walk_matches_the_harness_ladder_fill() {
        let asks = [(0.30, 100.0), (0.10, 20.0), (0.20, 10.0)];
        let w = ladder_walk(&asks, 0.35, 5.0).unwrap();
        assert!((w.spend_usd - 5.0).abs() < 1e-9);
        // cheapest-first: all 20sh @0.10 ($2), all 10sh @0.20 ($2), $1 @0.30
        assert!((w.shares - (20.0 + 10.0 + 1.0 / 0.30)).abs() < 1e-6);
        assert_eq!(w.limit_px, 0.30, "limit is the DEEPEST level the walk consumed");
    }

    #[test]
    fn thin_cheap_ladder_walks_up_to_the_cap() {
        let w = ladder_walk(&[(0.02, 50.0), (0.18, 1000.0)], 0.20, 5.0).unwrap();
        assert!((w.spend_usd - 5.0).abs() < 1e-9);
        assert_eq!(w.limit_px, 0.18);
        assert!((w.shares - (50.0 + 4.0 / 0.18)).abs() < 1e-6);
    }

    #[test]
    fn non_positive_and_nan_prices_are_skipped() {
        let asks = [(0.0, 100.0), (-0.05, 100.0), (f64::NAN, 100.0), (0.10, 20.0)];
        let with_bad_levels = ladder_walk(&asks, 0.35, 5.0).unwrap();
        let valid_only = ladder_walk(&[(0.10, 20.0)], 0.35, 5.0).unwrap();
        assert_eq!(with_bad_levels, valid_only);
    }

    #[test]
    fn nan_and_zero_size_levels_are_ignored_entirely() {
        // A NaN size makes `p * s` NaN, and `(budget - spend).min(NaN)` returns
        // `budget - spend` (f64::min ignores NaN) — the phantom level would
        // otherwise absorb the whole clip.
        let asks = [(0.02, f64::NAN), (0.03, 0.0), (0.10, 20.0)];
        let with_bad_levels = ladder_walk(&asks, 0.35, 5.0).unwrap();
        let valid_only = ladder_walk(&[(0.10, 20.0)], 0.35, 5.0).unwrap();
        assert_eq!(with_bad_levels, valid_only);
    }

    #[test]
    fn levels_above_the_cap_are_never_touched() {
        let w = ladder_walk(&[(0.02, 50.0), (0.30, 1000.0)], 0.05, 5.0).unwrap();
        assert_eq!(w.limit_px, 0.02);
        assert!((w.spend_usd - 1.0).abs() < 1e-9, "only the 2c level is inside the cap");
    }

    #[test]
    fn cap_comparison_uses_the_harness_1e4_tolerance() {
        assert!(ladder_walk(&[(0.05, 1000.0)], 0.05, 5.0).is_some());
        assert!(ladder_walk(&[(0.0502, 1000.0)], 0.05, 5.0).is_none());
    }

    #[test]
    fn under_one_dollar_of_displayed_notional_is_no_walk() {
        assert!(ladder_walk(&[(0.02, 20.0)], 0.20, 5.0).is_none(), "$0.40 displayed");
        assert!(ladder_walk(&[], 0.20, 5.0).is_none());
        assert!(ladder_walk(&[(0.02, 50.0)], 0.20, 5.0).is_some(), "$1.00 displayed");
    }
}
