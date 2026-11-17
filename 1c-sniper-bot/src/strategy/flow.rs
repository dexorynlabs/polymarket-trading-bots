//! PURE trailing taker-flow aggregates — the GATE input (POST-TWAP-SPEC §6).
//!
//! **This module's old rule ("telemetry only, never a gate") is REVERSED by
//! measurement, and the reversal is the point of the re-gating.** In the
//! earlier era taker-flow as a filter was a killed idea (added fills 0-for-60,
//! ANTI-selective at both bar and tick granularity). Post-TWAP the census over
//! 696 slots found the opposite: the trailing-1s LEAN is half of the only
//! trigger that is +EV at a 1c ask (spec §3.5, §4.5). The D-band cards went the
//! other way — gates to telemetry.
//!
//! The window convention is the census's, exactly (`census_surface.py`:
//! `j = searchsorted(w_ts, w_ts[i] - 1000, "left")`, summing `w_q[j:i+1]`):
//! **`[last_ts - window_ms, last_ts]`, INCLUSIVE at both ends**, anchored on
//! the most recent print. The earlier ring excluded the lower edge; that
//! half-open form is gone rather than kept alongside, because a gate and its
//! own telemetry must not describe two different windows.
//!
//! Sums are BTC quantity — the gate's unit (spec §3.5 is 0.5 **BTC**), not
//! notional. The USD sums stay for telemetry.
//!
//! Fed from the Binance `@aggTrade` stream, so it is only populated when
//! `[strategy.tick].enabled = true`; with the tick path off nothing fires at
//! all and every flow key serializes as JSON `null`.

use std::collections::VecDeque;

/// Default trailing window (`TickCfg::window_ms`) — the frozen gate's 1000ms.
pub const FLOW_WINDOW_MS: u64 = 1_000;

/// How much trade history the ring retains — the memory of `max_dose_20s_usd`
/// ("was there a burst recently"), sized to outlive the 6s silent pause the
/// 2026-07-29 winner erupted from with room to spare.
pub const DOSE_MEMORY_MS: u64 = 20_000;

struct Rec {
    ts_ms: u64,
    px: f64,
    qty: f64,
    usd: f64,
    taker_buy: bool,
}

/// Trailing `@aggTrade` history for ONE coin. `SlotEngine` rebuilds it at
/// every `SlotOpen`; pruning bounds it to `DOSE_MEMORY_MS` of prints.
pub struct FlowRing {
    window_ms: u64,
    trades: VecDeque<Rec>,
}

/// The gate's read of the trailing window: total volume and its split by
/// aggressor side, all in BTC. `buy_btc + sell_btc == vol_btc` by
/// construction, and the engine picks whichever half is the trailing side's
/// recovery direction (`engine::on_trade_side`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Burst {
    pub vol_btc: f64,
    pub buy_btc: f64,
    pub sell_btc: f64,
}

/// A snapshot of the ring at one instant, anchored on the LATEST trade's
/// venue time. `Copy` so the driver can stamp it on rows without borrowing
/// the ring. Telemetry only — the gate reads [`Burst`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FlowStats {
    /// Taker-BUY notional (px * qty, USD) over the trailing window.
    pub tbuy_usd_1s: f64,
    /// Total notional over the same window; taker-buy share = tbuy / notional.
    pub notional_usd_1s: f64,
    /// Total volume in BTC over the same window — the gate's own quantity, so
    /// a fire's telemetry row can be re-derived against the §3.5 threshold.
    pub vol_btc_1s: f64,
    /// `@aggTrade` prints in the window — compressed prints, NOT raw trades.
    pub trades_1s: u32,
    /// Largest |trailing-window displacement| observed anywhere in the
    /// retained `DOSE_MEMORY_MS`: per print, the price move vs the latest
    /// print at or before `ts - window_ms`, unsigned because flow context is
    /// side-independent.
    pub max_dose_20s_usd: f64,
}

impl FlowRing {
    pub fn new(window_ms: u64) -> Self {
        Self { window_ms, trades: VecDeque::new() }
    }

    /// Record one print at venue time `ts_ms`. `qty` is the print's BTC size,
    /// `taker_buy` is Binance's `!m` (the aggressor bought).
    ///
    /// Returns `false` for a print that arrived OUT of venue-time order,
    /// which is DROPPED: a superseded stamp must not corrupt the trailing
    /// window the gate sums over, and the caller must not fire on it either.
    /// The census reads a sorted array and never meets this case.
    pub fn push(&mut self, ts_ms: u64, px: f64, qty: f64, taker_buy: bool) -> bool {
        if self.trades.back().is_some_and(|last| ts_ms < last.ts_ms) {
            return false;
        }
        self.trades.push_back(Rec { ts_ms, px, qty, usd: px * qty, taker_buy });
        let cutoff = ts_ms.saturating_sub(DOSE_MEMORY_MS);
        // Keep one print at or before the cutoff so the oldest in-memory
        // print still has a displacement reference.
        while self.trades.len() >= 2 && self.trades[1].ts_ms <= cutoff {
            self.trades.pop_front();
        }
        true
    }

    /// The GATE's read: BTC sums over `[last_ts - window_ms, last_ts]`,
    /// inclusive both ends (module doc). `None` until the first print.
    ///
    /// Summed back-to-front over the retained prints and accumulated in
    /// ASCENDING venue order — the same order (and therefore the same
    /// floating-point result) as the census's own slice sum. The exporter
    /// asserts pairwise-vs-sequential agreement on every fixture slot.
    pub fn burst(&self) -> Option<Burst> {
        let last_ts = self.trades.back()?.ts_ms;
        let start = last_ts.saturating_sub(self.window_ms);
        let first = self.trades.iter().rposition(|t| t.ts_ms < start).map_or(0, |i| i + 1);
        let (mut vol, mut buy) = (0.0f64, 0.0f64);
        for t in self.trades.iter().skip(first) {
            vol += t.qty;
            if t.taker_buy {
                buy += t.qty;
            }
        }
        Some(Burst { vol_btc: vol, buy_btc: buy, sell_btc: vol - buy })
    }

    /// `None` until at least one print has been seen (distinguishing "no
    /// trades yet" from a genuinely quiet window, which reports zeros).
    pub fn stats(&self) -> Option<FlowStats> {
        let last_ts = self.trades.back()?.ts_ms;
        let window_start = last_ts.saturating_sub(self.window_ms);
        let (mut tbuy, mut notional, mut vol, mut count) = (0.0, 0.0, 0.0, 0u32);
        let mut max_dose = 0.0f64;
        let mut r = 0; // latest index with ts <= trades[i].ts - window_ms
        for i in 0..self.trades.len() {
            let t = &self.trades[i];
            // Sums over the INCLUSIVE trailing window (module doc) — the same
            // window `burst` gates on.
            if t.ts_ms >= window_start {
                notional += t.usd;
                vol += t.qty;
                count += 1;
                if t.taker_buy {
                    tbuy += t.usd;
                }
            }
            let cutoff = t.ts_ms.saturating_sub(self.window_ms);
            while r + 1 < i && self.trades[r + 1].ts_ms <= cutoff {
                r += 1;
            }
            if self.trades[r].ts_ms <= cutoff {
                max_dose = max_dose.max((t.px - self.trades[r].px).abs());
            }
        }
        Some(FlowStats {
            tbuy_usd_1s: tbuy,
            notional_usd_1s: notional,
            vol_btc_1s: vol,
            trades_1s: count,
            max_dose_20s_usd: max_dose,
        })
    }
}

impl Default for FlowRing {
    fn default() -> Self {
        Self::new(FLOW_WINDOW_MS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring() -> FlowRing {
        FlowRing::new(FLOW_WINDOW_MS)
    }

    #[test]
    fn empty_ring_reports_none_not_zeros() {
        assert_eq!(ring().burst(), None);
        assert_eq!(ring().stats(), None);
    }

    /// The census convention (module doc): a print sitting EXACTLY at
    /// `last_ts - window_ms` is INSIDE the window. The earlier ring
    /// excluded it; reproducing `entries_A.json` requires including it.
    #[test]
    fn the_trailing_window_is_inclusive_at_both_ends() {
        let mut r = ring();
        r.push(10_000, 100.0, 0.4, true);
        r.push(11_000, 100.0, 0.3, true);
        let b = r.burst().unwrap();
        assert!((b.vol_btc - 0.7).abs() < 1e-12, "the print at the window start counts");
        // ...and one millisecond older does not.
        let mut r2 = ring();
        r2.push(9_999, 100.0, 0.4, true);
        r2.push(11_000, 100.0, 0.3, true);
        assert!((r2.burst().unwrap().vol_btc - 0.3).abs() < 1e-12);
    }

    #[test]
    fn burst_splits_volume_by_aggressor_side() {
        let mut r = ring();
        r.push(10_000, 100.0, 0.5, true);
        r.push(10_400, 100.5, 0.3, false);
        r.push(10_900, 101.0, 0.2, true);
        let b = r.burst().unwrap();
        assert!((b.vol_btc - 1.0).abs() < 1e-12);
        assert!((b.buy_btc - 0.7).abs() < 1e-12);
        assert!((b.sell_btc - 0.3).abs() < 1e-12);
    }

    #[test]
    fn window_sums_split_taker_buy_from_total() {
        let mut r = ring();
        r.push(10_000, 100.0, 5.0, true);
        r.push(10_400, 100.0, 3.0, false);
        r.push(10_900, 100.0, 2.0, true);
        let s = r.stats().unwrap();
        assert_eq!(s.tbuy_usd_1s, 700.0);
        assert_eq!(s.notional_usd_1s, 1_000.0);
        assert_eq!(s.vol_btc_1s, 10.0);
        assert_eq!(s.trades_1s, 3);
    }

    #[test]
    fn max_dose_uses_the_latest_print_at_or_before_the_window_start() {
        let mut r = ring();
        r.push(10_000, 100.0, 0.01, true);
        r.push(10_100, 108.0, 0.01, true);
        r.push(11_050, 130.0, 0.01, true);
        // At 11_050 the cutoff is 10_050: the 10_100 print is too new, so the
        // reference is 10_000 -> dose |130 - 100| = 30, not |130 - 108|.
        assert_eq!(r.stats().unwrap().max_dose_20s_usd, 30.0);
    }

    #[test]
    fn max_dose_is_unsigned_and_remembers_past_bursts() {
        let mut r = ring();
        r.push(10_000, 100.0, 0.01, false);
        r.push(11_000, 70.0, 0.01, false); // a -$30 burst
        // 8s of drift later, the current 1s window is quiet...
        r.push(19_000, 70.5, 0.01, true);
        let s = r.stats().unwrap();
        // ...but the burst is still the 20s max, and it counted as |.|.
        assert_eq!(s.max_dose_20s_usd, 30.0);
        assert_eq!(s.trades_1s, 1, "the sums stay a 1s window");
    }

    #[test]
    fn prune_keeps_one_reference_beyond_the_memory_and_bounds_the_ring() {
        let mut r = ring();
        for i in 0..1_000u64 {
            r.push(10_000 + i * 100, 100.0, 0.01, true);
        }
        // 20s memory at one print per 100ms -> ~200 retained + 1 reference.
        assert!(r.trades.len() <= 202, "got {}", r.trades.len());
        let oldest = r.trades.front().unwrap().ts_ms;
        let newest = r.trades.back().unwrap().ts_ms;
        assert!(newest - oldest >= DOSE_MEMORY_MS, "the reference print must survive");
    }

    #[test]
    fn out_of_order_prints_are_rejected_and_never_enter_the_window() {
        let mut r = ring();
        assert!(r.push(10_000, 100.0, 0.4, true));
        assert!(r.push(11_000, 101.0, 0.3, true));
        assert!(!r.push(10_500, 999.0, 99.0, true), "a backwards venue stamp must be rejected");
        let b = r.burst().unwrap();
        assert!((b.vol_btc - 0.7).abs() < 1e-12, "the rejected print must not be summed");
    }

    #[test]
    fn a_quiet_window_after_activity_reports_zeros_not_none() {
        let mut r = ring();
        r.push(10_000, 100.0, 5.0, true);
        r.push(15_000, 100.0, 0.0, true);
        let s = r.stats().unwrap();
        assert_eq!(s.notional_usd_1s, 0.0);
        assert_eq!(s.trades_1s, 1, "the zero-size anchor print itself is in the window");
        assert_eq!(r.burst().unwrap().vol_btc, 0.0);
    }
}
