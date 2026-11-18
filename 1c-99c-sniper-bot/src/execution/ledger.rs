//! Rolling-24h ledger.
//!
//! Debit ring = gross USDC out. Feeds the 24h spend cap (risk gate).
//! Credit ring = MERGE + REDEEM + rebate proceeds. Tracked separately so the
//! cap check never treats contingent future proceeds as headroom.
//! `net_pnl_24h = credits_24h − total_24h` is the reporting view.

use std::collections::VecDeque;

const WINDOW_MICROS: u64 = 24 * 3600 * 1_000_000;

/// Ledger event classes. Separated so P&L can be attributed without
/// conflating trade spend with subsidy income.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EventClass {
    /// A TAKER or MAKER buy — debits USDC.
    TradeSpend,
    /// A REDEEM or MERGE credit on winning / paired shares — credits USDC.
    TradeCredit,
    /// MAKER_REBATE from Polymarket's taker-fee pool — credits USDC.
    Rebate,
    /// Polymarket Liquidity Incentive Program (LIP) accrual — credits USDC.
    Lip,
    /// Taker fee paid (in USD). Tracked separately from TradeSpend so the
    /// rebate task can consume the true fee total rather than approximating
    /// fees as 0.07 × gross_spend. Routed to the legacy spend counter so
    /// can_spend() continues to treat fees as cap-consuming — a pure
    /// accounting-polish split, not a semantics change to the cap.
    FeePaid,
    /// Gas paid on-chain for order placement, cancellation, MERGE, or REDEEM
    /// transactions. Live-mode only pays gas on MERGE/REDEEM (order book ops
    /// are off-chain on Polymarket's CLOB), but we record gas uniformly so the
    /// paper-mode calibration per `docs/wallet-951bd-factcheck-20260423-v2.md
    /// §7.9` can be audited against live on-chain reality. Routes to the
    /// legacy 24h spend counter (same as `FeePaid`) so `can_spend()` treats
    /// gas as inventory-cap-consuming.
    GasSpend,
    /// MERGE event; `amount` carries the pair count burned (not USD). Used
    /// by the health snapshot to surface `merges_24h` (count) and
    /// `pairs_burned_24h` (sum). Routes to neither debit nor credit ring —
    /// a metric-only class, recorded via `record_class`.
    MergeBurn,
}

/// A single tagged ledger entry. `amount` is always non-negative; the sign
/// on net P&L derives from `class` (TradeSpend → -, others → +).
#[derive(Clone, Copy, Debug)]
pub struct EntryDetail {
    pub class: EventClass,
    pub amount: f64,
    pub ts_micros: u64,
}

pub struct SpendLedger {
    cap_usd: f64,
    entries: VecDeque<(u64, f64)>,
    total: f64,
    credits: VecDeque<(u64, f64)>,
    credits_total: f64,
    tagged: Vec<EntryDetail>,
}

impl SpendLedger {
    pub fn new(cap_usd: f64) -> Self {
        Self {
            cap_usd,
            entries: VecDeque::new(),
            total: 0.0,
            credits: VecDeque::new(),
            credits_total: 0.0,
            tagged: Vec::new(),
        }
    }

    pub fn cap_usd(&self) -> f64 {
        self.cap_usd
    }

    pub fn can_spend(&mut self, usd: f64, now_micros: u64) -> bool {
        self.purge(now_micros);
        self.total + usd <= self.cap_usd
    }

    pub fn record(&mut self, usd: f64, ts_micros: u64) {
        self.entries.push_back((ts_micros, usd));
        self.total += usd;
    }

    pub fn total_24h(&mut self, now_micros: u64) -> f64 {
        self.purge(now_micros);
        self.total
    }

    /// Credit a payout (MERGE, REDEEM, rebate). Does not offset the spend cap.
    pub fn credit(&mut self, usd: f64, ts_micros: u64) {
        self.credits.push_back((ts_micros, usd));
        self.credits_total += usd;
    }

    pub fn credits_24h(&mut self, now_micros: u64) -> f64 {
        self.purge(now_micros);
        self.credits_total
    }

    pub fn net_pnl_24h(&mut self, now_micros: u64) -> f64 {
        self.purge(now_micros);
        self.credits_total - self.total
    }

    /// Record a tagged entry and mirror into the legacy counters so existing
    /// metrics (can_spend / net_pnl_24h / credits_24h) still work.
    pub fn record_class(&mut self, class: EventClass, amount: f64, ts_micros: u64) {
        self.tagged.push(EntryDetail {
            class,
            amount,
            ts_micros,
        });
        match class {
            // FeePaid + GasSpend both hit the legacy spend counter so can_spend()
            // keeps treating them as cap-consuming. Pure accounting split.
            EventClass::TradeSpend | EventClass::FeePaid | EventClass::GasSpend => {
                self.record(amount, ts_micros)
            }
            EventClass::TradeCredit | EventClass::Rebate | EventClass::Lip => {
                self.credit(amount, ts_micros)
            }
            // Metric-only: pair-count carried in `amount`. Doesn't route to
            // debit or credit rings — surfaced via `total_class` /
            // `count_class` for health snapshot consumption only.
            EventClass::MergeBurn => {}
        }
    }

    /// Record a tagged entry WITHOUT mirroring into the legacy debit/credit
    /// rings (compare `record_class`, which mirrors). For consumers that feed
    /// the rings on a different clock than the tagged P&L view — e.g. a maker
    /// that debits the spend CAP at fill time but recognises P&L (both legs,
    /// same timestamp) only at slot classification, so `net_trade_pnl_24h_at`
    /// never reads an in-flight fill as a transient gross loss.
    pub fn record_tagged(&mut self, class: EventClass, amount: f64, ts_micros: u64) {
        self.tagged.push(EntryDetail {
            class,
            amount,
            ts_micros,
        });
    }

    /// Number of entries of `class` whose timestamps fall inside the rolling
    /// 24h window ending at `now_micros`. Companion to `total_class`.
    pub fn count_class(&self, class: EventClass, now_micros: u64) -> usize {
        let cutoff = now_micros.saturating_sub(WINDOW_MICROS);
        self.tagged
            .iter()
            .filter(|e| e.class == class && e.ts_micros >= cutoff)
            .count()
    }

    /// Sum of all entries of a given class within the 24 h window ending at
    /// `now_micros`. O(n) over `tagged` — for the expected paper-mode traffic
    /// (≤10 k entries/day) that's negligible.
    pub fn total_class(&self, class: EventClass, now_micros: u64) -> f64 {
        let cutoff = now_micros.saturating_sub(WINDOW_MICROS);
        self.tagged
            .iter()
            .filter(|e| e.class == class && e.ts_micros >= cutoff)
            .map(|e| e.amount)
            .sum()
    }

    /// Net P&L within the 24 h window ending at `now_micros`, with sign taken
    /// from class. Unlike `net_pnl_24h` (which uses the legacy counters), this
    /// view attributes income to its source: TradeCredit / Rebate / Lip add;
    /// TradeSpend and FeePaid subtract.
    ///
    /// **Reporting view.** Use `net_trade_pnl_24h_at` for the daily-kill
    /// predicate — LIP credits subsidising trade losses must not delay the
    /// kill (strategy doc §17.5 / §16).
    pub fn net_pnl_24h_at(&self, now_micros: u64) -> f64 {
        let cutoff = now_micros.saturating_sub(WINDOW_MICROS);
        self.tagged
            .iter()
            .filter(|e| e.ts_micros >= cutoff)
            .map(|e| match e.class {
                EventClass::TradeSpend | EventClass::FeePaid | EventClass::GasSpend => -e.amount,
                // MergeBurn carries pair count, not USD — exclude from net P&L.
                EventClass::MergeBurn => 0.0,
                EventClass::TradeCredit | EventClass::Rebate | EventClass::Lip => e.amount,
            })
            .sum()
    }

    /// Trade-leg net P&L within the rolling 24 h window — **EXCLUDES `Lip`**.
    ///
    /// This is the view the daily kill switch (`src/risk.rs`) consumes:
    /// strategy doc §17.5 / §16 specifies the −$10 kill fires on trade-leg
    /// P&L *only*; LIP rewards are subsidy income and must not paper over a
    /// directional bleed. Rebates remain on the trade-leg side per spec
    /// (they're the trade-induced rebate share, not a separate income stack).
    ///
    /// Adds: `TradeCredit`, `Rebate`.
    /// Subtracts: `TradeSpend`, `FeePaid`, `GasSpend`.
    /// Excludes: `Lip`, `MergeBurn`.
    pub fn net_trade_pnl_24h_at(&self, now_micros: u64) -> f64 {
        let cutoff = now_micros.saturating_sub(WINDOW_MICROS);
        self.tagged
            .iter()
            .filter(|e| e.ts_micros >= cutoff)
            .map(|e| match e.class {
                EventClass::TradeSpend | EventClass::FeePaid | EventClass::GasSpend => -e.amount,
                EventClass::TradeCredit | EventClass::Rebate => e.amount,
                // LIP is subsidy income; excluded from the trade-leg view so
                // it cannot paper over a directional bleed.
                EventClass::Lip => 0.0,
                EventClass::MergeBurn => 0.0,
            })
            .sum()
    }

    fn purge(&mut self, now_micros: u64) {
        let cutoff = now_micros.saturating_sub(WINDOW_MICROS);
        while let Some(&(ts, usd)) = self.entries.front() {
            if ts < cutoff {
                self.entries.pop_front();
                self.total -= usd;
            } else {
                break;
            }
        }
        while let Some(&(ts, usd)) = self.credits.front() {
            if ts < cutoff {
                self.credits.pop_front();
                self.credits_total -= usd;
            } else {
                break;
            }
        }
        if self.entries.is_empty() {
            self.total = 0.0;
        }
        if self.credits.is_empty() {
            self.credits_total = 0.0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    fn secs(s: u64) -> u64 {
        s * 1_000_000
    }

    #[test]
    fn allows_up_to_cap() {
        let mut l = SpendLedger::new(100.0);
        assert!(l.can_spend(50.0, secs(0)));
        l.record(50.0, secs(0));
        assert!(l.can_spend(50.0, secs(0)));
        l.record(50.0, secs(0));
        assert_abs_diff_eq!(l.total_24h(secs(0)), 100.0, epsilon = 1e-9);
    }

    #[test]
    fn denies_over_cap() {
        let mut l = SpendLedger::new(100.0);
        l.record(80.0, secs(0));
        assert!(!l.can_spend(25.0, secs(0)));
        assert!(l.can_spend(20.0, secs(0)));
    }

    #[test]
    fn rolls_off_after_24h() {
        let mut l = SpendLedger::new(100.0);
        l.record(50.0, secs(0));
        assert_abs_diff_eq!(l.total_24h(secs(3600)), 50.0, epsilon = 1e-9);
        assert_abs_diff_eq!(l.total_24h(secs(24 * 3600)), 50.0, epsilon = 1e-9);
        assert_abs_diff_eq!(l.total_24h(secs(24 * 3600 + 1)), 0.0, epsilon = 1e-9);
    }

    #[test]
    fn multiple_rollouts_stay_consistent() {
        let mut l = SpendLedger::new(1000.0);
        for i in 0..48 {
            l.record(10.0, secs(i * 3600));
        }
        let total = l.total_24h(secs(48 * 3600));
        assert_abs_diff_eq!(total, 240.0, epsilon = 1e-9);
    }

    #[test]
    fn empty_ledger_reports_zero() {
        let mut l = SpendLedger::new(100.0);
        assert_eq!(l.total_24h(secs(1_000_000)), 0.0);
    }

    #[test]
    fn credit_accumulates_independent_of_debit_cap() {
        let mut l = SpendLedger::new(100.0);
        l.record(40.0, secs(0));
        l.credit(10.0, secs(0));
        assert!(l.can_spend(50.0, secs(0)));
        assert!(!l.can_spend(61.0, secs(0)));
        assert_abs_diff_eq!(l.total_24h(secs(0)), 40.0, epsilon = 1e-9);
        assert_abs_diff_eq!(l.credits_24h(secs(0)), 10.0, epsilon = 1e-9);
        assert_abs_diff_eq!(l.net_pnl_24h(secs(0)), -30.0, epsilon = 1e-9);
    }

    #[test]
    fn record_tagged_feeds_the_pnl_view_but_never_the_rings() {
        let mut l = SpendLedger::new(100.0);
        l.record(49.5, secs(0)); // cap ring fed at fill time by the caller…
        l.record_tagged(EventClass::TradeSpend, 49.5, secs(40)); // …P&L at classification
        l.record_tagged(EventClass::TradeCredit, 50.0, secs(40));
        // tagged view: net realized +0.5
        assert_abs_diff_eq!(l.net_trade_pnl_24h_at(secs(100)), 0.5, epsilon = 1e-9);
        // rings: exactly ONE debit (no double-count against the cap), no credit
        assert_abs_diff_eq!(l.total_24h(secs(100)), 49.5, epsilon = 1e-9);
        assert_abs_diff_eq!(l.credits_24h(secs(100)), 0.0, epsilon = 1e-9);
    }

    #[test]
    fn credits_roll_off_after_24h() {
        let mut l = SpendLedger::new(1000.0);
        l.credit(25.0, secs(0));
        assert_abs_diff_eq!(l.credits_24h(secs(24 * 3600)), 25.0, epsilon = 1e-9);
        assert_abs_diff_eq!(l.credits_24h(secs(24 * 3600 + 1)), 0.0, epsilon = 1e-9);
    }

    #[test]
    fn net_pnl_is_credits_minus_debits() {
        let mut l = SpendLedger::new(1000.0);
        l.record(30.0, secs(0));
        l.record(20.0, secs(10));
        l.credit(15.0, secs(20));
        l.credit(40.0, secs(30));
        assert_abs_diff_eq!(l.net_pnl_24h(secs(100)), 55.0 - 50.0, epsilon = 1e-9);
    }

    #[test]
    fn sums_trade_separately_from_rebate() {
        let mut l = SpendLedger::new(1_000.0);
        l.record_class(EventClass::TradeSpend, 100.0, 1_000);
        l.record_class(EventClass::Rebate, 5.0, 1_000);
        l.record_class(EventClass::Lip, 20.0, 1_000);
        assert_abs_diff_eq!(
            l.total_class(EventClass::TradeSpend, 2_000),
            100.0,
            epsilon = 1e-9
        );
        assert_abs_diff_eq!(
            l.total_class(EventClass::Rebate, 2_000),
            5.0,
            epsilon = 1e-9
        );
        assert_abs_diff_eq!(l.total_class(EventClass::Lip, 2_000), 20.0, epsilon = 1e-9);
    }

    #[test]
    fn net_pnl_includes_all_classes() {
        let mut l = SpendLedger::new(1_000.0);
        l.record_class(EventClass::TradeSpend, 100.0, 1_000); // -100
        l.record_class(EventClass::TradeCredit, 90.0, 1_000); // +90
        l.record_class(EventClass::Rebate, 5.0, 1_000); // +5
        l.record_class(EventClass::Lip, 20.0, 1_000); // +20
                                                      // Net = -100 + 90 + 5 + 20 = +15
        assert_abs_diff_eq!(l.net_pnl_24h_at(2_000), 15.0, epsilon = 1e-9);
    }

    /// F16 (strategy doc §17.5 / §16): the daily kill predicate reads
    /// `net_trade_pnl_24h_at`, which EXCLUDES `EventClass::Lip` so subsidy
    /// income cannot delay the kill when trade-leg drifts below threshold.
    /// Rebates remain on the trade side.
    #[test]
    fn net_trade_pnl_excludes_lip_but_includes_rebate() {
        let mut l = SpendLedger::new(1_000.0);
        l.record_class(EventClass::TradeSpend, 50.0, 1_000); // -50  (trade)
        l.record_class(EventClass::FeePaid, 2.0, 1_000); //  -2  (trade)
        l.record_class(EventClass::GasSpend, 1.0, 1_000); //  -1  (trade)
        l.record_class(EventClass::TradeCredit, 10.0, 1_000); // +10  (trade)
        l.record_class(EventClass::Rebate, 4.0, 1_000); //  +4  (trade)
        l.record_class(EventClass::Lip, 45.0, 1_000); // +45  (SUBSIDY — must NOT count)
        l.record_class(EventClass::MergeBurn, 12.0, 1_000); //   0  (pair count, not USD)

        // Trade-leg net = -50 -2 -1 +10 +4 = -39 (LIP excluded).
        assert_abs_diff_eq!(l.net_trade_pnl_24h_at(2_000), -39.0, epsilon = 1e-9);
        // Reporting view (includes LIP) = -50 -2 -1 +10 +4 +45 = +6.
        assert_abs_diff_eq!(l.net_pnl_24h_at(2_000), 6.0, epsilon = 1e-9);
        // Critical safety property: at the −$10 kill threshold, the reporting
        // view would NOT trip (it's +6), but the trade-leg view DOES (−39).
        // The kill predicate must use the trade-leg view.
        let kill_threshold = -10.0;
        assert!(
            l.net_trade_pnl_24h_at(2_000) < kill_threshold,
            "trade-leg view (−39) should trip the −$10 kill"
        );
        assert!(
            l.net_pnl_24h_at(2_000) >= kill_threshold,
            "reporting view (+6) would NOT trip — proves the bug fix matters"
        );
    }

    /// Regression for the rolling 24h window on the trade-leg view.
    #[test]
    fn net_trade_pnl_respects_24h_window() {
        let mut l = SpendLedger::new(1_000.0);
        // 25h-old TradeSpend rolls out of window.
        l.record_class(EventClass::TradeSpend, 50.0, 1_000);
        let now = 1_000 + 25 * 3_600 * 1_000_000;
        assert_abs_diff_eq!(l.net_trade_pnl_24h_at(now), 0.0, epsilon = 1e-9);
    }

    #[test]
    fn total_class_respects_24h_window() {
        let mut l = SpendLedger::new(1_000.0);
        // 25h old — outside the 24h window.
        l.record_class(EventClass::Rebate, 50.0, 1_000);
        let now = 1_000 + 25 * 3_600 * 1_000_000;
        assert_abs_diff_eq!(l.total_class(EventClass::Rebate, now), 0.0, epsilon = 1e-9);
    }

    #[test]
    fn record_class_mirrors_into_legacy_counters() {
        // Back-compat: record_class(TradeSpend) must still count as `record`
        // for can_spend / total_24h. record_class(TradeCredit|Rebate|Lip) must
        // credit the legacy ledger too.
        let mut l = SpendLedger::new(100.0);
        l.record_class(EventClass::TradeSpend, 40.0, 1_000);
        assert_abs_diff_eq!(l.total_24h(2_000), 40.0, epsilon = 1e-9);
        l.record_class(EventClass::TradeCredit, 15.0, 1_000);
        assert_abs_diff_eq!(l.credits_24h(2_000), 15.0, epsilon = 1e-9);
        assert_abs_diff_eq!(l.net_pnl_24h(2_000), 15.0 - 40.0, epsilon = 1e-9);
    }

    #[test]
    fn ledger_event_class_fee_paid_variant_exists() {
        let mut l = SpendLedger::new(1_000.0);
        l.record_class(EventClass::FeePaid, 1.25, 1_000);
        assert!((l.total_class(EventClass::FeePaid, 2_000) - 1.25).abs() < 1e-9);
    }

    #[test]
    fn ledger_fee_paid_is_separate_from_trade_spend() {
        let mut l = SpendLedger::new(1_000.0);
        l.record_class(EventClass::TradeSpend, 50.0, 1_000);
        l.record_class(EventClass::FeePaid, 1.0, 1_000);
        assert!((l.total_class(EventClass::TradeSpend, 2_000) - 50.0).abs() < 1e-9);
        assert!((l.total_class(EventClass::FeePaid, 2_000) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn ledger_fee_paid_hits_legacy_spend_cap() {
        // Option A choice: FeePaid routes to the 24h spend counter so can_spend()
        // continues to treat fees as part of inventory-cap budgeting.
        let mut l = SpendLedger::new(1_000.0);
        l.record_class(EventClass::FeePaid, 25.0, 1_000);
        assert!((l.total_24h(2_000) - 25.0).abs() < 1e-9);
    }

    #[test]
    fn ledger_fee_paid_subtracts_from_tagged_net_pnl() {
        let mut l = SpendLedger::new(10_000.0);
        l.record_class(EventClass::TradeCredit, 100.0, 1_000);
        l.record_class(EventClass::FeePaid, 10.0, 1_000);
        // credits(100) - fee(10) = 90
        assert!((l.net_pnl_24h_at(2_000) - 90.0).abs() < 1e-9);
    }

    #[test]
    fn ledger_event_class_gas_spend_variant_exists() {
        let mut l = SpendLedger::new(1_000.0);
        l.record_class(EventClass::GasSpend, 0.002, 1_000);
        assert!((l.total_class(EventClass::GasSpend, 2_000) - 0.002).abs() < 1e-9);
    }

    #[test]
    fn ledger_gas_spend_hits_legacy_spend_cap() {
        // Same choice as FeePaid (Option A): gas routes to the 24h cap so
        // can_spend() treats gas as part of the inventory budget.
        let mut l = SpendLedger::new(1_000.0);
        l.record_class(EventClass::GasSpend, 0.5, 1_000);
        assert!((l.total_24h(2_000) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn ledger_gas_spend_subtracts_from_tagged_net_pnl() {
        let mut l = SpendLedger::new(10_000.0);
        l.record_class(EventClass::TradeCredit, 100.0, 1_000);
        l.record_class(EventClass::GasSpend, 2.0, 1_000);
        // credits(100) - gas(2) = 98
        assert!((l.net_pnl_24h_at(2_000) - 98.0).abs() < 1e-9);
    }

    #[test]
    fn count_class_returns_count_of_class_in_window() {
        let mut l = SpendLedger::new(1000.0);
        let now = 100_000_000;
        l.record_class(EventClass::MergeBurn, 50.0, now);
        l.record_class(EventClass::MergeBurn, 75.0, now);
        l.record_class(EventClass::Lip, 10.0, now);
        assert_eq!(l.count_class(EventClass::MergeBurn, now + 1_000_000), 2);
        assert_eq!(l.count_class(EventClass::Lip, now + 1_000_000), 1);
        // Outside window:
        assert_eq!(
            l.count_class(EventClass::MergeBurn, now + WINDOW_MICROS + 1),
            0
        );
    }

    #[test]
    fn merge_burn_does_not_credit_or_debit() {
        // MergeBurn is a metric-only class: amount carries pair count, must not
        // affect can_spend() / credits_24h() / net_pnl_24h_at() (the dollar-pnl
        // view).
        let mut l = SpendLedger::new(1_000.0);
        l.record_class(EventClass::MergeBurn, 100.0, 1_000);
        assert_eq!(l.total_24h(2_000), 0.0, "MergeBurn must not hit debit ring");
        assert_eq!(
            l.credits_24h(2_000),
            0.0,
            "MergeBurn must not hit credit ring"
        );
        assert_eq!(
            l.net_pnl_24h_at(2_000),
            0.0,
            "MergeBurn must not affect net P&L"
        );
        // But total_class should surface the pair count.
        assert!((l.total_class(EventClass::MergeBurn, 2_000) - 100.0).abs() < 1e-9);
    }
}
