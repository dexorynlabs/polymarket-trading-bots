//! Per-market share accounting. In-memory only; no durable store in
//! paper mode.

use crate::types::{Coin, Fill, RedeemCredit, Side};
use std::collections::HashMap;

/// In-flight on-chain MERGE state. While `Some`, `effective_paired_shares`
/// subtracts `pairs` from both sides so reconcile does not requote against
/// pending-merged inventory and `merge_eligible` does not double-merge.
/// See spec §3 / Patch E "merge_in_flight state machine".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeInProgress {
    pub pairs: u64,
    /// Path (a): on-chain TxHash hex. Path (b): relayer correlation id.
    pub tx_hash: String,
    pub submitted_at_ms: u64,
}

#[derive(Clone, Debug)]
pub struct Position {
    pub slug: String,
    pub coin: Coin,
    pub up_shares: f64,
    pub up_usd: f64,
    pub down_shares: f64,
    pub down_usd: f64,
    pub fills: u64,
    /// Highest paired-share count at which we last fired a MERGE for this
    /// market. Used by the post-fill MERGE trigger predicate to ensure
    /// idempotency across rapid-fire fills inside a single tokio tick.
    /// Reset to 0 by default; updated by `apply_merge` on success.
    pub last_merge_pair_count: f64,
    /// Wall-clock micros at which we last successfully MERGEd this market.
    /// Read by `merge_eligible` (merge.rs) to enforce `MergeCfg::cooldown_secs`.
    ///
    /// **Sentinel:** `0` means "never merged" — `merge_eligible` skips the
    /// cooldown gate in that case. To preserve this invariant on a real merge
    /// at wall-clock `ts == 0`, `apply_merge` records `ts_micros.max(1)` so a
    /// successful merge always lands a non-zero timestamp.
    pub last_merge_ts_micros: u64,
    pub merge_in_flight: Option<MergeInProgress>,
    /// On-chain CTF condition_id (`0x`-prefixed 32-byte hex). Required by
    /// [`crate::execution::merger_onchain::OnChainMerger::merge`] (Phase 12). Fresh
    /// Position starts empty; lifecycle (`spawn_market`) / `record_fill` is
    /// expected to populate it via [`Position::set_condition_id`] using
    /// [`crate::types::MarketMeta::condition_id`] before the post-fill MERGE
    /// trigger fires. Empty string is the unambiguous "not yet populated"
    /// sentinel — `merge_now_onchain` checks for this and short-circuits.
    pub condition_id: String,
    /// T12 (strategy-agnostic): wall-clock ms at which a REDEEM was SUBMITTED
    /// for this slug (relayer accepted the payload; NOT on-chain confirmation). `None` = never
    /// submitted. Lets shutdown reports split "pending settlement" from "stranded — manual
    /// REDEEM required". Set via `PositionStore::settle`.
    pub redeem_submitted_at_ms: Option<u64>,
    /// Wall-clock micros at which the slug's underlying market started
    /// (= `MarketMeta::market_start_ts × 1_000_000`). Set by the quoter
    /// the first time it has both a Position and a MarketMeta in scope
    /// (alongside `set_condition_id`). Default `0` is the "not yet
    /// populated" sentinel — `merge_eligible`'s first-merge gate skips
    /// when this is 0 to remain test-permissive.
    pub market_start_ts_micros: u64,
}

impl Position {
    fn new(slug: String, coin: Coin) -> Self {
        Self {
            slug,
            coin,
            up_shares: 0.0,
            up_usd: 0.0,
            down_shares: 0.0,
            down_usd: 0.0,
            fills: 0,
            last_merge_pair_count: 0.0,
            last_merge_ts_micros: 0,
            merge_in_flight: None,
            condition_id: String::new(),
            redeem_submitted_at_ms: None,
            market_start_ts_micros: 0,
        }
    }

    /// Test-only constructor used by integration test crates that need a
    /// blank `Position` to drive trigger-state tests against `Guards`.
    /// Production code uses `PositionStore::record_fill` which auto-creates
    /// via the private `new`.
    #[doc(hidden)]
    pub fn test_new(slug: String, coin: Coin) -> Self {
        Self::new(slug, coin)
    }

    /// Populate the on-chain `condition_id`. Idempotent — overwrites any
    /// prior value. Called by lifecycle / submitter once the slug is paired
    /// with its `MarketMeta`.
    pub fn set_condition_id(&mut self, id: String) {
        self.condition_id = id;
    }

    /// Populate `market_start_ts_micros` from a seconds-precision wall-clock
    /// `market_start_ts`. Idempotent — overwrites any prior value. Called
    /// by the quoter (alongside `set_condition_id`) once the slug is
    /// paired with its `MarketMeta`. Pass the meta's
    /// `market_start_ts: u64` in seconds; the conversion to micros is done
    /// here so callers don't need to remember it.
    pub fn set_market_start_ts(&mut self, market_start_ts_secs: u64) {
        self.market_start_ts_micros = market_start_ts_secs.saturating_mul(1_000_000);
    }

    /// Borrow the on-chain `condition_id`. Empty string when not yet populated.
    pub fn condition_id(&self) -> &str {
        &self.condition_id
    }

    pub fn total_usd(&self) -> f64 {
        self.up_usd + self.down_usd
    }

    pub fn set_merge_in_flight(&mut self, pairs: u64, tx_hash: String, submitted_at_ms: u64) {
        self.merge_in_flight = Some(MergeInProgress {
            pairs,
            tx_hash,
            submitted_at_ms,
        });
    }

    pub fn clear_merge_in_flight(&mut self) {
        self.merge_in_flight = None;
    }

    pub fn effective_paired_shares(&self) -> (f64, f64) {
        let pending = self
            .merge_in_flight
            .as_ref()
            .map(|m| m.pairs as f64)
            .unwrap_or(0.0);
        (
            (self.up_shares - pending).max(0.0),
            (self.down_shares - pending).max(0.0),
        )
    }
}

#[derive(Default)]
pub struct PositionStore {
    by_slug: HashMap<String, Position>,
    total_fills: u64,
    /// Per-slug timestamp (micros) past which MERGE is allowed. Populated by
    /// `mark_asks_active_until` when the quoter places pre-MERGE asks; MERGE
    /// daemon checks with `is_merge_deferred` before consuming a pair.
    asks_active_until: HashMap<String, u64>,
}

impl PositionStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_fill(&mut self, fill: &Fill) {
        let p = self
            .by_slug
            .entry(fill.slug.clone())
            .or_insert_with(|| Position::new(fill.slug.clone(), fill.coin));
        p.fills += 1;

        // Update one side's shares and usd. Buys grow both; sells reduce shares
        // and scale usd proportionally so the avg basis on remaining shares is
        // preserved (mirrors apply_merge's pattern at position.rs:113-130).
        // Fix for audit-20260423 §C1 position half.
        fn update_side(shares: &mut f64, usd: &mut f64, fill_shares: f64, fill_usd: f64) {
            if fill_shares > 0.0 {
                *shares += fill_shares;
                *usd += fill_usd;
            } else if fill_shares < 0.0 {
                let old_shares = *shares;
                if old_shares <= 0.0 {
                    // Sell with no prior position: drop the position-side effect.
                    // The ledger still records proceeds via record_fill_to_ledger;
                    // the position store just has nothing to decrement.
                    return;
                }
                let avg = *usd / old_shares;
                let new_shares = (old_shares + fill_shares).max(0.0);
                *shares = new_shares;
                *usd = new_shares * avg;
                // Clamp tiny float residue when the side is fully cleared,
                // mirroring apply_merge's pattern (position.rs:161-168) so
                // sub-1e-9 residue doesn't look like a live position to
                // downstream gates (e.g. `shares > 0.0`).
                if shares.abs() < 1e-9 {
                    *shares = 0.0;
                    *usd = 0.0;
                }
            }
        }

        match fill.side {
            Side::Up => update_side(&mut p.up_shares, &mut p.up_usd, fill.shares, fill.usd),
            Side::Down => update_side(&mut p.down_shares, &mut p.down_usd, fill.shares, fill.usd),
        }
        self.total_fills += 1;
    }

    /// T12: mark a slug's REDEEM as SUBMITTED (`submitted_at_ms`) — distinct from
    /// `settle` (which credits the winner and zeroes the position). The flag survives the zeroing
    /// so shutdown reports can list the slug as PENDING-SETTLEMENT (awaiting on-chain credit)
    /// instead of STRANDED. No-op if the slug has no position.
    pub fn mark_redeem_submitted(&mut self, slug: &str, submitted_at_ms: u64) {
        if let Some(p) = self.get_mut(slug) {
            p.redeem_submitted_at_ms = Some(submitted_at_ms);
        }
    }

    pub fn get(&self, slug: &str) -> Option<&Position> {
        self.by_slug.get(slug)
    }

    /// Mutable accessor for the underlying `slug → Position` map. Used by
    /// strategy-side test helpers (`recon::tests::*`) that need to seed
    /// state cross-crate. `#[cfg(test)]` would hide this from those tests
    /// since the cfg is per-crate; the method is otherwise harmless.
    pub fn by_slug_mut(&mut self) -> &mut std::collections::HashMap<String, Position> {
        &mut self.by_slug
    }

    /// Mutable accessor for a single Position. Used by Phase 12's
    /// `merge_now_onchain` (and the `wp_end_cancel` arm) to set/clear
    /// `merge_in_flight` and to populate `condition_id` from `MarketMeta`
    /// just before the relayer call. Returns `None` for unknown slugs.
    pub fn get_mut(&mut self, slug: &str) -> Option<&mut Position> {
        self.by_slug.get_mut(slug)
    }

    /// Eagerly populate `Position::market_start_ts_micros` and `condition_id`
    /// at slot-spawn time, creating the Position if it doesn't yet exist.
    /// Called by `lifecycle::spawn_market` so the merge daemon's first
    /// `merge_eligible` call sees `market_start_ts_micros != 0` and runs
    /// the first-merge defensive gate (instead of skipping it with a WARN).
    ///
    /// Without this eager populate, `record_fill` creates the Position
    /// lazily with `market_start_ts_micros = 0`, the merge daemon ticks
    /// before the quoter's lazy setters in `on_fill` / `wp_end_cancel` can
    /// run, and the first-merge gate is silently disabled in production
    /// (`merge.rs::merge_eligible` line 384 WARN). See
    /// docs/runbooks/phase8-step1-2026-05-11.md.
    ///
    /// Idempotent: skips set when the field is already populated (any
    /// non-zero `market_start_ts_micros`, any non-empty `condition_id`).
    /// Safe to call from re-spawn paths.
    pub fn ensure_market_metadata(
        &mut self,
        slug: &str,
        coin: Coin,
        market_start_ts_secs: u64,
        condition_id: &str,
    ) {
        let p = self
            .by_slug
            .entry(slug.to_string())
            .or_insert_with(|| Position::new(slug.to_string(), coin));
        if p.market_start_ts_micros == 0 {
            p.set_market_start_ts(market_start_ts_secs);
        }
        if p.condition_id().is_empty() && !condition_id.is_empty() {
            p.set_condition_id(condition_id.to_string());
        }
    }

    /// Aggregate bilateral imbalance across all active positions
    /// (`Σ up_shares − Σ down_shares`). Spec §A.4 telemetry — the
    /// downstream sampler computes p50/p99 of this quantity over time.
    pub fn aggregate_bilateral_imbalance(&self) -> f64 {
        let (up, dn): (f64, f64) = self.by_slug.values().fold((0.0_f64, 0.0_f64), |(u, d), p| {
            (u + p.up_shares, d + p.down_shares)
        });
        up - dn
    }

    pub fn markets(&self) -> usize {
        self.by_slug.len()
    }

    pub fn total_fills(&self) -> u64 {
        self.total_fills
    }

    pub fn total_usd(&self) -> f64 {
        self.by_slug.values().map(|p| p.total_usd()).sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Position)> {
        self.by_slug.iter()
    }

    /// All slugs currently tracked in the store (any inventory state, including
    /// dormant `up=down=0` slugs that may still hold open orders / pending
    /// settlement bookkeeping).
    ///
    /// Used by the Phase 5 capital `Sweeper` task as the candidate set for its
    /// 5-second passive-merge tick: each slug is then re-checked under lock for
    /// `min(up_shares, down_shares) ≥ min_pair_size_to_merge_shares` before
    /// the relayer call. Returning every known slug (rather than pre-filtering
    /// for ≥0 paired inventory) keeps this method allocation-O(n) and lets the
    /// Sweeper observe condition_id population state in the same lock window
    /// as the inventory snapshot — preventing a TOCTOU window where a slug
    /// appears mergeable here, then is settled before the lock re-acquisition.
    pub fn active_slugs(&self) -> Vec<String> {
        self.by_slug.keys().cloned().collect()
    }

    /// Credit the winning side's shares as $1/share, then zero both sides.
    /// Returns `None` if the slug has no position recorded.
    pub fn settle(&mut self, slug: &str, winner: Side, ts_micros: u64) -> Option<RedeemCredit> {
        let p = self.by_slug.get_mut(slug)?;
        let (shares, coin) = match winner {
            Side::Up => (p.up_shares, p.coin),
            Side::Down => (p.down_shares, p.coin),
        };
        let usd = shares;
        p.up_shares = 0.0;
        p.up_usd = 0.0;
        p.down_shares = 0.0;
        p.down_usd = 0.0;
        // Reset the post-merge watermark so the dormant Position invariant
        // (`min(up,down) == 0` ⇒ `last_merge_pair_count == 0`) holds even
        // if a slug is ever recycled. Matches the post-clamp write in
        // `apply_merge` for the full-burn path.
        p.last_merge_pair_count = 0.0;
        Some(RedeemCredit {
            slug: slug.to_string(),
            coin,
            winner,
            shares,
            usd,
            ts_micros,
        })
    }

    /// Settle a venue-initiated `auto_redeem` (06-12): join the payout to a
    /// slug via `Position::condition_id` (0x-prefix and case insensitive),
    /// infer the winner as the side whose share count is nearest the $1×sh
    /// `payout_usd`, then mark the redeem submitted (so shutdown reports and
    /// the redeem-lane fallback see the slug as settled) and settle.
    /// `None` when no tracked position carries the condition id — the caller
    /// logs and drops (e.g. a settlement for a previous process's slot).
    pub fn settle_by_condition_id(
        &mut self,
        condition_id: &str,
        payout_usd: f64,
        now_ms: u64,
    ) -> Option<RedeemCredit> {
        fn norm(s: &str) -> String {
            s.trim_start_matches("0x").trim_start_matches("0X").to_ascii_lowercase()
        }
        let want = norm(condition_id);
        let (slug, winner) = self.by_slug.iter().find_map(|(slug, p)| {
            if p.condition_id.is_empty() || norm(&p.condition_id) != want {
                return None;
            }
            let winner = if (p.up_shares - payout_usd).abs() <= (p.down_shares - payout_usd).abs()
            {
                Side::Up
            } else {
                Side::Down
            };
            Some((slug.clone(), winner))
        })?;
        self.mark_redeem_submitted(&slug, now_ms);
        self.settle(&slug, winner, now_ms.saturating_mul(1_000))
    }

    /// Decrement both sides of `slug`'s position by `pairs` shares each, and
    /// reduce each side's tracked `usd` proportionally so the per-share avg
    /// cost basis on the *remaining* shares is preserved.
    ///
    /// Returns `false` if the slug is unknown, `pairs == 0`, either side
    /// holds fewer than `pairs` shares, or either side currently has zero
    /// shares (avg-basis math would divide by zero). All four cases are
    /// caller-side preconditions that `exec::merge::try_merge` already checks
    /// — the guards here are belt-and-braces.
    pub fn apply_merge(&mut self, slug: &str, pairs: u64, ts_micros: u64) -> bool {
        if pairs == 0 {
            return false;
        }
        let Some(p) = self.by_slug.get_mut(slug) else {
            return false;
        };
        if p.up_shares <= 0.0 || p.down_shares <= 0.0 {
            return false;
        }
        let pairs_f = pairs as f64;
        if pairs_f > p.up_shares.min(p.down_shares) + 1e-9 {
            return false;
        }
        let up_cost_burned = p.up_usd * (pairs_f / p.up_shares);
        let dn_cost_burned = p.down_usd * (pairs_f / p.down_shares);
        p.up_shares -= pairs_f;
        p.up_usd -= up_cost_burned;
        p.down_shares -= pairs_f;
        p.down_usd -= dn_cost_burned;
        // Clamp tiny float residue when a side is fully cleared.
        if p.up_shares.abs() < 1e-9 {
            p.up_shares = 0.0;
            p.up_usd = 0.0;
        }
        if p.down_shares.abs() < 1e-9 {
            p.down_shares = 0.0;
            p.down_usd = 0.0;
        }
        // Write the post-merge watermark for idempotency in the post-fill
        // trigger (spec §4.4). Computed AFTER the decrement and zero-clamp so
        // it reflects the true remaining paired-share count.
        p.last_merge_pair_count = p.up_shares.min(p.down_shares).floor();
        // Use max(1) so that a merge at ts=0 still produces a non-zero
        // sentinel, keeping 0 as the unambiguous "never merged" state for
        // merge_eligible's cooldown gate.
        p.last_merge_ts_micros = ts_micros.max(1);
        true
    }

    pub fn mark_asks_active_until(&mut self, slug: &str, ts_micros: u64) {
        self.asks_active_until.insert(slug.to_string(), ts_micros);
    }
    pub fn is_merge_deferred(&self, slug: &str, now_micros: u64) -> bool {
        self.asks_active_until.get(slug).copied().unwrap_or(0) > now_micros
    }
    pub fn clear_ask_defer(&mut self, slug: &str) {
        self.asks_active_until.remove(slug);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::now_micros;

    fn fill(slug: &str, side: Side, shares: f64, usd: f64) -> Fill {
        Fill {
            slug: slug.into(),
            coin: Coin::Eth,
            side,
            token_id: "T".into(),
            id: crate::types::RestingOrderId::from_string("test"),
            shares,
            price: if shares > 0.0 { usd / shares } else { 0.0 },
            usd,
            fee_usd: 0.0,
            ts_micros: now_micros(),
            liquidity_role: crate::types::LiquidityRole::Maker,
        }
    }

    #[test]
    fn accumulates_per_side() {
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("eth-updown-15m-1", Side::Up, 10.0, 5.0));
        ps.record_fill(&fill("eth-updown-15m-1", Side::Up, 20.0, 10.0));
        ps.record_fill(&fill("eth-updown-15m-1", Side::Down, 5.0, 4.0));
        let p = ps.get("eth-updown-15m-1").unwrap();
        assert_eq!(p.up_shares, 30.0);
        assert_eq!(p.up_usd, 15.0);
        assert_eq!(p.down_shares, 5.0);
        assert_eq!(p.down_usd, 4.0);
        assert_eq!(p.fills, 3);
    }

    #[test]
    fn aggregates_across_markets() {
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("a", Side::Up, 1.0, 0.5));
        ps.record_fill(&fill("b", Side::Down, 2.0, 1.0));
        ps.record_fill(&fill("c", Side::Up, 3.0, 1.5));
        assert_eq!(ps.markets(), 3);
        assert_eq!(ps.total_fills(), 3);
        assert_eq!(ps.total_usd(), 3.0);
    }

    #[test]
    fn empty_store_is_zero() {
        let ps = PositionStore::new();
        assert_eq!(ps.markets(), 0);
        assert_eq!(ps.total_fills(), 0);
        assert_eq!(ps.total_usd(), 0.0);
        assert!(ps.get("missing").is_none());
    }

    #[test]
    fn mark_redeem_submitted_survives_settle() {
        // T12: the submitted flag must survive `settle`'s zeroing so shutdown
        // reports can split PENDING-SETTLEMENT from STRANDED.
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("a", Side::Up, 8.0, 0.5));
        ps.mark_redeem_submitted("a", 123);
        let _ = ps.settle("a", Side::Up, 1);
        let p = ps.get("a").expect("position kept after settle");
        assert_eq!(p.redeem_submitted_at_ms, Some(123));
        assert_eq!(p.up_shares, 0.0, "settle zeroes the inventory");
    }

    #[test]
    fn settle_by_condition_id_infers_winner_and_marks_submitted() {
        // 06-12: venue auto_redeem carries (condition_id, amount) but no slug
        // and no winner. The store must join on condition_id (0x/case
        // tolerant), infer winner = side whose share count matches the $1×sh
        // payout, then mark + settle so shutdown reports and the redeem-lane
        // fallback both see the slug as venue-settled.
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("btc-updown-5m-1", Side::Up, 49.99, 25.0));
        ps.record_fill(&fill("btc-updown-5m-1", Side::Down, 54.98, 27.0));
        ps.ensure_market_metadata("btc-updown-5m-1", Coin::Btc, 1, "0xABCdef01");
        let credit = ps
            .settle_by_condition_id("0xabcDEF01", 55.0, 777)
            .expect("condition matches → settle");
        assert_eq!(credit.winner, Side::Down, "55 ≈ 54.98 dn shares");
        assert!((credit.usd - 54.98).abs() < 1e-9);
        let p = ps.get("btc-updown-5m-1").unwrap();
        assert_eq!(p.redeem_submitted_at_ms, Some(777));
        assert_eq!(p.up_shares, 0.0);
        assert_eq!(p.down_shares, 0.0);
    }

    #[test]
    fn settle_by_condition_id_unknown_condition_is_none() {
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("a", Side::Up, 8.0, 4.0));
        ps.ensure_market_metadata("a", Coin::Eth, 1, "0xaa");
        assert!(ps.settle_by_condition_id("0xbb", 8.0, 1).is_none());
        let p = ps.get("a").unwrap();
        assert_eq!(p.up_shares, 8.0, "non-matching settle must not touch inventory");
        assert_eq!(p.redeem_submitted_at_ms, None);
    }

    #[test]
    fn settle_credits_up_shares_and_zeroes_position() {
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("eth-updown-15m-1", Side::Up, 10.0, 5.0));
        ps.record_fill(&fill("eth-updown-15m-1", Side::Down, 3.0, 2.0));
        let credit = ps
            .settle("eth-updown-15m-1", Side::Up, 1_000_000)
            .expect("settle returns credit");
        assert_eq!(credit.shares, 10.0);
        assert_eq!(credit.usd, 10.0);
        assert_eq!(credit.winner, Side::Up);
        let p = ps.get("eth-updown-15m-1").unwrap();
        assert_eq!(p.up_shares, 0.0);
        assert_eq!(p.down_shares, 0.0);
        assert_eq!(p.up_usd, 0.0);
        assert_eq!(p.down_usd, 0.0);
    }

    #[test]
    fn settle_credits_down_shares_when_down_wins() {
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("sol-updown-15m-1", Side::Up, 4.0, 1.0));
        ps.record_fill(&fill("sol-updown-15m-1", Side::Down, 7.5, 3.5));
        let credit = ps
            .settle("sol-updown-15m-1", Side::Down, 1_000_000)
            .expect("settle returns credit");
        assert_eq!(credit.shares, 7.5);
        assert_eq!(credit.usd, 7.5);
    }

    #[test]
    fn settle_missing_slug_returns_none() {
        let mut ps = PositionStore::new();
        assert!(ps.settle("does-not-exist", Side::Up, 0).is_none());
    }

    #[test]
    fn settle_zero_winner_shares_returns_zero_credit() {
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("xrp-updown-15m-1", Side::Down, 5.0, 2.0));
        let credit = ps
            .settle("xrp-updown-15m-1", Side::Up, 1_000_000)
            .expect("settle returns a credit even when shares on winner side are 0");
        assert_eq!(credit.shares, 0.0);
        assert_eq!(credit.usd, 0.0);
        assert_eq!(credit.winner, Side::Up);
        let p = ps.get("xrp-updown-15m-1").unwrap();
        assert_eq!(p.down_shares, 0.0); // loser shares zeroed too
    }

    #[test]
    fn double_settle_is_noop() {
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("btc-updown-15m-1", Side::Up, 8.0, 4.0));
        let first = ps.settle("btc-updown-15m-1", Side::Up, 1_000_000).unwrap();
        assert_eq!(first.shares, 8.0);
        let second = ps.settle("btc-updown-15m-1", Side::Up, 1_000_000).unwrap();
        assert_eq!(second.shares, 0.0);
    }

    #[test]
    fn settle_resets_last_merge_pair_count() {
        // After a partial merge, the Position carries a non-zero watermark.
        // Settle must zero it so the dormant Position invariant
        // (`min(up,down) == 0` ⇒ `last_merge_pair_count == 0`) holds —
        // preventive against any future slug recycling.
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("eth-updown-15m-1", Side::Up, 100.0, 40.0));
        ps.record_fill(&fill("eth-updown-15m-1", Side::Down, 80.0, 40.0));
        assert!(ps.apply_merge("eth-updown-15m-1", 50, 0));
        let p = ps.get("eth-updown-15m-1").unwrap();
        assert!(
            (p.last_merge_pair_count - 30.0).abs() < 1e-9,
            "watermark left at floor(min(50, 30)) = 30 after partial merge",
        );
        ps.settle("eth-updown-15m-1", Side::Up, 1_000_000)
            .expect("settle returns credit");
        let p = ps.get("eth-updown-15m-1").unwrap();
        assert!(
            p.last_merge_pair_count.abs() < 1e-9,
            "settle must reset watermark to 0 (got {})",
            p.last_merge_pair_count,
        );
    }

    #[test]
    fn apply_merge_decrements_both_sides_and_preserves_avg() {
        let mut ps = PositionStore::new();
        // Up: 100 shares for $40 → avg 0.40
        ps.record_fill(&fill("eth-updown-15m-1", Side::Up, 100.0, 40.0));
        // Down: 80 shares for $40 → avg 0.50
        ps.record_fill(&fill("eth-updown-15m-1", Side::Down, 80.0, 40.0));
        let ok = ps.apply_merge("eth-updown-15m-1", 50, 0);
        assert!(ok);
        let p = ps.get("eth-updown-15m-1").unwrap();
        assert!(
            (p.up_shares - 50.0).abs() < 1e-9,
            "up_shares = {}",
            p.up_shares
        );
        assert!(
            (p.down_shares - 30.0).abs() < 1e-9,
            "down_shares = {}",
            p.down_shares
        );
        // Avg basis preserved on remaining: up_usd/up_shares should still be 0.40.
        assert!(
            (p.up_usd / p.up_shares - 0.40).abs() < 1e-9,
            "up avg = {}",
            p.up_usd / p.up_shares
        );
        assert!(
            (p.down_usd / p.down_shares - 0.50).abs() < 1e-9,
            "down avg = {}",
            p.down_usd / p.down_shares
        );
    }

    #[test]
    fn apply_merge_zeroes_a_side_when_pairs_equal_min() {
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("sol-updown-15m-1", Side::Up, 75.0, 30.0)); // avg 0.40
        ps.record_fill(&fill("sol-updown-15m-1", Side::Down, 60.0, 30.0)); // avg 0.50
        let ok = ps.apply_merge("sol-updown-15m-1", 60, 0);
        assert!(ok);
        let p = ps.get("sol-updown-15m-1").unwrap();
        assert!((p.up_shares - 15.0).abs() < 1e-9);
        assert_eq!(p.down_shares, 0.0);
        // After zero shares, usd should also be exactly 0.
        assert_eq!(p.down_usd, 0.0);
        // Up side keeps its avg cost basis intact.
        assert!((p.up_usd - 6.0).abs() < 1e-9, "up_usd = {}", p.up_usd);
    }

    #[test]
    fn apply_merge_returns_false_for_missing_slug() {
        let mut ps = PositionStore::new();
        assert!(!ps.apply_merge("does-not-exist", 1, 0));
    }

    #[test]
    fn apply_merge_returns_false_for_zero_pairs() {
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("xrp-updown-15m-1", Side::Up, 5.0, 2.0));
        ps.record_fill(&fill("xrp-updown-15m-1", Side::Down, 5.0, 2.0));
        assert!(!ps.apply_merge("xrp-updown-15m-1", 0, 0));
        let p = ps.get("xrp-updown-15m-1").unwrap();
        assert_eq!(p.up_shares, 5.0);
        assert_eq!(p.down_shares, 5.0);
        // No-mutation contract: usd untouched on rejection.
        assert_eq!(p.up_usd, 2.0);
        assert_eq!(p.down_usd, 2.0);
    }

    #[test]
    fn apply_merge_returns_false_when_pairs_exceed_min_side() {
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("btc-updown-15m-1", Side::Up, 10.0, 4.0));
        ps.record_fill(&fill("btc-updown-15m-1", Side::Down, 3.0, 1.5));
        // Caller violated the precondition: 5 > min(10, 3). Reject; no mutation.
        assert!(!ps.apply_merge("btc-updown-15m-1", 5, 0));
        let p = ps.get("btc-updown-15m-1").unwrap();
        assert_eq!(p.up_shares, 10.0);
        assert_eq!(p.down_shares, 3.0);
        // No-mutation contract: usd untouched on rejection.
        assert_eq!(p.up_usd, 4.0);
        assert_eq!(p.down_usd, 1.5);
    }

    #[test]
    fn apply_merge_returns_false_when_either_side_has_zero_shares() {
        // Case A: Down side never traded.
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("eth-updown-15m-7", Side::Up, 5.0, 2.0));
        assert!(!ps.apply_merge("eth-updown-15m-7", 1, 0));
        let p = ps.get("eth-updown-15m-7").unwrap();
        assert_eq!(p.up_shares, 5.0);
        assert_eq!(p.down_shares, 0.0);

        // Case B: Up side never traded (symmetric guard branch).
        ps.record_fill(&fill("eth-updown-15m-8", Side::Down, 7.0, 3.0));
        assert!(!ps.apply_merge("eth-updown-15m-8", 1, 0));
        let p2 = ps.get("eth-updown-15m-8").unwrap();
        assert_eq!(p2.up_shares, 0.0);
        assert_eq!(p2.down_shares, 7.0);
        assert_eq!(p2.down_usd, 3.0);
    }

    #[test]
    fn apply_merge_repeated_calls_compound_correctly() {
        let mut ps = PositionStore::new();
        ps.record_fill(&fill("eth-updown-15m-rep", Side::Up, 100.0, 40.0)); // avg 0.40
        ps.record_fill(&fill("eth-updown-15m-rep", Side::Down, 100.0, 50.0)); // avg 0.50
        assert!(ps.apply_merge("eth-updown-15m-rep", 30, 0));
        assert!(ps.apply_merge("eth-updown-15m-rep", 20, 0));
        let p = ps.get("eth-updown-15m-rep").unwrap();
        assert!((p.up_shares - 50.0).abs() < 1e-9);
        assert!((p.down_shares - 50.0).abs() < 1e-9);
        // Avg basis preserved on remaining shares.
        assert!((p.up_usd / p.up_shares - 0.40).abs() < 1e-9);
        assert!((p.down_usd / p.down_shares - 0.50).abs() < 1e-9);
    }

    #[test]
    fn is_merge_deferred_after_mark_until_future() {
        let mut ps = PositionStore::new();
        ps.mark_asks_active_until("btc-updown-5m-1", 2_000);
        assert!(ps.is_merge_deferred("btc-updown-5m-1", 1_000));
        assert!(!ps.is_merge_deferred("btc-updown-5m-1", 3_000));
        ps.clear_ask_defer("btc-updown-5m-1");
        assert!(!ps.is_merge_deferred("btc-updown-5m-1", 1_000));
    }

    #[test]
    fn record_fill_sell_preserves_avg_basis_on_remaining_shares() {
        use crate::types::{Coin, Fill, LiquidityRole, RestingOrderId, Side};
        let mut store = PositionStore::new();
        // Seed: buy 50 @ 0.50.
        store.record_fill(&Fill {
            slug: "s".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "T".into(),
            id: RestingOrderId::from_string("test"),
            price: 0.50,
            shares: 50.0,
            usd: 25.0,
            fee_usd: 0.0,
            ts_micros: 1,
            liquidity_role: LiquidityRole::Maker,
        });
        // Sell 20 @ 0.55.
        store.record_fill(&Fill {
            slug: "s".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "T".into(),
            id: RestingOrderId::from_string("test"),
            price: 0.55,
            shares: -20.0,
            usd: 11.0,
            fee_usd: 0.0,
            ts_micros: 2,
            liquidity_role: LiquidityRole::Maker,
        });
        let p = store.get("s").expect("position");
        assert!(
            (p.up_shares - 30.0).abs() < 1e-9,
            "up_shares = {}",
            p.up_shares
        );
        // Avg basis stays 0.50 → up_usd = 30 × 0.50 = 15.0.
        assert!((p.up_usd - 15.0).abs() < 1e-9, "up_usd = {}", p.up_usd);
    }

    #[test]
    fn record_fill_full_sell_zeros_side() {
        use crate::types::{Coin, Fill, LiquidityRole, RestingOrderId, Side};
        let mut store = PositionStore::new();
        store.record_fill(&Fill {
            slug: "s".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "T".into(),
            id: RestingOrderId::from_string("test"),
            price: 0.50,
            shares: 50.0,
            usd: 25.0,
            fee_usd: 0.0,
            ts_micros: 1,
            liquidity_role: LiquidityRole::Maker,
        });
        store.record_fill(&Fill {
            slug: "s".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "T".into(),
            id: RestingOrderId::from_string("test"),
            price: 0.55,
            shares: -50.0,
            usd: 27.5,
            fee_usd: 0.0,
            ts_micros: 2,
            liquidity_role: LiquidityRole::Maker,
        });
        let p = store.get("s").expect("position");
        assert!(p.up_shares.abs() < 1e-9, "up_shares = {}", p.up_shares);
        assert!(p.up_usd.abs() < 1e-9, "up_usd = {}", p.up_usd);
    }

    #[test]
    fn record_fill_over_sell_clamps_to_zero() {
        use crate::types::{Coin, Fill, LiquidityRole, RestingOrderId, Side};
        let mut store = PositionStore::new();
        store.record_fill(&Fill {
            slug: "s".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "T".into(),
            id: RestingOrderId::from_string("test"),
            price: 0.50,
            shares: 10.0,
            usd: 5.0,
            fee_usd: 0.0,
            ts_micros: 1,
            liquidity_role: LiquidityRole::Maker,
        });
        // Over-sell: 20 shares when only 10 are held.
        store.record_fill(&Fill {
            slug: "s".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "T".into(),
            id: RestingOrderId::from_string("test"),
            price: 0.55,
            shares: -20.0,
            usd: 11.0,
            fee_usd: 0.0,
            ts_micros: 2,
            liquidity_role: LiquidityRole::Maker,
        });
        let p = store.get("s").expect("position");
        assert!(p.up_shares.abs() < 1e-9);
        assert!(p.up_usd.abs() < 1e-9);
    }

    #[test]
    fn record_fill_sell_without_position_is_ignored() {
        use crate::types::{Coin, Fill, LiquidityRole, RestingOrderId, Side};
        let mut store = PositionStore::new();
        store.record_fill(&Fill {
            slug: "s".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "T".into(),
            id: RestingOrderId::from_string("test"),
            price: 0.55,
            shares: -50.0,
            usd: 27.5,
            fee_usd: 0.0,
            ts_micros: 1,
            liquidity_role: LiquidityRole::Maker,
        });
        // Position entry exists (created by record_fill) but shares/usd should be 0.
        let p = store.get("s").expect("position");
        assert!(p.up_shares.abs() < 1e-9);
        assert!(p.up_usd.abs() < 1e-9);
    }

    #[test]
    fn position_watermark_initialised_to_zero_and_unchanged_by_record_fill() {
        let mut ps = PositionStore::default();
        ps.record_fill(&fill("btc-updown-5m-1", Side::Up, 50.0, 25.0));
        let p = ps.get("btc-updown-5m-1").unwrap();
        assert_eq!(
            p.last_merge_pair_count, 0.0,
            "record_fill must not touch watermark"
        );
    }

    #[test]
    fn position_watermark_updated_by_apply_merge() {
        let mut ps = PositionStore::default();
        ps.record_fill(&fill("btc-updown-5m-1", Side::Up, 100.0, 50.0));
        ps.record_fill(&fill("btc-updown-5m-1", Side::Down, 100.0, 50.0));
        // Apply a merge of 50 pairs.
        let merged = ps.apply_merge("btc-updown-5m-1", 50, 0);
        assert!(merged, "merge must succeed");
        let p = ps.get("btc-updown-5m-1").unwrap();
        // After merging 50 pairs, watermark == floor(min(50, 50)) == 50.
        // (up_shares post-merge = 100 - 50 = 50; down_shares = 50.)
        assert!(
            (p.last_merge_pair_count - 50.0).abs() < 1e-9,
            "watermark must equal floor(min(up,down)) post-merge; got {}",
            p.last_merge_pair_count
        );
    }

    #[test]
    fn position_apply_merge_records_ts_micros() {
        let mut ps = PositionStore::default();
        ps.record_fill(&fill("btc-updown-5m-1", Side::Up, 100.0, 50.0));
        ps.record_fill(&fill("btc-updown-5m-1", Side::Down, 100.0, 50.0));

        let merged = ps.apply_merge("btc-updown-5m-1", 50, 1_700_000_000_000_000);
        assert!(merged, "merge must succeed");

        let p = ps.get("btc-updown-5m-1").unwrap();
        assert_eq!(
            p.last_merge_ts_micros, 1_700_000_000_000_000,
            "apply_merge must persist the timestamp on success"
        );
    }

    #[test]
    fn position_failed_apply_merge_does_not_update_ts_micros() {
        let mut ps = PositionStore::default();
        ps.record_fill(&fill("btc-updown-5m-2", Side::Up, 5.0, 2.5));
        // No down side — apply_merge will return false.

        let pre_ts = ps
            .get("btc-updown-5m-2")
            .map(|p| p.last_merge_ts_micros)
            .unwrap_or(99);
        assert_eq!(pre_ts, 0, "fresh position has zero ts_micros");

        let merged = ps.apply_merge("btc-updown-5m-2", 1, 1_700_000_000_000_000);
        assert!(!merged, "merge must fail (no down side)");
        // Position still has the up side; check ts unchanged.
        let p = ps.get("btc-updown-5m-2").unwrap();
        assert_eq!(p.last_merge_ts_micros, 0, "failed merge must not bump ts");
    }

    #[test]
    fn record_fill_sell_on_down_side_symmetric() {
        use crate::types::{Coin, Fill, LiquidityRole, RestingOrderId, Side};
        let mut store = PositionStore::new();
        store.record_fill(&Fill {
            slug: "s".into(),
            coin: Coin::Btc,
            side: Side::Down,
            token_id: "T".into(),
            id: RestingOrderId::from_string("test"),
            price: 0.50,
            shares: 50.0,
            usd: 25.0,
            fee_usd: 0.0,
            ts_micros: 1,
            liquidity_role: LiquidityRole::Maker,
        });
        store.record_fill(&Fill {
            slug: "s".into(),
            coin: Coin::Btc,
            side: Side::Down,
            token_id: "T".into(),
            id: RestingOrderId::from_string("test"),
            price: 0.55,
            shares: -20.0,
            usd: 11.0,
            fee_usd: 0.0,
            ts_micros: 2,
            liquidity_role: LiquidityRole::Maker,
        });
        let p = store.get("s").expect("position");
        assert!((p.down_shares - 30.0).abs() < 1e-9);
        assert!((p.down_usd - 15.0).abs() < 1e-9);
    }
}

#[cfg(test)]
mod merge_in_flight_tests {
    use super::*;

    fn fresh_pos() -> Position {
        Position::new("btc-updown-5m-1".into(), Coin::Btc)
    }

    #[test]
    fn merge_in_flight_default_is_none() {
        let p = fresh_pos();
        assert!(p.merge_in_flight.is_none());
    }

    #[test]
    fn set_and_clear_merge_in_flight_round_trip() {
        let mut p = fresh_pos();
        p.up_shares = 100.0;
        p.down_shares = 100.0;
        p.set_merge_in_flight(50, "0xpending".into(), 1_700_000_000_000);
        let mip = p.merge_in_flight.as_ref().expect("should be in-flight");
        assert_eq!(mip.pairs, 50);
        assert_eq!(mip.tx_hash, "0xpending");
        assert_eq!(mip.submitted_at_ms, 1_700_000_000_000);
        p.clear_merge_in_flight();
        assert!(p.merge_in_flight.is_none());
    }

    #[test]
    fn effective_paired_shares_subtracts_pending_from_both_sides() {
        let mut p = fresh_pos();
        p.up_shares = 100.0;
        p.down_shares = 80.0;
        let (eup, edn) = p.effective_paired_shares();
        assert_eq!(eup, 100.0);
        assert_eq!(edn, 80.0);
        p.set_merge_in_flight(30, "0x".into(), 0);
        let (eup, edn) = p.effective_paired_shares();
        assert_eq!(eup, 70.0);
        assert_eq!(edn, 50.0);
    }

    #[test]
    fn effective_paired_shares_clamps_at_zero() {
        let mut p = fresh_pos();
        p.up_shares = 10.0;
        p.down_shares = 5.0;
        p.set_merge_in_flight(20, "0x".into(), 0);
        let (eup, edn) = p.effective_paired_shares();
        assert_eq!(eup, 0.0);
        assert_eq!(edn, 0.0);
    }

    #[test]
    fn condition_id_default_is_empty_and_settable() {
        // Phase 12: Position carries the on-chain condition_id required by
        // OnChainMerger::merge. Fresh Position starts with an empty placeholder
        // (lifecycle / submitter populates it from MarketMeta::condition_id at
        // first fill / market spawn).
        let mut p = fresh_pos();
        assert_eq!(
            p.condition_id(),
            "",
            "fresh Position has empty condition_id"
        );
        p.set_condition_id("0xabc".into());
        assert_eq!(p.condition_id(), "0xabc", "setter populates the field");
    }

    #[test]
    fn ensure_market_metadata_creates_position_with_fields_populated() {
        // Regression for phase-8-step1 silent failure (2026-05-11): without
        // an eager populate at spawn_market time, merge_eligible's first
        // tick saw `market_start_ts_micros == 0` and emitted a WARN with
        // the gate skipped — disabling the first-merge defensive guard.
        let mut store = PositionStore::new();
        store.ensure_market_metadata(
            "eth-updown-5m-1778510400",
            Coin::Eth,
            1_778_510_400,
            "0xabc",
        );
        let p = store
            .get("eth-updown-5m-1778510400")
            .expect("position created");
        assert_eq!(p.market_start_ts_micros, 1_778_510_400 * 1_000_000);
        assert_eq!(p.condition_id(), "0xabc");
        assert_eq!(p.up_shares, 0.0, "no fills yet");
        assert_eq!(p.down_shares, 0.0, "no fills yet");
    }

    #[test]
    fn ensure_market_metadata_is_idempotent_and_does_not_overwrite() {
        // Re-spawn of the same slug must not clobber values populated by
        // an earlier spawn or by quoter's lazy setters. Mirrors the
        // idempotence of `set_condition_id` / `set_market_start_ts`.
        let mut store = PositionStore::new();
        store.ensure_market_metadata("eth-updown-5m-1", Coin::Eth, 100, "0xfirst");
        store.ensure_market_metadata("eth-updown-5m-1", Coin::Eth, 999, "0xsecond");
        let p = store.get("eth-updown-5m-1").expect("position exists");
        assert_eq!(
            p.market_start_ts_micros,
            100 * 1_000_000,
            "first call's value retained"
        );
        assert_eq!(p.condition_id(), "0xfirst", "first call's value retained");
    }

    #[test]
    fn ensure_market_metadata_after_record_fill_populates_unset_fields() {
        // Operational ordering: record_fill creates the Position lazily
        // with both fields unset (market_start_ts_micros=0,
        // condition_id=""). A later `ensure_market_metadata` (e.g.,
        // spawn_market firing after a fill races in) must populate them.
        use crate::types::{Fill, LiquidityRole, RestingOrderId};
        let mut store = PositionStore::new();
        store.record_fill(&Fill {
            slug: "eth-updown-5m-1".into(),
            coin: Coin::Eth,
            side: Side::Up,
            token_id: "T".into(),
            id: RestingOrderId::from_string("test"),
            shares: 50.0,
            price: 0.50,
            usd: 25.0,
            fee_usd: 0.0,
            ts_micros: 1,
            liquidity_role: LiquidityRole::Maker,
        });
        // Before ensure: fields are at their `Position::new` defaults.
        {
            let p = store.get("eth-updown-5m-1").unwrap();
            assert_eq!(p.market_start_ts_micros, 0);
            assert_eq!(p.condition_id(), "");
        }
        store.ensure_market_metadata("eth-updown-5m-1", Coin::Eth, 42, "0xcid");
        let p = store.get("eth-updown-5m-1").unwrap();
        assert_eq!(p.market_start_ts_micros, 42 * 1_000_000);
        assert_eq!(p.condition_id(), "0xcid");
        // record_fill state must be preserved.
        assert_eq!(p.up_shares, 50.0);
        assert_eq!(p.up_usd, 25.0);
    }
}
