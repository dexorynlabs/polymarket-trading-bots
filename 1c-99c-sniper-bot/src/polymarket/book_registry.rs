//! Shared order-book snapshot, written by `ClobWsIngest` and read by
//! `PaperClob` post-latency to model book drift AND partial fills between
//! decision and fill.

use crate::types::{BookUpdate, Side};
use crate::polymarket::rolling::RollingWindow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::broadcast;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

/// Full-depth book snapshot. `asks` is sorted ascending by price, `bids`
/// descending. Either may be empty.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BookSnapshot {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub ts_micros: u64,
}

impl BookSnapshot {
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first().map(|l| l.price)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first().map(|l| l.price)
    }

    /// Size-cutoff midpoint: walk each side from top accumulating size until
    /// `size_sh` is reached, take that level's price; mid = (bid_walk +
    /// ask_walk) / 2. Returns `None` when either side does not have enough
    /// cumulative depth. NQ4 resolution per docs/alignment-gaps §3.1+§3.2.
    /// Passing `size_sh = 0.0` returns the best-bid/best-ask midpoint regardless of level sizes.
    pub fn mid_at_cumulative_size(&self, size_sh: f64) -> Option<f64> {
        let bid_walk = walk_to_cum_size(&self.bids, size_sh)?;
        let ask_walk = walk_to_cum_size(&self.asks, size_sh)?;
        Some((bid_walk + ask_walk) / 2.0)
    }
}

#[inline]
fn walk_to_cum_size(levels: &[Level], target: f64) -> Option<f64> {
    let mut cum = 0.0;
    for level in levels {
        cum += level.size;
        if cum >= target {
            return Some(level.price);
        }
    }
    None
}

/// Per-slot derived signals computed from the live book + trade stream.
/// Strategy §15 Module 2 + §13 kill switch inputs.
#[derive(Debug, Clone)]
pub struct SlotSignals {
    /// `rewardsMaxSpread` from market metadata (e.g. 0.045 for crypto-updown).
    /// Used to compute depth-within-v consistently.
    pub v_pp: f64,
    /// Per-second mid samples, 60s window.
    pub mid_samples: RollingWindow<f64>,
    /// Trade events count, 5s window.
    pub trades_5s: RollingWindow<u32>,
    /// Trade events count, 30s window.
    pub trades_30s: RollingWindow<u32>,
    /// Trade events count, 60s window.
    pub trades_60s: RollingWindow<u32>,
    /// Sum of size-within-v on bid side, 60s window (depth MA).
    pub depth_within_v_bid: RollingWindow<f64>,
    /// Sum of size-within-v on ASK side, 60s window. Per strategy §15 Module 2:
    /// "depth-within-v per side, both tokens" — needed for kill-switch parity.
    pub depth_within_v_ask: RollingWindow<f64>,
    /// Last second when we sampled the mid (avoids over-sampling). `None`
    /// means no sample has landed yet — the next `update()` always samples
    /// regardless of its `ts_micros`.
    pub last_mid_sample_secs: Option<u64>,
}

impl SlotSignals {
    pub fn new(v_pp: f64) -> Self {
        Self {
            v_pp,
            mid_samples: RollingWindow::new(60),
            trades_5s: RollingWindow::new(5),
            trades_30s: RollingWindow::new(30),
            trades_60s: RollingWindow::new(60),
            depth_within_v_bid: RollingWindow::new(60),
            depth_within_v_ask: RollingWindow::new(60),
            last_mid_sample_secs: None,
        }
    }

    /// 1-second mid jump = max-min of mid samples in the last 1s.
    pub fn one_sec_mid_jump(&self, now_micros: u64) -> Option<f64> {
        let cutoff = now_micros.saturating_sub(1_000_000);
        let recent: Vec<f64> = self.mid_samples.iter_recent(cutoff).collect();
        if recent.len() < 2 {
            return None;
        }
        let max = recent.iter().cloned().fold(f64::MIN, f64::max);
        let min = recent.iter().cloned().fold(f64::MAX, f64::min);
        Some(max - min)
    }

    /// Trade rate per second over the 5s window.
    pub fn trade_rate_5s(&self) -> f64 {
        self.trades_5s.count() as f64 / 5.0
    }

    /// Depth-within-v as % of the 60s moving average (bid side).
    pub fn depth_within_v_pct_of_ma_bid(&self, current_depth: f64) -> Option<f64> {
        let avg = self.depth_within_v_bid.avg()?;
        if avg <= 0.0 {
            return None;
        }
        Some(current_depth / avg * 100.0)
    }

    /// Depth-within-v as % of the 60s moving average (ask side, per strategy §15).
    pub fn depth_within_v_pct_of_ma_ask(&self, current_depth: f64) -> Option<f64> {
        let avg = self.depth_within_v_ask.avg()?;
        if avg <= 0.0 {
            return None;
        }
        Some(current_depth / avg * 100.0)
    }
}

/// Thread-safe registry of book state by (slug, side). Writers are the
/// `ClobWsIngest` emit path (one per book/price_change event); readers are
/// the `PaperClob` fill path (one per order submission, post-latency).
/// Contention is negligible at expected traffic (≤20 markets × 2 sides,
/// ≤10 intents/min).
///
/// Every `update` additionally fires a `BookUpdate` on a broadcast channel;
/// subscribers (e.g. the `PaperClob` bridge task) observe updates without
/// needing a direct callback handle.
///
/// Per-slot derived signals (mid samples, trade rate, depth MA) are tracked
/// in `signals` alongside the raw book state. `update()` samples mid + depth
/// once per wall-second; trade events drive `record_trade()`.
pub struct BookRegistry {
    // Arc-wrapped snapshots so readers (`get` / `ask_ladder`) clone a refcount
    // instead of the 100+-level `Vec<Level>` inside. In production the quoter
    // calls `ask_ladder` on every book_update (opp-cross scan) for every
    // market — the saved Vec clone per read is the big production win.
    inner: RwLock<HashMap<(String, Side), Arc<BookSnapshot>>>,
    tx: broadcast::Sender<BookUpdate>,
    /// Per-slot derived signals, keyed by slug. Lazy-initialized on first
    /// access via `signals_for()`. Pre-init via `init_signals_for_market()`
    /// to bind the market's actual `rewardsMaxSpread` before traffic starts.
    signals: RwLock<HashMap<String, Arc<Mutex<SlotSignals>>>>,
}

impl Default for BookRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl BookRegistry {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(256);
        Self {
            inner: RwLock::new(HashMap::new()),
            tx,
            signals: RwLock::new(HashMap::new()),
        }
    }

    /// Get-or-init signals for a slug. Lazy-initialized with the
    /// crypto-updown default `v_pp = 0.045`; production callers should
    /// pre-init via `init_signals_for_market()` once the actual
    /// `rewardsMaxSpread` is known so kill-switch percentages are correct.
    pub fn signals_for(&self, slug: &str) -> Arc<Mutex<SlotSignals>> {
        if let Some(s) = self.signals.read().unwrap().get(slug) {
            return s.clone();
        }
        let mut w = self.signals.write().unwrap();
        w.entry(slug.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(SlotSignals::new(0.045))))
            .clone()
    }

    /// Pre-initialize signals with the market's actual `rewardsMaxSpread`.
    /// Idempotent: a second call with a different `v_pp` is a no-op (so
    /// repeated `spawn_market` calls don't reset accumulated signals).
    pub fn init_signals_for_market(&self, slug: &str, v_pp: f64) {
        let mut w = self.signals.write().unwrap();
        w.entry(slug.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(SlotSignals::new(v_pp))));
    }

    /// Record one trade event for a slot's trade-rate windows. Called from
    /// the WS `last_trade_price` event handler; replay tests drive it
    /// directly.
    pub fn record_trade(&self, slug: &str, ts_micros: u64) {
        let signals = self.signals_for(slug);
        let mut sig = signals.lock().unwrap();
        sig.trades_5s.push(1, ts_micros);
        sig.trades_30s.push(1, ts_micros);
        sig.trades_60s.push(1, ts_micros);
    }

    pub fn update(&self, slug: &str, side: Side, snap: BookSnapshot) {
        // Read fields off `snap` BEFORE moving it into the Arc so we don't
        // need a clone inside the write lock. Also wrap in Arc once here so
        // every subsequent `get()` is a cheap refcount bump.
        let best_bid = snap.best_bid();
        let best_ask = snap.best_ask();
        let ts_micros = snap.ts_micros;

        // SlotSignals sampling: mid (1Hz cap) + per-side depth-within-v.
        // Borrow `snap` here before moving into the Arc below. G2 fix per
        // strategy §15 Module 2: track depth on BOTH sides.
        if let (Some(bb), Some(ba)) = (best_bid, best_ask) {
            let mid = (bb + ba) / 2.0;
            let now_secs = ts_micros / 1_000_000;
            let signals = self.signals_for(slug);
            let mut sig = signals.lock().unwrap();
            if sig
                .last_mid_sample_secs
                .map_or(true, |last| now_secs > last)
            {
                sig.mid_samples.push(mid, ts_micros);
                sig.last_mid_sample_secs = Some(now_secs);
            }
            let v_pp = sig.v_pp;
            match side {
                Side::Up => {
                    let depth_bid = snap
                        .bids
                        .iter()
                        .filter(|lv| (mid - lv.price) <= v_pp)
                        .map(|lv| lv.size)
                        .sum::<f64>();
                    sig.depth_within_v_bid.push(depth_bid, ts_micros);
                }
                Side::Down => {
                    let depth_ask = snap
                        .asks
                        .iter()
                        .filter(|lv| (lv.price - mid) <= v_pp)
                        .map(|lv| lv.size)
                        .sum::<f64>();
                    sig.depth_within_v_ask.push(depth_ask, ts_micros);
                }
            }
        }

        let snap = Arc::new(snap);
        {
            let mut g = self.inner.write().unwrap();
            g.insert((slug.to_string(), side), snap);
        }
        // Forensic per-update log (2026-05-08). 20:50 ETH-5m slot showed
        // 10s+ stretches of `reconcile: skipping — book is stale`; without
        // a per-update cadence signal we can't tell whether the WS feed
        // throttled or the registry simply wasn't being called. DEBUG so
        // the live default (INFO) doesn't drown; enable with
        // `RUST_LOG=recon::book_registry=debug` for diagnosis.
        tracing::debug!(
            target: "recon::book_registry",
            slug, ?side, ts_micros,
            best_bid = ?best_bid,
            best_ask = ?best_ask,
            "book_update_applied"
        );
        let _ = self.tx.send(BookUpdate {
            slug: slug.to_string(),
            side,
            best_bid,
            best_ask,
            ts_micros,
        });
    }

    /// Subscribe to every future `update`. The channel has capacity 256; lagging
    /// subscribers receive `RecvError::Lagged` and should resync from `get`.
    pub fn subscribe(&self) -> broadcast::Receiver<BookUpdate> {
        self.tx.subscribe()
    }

    pub fn get(&self, slug: &str, side: Side) -> Option<Arc<BookSnapshot>> {
        let g = self.inner.read().unwrap();
        g.get(&(slug.to_string(), side)).cloned()
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().unwrap().is_empty()
    }

    /// Full ask ladder (ascending price, `(price, depth)` pairs) for
    /// `(slug, side)`. Empty if the book hasn't been observed yet. Intended
    /// for the paper-mode taker walk — callers receiving the tuple shape
    /// expected by `exec::matching::take_from_asks`.
    pub fn ask_ladder(&self, slug: &str, side: Side) -> Vec<(f64, f64)> {
        let _t = crate::monitoring::latency::ScopeTimer::new("ladder_fetch");
        self.get(slug, side)
            .map(|s| s.asks.iter().map(|l| (l.price, l.size)).collect())
            .unwrap_or_default()
    }

    /// Full bid ladder (`(price, depth)` pairs) for `(slug, side)`. Empty if the
    /// book hasn't been observed yet. Mirrors [`Self::ask_ladder`] for the
    /// resting-bid (maker queue) depth a strategy telemeters.
    pub fn bid_ladder(&self, slug: &str, side: Side) -> Vec<(f64, f64)> {
        let _t = crate::monitoring::latency::ScopeTimer::new("ladder_fetch");
        self.get(slug, side)
            .map(|s| s.bids.iter().map(|l| (l.price, l.size)).collect())
            .unwrap_or_default()
    }

    /// Reduce the ask depth by `shares` consumed from the top of the ladder.
    /// Walks levels ascending, subtracting until either `shares` is exhausted
    /// or the whole visible ladder is drained. Empty levels (size == 0) are
    /// removed. No broadcast — this is an internal bookkeeping mutation the
    /// quoter should not react to.
    pub fn deplete(&self, slug: &str, side: Side, shares: f64) {
        if shares <= 0.0 {
            return;
        }
        let mut g = self.inner.write().unwrap();
        if let Some(arc_snap) = g.get_mut(&(slug.to_string(), side)) {
            // `make_mut` clones only if the Arc has >1 strong count; otherwise
            // it mutates in place. Paper-only cold path, so either way fine.
            let snap = Arc::make_mut(arc_snap);
            let mut remaining = shares;
            for level in snap.asks.iter_mut() {
                if remaining <= 0.0 {
                    break;
                }
                let take = remaining.min(level.size);
                level.size -= take;
                remaining -= take;
            }
            snap.asks.retain(|l| l.size > 0.0);
        }
    }

    /// Remove every `(slug, side)` entry for `slug` AND drop its `SlotSignals`.
    /// Used by a LifecycleSupervisor at slot reap so the registry
    /// doesn't grow unbounded (~576 slugs/day per coin × ~125kB/slug → 70MB+
    /// over 24h without cleanup). Idempotent: clearing an unknown slug is a
    /// no-op. No broadcast event is emitted — readers should treat absence as
    /// "slot is over, stop computing on it".
    pub fn clear_slug(&self, slug: &str) {
        {
            let mut g = self.inner.write().unwrap();
            g.retain(|(s, _), _| s != slug);
        }
        {
            let mut s = self.signals.write().unwrap();
            s.remove(slug);
        }
    }

    /// Seed ask levels for tests. Overwrites any existing asks on `(slug, side)`;
    /// leaves bids untouched. Not cfg-gated because integration tests consume
    /// the library as an external crate.
    pub fn set_asks_for_test(&self, slug: &str, side: Side, asks: Vec<(f64, f64)>) {
        let mut g = self.inner.write().unwrap();
        let arc_snap = g
            .entry((slug.to_string(), side))
            .or_insert_with(|| Arc::new(BookSnapshot::default()));
        let snap = Arc::make_mut(arc_snap);
        snap.asks = asks
            .into_iter()
            .map(|(p, s)| Level { price: p, size: s })
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn l(price: f64, size: f64) -> Level {
        Level { price, size }
    }

    fn snap_tob(bid: Option<f64>, ask: Option<f64>) -> BookSnapshot {
        BookSnapshot {
            bids: bid.map(|p| vec![l(p, 1e9)]).unwrap_or_default(),
            asks: ask.map(|p| vec![l(p, 1e9)]).unwrap_or_default(),
            ts_micros: 0,
        }
    }

    #[test]
    fn new_registry_is_empty() {
        let r = BookRegistry::new();
        assert!(r.is_empty());
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn update_then_get_roundtrips() {
        let r = BookRegistry::new();
        r.update("eth-updown-15m-1", Side::Up, snap_tob(Some(0.3), Some(0.7)));
        let got = r.get("eth-updown-15m-1", Side::Up).unwrap();
        assert_eq!(got.best_bid(), Some(0.3));
        assert_eq!(got.best_ask(), Some(0.7));
    }

    #[test]
    fn get_missing_is_none() {
        let r = BookRegistry::new();
        assert!(r.get("missing", Side::Up).is_none());
    }

    #[test]
    fn sides_and_slugs_are_independent() {
        let r = BookRegistry::new();
        r.update("eth-updown-15m-1", Side::Up, snap_tob(None, Some(0.55)));
        r.update("eth-updown-15m-1", Side::Down, snap_tob(None, Some(0.47)));
        r.update("btc-updown-15m-1", Side::Up, snap_tob(None, Some(0.51)));
        assert_eq!(
            r.get("eth-updown-15m-1", Side::Up).unwrap().best_ask(),
            Some(0.55)
        );
        assert_eq!(
            r.get("eth-updown-15m-1", Side::Down).unwrap().best_ask(),
            Some(0.47)
        );
        assert_eq!(
            r.get("btc-updown-15m-1", Side::Up).unwrap().best_ask(),
            Some(0.51)
        );
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn update_overwrites_existing() {
        let r = BookRegistry::new();
        r.update("a", Side::Up, snap_tob(None, Some(0.50)));
        r.update("a", Side::Up, snap_tob(None, Some(0.48)));
        assert_eq!(r.get("a", Side::Up).unwrap().best_ask(), Some(0.48));
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn snapshot_stores_full_depth_per_side() {
        let snap = BookSnapshot {
            bids: vec![l(0.40, 100.0), l(0.39, 200.0), l(0.38, 300.0)],
            asks: vec![l(0.50, 50.0), l(0.51, 100.0), l(0.52, 150.0)],
            ts_micros: 10,
        };
        assert_eq!(snap.best_bid(), Some(0.40));
        assert_eq!(snap.best_ask(), Some(0.50));
        assert_eq!(snap.asks.len(), 3);
        assert_eq!(snap.bids.len(), 3);
    }

    #[test]
    fn ask_ladder_returns_ascending_price_depth_tuples() {
        let r = BookRegistry::new();
        r.update(
            "s",
            Side::Up,
            BookSnapshot {
                bids: Vec::new(),
                asks: vec![l(0.50, 30.0), l(0.55, 40.0)],
                ts_micros: 0,
            },
        );
        let lad = r.ask_ladder("s", Side::Up);
        assert_eq!(lad, vec![(0.50, 30.0), (0.55, 40.0)]);
    }

    #[test]
    fn ask_ladder_empty_for_unknown_book() {
        let r = BookRegistry::new();
        assert!(r.ask_ladder("missing", Side::Up).is_empty());
    }

    #[test]
    fn bid_ladder_returns_price_depth_tuples() {
        let r = BookRegistry::new();
        r.update(
            "s",
            Side::Up,
            BookSnapshot {
                bids: vec![l(0.99, 30.0), l(0.98, 40.0)],
                asks: Vec::new(),
                ts_micros: 0,
            },
        );
        let lad = r.bid_ladder("s", Side::Up);
        assert_eq!(lad, vec![(0.99, 30.0), (0.98, 40.0)]);
    }

    #[test]
    fn bid_ladder_empty_for_unknown_book() {
        let r = BookRegistry::new();
        assert!(r.bid_ladder("missing", Side::Up).is_empty());
    }

    #[test]
    fn deplete_consumes_shares_from_top_levels() {
        let r = BookRegistry::new();
        r.update(
            "s",
            Side::Up,
            BookSnapshot {
                bids: Vec::new(),
                asks: vec![l(0.50, 30.0), l(0.55, 40.0), l(0.60, 50.0)],
                ts_micros: 0,
            },
        );
        r.deplete("s", Side::Up, 50.0); // 30 + 20 → level 0 emptied, level 1 at 20
        let lad = r.ask_ladder("s", Side::Up);
        assert_eq!(lad, vec![(0.55, 20.0), (0.60, 50.0)]);
    }

    #[test]
    fn deplete_noop_on_missing_book_or_zero_shares() {
        let r = BookRegistry::new();
        r.deplete("missing", Side::Up, 5.0); // no panic, no effect
        r.update(
            "s",
            Side::Up,
            BookSnapshot {
                bids: Vec::new(),
                asks: vec![l(0.50, 30.0)],
                ts_micros: 0,
            },
        );
        r.deplete("s", Side::Up, 0.0);
        assert_eq!(r.ask_ladder("s", Side::Up), vec![(0.50, 30.0)]);
    }

    #[test]
    fn mid_at_cumulative_size_walks_until_size_reached() {
        // bids descending, asks ascending. cumulative ≥ 50sh:
        //   bids: 0.40@30, 0.39@20  → walk hits 0.39 at 50 cum.
        //   asks: 0.42@40, 0.43@15  → walk hits 0.43 at 55 cum.
        //   mid = (0.39 + 0.43) / 2 = 0.41
        let snap = BookSnapshot {
            bids: vec![l(0.40, 30.0), l(0.39, 20.0)],
            asks: vec![l(0.42, 40.0), l(0.43, 15.0)],
            ts_micros: 0,
        };
        let mid = snap.mid_at_cumulative_size(50.0).unwrap();
        assert!((mid - 0.41).abs() < 1e-9, "got {mid}");
    }

    #[test]
    fn mid_at_cumulative_size_returns_none_when_either_side_too_thin() {
        let snap = BookSnapshot {
            bids: vec![l(0.40, 30.0)], // only 30sh, need 50
            asks: vec![l(0.42, 100.0)],
            ts_micros: 0,
        };
        assert!(snap.mid_at_cumulative_size(50.0).is_none());
    }

    #[test]
    fn mid_at_cumulative_size_falls_back_to_top_when_size_zero() {
        // size=0.0 ⇒ first level on each side qualifies; mid = (best_bid + best_ask) / 2
        let snap = BookSnapshot {
            bids: vec![l(0.40, 5.0)],
            asks: vec![l(0.42, 5.0)],
            ts_micros: 0,
        };
        let mid = snap.mid_at_cumulative_size(0.0).unwrap();
        assert!((mid - 0.41).abs() < 1e-9);
    }

    fn snap_at(ts_us: u64, mid: f64) -> BookSnapshot {
        BookSnapshot {
            bids: vec![l(mid - 0.005, 100.0)],
            asks: vec![l(mid + 0.005, 100.0)],
            ts_micros: ts_us,
        }
    }

    #[test]
    fn signals_track_mid_samples_at_one_per_second() {
        let registry = BookRegistry::new();
        registry.update("test-slug", Side::Up, snap_at(0, 0.50));
        registry.update("test-slug", Side::Up, snap_at(500_000, 0.51)); // same second
        registry.update("test-slug", Side::Up, snap_at(1_000_000, 0.52)); // next second
        registry.update("test-slug", Side::Up, snap_at(2_000_000, 0.53));

        let sig = registry.signals_for("test-slug");
        let s = sig.lock().unwrap();
        assert_eq!(s.mid_samples.len(), 3, "sampled at 0s, 1s, 2s only");
    }

    #[test]
    fn signals_compute_one_sec_mid_jump() {
        let registry = BookRegistry::new();
        registry.update("s1", Side::Up, snap_at(0, 0.50));
        registry.update("s1", Side::Up, snap_at(1_000_000, 0.55)); // +5¢ in 1s
        let sig = registry.signals_for("s1");
        // Query at ts=1_000_000 → cutoff=0; both samples (ts=0, ts=1_000_000)
        // fall in [now-1s, now] and contribute to the jump.
        let jump = sig.lock().unwrap().one_sec_mid_jump(1_000_000).unwrap();
        assert!((jump - 0.05).abs() < 1e-9, "jump = {jump}");
    }

    #[test]
    fn signals_one_sec_mid_jump_returns_none_with_only_one_recent_sample() {
        let registry = BookRegistry::new();
        registry.update("s1b", Side::Up, snap_at(0, 0.50));
        registry.update("s1b", Side::Up, snap_at(1_000_000, 0.55));
        let sig = registry.signals_for("s1b");
        // At now=1_500_000, cutoff=500_000 → only sample at ts=1_000_000
        // qualifies. Returns None: a jump needs ≥2 samples within the window.
        let jump = sig.lock().unwrap().one_sec_mid_jump(1_500_000);
        assert!(jump.is_none());
    }

    #[test]
    fn signals_track_depth_within_v_on_bid_side_for_up_snapshots() {
        let registry = BookRegistry::new();
        // mid = 0.50, v_pp default = 0.045 → admit bids ≥ 0.455
        let snap = BookSnapshot {
            bids: vec![l(0.495, 100.0), l(0.46, 50.0), l(0.40, 99.0)],
            asks: vec![l(0.505, 200.0)],
            ts_micros: 0,
        };
        registry.update("s2", Side::Up, snap);
        let sig = registry.signals_for("s2");
        let s = sig.lock().unwrap();
        // 0.495 admitted, 0.46 admitted, 0.40 rejected → depth = 150
        assert_eq!(s.depth_within_v_bid.len(), 1);
        assert!((s.depth_within_v_bid.avg().unwrap() - 150.0).abs() < 1e-9);
        // ASK window untouched on Up snapshots
        assert_eq!(s.depth_within_v_ask.len(), 0);
    }

    #[test]
    fn signals_track_depth_within_v_on_ask_side_for_down_snapshots() {
        let registry = BookRegistry::new();
        let snap = BookSnapshot {
            bids: vec![l(0.495, 50.0)],
            asks: vec![l(0.505, 200.0), l(0.54, 30.0), l(0.60, 99.0)],
            ts_micros: 0,
        };
        registry.update("s3", Side::Down, snap);
        let sig = registry.signals_for("s3");
        let s = sig.lock().unwrap();
        // mid = 0.50, v_pp = 0.045 → admit asks ≤ 0.545
        // 0.505 admitted, 0.54 admitted, 0.60 rejected → depth = 230
        assert_eq!(s.depth_within_v_ask.len(), 1);
        assert!((s.depth_within_v_ask.avg().unwrap() - 230.0).abs() < 1e-9);
        assert_eq!(s.depth_within_v_bid.len(), 0);
    }

    #[test]
    fn record_trade_increments_all_three_windows() {
        let registry = BookRegistry::new();
        registry.record_trade("s4", 0);
        registry.record_trade("s4", 1_000_000);
        registry.record_trade("s4", 4_500_000);
        let sig = registry.signals_for("s4");
        let s = sig.lock().unwrap();
        assert_eq!(s.trades_5s.count(), 3);
        assert_eq!(s.trades_30s.count(), 3);
        assert_eq!(s.trades_60s.count(), 3);
        assert!(
            (s.trade_rate_5s() - 0.6).abs() < 1e-9,
            "3 trades / 5s = 0.6/s"
        );
    }

    #[test]
    fn init_signals_for_market_binds_v_pp_before_first_update() {
        let registry = BookRegistry::new();
        registry.init_signals_for_market("s5", 0.10); // non-default v_pp
        let sig = registry.signals_for("s5");
        assert!((sig.lock().unwrap().v_pp - 0.10).abs() < 1e-9);
    }

    #[test]
    fn init_signals_for_market_is_idempotent() {
        let registry = BookRegistry::new();
        registry.init_signals_for_market("s6", 0.10);
        // Push some state through the first init.
        registry.update("s6", Side::Up, snap_at(0, 0.50));
        // Second init must not reset.
        registry.init_signals_for_market("s6", 0.20);
        let sig = registry.signals_for("s6");
        let s = sig.lock().unwrap();
        assert!((s.v_pp - 0.10).abs() < 1e-9, "v_pp must not be overwritten");
        assert_eq!(s.mid_samples.len(), 1, "samples must not be reset");
    }

    #[test]
    fn clear_slug_removes_both_sides_and_signals() {
        let r = BookRegistry::new();
        r.update(
            "sol-updown-5m-100",
            Side::Up,
            snap_tob(Some(0.4), Some(0.6)),
        );
        r.update(
            "sol-updown-5m-100",
            Side::Down,
            snap_tob(Some(0.3), Some(0.7)),
        );
        r.update(
            "sol-updown-5m-200",
            Side::Up,
            snap_tob(Some(0.5), Some(0.5)),
        );
        r.init_signals_for_market("sol-updown-5m-100", 0.045);
        assert_eq!(r.len(), 3);

        r.clear_slug("sol-updown-5m-100");

        assert!(r.get("sol-updown-5m-100", Side::Up).is_none());
        assert!(r.get("sol-updown-5m-100", Side::Down).is_none());
        assert_eq!(r.len(), 1, "other slug unaffected");
        assert!(r.get("sol-updown-5m-200", Side::Up).is_some());

        // Subsequent subscribe() and update() must still work after clear.
        let mut rx = r.subscribe();
        r.update("sol-updown-5m-300", Side::Up, snap_tob(None, Some(0.6)));
        let u = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await
            })
            .expect("timeout")
            .expect("recv");
        assert_eq!(u.slug, "sol-updown-5m-300");
    }
}
