//! Reconstruct the Binance 1s close AT THE BAR BOUNDARY from `@aggTrade`,
//! instead of waiting for Binance to push the closed kline (~152 ms later).
//!
//! See the boundary-close probe (2026-07-27). The prior
//! `@aggTrade` refutations tested a rule that waited for the NEXT second's
//! first trade to *prove* the second had closed; at 135 ms of stream latency
//! that proof lands after the kline does. We never needed proof — our own
//! NTP-synced clock says the bar is over at `t+1000`, and the close is simply
//! the last trade with `T < t+1000`. Firing at `t+1000+delay` means we are
//! blind only to the final `(135 - delay)` ms of the bar. Measured on the
//! deployed box: at `delay = 0` the reconstruction is bit-exact on 90.6% of bars
//! with a p99 error of $0.01, against a $12 gate floor.
//!
//! PURE — no clock, no I/O. `on_tick` takes `now_ms` from the caller so the
//! whole state machine is testable without a runtime.

use crate::types::{Coin, SpotClose1s};
use std::collections::BTreeMap;

/// Binance reports a 1s kline's close time as `open_ms + 999`, NOT
/// `open_ms + 1000`, and `runtime::resolve_p0` gates on
/// `close_ts_ms / 1000 + 1 == slot_open`. Emitting the +1000 form would shift
/// every reconstructed close into the next second and fail p0 on every slot.
const BINANCE_CLOSE_OFFSET_MS: u64 = 999;

/// How long a silent bar may be carried forward before emission stops.
///
/// Binance's own kline carries the prior close through a bar nobody traded, and
/// we must match that or `SlotSeries` loses contiguity. But Binance knows its
/// feed is alive and we do not: an aggTrade socket that dies looks exactly like
/// a quiet market. Unbounded carry-forward would publish a frozen price forever
/// AND — because these synthetic closes drive `runtime::spawn_kline_watch_task`
/// (O1) — would show the feed-health watchdog a healthy 1 Hz cadence while the
/// real feed was gone. Bounded at 3 s, matching `engine::MAX_SIGNAL_AGE_S`.
pub const MAX_CARRY_MS: u64 = 3_000;

pub struct BoundaryClose {
    coin: Coin,
    delay_ms: u64,
    /// second -> (latest trade ts seen in it, its price)
    pending: BTreeMap<u64, (u64, f64)>,
    /// last finalized close, used to carry a silent second forward the way
    /// Binance's own kline does
    carry: Option<f64>,
    /// venue trade time of the most recent trade seen — bounds carry-forward
    last_trade_ms: Option<u64>,
    emitted_through: Option<u64>,
}

impl BoundaryClose {
    pub fn new(coin: Coin, delay_ms: u64) -> Self {
        Self {
            coin,
            delay_ms,
            pending: BTreeMap::new(),
            carry: None,
            last_trade_ms: None,
            emitted_through: None,
        }
    }

    /// Record a trade. `trade_ts_ms` is the VENUE's trade time (Binance `T`),
    /// never local receive — bucketing by arrival would misfile any trade that
    /// crosses a second boundary in flight.
    pub fn on_trade(&mut self, trade_ts_ms: u64, price: f64) {
        let sec = trade_ts_ms / 1000;
        if self.emitted_through.is_some_and(|done| sec <= done) {
            return; // that bar has already gone out; never revise it
        }
        self.last_trade_ms = Some(self.last_trade_ms.map_or(trade_ts_ms, |p| p.max(trade_ts_ms)));
        match self.pending.get(&sec) {
            Some(&(seen_ts, _)) if seen_ts > trade_ts_ms => {}
            _ => {
                self.pending.insert(sec, (trade_ts_ms, price));
            }
        }
    }

    /// Finalize every second whose boundary + `delay_ms` has passed.
    pub fn on_tick(&mut self, now_ms: u64) -> Vec<SpotClose1s> {
        let mut out = Vec::new();
        // second `s` spans [s*1000, (s+1)*1000) and is finalizable once
        // (s+1)*1000 + delay <= now_ms
        let Some(cutoff) = now_ms.checked_sub(self.delay_ms) else {
            return out;
        };
        let Some(s_max) = (cutoff / 1000).checked_sub(1) else {
            return out;
        };
        // Before the first trade there is nothing to carry forward, and a
        // reconstructed p0 must never be invented — start at the first second
        // we actually observed.
        let Some(start) = self
            .emitted_through
            .map(|e| e + 1)
            .or_else(|| self.pending.keys().next().copied())
        else {
            return out;
        };
        for sec in start..=s_max {
            let px = match self.pending.remove(&sec) {
                Some((_, p)) => Some(p),
                // A silent second carries the prior close, exactly as Binance's
                // own kline does — but only while the trade feed is provably
                // alive. Past MAX_CARRY_MS we emit nothing, so a dead socket
                // surfaces as staleness instead of a frozen price.
                None => match self.last_trade_ms {
                    Some(t) if sec * 1000 <= t + MAX_CARRY_MS => self.carry,
                    _ => None,
                },
            };
            let Some(px) = px else { continue };
            self.carry = Some(px);
            self.emitted_through = Some(sec);
            out.push(SpotClose1s {
                coin: self.coin,
                close_px: px,
                close_ts_ms: sec * 1000 + BINANCE_CLOSE_OFFSET_MS,
            });
        }
        out
    }
}

/// The next wall-clock instant of the form `k*1000 + delay_ms`, strictly after
/// `now_ms`. The driver sleeps to exactly this rather than polling on a short
/// interval: at `delay = 0` a 10 ms poll would hand back 10 ms of the ~152 this
/// feature exists to win, and it would do so on every single bar.
pub fn next_tick_ms(now_ms: u64, delay_ms: u64) -> u64 {
    let k = now_ms.checked_sub(delay_ms).map_or(0, |e| e / 1000 + 1);
    k * 1000 + delay_ms
}

/// One paired bar: what we reconstructed vs what Binance actually said.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShadowSample {
    pub close_ts_ms: u64,
    pub recon_px: f64,
    pub kline_px: f64,
    pub delta_usd: f64,
    pub exact: bool,
}

/// Pairs each reconstructed close with the real `@kline_1s` close for the same
/// bar, so the measured "90.6% bit-exact" is VERIFIED in production rather than
/// trusted. Without this, a drift in the reconstruction would surface only as
/// unexplained gate behaviour.
#[derive(Default)]
pub struct ShadowCompare {
    /// close_ts_ms -> (recon, kline); whichever arrives first waits
    seen: BTreeMap<u64, (Option<f64>, Option<f64>)>,
}

/// Unpaired halves are dropped beyond this age so a permanently one-sided feed
/// (e.g. the kline socket dying) cannot grow the map without bound.
const SHADOW_EVICT_MS: u64 = 60_000;

impl ShadowCompare {
    pub fn on_recon(&mut self, close_ts_ms: u64, px: f64) -> Option<ShadowSample> {
        self.record(close_ts_ms, Some(px), None)
    }

    pub fn on_kline(&mut self, close_ts_ms: u64, px: f64) -> Option<ShadowSample> {
        self.record(close_ts_ms, None, Some(px))
    }

    fn record(&mut self, ts: u64, r: Option<f64>, k: Option<f64>) -> Option<ShadowSample> {
        let e = self.seen.entry(ts).or_insert((None, None));
        if r.is_some() {
            e.0 = r;
        }
        if k.is_some() {
            e.1 = k;
        }
        let paired = match *e {
            (Some(recon_px), Some(kline_px)) => Some(ShadowSample {
                close_ts_ms: ts,
                recon_px,
                kline_px,
                delta_usd: recon_px - kline_px,
                exact: (recon_px - kline_px).abs() < 1e-9,
            }),
            _ => None,
        };
        if paired.is_some() {
            self.seen.remove(&ts);
        }
        if let Some(cut) = ts.checked_sub(SHADOW_EVICT_MS) {
            self.seen.retain(|&k, _| k >= cut);
        }
        paired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: u64 = 1_785_000_000; // an arbitrary whole second
    fn bc(delay: u64) -> BoundaryClose {
        BoundaryClose::new(Coin::Btc, delay)
    }

    #[test]
    fn emits_the_last_trade_of_the_second_as_that_seconds_close() {
        let mut b = bc(0);
        b.on_trade(S * 1000 + 100, 64_000.0);
        b.on_trade(S * 1000 + 900, 64_010.0);
        let out = b.on_tick((S + 1) * 1000);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].close_px, 64_010.0, "the LAST trade of the second is the close");
    }

    /// The p0 off-by-one guard. `resolve_p0` gates on
    /// `close_ts_ms / 1000 + 1 == slot_open`; the +1000 form would break it.
    #[test]
    fn close_ts_ms_uses_binances_t_plus_999_convention() {
        let mut b = bc(0);
        b.on_trade(S * 1000 + 500, 64_000.0);
        let out = b.on_tick((S + 1) * 1000);
        assert_eq!(out[0].close_ts_ms, S * 1000 + 999);
        assert_eq!(out[0].close_ts_ms / 1000 + 1, S + 1, "must satisfy resolve_p0's gate");
    }

    #[test]
    fn does_not_finalize_a_second_before_boundary_plus_delay() {
        let mut b = bc(100);
        b.on_trade(S * 1000 + 500, 64_000.0);
        assert!(b.on_tick((S + 1) * 1000 + 99).is_empty(), "99 ms < delay 100");
        assert_eq!(b.on_tick((S + 1) * 1000 + 100).len(), 1, "exactly at the delay");
    }

    #[test]
    fn a_silent_second_carries_the_previous_close_forward() {
        let mut b = bc(0);
        b.on_trade(S * 1000 + 500, 64_000.0);
        b.on_tick((S + 1) * 1000);
        let out = b.on_tick((S + 2) * 1000); // nothing traded in second S+1
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].close_px, 64_000.0, "Binance carries the prior close on a silent bar");
        assert_eq!(out[0].close_ts_ms, (S + 1) * 1000 + 999);
    }

    #[test]
    fn never_emits_the_same_second_twice() {
        let mut b = bc(0);
        b.on_trade(S * 1000 + 500, 64_000.0);
        assert_eq!(b.on_tick((S + 1) * 1000).len(), 1);
        assert!(b.on_tick((S + 1) * 1000 + 50).is_empty());
        assert!(b.on_tick((S + 1) * 1000 + 999).is_empty());
    }

    #[test]
    fn a_trade_arriving_after_its_second_was_emitted_is_ignored() {
        let mut b = bc(0);
        b.on_trade(S * 1000 + 500, 64_000.0);
        b.on_tick((S + 1) * 1000);
        b.on_trade(S * 1000 + 950, 99_999.0); // too late; that bar already went out
        let out = b.on_tick((S + 2) * 1000);
        assert_eq!(out[0].close_px, 64_000.0, "a superseded bar must not be revised");
    }

    #[test]
    fn buckets_trades_by_trade_time_not_arrival_order() {
        let mut b = bc(0);
        b.on_trade(S * 1000 + 900, 64_010.0);
        b.on_trade(S * 1000 + 100, 64_000.0); // out-of-order arrival, EARLIER trade
        let out = b.on_tick((S + 1) * 1000);
        assert_eq!(out[0].close_px, 64_010.0, "the later TRADE TIME wins, not the later arrival");
    }

    #[test]
    fn emits_nothing_before_any_trade_has_been_seen() {
        let mut b = bc(0);
        assert!(b.on_tick((S + 5) * 1000).is_empty(), "must never invent a close from nothing");
    }

    /// The driver sleeps until the exact finalize instant rather than polling
    /// near it — at `delay = 0` a 10 ms poll would give back 10 ms of the 152
    /// this whole feature exists to win.
    #[test]
    fn next_tick_lands_exactly_on_the_next_boundary_plus_delay() {
        assert_eq!(next_tick_ms(S * 1000, 0), (S + 1) * 1000);
        assert_eq!(next_tick_ms(S * 1000 + 1, 0), (S + 1) * 1000);
        assert_eq!(next_tick_ms(S * 1000 + 999, 0), (S + 1) * 1000);
        // with a delay the instant shifts, and only forward
        assert_eq!(next_tick_ms(S * 1000, 100), S * 1000 + 100);
        assert_eq!(next_tick_ms(S * 1000 + 100, 100), (S + 1) * 1000 + 100);
        assert_eq!(next_tick_ms(S * 1000 + 500, 100), (S + 1) * 1000 + 100);
    }

    #[test]
    fn next_tick_is_always_strictly_in_the_future() {
        for now in [0u64, 1, 999, 1000, 1001, S * 1000 + 137] {
            for delay in [0u64, 50, 100, 200] {
                assert!(
                    next_tick_ms(now, delay) > now,
                    "next_tick_ms({now}, {delay}) must advance or the driver spins"
                );
            }
        }
    }

    /// Carry-forward is how Binance itself fills a silent bar, but WE cannot
    /// tell "nobody traded" from "our socket died". Without a bound, a dead
    /// aggTrade feed would emit a frozen price forever — and because those
    /// synthetic closes drive `spawn_kline_watch_task` (O1), the feed-health
    /// watchdog would see a perfectly healthy cadence and never disarm.
    #[test]
    fn stops_carrying_forward_once_the_trade_feed_goes_silent() {
        let mut b = bc(0);
        b.on_trade(S * 1000 + 500, 64_000.0);
        let out = b.on_tick((S + 20) * 1000);
        assert!(!out.is_empty(), "the observed second still emits");
        assert!(
            out.len() < 20,
            "must not carry a dead feed forward indefinitely; got {} bars",
            out.len()
        );
        let last_sec = out.last().unwrap().close_ts_ms / 1000;
        assert!(
            last_sec <= S + MAX_CARRY_MS / 1000,
            "carried {last_sec} — past the {MAX_CARRY_MS} ms bound from the last trade"
        );
    }

    #[test]
    fn a_resumed_trade_feed_starts_emitting_again() {
        let mut b = bc(0);
        b.on_trade(S * 1000 + 500, 64_000.0);
        b.on_tick((S + 20) * 1000); // goes silent, emission stops
        b.on_trade((S + 30) * 1000 + 500, 64_100.0);
        let out = b.on_tick((S + 31) * 1000);
        assert!(
            out.iter().any(|c| c.close_px == 64_100.0),
            "a recovered feed must resume, not stay latched off"
        );
    }

    // --- ShadowCompare: verifying the 90.6% in production, not trusting it ---

    #[test]
    fn pairs_a_reconstruction_with_its_kline_and_reports_the_delta() {
        let mut s = ShadowCompare::default();
        assert!(s.on_recon(S * 1000 + 999, 64_000.01).is_none(), "one half is not a pair");
        let got = s.on_kline(S * 1000 + 999, 64_000.00).expect("pairs on the second half");
        assert_eq!(got.close_ts_ms, S * 1000 + 999);
        assert!((got.delta_usd - 0.01).abs() < 1e-9, "delta is recon - kline");
        assert!(!got.exact);
    }

    #[test]
    fn an_identical_reconstruction_is_reported_as_exact() {
        let mut s = ShadowCompare::default();
        s.on_recon(S * 1000 + 999, 64_000.0);
        assert!(s.on_kline(S * 1000 + 999, 64_000.0).expect("pairs").exact);
    }

    #[test]
    fn pairs_regardless_of_which_half_arrives_first() {
        let mut s = ShadowCompare::default();
        assert!(s.on_kline(S * 1000 + 999, 64_000.0).is_none());
        assert!(s.on_recon(S * 1000 + 999, 64_000.0).is_some(), "kline-first must pair too");
    }

    #[test]
    fn different_bars_do_not_pair_with_each_other() {
        let mut s = ShadowCompare::default();
        s.on_recon(S * 1000 + 999, 64_000.0);
        assert!(s.on_kline((S + 1) * 1000 + 999, 64_010.0).is_none(), "a neighbour is not a pair");
    }

    /// A permanently one-sided feed (e.g. the kline socket dying) must not grow
    /// the map without bound.
    #[test]
    fn an_unpaired_half_is_evicted_once_stale() {
        let mut s = ShadowCompare::default();
        s.on_recon(S * 1000 + 999, 64_000.0);
        s.on_kline((S + 120) * 1000 + 999, 64_500.0); // 120 s later, unrelated bar
        assert!(
            s.on_kline(S * 1000 + 999, 64_000.0).is_none(),
            "the stale unpaired half must have been dropped, not held forever"
        );
    }

    #[test]
    fn a_late_tick_catches_up_every_intervening_second() {
        let mut b = bc(0);
        b.on_trade(S * 1000 + 500, 64_000.0);
        b.on_trade((S + 2) * 1000 + 500, 64_020.0);
        let out = b.on_tick((S + 3) * 1000); // three seconds finalizable at once
        assert_eq!(out.len(), 3, "a stalled tick must not silently drop bars");
        let secs: Vec<u64> = out.iter().map(|c| c.close_ts_ms / 1000).collect();
        assert_eq!(secs, vec![S, S + 1, S + 2], "in order, contiguous");
        assert_eq!(out[1].close_px, 64_000.0, "silent second carries forward");
        assert_eq!(out[2].close_px, 64_020.0);
    }
}
