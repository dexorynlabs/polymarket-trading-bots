//! PURE decision core: ONE automaton per slot, shared by the `1c` and `99c`
//! modes. The engine has no mode switch of its own — the configured ask band
//! decides which token can be bought (`strategy::mode`), and
//! `cards::entry_gates_pass` (`burst` or `calm`, `tick.rs`) decides when.
//!
//! No I/O, no clock, no async — deliberately, so the behaviour is a total
//! function of its inputs and the tests (`tests/one_cent_trigger.rs`,
//! `tests/leader_trigger.rs`, `tests/posttwap_parity.rs`) can pin it.
//!
//! - A book event only records the ladder; it can never fire.
//! - The tick (`@aggTrade`) path is the ONLY fire path.
//! - **One shot per SLOT**, no re-arm after a zero-fill: a re-arm only
//!   double-buys the slots that were already going wrong.
//!
//! States: Ready -> Spent. **Spent is ABSORBING** — the shot is consumed on
//! ATTEMPT, and neither a rail veto, a zero-fill, nor a failed POST gives it
//! back (`slot_driver`/`live` own that half of the rule).

use crate::strategy::cards::{ask_qualifies, entry_gates_pass, placebo_gates_pass, CardId, BURST};
use crate::strategy::features::{SideFeatures, SlotSeries, SLOT_SECS};
use crate::strategy::flow::{Burst, FlowRing, FlowStats};
use crate::strategy::tick::TickCfg;
use crate::types::Side;

pub const SIDES: [Side; 2] = [Side::Up, Side::Down];

#[derive(Debug, Clone)]
pub enum Event {
    /// `p0` is the close of the kline immediately PRECEDING slot open, and the
    /// caller must have verified that kline is contiguous (`runtime::
    /// resolve_p0`). Under this spec `p0` feeds TELEMETRY only — no gate reads
    /// it — but the runtime still refuses to drive a slot without one, which
    /// is inherited fail-closed conservatism, not a §3 requirement.
    SlotOpen { slot_open: u64, p0: f64 },
    Kline { t: usize, close_px: f64 },
    /// `ingest_ms` is the event's OWN arrival stamp (`BookUpdate::ts_micros`
    /// at WS parse time). It carries NO slot-second: the fire bounds the held
    /// book's age in milliseconds, which a slot-second cannot express, and
    /// nothing else about a book event is time-gated — the caller is the one
    /// that drops out-of-slot updates (`slot_driver::book_to_t`).
    Book { ingest_ms: u64, side: Side, asks: Vec<(f64, f64)> },
    /// One Binance `@aggTrade`. `venue_ts_ms` is Binance's own `T` (the
    /// GATE's clock: it picks the slot-second the frame is checked against and
    /// anchors the trailing window); `recv_ms` is our local arrival stamp (the
    /// FIRE's clock: the second the order is labelled with, and the reference
    /// for the held book's age). The two differ by the ~135ms stream latency
    /// (spec §5). `qty` is the print's BTC size and `taker_buy` is Binance's
    /// `!m` — together they ARE the gate (§3.5).
    Trade { venue_ts_ms: u64, px: f64, qty: f64, taker_buy: bool, recv_ms: u64 },
    /// The book WS has gone silent. Drops the held ladders — see `on_event`.
    FeedUnhealthy,
    SlotEnd,
}

/// The trigger metadata of a fire, carried to telemetry and nowhere else.
/// There is exactly ONE fire path now (module doc), so this is a struct rather
/// than the earlier bar/tick enum.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TickFire {
    /// Binance's `T` for the tick that cleared the gate.
    pub venue_ts_ms: u64,
    /// The OBSERVED trailing volume in BTC — the realised burst, never the
    /// configured threshold. A distribution of realised bursts is what tells
    /// an operator whether the gate fires at the margin or well inside it.
    pub vol_btc: f64,
    /// The OBSERVED share of that volume toward the bought token, in [0, 1].
    pub lean: f64,
    /// The configured trailing window it was measured over.
    pub window_ms: u64,
    /// Age of the held book this fire bought against, at the tick's arrival.
    pub book_age_ms: u64,
}

#[derive(Debug, Clone)]
pub enum Action {
    /// SEND THIS. `size_shares` is the clip at the LIMIT
    /// (`clip_usd / limit_px`, see `sizing::fak_shares`) and `limit_px` is the
    /// FAK's limit (2c in `1c` mode, 99c in `99c` mode) — price priority fills
    /// at the standing ask when it is still there. The exposure commit is
    /// `size_shares * limit_px`, which under this sizing is exactly
    /// `clip_usd`; the ask decides how many shares that money returns
    /// (`clip_usd / fill_px`), never how much money goes out.
    Fire {
        t: usize,
        side: Side,
        card: CardId,
        /// The standing ask this fire qualified on.
        best_ask: f64,
        limit_px: f64,
        size_shares: f64,
        clip_usd: f64,
        /// Closed-bar features at the last delivered kline, TELEMETRY only
        /// (spec §4.1). `None` before the first kline of the slot — which no
        /// longer blocks anything, since no gate reads them.
        feats: Option<SideFeatures>,
        /// The FILTERED ladder (positive price, positive size) — the same
        /// levels `best_ask` was computed from.
        asks: Vec<(f64, f64)>,
        tick: TickFire,
    },
    /// Would-be trade for the ADVERSE-lean mirror. LOG ONLY, forever — its
    /// cumulative PnL must stay negative. (The mirror is inherited from the
    /// earlier design's §11.5 and RE-POINTED at the new gate by
    /// POST-TWAP-SPEC §6 — it now flips the lean, not a velocity.)
    Placebo {
        t: usize,
        side: Side,
        card: CardId,
        /// Binance's `T` for the tick the mirror observed — without it a
        /// mirror row cannot be joined to the print that produced it, and the
        /// alarm is only as good as its provenance.
        venue_ts_ms: u64,
        vol_btc: f64,
        lean: f64,
        best_ask: f64,
    },
}

/// The slot's single shot (§3.6). `Spent` is absorbing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShotState {
    Ready,
    Spent,
}

#[derive(Debug, Clone, Copy)]
pub struct EngineCfg {
    /// The entry BAND, inclusive: the standing ask must sit in
    /// `[ask_price_min, ask_price_max]`. This band is the whole difference
    /// between the `1c` and `99c` modes (`strategy::mode`).
    pub ask_price_min: f64,
    pub ask_price_max: f64,
    /// The FAK's limit price. Never an entry permission — an ask at
    /// this price does NOT qualify.
    pub limit_price: f64,
    /// Flat per-fire budget in USD (§3.7).
    pub clip_usd: f64,
    /// The trigger. `None` disables firing entirely — `Event::Trade` becomes a
    /// no-op and this engine can only ever record books and features, which is
    /// what `--dry-run` with the tick path off means.
    pub tick: Option<TickCfg>,
}

/// The most recent ask ladder delivered for one side, kept so a tick can fire
/// against a book it was ALREADY given rather than looking one up by a future
/// time.
#[derive(Debug, Clone)]
struct HeldBook {
    ingest_ms: u64,
    /// Already filtered to positive price and positive size.
    levels: Vec<(f64, f64)>,
}

pub struct SlotEngine {
    cfg: EngineCfg,
    slot_open: u64,
    series: Option<SlotSeries>,
    /// Features at the most recent kline close, per side. TELEMETRY only.
    last_feats: [Option<SideFeatures>; 2],
    last_kline_t: Option<usize>,
    /// §3.6: the slot's one shot.
    shot: ShotState,
    /// The placebo mirror's OWN one-per-slot latch, deliberately separate: if
    /// the real shot gated it, the mirror's sample would condition on "the
    /// real lane never fired here" and the alarm would weaken exactly when it
    /// matters.
    placebo_spent: bool,
    /// Trailing `@aggTrade` flow — the gate input (`flow.rs`). Rebuilt at
    /// every `SlotOpen`.
    flow: FlowRing,
    /// Last ask ladder delivered per side — the fire target.
    last_book: [Option<HeldBook>; 2],
}

fn side_idx(s: Side) -> usize {
    match s {
        Side::Up => 0,
        Side::Down => 1,
    }
}

/// The aggressor direction that moves BTC this token's way: taker BUYs move
/// it toward `up`, taker SELLs toward `down`. For a 1c trailer that flow is a
/// recovery; for a 99c leader it confirms the lead.
fn toward_is_buy(side: Side) -> bool {
    matches!(side, Side::Up)
}

impl SlotEngine {
    pub fn new(cfg: EngineCfg) -> Self {
        let window_ms = cfg.tick.map_or(crate::strategy::flow::FLOW_WINDOW_MS, |t| t.window_ms);
        Self {
            cfg,
            slot_open: 0,
            series: None,
            last_feats: [None; 2],
            last_kline_t: None,
            shot: ShotState::Ready,
            placebo_spent: false,
            flow: FlowRing::new(window_ms),
            last_book: [None, None],
        }
    }

    pub fn slot_open(&self) -> u64 {
        self.slot_open
    }

    /// The slot's shot state (§3.6). Read-only.
    pub fn shot(&self) -> ShotState {
        self.shot
    }

    /// Features at the most recent kline close, per side. Read-only: the shell
    /// needs this to emit `Feat` telemetry rows without maintaining a second
    /// implementation of the feature math.
    pub fn feats(&self, side: Side) -> Option<SideFeatures> {
        self.last_feats[side_idx(side)]
    }

    /// The second whose kline advance last updated `last_feats`. `None` before
    /// the first successful advance, and reset on `SlotOpen`. A replayed or
    /// gap-skipped second leaves this UNCHANGED — which is what distinguishes
    /// "this `t` just advanced the series" from "`feats()` still reports
    /// `Some` from an earlier second".
    pub fn last_kline_t(&self) -> Option<usize> {
        self.last_kline_t
    }

    /// Trailing flow context for telemetry rows (`flow.rs`). The gate reads
    /// `Burst` from the same ring; this is the descriptive view of it.
    pub fn flow_stats(&self) -> Option<FlowStats> {
        self.flow.stats()
    }

    pub fn on_event(&mut self, ev: Event) -> Vec<Action> {
        match ev {
            Event::SlotOpen { slot_open, p0 } => {
                // A duplicate/stale SlotOpen (e.g. a WS-reconnect re-running
                // market discovery mid-slot) must not give back a spent shot.
                // Only a strictly-greater slot_open is a real new slot; the
                // initial field is 0, so the first real SlotOpen always wins.
                if slot_open <= self.slot_open {
                    tracing::warn!(
                        slot_open,
                        current = self.slot_open,
                        "ignoring non-advancing SlotOpen"
                    );
                    return Vec::new();
                }
                self.slot_open = slot_open;
                self.series = Some(SlotSeries::new(p0));
                self.last_feats = [None; 2];
                self.last_kline_t = None;
                self.shot = ShotState::Ready;
                self.placebo_spent = false;
                self.flow =
                    FlowRing::new(self.cfg.tick.map_or(crate::strategy::flow::FLOW_WINDOW_MS, |t| t.window_ms));
                self.last_book = [None, None];
                Vec::new()
            }
            Event::Kline { t, close_px } => {
                self.on_kline(t, close_px);
                Vec::new()
            }
            Event::Book { ingest_ms, side, asks } => {
                self.on_book(ingest_ms, side, &asks);
                Vec::new()
            }
            Event::Trade { venue_ts_ms, px, qty, taker_buy, recv_ms } => {
                self.on_trade(venue_ts_ms, px, qty, taker_buy, recv_ms)
            }
            // The held ladders are the only state a dead book feed corrupts,
            // and dropping them is exactly what the freshness bound would do
            // a moment later anyway. There is nothing armed to disarm.
            Event::FeedUnhealthy => {
                self.last_book = [None, None];
                Vec::new()
            }
            Event::SlotEnd => {
                self.shot = ShotState::Spent;
                Vec::new()
            }
        }
    }

    /// Klines feed `features.rs` and nothing else — D/zD/σ60 are TELEMETRY
    /// under this spec (§4.1). A gap disarms nothing (there is nothing armed);
    /// it simply stops the feature series advancing, and the `Feat` rows stop
    /// with it.
    fn on_kline(&mut self, t: usize, close_px: f64) {
        let Some(series) = self.series.as_mut() else { return };
        // A replayed second the feed already gave us is benign and must be a
        // no-op — a reconnect burst must not perturb the series. A SKIPPED
        // second is not: velocities computed across a hole are never trusted,
        // so `push` refuses and the series simply stops here.
        if t < series.len() {
            tracing::debug!(t, "stale kline replay ignored");
            return;
        }
        if series.push(t, close_px).is_err() {
            tracing::debug!(t, "kline gap: the feature series stops advancing");
            return;
        }
        self.last_kline_t = Some(t);
        for s in SIDES {
            self.last_feats[side_idx(s)] = self.series.as_ref().and_then(|se| se.side(t, s));
        }
    }

    /// Records the ladder. **It cannot fire** — the bar path is retired
    /// (module doc). Held BEFORE any filtering can produce an empty vector,
    /// including the empty-ladder case: an ask that jumped out of the cheap
    /// band must overwrite the cheap book it replaced, or the freshness bound
    /// would be measuring a phantom the venue no longer displays.
    fn on_book(&mut self, ingest_ms: u64, side: Side, asks: &[(f64, f64)]) {
        // A phantom zero-size level must not set `best_ask`. `Event::Book`
        // requires positive-size levels; the engine defends anyway.
        let levels: Vec<(f64, f64)> =
            asks.iter().copied().filter(|(p, s)| *p > 0.0 && *s > 0.0).collect();
        self.last_book[side_idx(side)] = Some(HeldBook { ingest_ms, levels });
    }

    /// The ONLY fire path (spec §3.5, §6).
    fn on_trade(
        &mut self,
        venue_ts_ms: u64,
        px: f64,
        qty: f64,
        taker_buy: bool,
        recv_ms: u64,
    ) -> Vec<Action> {
        let Some(cfg) = self.cfg.tick else { return Vec::new() };
        // A print that arrived out of venue-time order is dropped whole: it
        // must neither enter the window the gate sums nor fire on its own.
        if !self.flow.push(venue_ts_ms, px, qty, taker_buy) {
            return Vec::new();
        }
        let Some(burst) = self.flow.burst() else { return Vec::new() };
        // The GATE's clock: the frame is Binance's own trade time (spec §3.3),
        // exactly as the census scans it.
        let Some(t_tick) = self.slot_second(venue_ts_ms) else { return Vec::new() };
        if !BURST.frame_contains(t_tick) {
            return Vec::new();
        }
        // The FIRE's clock: the second the order is labelled with.
        let Some(t_fire) = self.slot_second(recv_ms) else { return Vec::new() };

        let mut out = Vec::new();
        for side in SIDES {
            out.extend(self.on_trade_side(side, &burst, &cfg, venue_ts_ms, recv_ms, t_fire));
        }
        out
    }

    /// One side's evaluation. Sides share the print and the ring but nothing
    /// else, and `Up` is scanned first — which is also the 1c census's own
    /// tie-break when both tokens show a qualifying ask.
    fn on_trade_side(
        &mut self,
        side: Side,
        burst: &Burst,
        cfg: &TickCfg,
        venue_ts_ms: u64,
        recv_ms: u64,
        t_fire: usize,
    ) -> Vec<Action> {
        let si = side_idx(side);
        // §3.2/§3.4: the trailing side is the one whose STANDING ask is the
        // entry price, read off the book we already hold.
        let Some(held) = self.last_book[si].as_ref() else { return Vec::new() };
        let book_age_ms = recv_ms.saturating_sub(held.ingest_ms);
        if book_age_ms > cfg.max_book_age_ms {
            return Vec::new();
        }
        let Some(best_ask) = held
            .levels
            .iter()
            .map(|(p, _)| *p)
            .fold(None::<f64>, |acc, p| Some(acc.map_or(p, |a: f64| a.min(p))))
        else {
            return Vec::new();
        };
        if !ask_qualifies(best_ask, self.cfg.ask_price_min, self.cfg.ask_price_max) {
            return Vec::new();
        }

        let toward_buy = toward_is_buy(side);
        let dir_btc = if toward_buy { burst.buy_btc } else { burst.sell_btc };
        let lean = if burst.vol_btc > 0.0 { dir_btc / burst.vol_btc } else { 0.0 };
        let mut out = Vec::new();

        if self.shot == ShotState::Ready && entry_gates_pass(burst, toward_buy, cfg) {
            // Spent HERE, before anything downstream can decline to send: a
            // rail veto, a zero-fill and a failed POST all leave it spent
            // (§3.6 — a retry double-buys, a miss is free).
            self.shot = ShotState::Spent;
            out.push(Action::Fire {
                t: t_fire,
                side,
                card: BURST.id,
                best_ask,
                limit_px: self.cfg.limit_price,
                // Converted at the LIMIT, not the observed ask: a BUY is a
                // dollar commitment (`shares * limit_px`) and price
                // improvement pays in extra shares, so this is what makes a
                // fire spend exactly `clip_usd` (§3.7) wherever in the band
                // the ask sits. `sizing::fak_shares` carries the mechanism.
                size_shares: crate::strategy::sizing::fak_shares(self.cfg.clip_usd, self.cfg.limit_price),
                clip_usd: self.cfg.clip_usd,
                feats: self.last_feats[si],
                asks: held.levels.clone(),
                tick: TickFire {
                    venue_ts_ms,
                    vol_btc: burst.vol_btc,
                    lean,
                    window_ms: cfg.window_ms,
                    book_age_ms,
                },
            });
        } else if !self.placebo_spent && placebo_gates_pass(burst, toward_buy, cfg) {
            self.placebo_spent = true;
            out.push(Action::Placebo {
                t: t_fire,
                side,
                card: BURST.id,
                venue_ts_ms,
                vol_btc: burst.vol_btc,
                lean,
                best_ask,
            });
        }
        out
    }

    /// Slot-second of a wall-clock millisecond. The one conversion that
    /// happens INSIDE the engine: the fire path needs raw ms for the trailing
    /// window and the book age, so the shell cannot pre-convert for it.
    fn slot_second(&self, ms: u64) -> Option<usize> {
        let secs = ms / 1000;
        let t = secs.checked_sub(self.slot_open)? as usize;
        if t >= SLOT_SECS {
            None
        } else {
            Some(t)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::tick::TriggerMode;

    const SLOT: u64 = 1_786_098_600;
    const BASE: u64 = SLOT * 1000;

    fn cfg() -> EngineCfg {
        EngineCfg {
            ask_price_min: 0.97,
            ask_price_max: 0.99,
            limit_price: 0.99,
            clip_usd: 20.0,
            tick: Some(TickCfg {
                mode: TriggerMode::Burst,
                v_btc: 0.5,
                lean_min: 0.6,
                adverse_max_btc: 0.3,
                window_ms: 1_000,
                max_book_age_ms: 15_000,
            }),
        }
    }

    /// An engine at slot open with a 99c ask standing on `side` since second
    /// `t`, and nothing else.
    fn engine_with_99c_ask(side: Side, t: usize) -> SlotEngine {
        let mut e = SlotEngine::new(cfg());
        e.on_event(Event::SlotOpen { slot_open: SLOT, p0: 100_000.0 });
        e.on_event(Event::Book {
            ingest_ms: BASE + (t as u64) * 1000,
            side,
            asks: vec![(0.99, 5_000.0)],
        });
        e
    }

    /// A burst of `n` prints of `qty` BTC each inside second `t`, all in the
    /// given aggressor direction, one per 10ms.
    fn burst_in(e: &mut SlotEngine, t: usize, n: u64, qty: f64, taker_buy: bool) -> Vec<Action> {
        let mut out = Vec::new();
        for i in 0..n {
            let ms = BASE + (t as u64) * 1000 + i * 10;
            out.extend(e.on_event(Event::Trade {
                venue_ts_ms: ms,
                px: 100_000.0,
                qty,
                taker_buy,
                recv_ms: ms,
            }));
        }
        out
    }

    fn fires(actions: &[Action]) -> Vec<&Action> {
        actions.iter().filter(|a| matches!(a, Action::Fire { .. })).collect()
    }

    #[test]
    fn a_burst_toward_a_99c_leader_fires_once() {
        let mut e = engine_with_99c_ask(Side::Up, 200);
        // 6 x 0.1 BTC of taker BUYs = 0.6 BTC, 100% toward UP.
        let acts = burst_in(&mut e, 200, 6, 0.1, true);
        let f = fires(&acts);
        assert_eq!(f.len(), 1, "the burst must fire exactly once");
        let Action::Fire { side, best_ask, limit_px, size_shares, tick, .. } = f[0] else {
            unreachable!()
        };
        assert_eq!(*side, Side::Up);
        assert_eq!(*best_ask, 0.99);
        assert_eq!(*limit_px, 0.99);
        assert!((*size_shares * 0.99 - 20.0).abs() < 1e-9, "$20 committed at the 99c limit");
        assert!(tick.vol_btc >= 0.5 && (tick.lean - 1.0).abs() < 1e-12);
        assert_eq!(e.shot(), ShotState::Spent);
    }

    #[test]
    fn the_shot_is_spent_for_the_whole_slot_and_never_re_arms() {
        let mut e = engine_with_99c_ask(Side::Up, 200);
        assert_eq!(fires(&burst_in(&mut e, 200, 6, 0.1, true)).len(), 1);
        // A second, larger burst on a still-standing ask, seconds later.
        e.on_event(Event::Book {
            ingest_ms: BASE + 240_000,
            side: Side::Up,
            asks: vec![(0.99, 5_000.0)],
        });
        assert!(
            fires(&burst_in(&mut e, 240, 20, 0.5, true)).is_empty(),
            "spec §3.6: one shot per slot, no re-arm"
        );
    }

    #[test]
    fn a_burst_leaning_the_wrong_way_does_not_fire() {
        let mut e = engine_with_99c_ask(Side::Up, 200);
        // Taker SELLs push an UP token further behind: adverse, not recovery.
        assert!(fires(&burst_in(&mut e, 200, 6, 0.1, false)).is_empty());
        assert_eq!(e.shot(), ShotState::Ready, "a non-firing burst must not spend the shot");
    }

    #[test]
    fn a_down_entry_recovers_on_taker_sells() {
        let mut e = engine_with_99c_ask(Side::Down, 200);
        assert!(fires(&burst_in(&mut e, 200, 6, 0.1, true)).is_empty(), "buys are adverse to down");
        let mut e2 = engine_with_99c_ask(Side::Down, 200);
        assert_eq!(fires(&burst_in(&mut e2, 200, 6, 0.1, false)).len(), 1);
    }

    /// The clip is a DOLLAR budget: the fire commits exactly `clip_usd` at
    /// every ask in the band, and price improvement returns extra shares.
    #[test]
    fn a_fire_commits_exactly_the_clip_at_every_ask_in_the_band() {
        for ask in [0.97, 0.98, 0.99] {
            let mut e = SlotEngine::new(cfg());
            e.on_event(Event::SlotOpen { slot_open: SLOT, p0: 100_000.0 });
            e.on_event(Event::Book {
                ingest_ms: BASE + 200_000,
                side: Side::Up,
                asks: vec![(ask, 5_000.0)],
            });
            let acts = burst_in(&mut e, 200, 6, 0.1, true);
            let f = fires(&acts);
            assert_eq!(f.len(), 1, "ask {ask} must fire in the shipped band");
            let Action::Fire { size_shares, limit_px, clip_usd, best_ask, .. } = f[0] else {
                unreachable!()
            };
            let commit = size_shares * limit_px;
            assert!((commit - clip_usd).abs() < 1e-9, "ask {ask}: commits ${commit}, clip ${clip_usd}");
            assert_eq!(*best_ask, ask);
        }
    }

    #[test]
    fn the_trailing_one_cent_token_never_fires() {
        // The 1c bot's target. Here it is out of band on BOTH sides of the
        // direction question: a leader-confirming burst for DOWN is a SELL
        // burst, and the UP 1c ask simply does not qualify.
        let mut e = SlotEngine::new(cfg());
        e.on_event(Event::SlotOpen { slot_open: SLOT, p0: 100_000.0 });
        e.on_event(Event::Book { ingest_ms: BASE + 200_000, side: Side::Up, asks: vec![(0.01, 5_000.0)] });
        assert!(fires(&burst_in(&mut e, 200, 6, 0.1, true)).is_empty());
        assert_eq!(e.shot(), ShotState::Ready);
    }

    /// The same engine with the `1c` preset buys the TRAILER and ignores the
    /// leader — the mode is nothing but the band.
    #[test]
    fn a_1c_config_buys_the_trailer_and_ignores_the_leader() {
        let one_cent = EngineCfg {
            ask_price_min: 0.009,
            ask_price_max: 0.011,
            limit_price: 0.02,
            clip_usd: 10.0,
            ..cfg()
        };
        let mut e = SlotEngine::new(one_cent);
        e.on_event(Event::SlotOpen { slot_open: SLOT, p0: 100_000.0 });
        e.on_event(Event::Book { ingest_ms: BASE + 200_000, side: Side::Up, asks: vec![(0.99, 5_000.0)] });
        e.on_event(Event::Book { ingest_ms: BASE + 200_000, side: Side::Down, asks: vec![(0.01, 5_000.0)] });
        // SELLs move BTC toward DOWN: the trailer's recovery.
        let acts = burst_in(&mut e, 200, 6, 0.1, false);
        let f = fires(&acts);
        assert_eq!(f.len(), 1);
        let Action::Fire { side, best_ask, limit_px, size_shares, .. } = f[0] else { unreachable!() };
        assert_eq!(*side, Side::Down);
        assert_eq!(*best_ask, 0.01);
        assert_eq!(*limit_px, 0.02);
        assert!((*size_shares - 500.0).abs() < 1e-9, "$10 at the 2c limit");
    }

    #[test]
    fn an_ask_above_the_band_never_fires() {
        for ask in [0.991, 0.995, 0.999] {
            let mut e = SlotEngine::new(cfg());
            e.on_event(Event::SlotOpen { slot_open: SLOT, p0: 100_000.0 });
            e.on_event(Event::Book { ingest_ms: BASE + 200_000, side: Side::Up, asks: vec![(ask, 5_000.0)] });
            assert!(fires(&burst_in(&mut e, 200, 6, 0.1, true)).is_empty(), "ask {ask}");
            assert_eq!(e.shot(), ShotState::Ready);
        }
    }

    #[test]
    fn calm_mode_fires_on_a_quiet_print_and_vetoes_an_adverse_burst() {
        let calm = EngineCfg {
            tick: Some(TickCfg { mode: TriggerMode::Calm, ..cfg().tick.unwrap() }),
            ..cfg()
        };
        // A single tiny print with a 99c UP leader standing: calm, fire.
        let mut e = SlotEngine::new(calm);
        e.on_event(Event::SlotOpen { slot_open: SLOT, p0: 100_000.0 });
        e.on_event(Event::Book { ingest_ms: BASE + 200_000, side: Side::Up, asks: vec![(0.99, 5_000.0)] });
        assert_eq!(fires(&burst_in(&mut e, 200, 1, 0.01, false)).len(), 1);

        // An adverse (SELL) burst against the UP leader: every print after the
        // window's adverse volume reaches 0.3 is vetoed; the first two (0.1,
        // 0.2 BTC) are still calm and would fire, so start already adverse.
        let mut e = SlotEngine::new(calm);
        e.on_event(Event::SlotOpen { slot_open: SLOT, p0: 100_000.0 });
        burst_in(&mut e, 199, 1, 0.0, false);
        e.on_event(Event::Trade {
            venue_ts_ms: BASE + 200_000,
            px: 100_000.0,
            qty: 0.6,
            taker_buy: false,
            recv_ms: BASE + 200_000,
        });
        e.on_event(Event::Book { ingest_ms: BASE + 200_100, side: Side::Up, asks: vec![(0.99, 5_000.0)] });
        let acts = burst_in(&mut e, 200, 3, 0.1, false);
        assert!(fires(&acts).is_empty(), "adverse window must veto calm entry");
        assert!(
            acts.iter().any(|a| matches!(a, Action::Placebo { .. })),
            "the adverse burst is exactly the placebo mirror"
        );
        assert_eq!(e.shot(), ShotState::Ready);
    }

    #[test]
    fn the_frame_is_enforced_on_binances_clock() {
        // t=149 is outside [150, 291); t=150 is inside.
        let mut early = engine_with_99c_ask(Side::Up, 149);
        assert!(fires(&burst_in(&mut early, 149, 6, 0.1, true)).is_empty());
        let mut ok = engine_with_99c_ask(Side::Up, 150);
        assert_eq!(fires(&burst_in(&mut ok, 150, 6, 0.1, true)).len(), 1);
        // ...and t=291 is past the upper bound.
        let mut late = engine_with_99c_ask(Side::Up, 291);
        assert!(fires(&burst_in(&mut late, 291, 6, 0.1, true)).is_empty());
        let mut last = engine_with_99c_ask(Side::Up, 290);
        assert_eq!(fires(&burst_in(&mut last, 290, 6, 0.1, true)).len(), 1);
    }

    #[test]
    fn a_stale_held_book_is_a_skip_and_never_spends_the_shot() {
        let mut e = engine_with_99c_ask(Side::Up, 200);
        // The book has not been re-delivered for 16s — past the bound.
        let acts = burst_in(&mut e, 216, 6, 0.1, true);
        assert!(fires(&acts).is_empty(), "a book older than max_book_age_ms must not be bought");
        assert_eq!(e.shot(), ShotState::Ready);
    }

    #[test]
    fn a_feed_unhealthy_event_drops_the_held_ladders() {
        let mut e = engine_with_99c_ask(Side::Up, 200);
        e.on_event(Event::FeedUnhealthy);
        assert!(fires(&burst_in(&mut e, 200, 6, 0.1, true)).is_empty());
        // ...and a fresh book restores the ability to fire.
        e.on_event(Event::Book {
            ingest_ms: BASE + 201_000,
            side: Side::Up,
            asks: vec![(0.99, 5_000.0)],
        });
        assert_eq!(fires(&burst_in(&mut e, 201, 6, 0.1, true)).len(), 1);
    }

    #[test]
    fn a_book_event_can_no_longer_fire() {
        let mut e = SlotEngine::new(cfg());
        e.on_event(Event::SlotOpen { slot_open: SLOT, p0: 100_000.0 });
        // Prime a qualifying burst first, then deliver the cheap ask: under
        // the retired BAR path this ordering fired. It must not now.
        burst_in(&mut e, 200, 6, 0.1, true);
        let acts = e.on_event(Event::Book {
            ingest_ms: BASE + 200_500,
            side: Side::Up,
            asks: vec![(0.99, 5_000.0)],
        });
        assert!(acts.is_empty(), "the bar fire path is retired (spec §6)");
    }

    #[test]
    fn a_duplicate_slot_open_does_not_give_back_a_spent_shot() {
        let mut e = engine_with_99c_ask(Side::Up, 200);
        assert_eq!(fires(&burst_in(&mut e, 200, 6, 0.1, true)).len(), 1);
        let acts = e.on_event(Event::SlotOpen { slot_open: SLOT, p0: 100_000.0 });
        assert!(acts.is_empty());
        assert_eq!(e.shot(), ShotState::Spent, "Spent must survive a duplicate SlotOpen");
        // A strictly-greater slot_open IS a real new slot: full reset.
        e.on_event(Event::SlotOpen { slot_open: SLOT + 300, p0: 100_000.0 });
        assert_eq!(e.shot(), ShotState::Ready);
    }

    #[test]
    fn the_placebo_mirror_logs_an_adverse_burst_and_never_spends_the_shot() {
        let mut e = engine_with_99c_ask(Side::Up, 200);
        let acts = burst_in(&mut e, 200, 6, 0.1, false);
        let p: Vec<_> = acts.iter().filter(|a| matches!(a, Action::Placebo { .. })).collect();
        assert_eq!(p.len(), 1, "one mirror entry per slot");
        assert_eq!(e.shot(), ShotState::Ready, "the mirror must never reach an order path");
        // ...and the real gate still owns its shot afterwards.
        e.on_event(Event::Book {
            ingest_ms: BASE + 240_000,
            side: Side::Up,
            asks: vec![(0.99, 5_000.0)],
        });
        assert_eq!(fires(&burst_in(&mut e, 240, 6, 0.1, true)).len(), 1);
    }

    #[test]
    fn out_of_order_prints_are_dropped_whole() {
        let mut e = engine_with_99c_ask(Side::Up, 200);
        // Build a qualifying window, then deliver a backwards-stamped print.
        burst_in(&mut e, 200, 4, 0.1, true);
        let late = e.on_event(Event::Trade {
            venue_ts_ms: BASE + 200_000,
            px: 100_000.0,
            qty: 99.0,
            taker_buy: true,
            recv_ms: BASE + 200_100,
        });
        assert!(late.is_empty(), "a backwards venue stamp must not fire");
        assert_eq!(e.shot(), ShotState::Ready);
    }

    #[test]
    fn slot_end_spends_the_shot() {
        let mut e = engine_with_99c_ask(Side::Up, 200);
        e.on_event(Event::SlotEnd);
        assert_eq!(e.shot(), ShotState::Spent);
        assert!(fires(&burst_in(&mut e, 200, 6, 0.1, true)).is_empty());
    }

    #[test]
    fn with_the_tick_path_off_nothing_can_fire() {
        let mut e = SlotEngine::new(EngineCfg { tick: None, ..cfg() });
        e.on_event(Event::SlotOpen { slot_open: SLOT, p0: 100_000.0 });
        e.on_event(Event::Book {
            ingest_ms: BASE + 200_000,
            side: Side::Up,
            asks: vec![(0.99, 5_000.0)],
        });
        assert!(burst_in(&mut e, 200, 6, 0.1, true).is_empty());
        assert_eq!(e.shot(), ShotState::Ready);
    }

    #[test]
    fn klines_feed_features_but_gate_nothing() {
        let mut e = engine_with_99c_ask(Side::Up, 200);
        // No kline has been delivered: `feats` is None, and the fire happens
        // anyway — features are telemetry under this spec (§4.1).
        assert_eq!(e.feats(Side::Up), None);
        let acts = burst_in(&mut e, 200, 6, 0.1, true);
        let Action::Fire { feats, .. } = fires(&acts)[0] else { unreachable!() };
        assert!(feats.is_none(), "a fire must not require a feature vector");
    }
}
