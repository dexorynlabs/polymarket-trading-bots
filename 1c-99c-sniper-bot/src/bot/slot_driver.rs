//! Per-slot async shell (Task 13). Owns exactly one `SlotEngine`, turns
//! stream items into `Event`s, and executes `Action`s. **This module
//! contains NO gate logic** — every card-threshold comparison lives in
//! `engine.rs`; if you find yourself writing one here, it belongs there.
//!
//! Every serious defect found in Phase 1 — a duplicate `SlotOpen`
//! resurrecting consumed shots, a NaN ladder level buying the whole clip,
//! the book path firing outside its window — lived in lifecycle and
//! boundary territory, which is exactly what this module is. See the
//! per-function docs below for the specific invariants this shell must
//! hold.
//!
//! **Two clocks, on purpose.** `SpotClose1s::close_ts_ms` is the VENUE's own
//! close time for the kline; the kline for second `t` is only published once
//! second `t` has ended. A `BookUpdate` carries no slot-relative time at all,
//! so its `t` comes from the LOCAL clock — see `book_to_t`'s doc for why that
//! must be the event's own ingest stamp, not a fresh clock read at dequeue
//! time. Under POST-TWAP-SPEC §3 klines gate NOTHING (they feed `Feat` rows
//! only) and books gate nothing either (they supply the held ladder the tick
//! path fires against) — the frame is enforced inside the engine, on Binance's
//! own trade clock.

use crate::strategy::engine::{Action, Event, SlotEngine, SIDES};
use crate::strategy::features::SLOT_SECS;
use crate::risk::rails::Rails;
use crate::bot::settle::{taker_fee_usd, OpenPosition, SettleLane};
use crate::strategy::flow::FlowStats;
use crate::bot::telemetry::{actions_to_events, side_str, TelemetryEvent, Writer};
use crate::execution::venue::{FakOutcome, FakRequest, Venue, VenueEvent};
use crate::types::{now_micros, BookUpdate, Side, SpotClose1s, SpotTrade};
use crate::polymarket::book_registry::BookRegistry;
use crate::polymarket::clob_http::MarketFetch;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Market metadata for one slot, resolved before `drive_slot` is spawned.
#[derive(Clone, Debug)]
pub struct SlotCtx {
    pub slug: String,
    pub slot_open: u64,
    pub token_up: String,
    pub token_dn: String,
    pub condition_id: String,
}

#[derive(Clone, Copy, Debug)]
pub struct DriverCfg {
    pub engine: crate::strategy::engine::EngineCfg,
    /// Drop the engine's held ladders when the book WS has been silent this
    /// many ms. (Kline staleness has NO counterpart: klines gate nothing.)
    pub book_stale_disarm_ms: u64,
    /// How long to wait for a fill notification after an Acked FAK before
    /// treating it as a clean miss.
    pub fak_grace_ms: u64,
    /// Seconds past `slot_open` at which `drive_slot`'s absolute backstop
    /// fires (§13.6) — see `HARD_DEADLINE_SECS` for the recommended
    /// production value and full rationale. A `DriverCfg` field rather than
    /// a fixed constant (Task 13 re-review Important 2) so a caller whose
    /// `slot_open` is deliberately anchored in the real past — the ONE
    /// legitimate reason being a test that needs `SettleLane`'s own
    /// eligibility window to already hold — can widen the budget without
    /// perturbing the value every other (production) caller uses.
    pub hard_deadline_secs: u64,
}

/// Process-scoped handles shared across every slot driven in this process
/// (Task 15 spawns one `drive_slot` per slot, not one per process) — Task 13
/// review A1. Bundling these five cuts `drive_slot` from 11 parameters to 7
/// (`clippy::too_many_arguments`) and collapses `Driver`'s four separate
/// `&'a` fields into one. `Arc`-wrapped as a whole, not just per-field:
/// `tele: Writer` is not `Clone`, so taking `SlotDeps` by value would still
/// let `drive_slot` be called at most once per process — the same latent bug
/// A1 exists to fix.
pub struct SlotDeps<V: Venue, F: MarketFetch> {
    pub venue: Arc<V>,
    pub reg: Arc<BookRegistry>,
    pub rails: Arc<Rails>,
    pub settle: Arc<SettleLane<F>>,
    pub tele: Writer,
}

/// Health/grace sweep cadence. Must be well under `fak_grace_ms` (design
/// default 1500ms) so a grace expiry is detected promptly; 250ms mirrors
/// the earlier taker's `TICK_MS`.
const HEALTH_TICK_MS: u64 = 250;
/// Recommended production value for `DriverCfg::hard_deadline_secs` — the
/// absolute backstop so a slot whose feeds never deliver still terminates
/// (§13.6). 30s past the 300s slot duration — comfortably longer than any
/// single `fak_grace_ms` — so under normal operation every pending order is
/// finalized by the periodic grace sweep long before this fires; it exists
/// only for the case where feeds go completely silent. `pub` so callers
/// (Task 15, and this module's own tests) can reference the same default
/// rather than re-declaring the literal `330`.
pub const HARD_DEADLINE_SECS: u64 = 330;
/// First second `Feat` telemetry rows are emitted from — 5s of lead before
/// the card's own frame (`t_lo = 150`, cards.rs) so the ramp-in is visible in
/// the conformance stream.
const FEAT_WINDOW_START: usize = 145;

/// `t` for a kline, from the VENUE's close time. The kline opening at
/// `slot_open` closes at `slot_open + 0.999s`, giving t = 0.
pub fn kline_to_t(close_ts_ms: u64, slot_open: u64) -> Option<usize> {
    let secs = close_ts_ms / 1000;
    if secs < slot_open {
        return None;
    }
    let t = (secs - slot_open) as usize;
    if t >= SLOT_SECS {
        None
    } else {
        Some(t)
    }
}

/// `t` for a book update, from the LOCAL clock — book events carry no
/// slot-relative time. Callers MUST pass the event's own ingest timestamp
/// (`BookUpdate::ts_micros`, stamped at WS parse time), never a fresh clock
/// read taken when the event is dequeued: the driver `.await`s
/// `venue.send_fak` while executing a `Fire` action, and book updates queue
/// during that await. Re-stamping at dequeue time can label a second-294
/// event as second 295 — the Task 5 book-path window defect. This is the one
/// place NTP drift matters.
pub fn book_to_t(now_ms: u64, slot_open: u64) -> Option<usize> {
    let secs = now_ms / 1000;
    if secs < slot_open {
        return None;
    }
    let t = (secs - slot_open) as usize;
    if t >= SLOT_SECS {
        None
    } else {
        Some(t)
    }
}

fn now_ms() -> u64 {
    now_micros() / 1_000
}

/// Seeds `last_book_ms` from whatever this slot's book registry entries
/// already hold, rather than a synthetic `now_ms()` read (Task 13 re-review
/// Important 1b). §13.6 spawns a driver ahead of slot open for
/// market-discovery/subscribe warm-up, so the registry can already carry
/// this slug's book by the time `drive_slot` starts its health ticker;
/// starting from `now_ms()` instead would let the ticker manufacture
/// staleness purely from elapsed wall-clock time, with zero book evidence
/// either way, before the first real `BookUpdate` has even had a chance to
/// arrive. `None` (never seen, for either side) is the correct starting
/// state otherwise — see the health ticker's own match on this field.
fn seed_last_book_ms(reg: &BookRegistry, slug: &str) -> Option<u64> {
    SIDES.iter().filter_map(|&side| reg.get(slug, side).map(|snap| snap.ts_micros / 1_000)).max()
}

/// Why `finalize_order` is booking a zero-fill `Miss` — its telemetry
/// `reason` string. Task 13 re-review Minor 5: previously a plain `timeout:
/// bool`, which forced the venue-duplicate-`order_id` displacement path
/// (§13.4's `exec`, the `Ok(FakOutcome::Acked { .. })` arm) to borrow
/// `timeout=true` and mislabel itself `"grace_expired"` — a real grace
/// timeout never happened there at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MissReason {
    /// The grace window elapsed with no fill/Terminal notification
    /// (`sweep_grace_expired`), or the slot ended with the order still
    /// outstanding (the post-loop force-finalize — same shape, a grace
    /// window that never got the chance to run out on its own).
    GraceExpired,
    /// An async-discovered zero-match `Terminal` — the same shape as a
    /// synchronous `ZeroFill`, just learned about later.
    FakNoMatch,
    /// The venue sent a duplicate `order_id`, displacing an unreconciled
    /// pending entry before we ever heard anything about IT specifically.
    /// Only reachable on a venue bug — distinct from both of the above, so
    /// an investigator isn't sent looking for a grace timeout or a zero-fill
    /// that never happened.
    DuplicateOrderId,
}

impl MissReason {
    fn as_str(self) -> &'static str {
        match self {
            MissReason::GraceExpired => "grace_expired",
            MissReason::FakNoMatch => "fak_no_match",
            MissReason::DuplicateOrderId => "duplicate_order_id",
        }
    }
}

/// State for one FAK we've sent and are waiting to hear back about.
struct PendingOrder {
    t: usize,
    side: Side,
    commit_usd: f64,
    filled_shares: f64,
    filled_cost_usd: f64,
    filled_fee_usd: f64,
    /// Tokio-virtual-time instant (not a wall-clock timestamp): grace expiry
    /// is a pure duration check, and using `tokio::time::Instant` lets tests
    /// drive it deterministically under a paused clock via `sleep`, per
    /// Task 12's guidance (`time::advance()` does not reliably wait for the
    /// runtime to go idle).
    acked_at: tokio::time::Instant,
    /// Task 13 resolution 6: `rails.reconcile` must run EXACTLY ONCE per
    /// fire. This flag is the guard — every finalization path checks it
    /// before touching the rail, so a late-arriving fill/Terminal after a
    /// grace-expiry finalize (or vice versa) is a no-op, not a double
    /// release.
    reconciled: bool,
}

/// Bundles the per-slot context + shared deps + in-flight order book so the
/// `tokio::select!` loop body in `drive_slot` doesn't thread parameters
/// through every helper call by hand. `ctx` stays a separate field (it's
/// per-slot, unlike everything in `SlotDeps`).
struct Driver<'a, V: Venue, F: MarketFetch> {
    ctx: &'a SlotCtx,
    deps: &'a SlotDeps<V, F>,
    pending: HashMap<String, PendingOrder>,
    /// Trailing taker-flow snapshot, refreshed from the ENGINE's own ring on
    /// every `@aggTrade` (`flow.rs`). Stamped on `Fire` and `Feat` rows;
    /// `None` until the first print (or forever, tick path off). The engine
    /// owns the ring because the ring is now a GATE input (spec §6) — this is
    /// the descriptive view of the same data, never a second copy of it.
    flow: Option<FlowStats>,
}

impl<'a, V: Venue, F: MarketFetch> Driver<'a, V, F> {
    /// Execute one `Action`. This is the ENTIRE action-execution table:
    /// `Placebo` is telemetry-only and never touches the venue; `Fire` is the
    /// only variant that can spend money, and it does so strictly
    /// sequentially — no `.await` runs between `rails.check_fire` and
    /// `rails.record_send` (the concurrent-exposure invariant), because this
    /// function is called in-line, once per action, on the driver's own task,
    /// never spawned or joined concurrently with another fire.
    async fn exec(&mut self, action: Action) {
        match &action {
            Action::Placebo { .. } => {
                for ev in actions_to_events(std::slice::from_ref(&action), self.ctx.slot_open, self.flow) {
                    self.deps.tele.emit(ev);
                }
            }
            Action::Fire { t, side, limit_px, size_shares, card, .. } => {
                // The exposure rail commits the FAK's worst-case spend
                // (shares * limit) — every share filling at the 2c limit
                // instead of the 1c ask we qualified on — never the clip.
                let commit_usd = size_shares * limit_px;
                let now = now_ms();
                match self.deps.rails.check_fire(commit_usd, now) {
                    Err(trip) => {
                        // Task 13 review D: the rail reports its own
                        // value/threshold now (rails.rs's `RailTripInfo`) —
                        // no more NaN placeholder, and no reconstruction of
                        // rail internals from getters on this side.
                        self.deps.tele.emit(TelemetryEvent::RailTrip {
                            slot_open: self.ctx.slot_open,
                            rail: trip.veto.as_str().to_string(),
                            value: trip.value,
                            threshold: trip.threshold,
                            t: *t,
                            side: side_str(*side),
                            card: card.as_str().to_string(),
                            commit_usd,
                        });
                    }
                    Ok(()) => {
                        self.deps.rails.record_send(commit_usd, now);
                        for ev in actions_to_events(std::slice::from_ref(&action), self.ctx.slot_open, self.flow) {
                            self.deps.tele.emit(ev);
                        }

                        let token_id = match side {
                            Side::Up => self.ctx.token_up.clone(),
                            Side::Down => self.ctx.token_dn.clone(),
                        };
                        let req = FakRequest {
                            token_id,
                            slug: self.ctx.slug.clone(),
                            side: *side,
                            limit_px: *limit_px,
                            size_shares: *size_shares,
                        };
                        let send_started = now_ms();
                        match self.deps.venue.send_fak(req).await {
                            Ok(FakOutcome::Acked { order_id }) => {
                                let latency_ms = now_ms().saturating_sub(send_started);
                                self.deps.tele.emit(TelemetryEvent::OrderAck {
                                    slot_open: self.ctx.slot_open,
                                    t: *t,
                                    side: side_str(*side),
                                    order_id: order_id.clone(),
                                    limit_px: *limit_px,
                                    size_shares: *size_shares,
                                    latency_ms,
                                });
                                // Task 13 review E3: a duplicate order_id
                                // from the venue would otherwise silently
                                // overwrite the map slot, orphaning the
                                // displaced entry's commit forever (the
                                // only reconcile-zero path). `finalize_order`
                                // is idempotent (resolution-6 guard) — a
                                // no-op if the displaced entry is already
                                // reconciled, otherwise it releases it
                                // before we overwrite the slot below.
                                if self.pending.contains_key(&order_id) {
                                    tracing::warn!(
                                        order_id,
                                        "tickburst: duplicate order_id from venue; reconciling the displaced entry"
                                    );
                                    // Task 13 re-review Minor 5: its own
                                    // reason, not "grace_expired" — no grace
                                    // window actually ran out here.
                                    self.finalize_order(&order_id, MissReason::DuplicateOrderId);
                                }
                                self.pending.insert(
                                    order_id,
                                    PendingOrder {
                                        t: *t,
                                        side: *side,
                                        commit_usd,
                                        filled_shares: 0.0,
                                        filled_cost_usd: 0.0,
                                        filled_fee_usd: 0.0,
                                        acked_at: tokio::time::Instant::now(),
                                        reconciled: false,
                                    },
                                );
                            }
                            // Spec §3.6: the shot was spent on ATTEMPT. Do
                            // NOT re-arm — a zero-fill is a clean, free miss
                            // and is the race-haircut measurement itself.
                            Ok(FakOutcome::ZeroFill { order_id }) => {
                                self.deps.tele.emit(TelemetryEvent::Miss {
                                    slot_open: self.ctx.slot_open,
                                    t: *t,
                                    side: side_str(*side),
                                    order_id,
                                    reason: "fak_no_match".to_string(),
                                });
                                self.deps.rails.reconcile(commit_usd, 0.0);
                            }
                            // We do not know whether the order landed.
                            // NEVER retry — a retry could double-buy.
                            Err(e) => {
                                tracing::error!(
                                    slot_open = self.ctx.slot_open, t = *t, side = ?side, error = ?e,
                                    "tickburst: send_fak POST failed; not retrying (unknown fill state)"
                                );
                                self.deps.tele.emit(TelemetryEvent::Miss {
                                    slot_open: self.ctx.slot_open,
                                    t: *t,
                                    side: side_str(*side),
                                    order_id: String::new(),
                                    reason: "post_failed".to_string(),
                                });
                                self.deps.rails.reconcile(commit_usd, 0.0);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Reconcile + (if any shares filled) hand off to `SettleLane`, exactly
    /// once (resolution 6). `reason` (Task 13 re-review Minor 5) is only
    /// used if `filled_shares <= 0.0` — see `MissReason`'s doc for what each
    /// caller passes and why.
    fn finalize_order(&mut self, order_id: &str, reason: MissReason) {
        let Some(po) = self.pending.get_mut(order_id) else { return };
        if po.reconciled {
            // A second notification for an order already finalized (a late
            // fill after grace expiry, or a duplicate Terminal). Ignored,
            // not double-processed — this IS the resolution-6 guard.
            tracing::warn!(
                order_id, "tickburst: order already reconciled; ignoring late notification"
            );
            return;
        }
        po.reconciled = true;
        let (t, side, commit_usd, filled_shares, filled_cost_usd, filled_fee_usd) =
            (po.t, po.side, po.commit_usd, po.filled_shares, po.filled_cost_usd, po.filled_fee_usd);

        // Task 13 review E2: a PARTIAL fill reaching grace expiry is not a
        // Miss — fill-rate is the race-haircut statistic, and counting a
        // partly-filled order as a miss double-counts it. Only a fully
        // unfilled order (whichever path learns that: grace timeout, an
        // async-discovered zero-match Terminal, or a duplicate-order_id
        // displacement) is a Miss. The reconcile arithmetic below is
        // unaffected — it already only released the commit-vs-actual
        // overshoot regardless of this label.
        if filled_shares <= 0.0 {
            self.deps.tele.emit(TelemetryEvent::Miss {
                slot_open: self.ctx.slot_open,
                t,
                side: side_str(side),
                order_id: order_id.to_string(),
                reason: reason.as_str().to_string(),
            });
        }
        self.deps.rails.reconcile(commit_usd, filled_cost_usd);
        if filled_shares > 0.0 {
            self.deps.settle.track(OpenPosition {
                slug: self.ctx.slug.clone(),
                slot_open: self.ctx.slot_open,
                side,
                condition_id: self.ctx.condition_id.clone(),
                shares: filled_shares,
                cost_usd: filled_cost_usd,
                fee_usd: filled_fee_usd,
            });
        }
    }

    fn sweep_grace_expired(&mut self, grace: Duration) {
        let now = tokio::time::Instant::now();
        let expired: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, po)| !po.reconciled && now.duration_since(po.acked_at) >= grace)
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            self.finalize_order(&id, MissReason::GraceExpired);
        }
    }

    /// Returns false once the channel is closed (the caller disables this
    /// select arm from then on, matching the earlier taker's `venue_events_open`
    /// pattern).
    async fn absorb_venue_event(
        &mut self,
        ev: Result<VenueEvent, broadcast::error::RecvError>,
    ) -> bool {
        match ev {
            Err(broadcast::error::RecvError::Closed) => {
                tracing::error!(slot_open = self.ctx.slot_open, "tickburst: venue events channel closed");
                false
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(slot_open = self.ctx.slot_open, n, "tickburst: venue events lagged");
                true
            }
            Ok(VenueEvent::Fill(fill)) => {
                // Task 13 review B1: with multiple `drive_slot` instances
                // sharing one venue-events broadcast channel, a driver whose
                // slot has ended (B1's other two parts fix that overlap, but
                // this stays a defensive backstop) can still see fills for
                // an order_id it never sent — an ENTIRELY different slot's
                // order. That is NOT this slot's fill in any sense, so
                // unlike the "already reconciled" case below it must not
                // produce a `Fill` telemetry row at all — emitting one would
                // mis-attribute a duplicate/foreign fill into this slot's
                // stream, exactly the fill-rate corruption B1 exists to
                // prevent.
                let emit = match self.pending.get_mut(&fill.order_id) {
                    Some(po) if !po.reconciled => {
                        po.filled_shares += fill.shares;
                        po.filled_cost_usd += fill.shares * fill.price;
                        po.filled_fee_usd += taker_fee_usd(fill.price, fill.shares);
                        Some(true)
                    }
                    Some(_) => {
                        tracing::warn!(
                            order_id = fill.order_id, shares = fill.shares, price = fill.price,
                            "tickburst: fill for an order already reconciled; not re-booked"
                        );
                        Some(false)
                    }
                    None => {
                        tracing::debug!(
                            order_id = fill.order_id, shares = fill.shares, price = fill.price,
                            "tickburst: fill for an order this driver never sent (another slot's driver); skipping"
                        );
                        None
                    }
                };
                if let Some(attributable) = emit {
                    self.deps.tele.emit(TelemetryEvent::Fill {
                        slot_open: self.ctx.slot_open,
                        order_id: fill.order_id,
                        price: fill.price,
                        shares: fill.shares,
                        fee_usd: taker_fee_usd(fill.price, fill.shares),
                        venue_ts_ms: fill.venue_ts_ms,
                        attributable,
                    });
                }
                true
            }
            Ok(VenueEvent::Terminal { order_id }) => {
                self.finalize_order(&order_id, MissReason::FakNoMatch);
                true
            }
        }
    }
}

/// The three streams one slot consumes. Bundled rather than passed as three
/// more parameters (this crate carries no `#[allow(clippy::
/// too_many_arguments)]` anywhere) — `trades` is the addition this crate
/// makes, and `None` means the tick path is off for this run.
pub struct SlotFeeds {
    pub klines: broadcast::Receiver<SpotClose1s>,
    pub books: broadcast::Receiver<BookUpdate>,
    /// Binance `@aggTrade`. `None` when `SNIPER_TICK_ENABLED=false`,
    /// in which case the process never even subscribes the stream.
    pub trades: Option<broadcast::Receiver<SpotTrade>>,
}

/// Drive one slot from open to end. Owns exactly one `SlotEngine`; contains
/// no gate logic of its own (§13, module doc).
pub async fn drive_slot<V: Venue, F: MarketFetch>(
    ctx: SlotCtx,
    p0: f64,
    cfg: DriverCfg,
    deps: Arc<SlotDeps<V, F>>,
    feeds: SlotFeeds,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let SlotFeeds { mut klines, mut books, mut trades } = feeds;
    // Task 13 re-review Important 2: a real check, not `debug_assert!` — the
    // documented test command (`cargo test -p taker-tickburst --profile
    // release-test`) leaves `debug-assertions` at the release default of
    // `false` (`[profile.release-test]` inherits `[profile.release]`,
    // Cargo.toml), so a `debug_assert!` here never ran under this crate's own
    // test suite. Checked FIRST, before any side effect below (`prepare_slot`,
    // the `SlotOpen` telemetry row) — this is a pure config-shape problem,
    // not a per-slot one, so a bad value should fail before touching the
    // venue at all. The coupling: the LATEST possible fire is t < 291
    // (`cards::BURST.t_hi`); the venue publishes the t=299 kline at
    // ~`slot_open + 300.9s`, and the post-loop force-finalize follows
    // immediately after that — so a pending order from the last possible
    // fire has ~9s of real slack between being sent and the force-finalize
    // running. Safe at the 1500ms design default, but a `fak_grace_ms` past
    // that slack would silently truncate real pending orders into
    // `grace_expired` misses at slot end instead of letting the grace sweep
    // (or a genuine late notification) resolve them first.
    anyhow::ensure!(
        cfg.fak_grace_ms <= 5_000,
        "fak_grace_ms ({}) exceeds the slack between the last possible fire (t < 291) and the \
         t=299 loop exit — pending orders would be truncated into grace_expired misses at slot \
         end instead of being resolved normally",
        cfg.fak_grace_ms
    );

    let slot_open = ctx.slot_open;

    // Task 13 review B1: anchor the hard-deadline backstop to `slot_open`
    // (a real epoch value), not to whenever this line happens to execute —
    // §13.6 spawns the driver BEFORE slot open (for market-discovery /
    // subscribe warm-up), so an anchor-at-"now" budget doesn't land at a
    // fixed point relative to the slot itself.
    //
    // Task 13 re-review Important 2: computed and checked FIRST, before
    // `prepare_slot`/`SlotOpen` below — an already-expired slot
    // (`slot_open + cfg.hard_deadline_secs <= now`) is, by construction, a
    // slot with NOTHING left to do, so it must not do ANY of that work
    // either. Production should never spawn one (Task 15's job), so this is
    // a defensive backstop, not a normal path. The old fallback handed that
    // case the LONGEST possible life (a fresh `hard_deadline_secs`-sized
    // budget from whenever this line happened to run) instead of the
    // shortest; that both did needless work (a frozen test fixture was the
    // only thing that ever took this path — see the test file) and
    // maximized Important 1's blast radius for a zombie driver. An
    // already-expired slot now returns immediately instead, before
    // `prepare_slot` ever touches the venue.
    let now_wall_secs = now_ms() / 1000;
    let remaining_secs = (slot_open + cfg.hard_deadline_secs).saturating_sub(now_wall_secs);
    if remaining_secs == 0 {
        tracing::warn!(
            slot_open, hard_deadline_secs = cfg.hard_deadline_secs,
            "tickburst: drive_slot spawned for an already-expired slot; exiting immediately \
             (Task 15 should never spawn one)"
        );
        return Ok(());
    }
    let deadline_at = tokio::time::Instant::now() + Duration::from_secs(remaining_secs);

    let mut engine = SlotEngine::new(cfg.engine);

    deps.venue.prepare_slot(&ctx.slug, &ctx.token_up, &ctx.token_dn).await;

    // "Emits SlotOpen exactly once" (§13.6): the engine-level event (arms
    // the state machine) and the telemetry row (a distinct, one-time wire
    // event) both happen here, before the loop, so nothing races them.
    engine.on_event(Event::SlotOpen { slot_open, p0 });
    deps.tele.emit(TelemetryEvent::SlotOpen {
        slot_open,
        slug: ctx.slug.clone(),
        p0,
        token_up: ctx.token_up.clone(),
        token_dn: ctx.token_dn.clone(),
        condition_id: ctx.condition_id.clone(),
    });

    let mut venue_events = deps.venue.events_rx();
    let mut venue_events_open = true;

    let mut d: Driver<'_, V, F> =
        Driver { ctx: &ctx, deps: deps.as_ref(), pending: HashMap::new(), flow: None };

    // Task 13 re-review Important 1b: `Option`, seeded from the registry
    // rather than `now_ms()` — see `seed_last_book_ms`'s doc.
    let mut last_book_ms: Option<u64> = seed_last_book_ms(&deps.reg, &ctx.slug);
    let mut skew_logged = false;

    let mut ticker = tokio::time::interval(Duration::from_millis(HEALTH_TICK_MS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Task 13 re-review Minor 6: the post-loop force-finalize below runs once
    // this loop exits — normally right after the t=299 kline (`t == SLOT_SECS
    // - 1` below), which the venue publishes at ~`slot_open + 300.9s`. See
    // the `fak_grace_ms <= 5_000` check above for why that slack bounds
    // `fak_grace_ms`.
    let grace = Duration::from_millis(cfg.fak_grace_ms);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,

            // biased + kline-first: kept from the earlier inheritance, now
            // as a telemetry-ordering choice rather than a decision one — a
            // `Feat` row for second `t` should precede any fire stamped in it.
            k = klines.recv() => {
                match k {
                    Ok(k) => {
                        if !skew_logged {
                            skew_logged = true;
                            tracing::info!(
                                slot_open,
                                skew_ms = now_ms() as i64 - k.close_ts_ms as i64,
                                "tickburst: kline local-vs-venue clock skew"
                            );
                        }
                        if let Some(t) = kline_to_t(k.close_ts_ms, slot_open) {
                            let actions = engine.on_event(Event::Kline { t, close_px: k.close_px });
                            for a in actions {
                                d.exec(a).await;
                            }
                            // Task 13 review B3: gate `Feat` emission on the
                            // ENGINE's own advance signal (`last_kline_t`),
                            // not a driver-local mirror of `SlotSeries::
                            // push`'s contiguity contract (`t == len &&
                            // close_px finite`). `last_kline_t` is set on
                            // exactly the advance path (engine.rs) and reset
                            // on `SlotOpen`; a replayed or gap-skipped
                            // second leaves it unchanged, which is precisely
                            // what distinguishes "this `t` just advanced the
                            // series" from "`feats()` still reports `Some`
                            // from an earlier second" — `feats()` alone
                            // can't make that distinction (see
                            // `SlotEngine::last_kline_t`'s doc).
                            if engine.last_kline_t() == Some(t) && (FEAT_WINDOW_START..SLOT_SECS).contains(&t) {
                                for side in SIDES {
                                    if let Some(f) = engine.feats(side) {
                                        d.deps.tele.emit(TelemetryEvent::Feat {
                                            slot_open, t, side: side_str(side),
                                            d: f.d, v2: f.v2, v3: f.v3, sigma60: f.sigma60, z_d: f.z_d,
                                            flow: d.flow,
                                        });
                                    }
                                }
                            }
                            // Task 13 review B1: §13.6's `t >= 300`
                            // termination. Once the venue has given us the
                            // LAST valid in-slot second, there is nothing
                            // further this slot's kline feed will ever
                            // deliver — exit now rather than relying solely
                            // on the hard-deadline backstop (which exists
                            // for the feed-goes-silent case, not the normal
                            // path).
                            if t == SLOT_SECS - 1 {
                                tracing::info!(slot_open, "tickburst: reached the last in-slot second; ending drive_slot");
                                break;
                            }
                        }
                    }
                    // Lagged: n events were DROPPED, not merely seen again —
                    // categorically different from a replay (§13.3 / resolution
                    // 4). A dropped kline leaves a genuine gap; the engine's own
                    // `t > len` guard fails closed and disarms, which is the
                    // correct outcome, not something to backfill or paper over.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(slot_open, n, "tickburst: kline feed lagged; seconds DROPPED");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::error!(slot_open, "tickburst: kline feed closed");
                        break;
                    }
                }
            }
            b = books.recv() => {
                match b {
                    Ok(b) => {
                        // Task 13 review B2: only THIS slot's book traffic
                        // counts as "healthy" — moved inside the slug filter
                        // so another market's book updates on the shared
                        // channel can't mask this slot's book actually being
                        // dead (`DriverCfg::book_stale_disarm_ms`'s own doc:
                        // "the book WS has been silent").
                        if b.slug == ctx.slug {
                            last_book_ms = Some(now_ms());
                            // Resolution 3: `t` from the event's OWN ingest
                            // stamp (`ts_micros`, set at WS parse time in
                            // polymarket-client), never a fresh `now_ms()`
                            // read here — see `book_to_t`'s doc.
                            // `book_to_t` is now purely the in-slot GUARD: it
                            // drops updates stamped outside [slot_open,
                            // slot_open+300), which is the only thing a book
                            // event's slot-second was ever used for once the
                            // bar fire path went (spec §6).
                            if book_to_t(b.ts_micros / 1_000, slot_open).is_some() {
                                let asks = d.deps.reg.ask_ladder(&ctx.slug, b.side);
                                // `ingest_ms` is the SAME stamp `book_to_t`
                                // reads, at full resolution: the held-book
                                // freshness bound is in ms (15s by default),
                                // so it cannot be rebuilt from a slot-second,
                                // and it must not be a fresh
                                // clock read here for exactly the reason
                                // `book_to_t`'s doc gives (this dequeue can
                                // trail the event by a whole `send_fak` await).
                                let actions = engine.on_event(Event::Book {
                                    ingest_ms: b.ts_micros / 1_000,
                                    side: b.side,
                                    asks,
                                });
                                for a in actions {
                                    d.exec(a).await;
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(slot_open, n, "tickburst: book feed lagged; a missed fire is possible");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::error!(slot_open, "tickburst: book feed closed");
                        break;
                    }
                }
            }
            // AFTER klines and books, deliberately, and the BOOK half is
            // load-bearing: a tick fires against the HELD ladder, so a fresher
            // snapshot delivered in the same instant is strictly the better
            // view to act on. `tests/posttwap_parity.rs` merges its fixture
            // stream the same way.
            //
            // The arm is disabled outright when the tick path is off — no
            // stream, no engine event, nothing to reason about.
            tr = async { trades.as_mut().expect("arm disabled when None").recv().await },
                 if trades.is_some() => {
                match tr {
                    Ok(tr) => {
                        let actions = engine.on_event(Event::Trade {
                            venue_ts_ms: tr.exch_ts_ms,
                            px: tr.price,
                            qty: tr.qty,
                            // Binance's `m` is "the BUYER was the maker", so
                            // the aggressor bought when `!m` — the recovery
                            // direction for a trailing-Up entry (spec §3.5).
                            taker_buy: !tr.buyer_is_maker,
                            // The event's OWN parse-time stamp, never a fresh
                            // clock read at dequeue — same discipline as
                            // `book_to_t`, and for the same reason: this loop
                            // can trail a trade by a whole `send_fak` await.
                            recv_ms: tr.recv_ts_us / 1_000,
                        });
                        // AFTER the engine event: the engine owns the ring, so
                        // this snapshot includes the print that just fired and
                        // reports the window the gate actually read.
                        d.flow = engine.flow_stats();
                        for a in actions {
                            d.exec(a).await;
                        }
                    }
                    // Dropped ticks are a missed FIRE, never a wrong one: the
                    // trailing window sums whatever survives in the ring, and
                    // a hole in it can only make the measured volume SMALLER
                    // (the gate is one-sided). Nothing to disarm.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(slot_open, n, "tickburst: trade feed lagged; ticks DROPPED");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::error!(slot_open, "tickburst: trade feed closed; this slot can no \
                            longer fire at all — the tick path is the ONLY fire path (spec §6)");
                        trades = None;
                    }
                }
            }
            ev = venue_events.recv(), if venue_events_open => {
                venue_events_open = d.absorb_venue_event(ev).await;
            }
            _ = ticker.tick() => {
                let now = now_ms();
                let book_stale = match last_book_ms {
                    Some(ms) => now.saturating_sub(ms) > cfg.book_stale_disarm_ms,
                    // Task 13 re-review Important 1b: never having seen this
                    // slot's book is not the same fact as it having gone
                    // stale — don't let elapsed wall-clock time alone
                    // manufacture staleness before the first `BookUpdate`
                    // (or a registry-seeded value) has even arrived.
                    None => false,
                };
                // Kline staleness deliberately does NOT appear here: klines
                // gate nothing under this spec (§4.1), so a stalled kline feed
                // costs telemetry, never a decision.
                let unhealthy = book_stale;
                // Task 13 re-review Important 1a: `rails.set_feed_healthy`
                // is INTENTIONALLY not called here. `Rails::feed_healthy` is
                // process-global (`SlotDeps`'s own doc: "process-scoped
                // handles shared across every slot"), but `unhealthy` above
                // is a per-SLOT fact — this slug's kline/book traffic, not
                // "our WS connection is down". Writing it here means
                // whichever `drive_slot` last ran its ticker decides
                // `check_fire`'s feed-health veto for every OTHER
                // concurrently-driven slot too: a slot lingering past close
                // (its book goes quiet, `book_stale_disarm_ms` later this
                // ticker sees it as unhealthy) or a driver spawned ahead of
                // its own slot open with no book traffic yet can each veto
                // an unrelated slot's fires, attributed to the WRONG slot's
                // `RailTrip` row. Feeding `Event::FeedUnhealthy` to THIS
                // engine below is correct and sufficient — it drops only this
                // slot's own held ladders. Only the process's actual feed
                // owner may call `rails.set_feed_healthy`: Task 15's
                // runtime, which alone can tell "our WS connection is down"
                // (a process fact) from "this slug is quiet" (a slot fact).
                if unhealthy {
                    let actions = engine.on_event(Event::FeedUnhealthy);
                    for a in actions {
                        d.exec(a).await;
                    }
                }
                d.sweep_grace_expired(grace);
            }
            _ = tokio::time::sleep_until(deadline_at) => {
                tracing::warn!(slot_open, "tickburst: hit the hard deadline; a feed never delivered t>=300");
                break;
            }
        }
    }

    // Force-finalize anything still outstanding rather than leaving it
    // reserved against the concurrent-exposure rail forever (rails.rs's
    // step-2/3 lifecycle: every commit must be reconciled exactly once).
    // `fak_grace_ms` is far smaller than the slack to the hard deadline, so
    // under normal operation this is a no-op; it only bites on an early
    // shutdown or a wedged venue-events channel.
    //
    // Task 13 review E1: filtered on `!po.reconciled`, matching
    // `sweep_grace_expired` — otherwise every already-finalized entry left
    // in the map (finalization never removes entries, only marks them)
    // hits `finalize_order`'s early "already reconciled" `warn!` again, up
    // to 6 spurious WARNs per slot.
    let outstanding: Vec<String> =
        d.pending.iter().filter(|(_, po)| !po.reconciled).map(|(id, _)| id.clone()).collect();
    for order_id in outstanding {
        d.finalize_order(&order_id, MissReason::GraceExpired);
    }

    // SlotEnd spends the slot's shot so a late event cannot fire into a slot
    // that is over. It emits no actions and has no telemetry row of its own.
    engine.on_event(Event::SlotEnd);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::book_registry::{BookSnapshot, Level};

    /// Task 13 re-review Important 1b, mutation-verify: `seed_last_book_ms`
    /// is the ONLY thing standing between "this slot's book has never been
    /// seen" and "the health ticker manufactures staleness from elapsed
    /// wall-clock time alone". A direct unit test of the pure function,
    /// deliberately NOT a timing-based `drive_slot` integration test: the
    /// health ticker's staleness check runs on REAL wall-clock time
    /// (`now_ms()` -> `SystemTime`), which `tokio::time::pause` does not
    /// control — a paused-clock async test can advance `tokio::time::sleep`
    /// by any virtual duration in near-zero REAL time, so it can never
    /// reliably breach a `book_stale_disarm_ms` threshold either way. This
    /// function is what actually needs to be right.
    #[test]
    fn seed_last_book_ms_is_none_when_the_registry_has_never_seen_this_slug() {
        let reg = BookRegistry::new();
        assert_eq!(seed_last_book_ms(&reg, "btc-updown-5m-123"), None);
    }

    #[test]
    fn seed_last_book_ms_reads_the_registrys_existing_entry_for_this_slug_only() {
        let reg = BookRegistry::new();
        let ts_micros = 1_800_000_000_123_456u64;
        reg.update(
            "btc-updown-5m-123",
            Side::Up,
            BookSnapshot { bids: vec![], asks: vec![Level { price: 0.05, size: 10.0 }], ts_micros },
        );
        assert_eq!(
            seed_last_book_ms(&reg, "btc-updown-5m-123"), Some(ts_micros / 1_000),
            "must seed from the registry's existing entry, not a fresh now_ms() read"
        );
        // A DIFFERENT slug's driver must not borrow this slug's timestamp —
        // "never seen" is per-slug, matching the driver's own `b.slug ==
        // ctx.slug` filter on the write side.
        assert_eq!(seed_last_book_ms(&reg, "btc-updown-5m-999"), None);
    }

    #[test]
    fn seed_last_book_ms_takes_the_later_of_the_two_sides() {
        let reg = BookRegistry::new();
        reg.update(
            "btc-updown-5m-123", Side::Up,
            BookSnapshot { bids: vec![], asks: vec![], ts_micros: 1_000_000 },
        );
        reg.update(
            "btc-updown-5m-123", Side::Down,
            BookSnapshot { bids: vec![], asks: vec![], ts_micros: 2_000_000 },
        );
        assert_eq!(seed_last_book_ms(&reg, "btc-updown-5m-123"), Some(2_000));
    }
}
