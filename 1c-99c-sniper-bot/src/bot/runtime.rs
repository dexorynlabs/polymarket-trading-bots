//! Process runtime (Task 15): gamma discovery ~10s before each slot, one
//! process-wide Binance `@kline_1s` ingest fanned out to every driver, one
//! book-WS subscription pair per slot (torn down between slots, §15.4), and
//! the shared process-scoped `SlotDeps` handed to `drive_slot`. This module
//! is the assembly point for Tasks 1-14 — see `lib.rs`'s module map.
//!
//! ## The four inherited obligations (task brief §O1-O4)
//!
//! **O1 — process-wide feed health.** `Rails::set_feed_healthy` is wired
//! HERE, from the ONE Binance kline ingest's own staleness — the only thing
//! in this process that can tell "our WS connection is down" (a process
//! fact) from "this slug is quiet" (a slot fact). `slot_driver.rs`'s health
//! ticker deliberately does NOT call it (its own "Important 1a" doc) — a
//! per-slot driver writing a process-global flag could veto a *different*
//! slot's entire firing window. See `spawn_kline_watch_task`.
//!
//! **O2 — the auth halt.** `live::LiveVenue` cancels its own
//! `CancellationToken` on a proven 401/403 and exposes `shutdown_token()`.
//! The CLI (`main.rs`) constructs ONE `CancellationToken`, passes it to
//! `LiveVenue::new` **and** into [`run`] here as the `shutdown` parameter,
//! which threads it — unmodified, never a derived/child token — into every
//! `drive_slot` call via `slot_loop`. An auth halt and a `run()`-level
//! shutdown are therefore the SAME event as far as a slot driver is
//! concerned: nothing extra needs to be selected on inside `run()` itself.
//! `slot_loop` stops spawning new slots the instant it observes
//! cancellation (checked at the top of every iteration and on every wait —
//! including the p0 poll, see `poll_for_p0`), never merely draining the one
//! already in flight. Distinguishing an operator ctrl-c from an
//! auth-triggered halt (for the process exit code) is main.rs's job — see
//! its own doc.
//!
//! **O3 — supervise `SettleLane`.** Its `JoinHandle` is kept, not dropped:
//! a settle task that dies (e.g. one of `settle.rs`'s `.expect()` mutex
//! sites) is now a loud `tracing::error!`, not a silent stop that leaves
//! `concurrent_usd` ratcheting to the cap with no alarm. (The live user-WS
//! ingest task gets the SAME treatment, one file over — see `main.rs`'s
//! `build_live_venue`.)
//!
//! **O4 — telemetry is drained at shutdown.** See `telemetry.rs`'s
//! `Writer::new_with_handle` (added for this task) and this module's
//! teardown sequence in [`run`].

use crate::config::{BotConfig, StrategySection};
use crate::strategy::engine::EngineCfg;
use crate::risk::rails::Rails;
use crate::bot::settle::SettleLane;
use crate::bot::slot_driver::{drive_slot, DriverCfg, SlotCtx, SlotDeps, SlotFeeds, HARD_DEADLINE_SECS};
use crate::bot::telemetry::Writer;
use crate::strategy::tick::TickCfg;
use crate::execution::venue::Venue;
use anyhow::Context;
use crate::strategy::boundary_close::{next_tick_ms, BoundaryClose, ShadowCompare};
use crate::types::{
    now_micros, BookUpdate, Coin, MarketMeta, Side, SpotClose1s, SpotTrade,
};
use crate::polymarket::book_registry::BookRegistry;
use crate::polymarket::clob_http::ClobHttpClient;
use crate::polymarket::clob_ws::{build_shared_tls_connector, ClobWsIngest, Subscription};
use crate::polymarket::gamma::GammaClient;
use crate::risk::kill_switches::{check_ws_staleness, KillSwitchCfg};
use crate::binance::feed::{build_spot_watch_channels, BinanceIngest, BinanceVenue};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
const CLOB_MARKET_WS: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const CLOB_HTTP_BASE: &str = "https://clob.polymarket.com";
const SLOT_SECS_U64: u64 = crate::strategy::features::SLOT_SECS as u64;
/// Design §15.4: discovery runs ~10s before slot open.
const DISCOVERY_LEAD_SECS: u64 = 10;
/// `poll_for_p0`'s re-check cadence (Important 3/4, task-15 review).
const P0_POLL_INTERVAL_MS: u64 = 50;
/// Total budget `poll_for_p0` allows the immediately-preceding kline to land
/// on the latch before giving up. §15.1: the kline for second `slot_open -
/// 1` is only published once `slot_open` itself has arrived on the wall
/// clock, so this covers ordinary WS delivery jitter, not a retry budget —
/// `resolve_p0`'s contiguity check is the hard gate regardless of how long
/// this polls. Polling is strictly SAFER than a single fixed sleep, never
/// weaker: contiguity is an exact equality on the latched close, so once the
/// latch advances past `slot_open - 1` it goes non-contiguous again and
/// `resolve_p0` returns `None` — polling longer can only let a slightly-late
/// kline land in time, it can never admit a WRONG p0.
const P0_POLL_BUDGET_MS: u64 = 1_200;
const KLINE_BCAST_CAP: usize = 4_096;
/// `@aggTrade` carries tens of events a second against the kline's one, and a
/// slot's receiver is created up to ~11s before `drive_slot` starts draining
/// it (`DISCOVERY_LEAD_SECS` + `P0_POLL_BUDGET_MS`). Sized so even a heavy
/// burst over that window cannot lag it: a `Lagged` here is dropped ticks,
/// i.e. silently missed fires.
const TRADE_BCAST_CAP: usize = 16_384;
const BOOK_MPSC_CAP: usize = 1_024;
const BOOK_BCAST_CAP: usize = 1_024;
const FEED_HEALTH_TICK_MS: u64 = 1_000;

fn now_ms() -> u64 {
    now_micros() / 1_000
}

/// The newest Binance 1s kline close the process has observed, latched
/// process-wide. Survives WS reconnects (§15.3) — it is NOT reset per-slot;
/// after a gap the latch may simply be stale, which is exactly what
/// `resolve_p0`'s contiguity check catches.
#[derive(Clone, Copy, Debug)]
pub struct LastClose {
    pub close_px: f64,
    pub close_ts_ms: u64,
}

/// `p0` = the close of the kline immediately preceding slot open.
/// `None` (slot never arms) when the latch is missing or NON-CONTIGUOUS.
///
/// Contiguity is load-bearing: a stale latch would supply a p0 from minutes
/// ago, and D = (p0 - close) would be garbage against a $12 gate floor. The
/// design measured the correct substitute at p50 $0.0000 / p90 $0.010 vs the
/// harness's kline open — that accuracy holds ONLY for the immediately
/// preceding second.
///
/// Do NOT fall back to the first close inside the slot. That is the
/// nearby-close idiom and it is a $3.09-mean error against a $12 gate floor —
/// the single highest-value catch of the whole design review.
pub fn resolve_p0(latch: Option<LastClose>, slot_open: u64) -> Option<f64> {
    let l = latch?;
    if l.close_ts_ms / 1000 + 1 == slot_open { Some(l.close_px) } else { None }
}

type KlineLatch = Arc<Mutex<Option<LastClose>>>;

/// Abstracts "fetch a slot's metadata by slug" so `slot_loop`'s pipelined
/// discovery (Important 1, task-15 review) is unit-testable without a live
/// gamma HTTP round-trip. Production uses `GammaClient` (its own
/// `fetch_by_slug` already has exactly this shape — this just names it as a
/// trait); tests use a scripted stub. Return-position-impl-trait, matching
/// this crate's own `Venue` trait style (`venue.rs`) rather than
/// `async_trait`.
trait Discover: Send + Sync + 'static {
    fn fetch_by_slug(&self, slug: &str) -> impl Future<Output = anyhow::Result<Option<MarketMeta>>> + Send;
}

impl Discover for GammaClient {
    fn fetch_by_slug(&self, slug: &str) -> impl Future<Output = anyhow::Result<Option<MarketMeta>>> + Send {
        // Delegate to the inherent method (same pattern as
        // `crate::polymarket::gamma::Gamma`'s own `impl for
        // GammaClient`) so the existing retry/parse path is unchanged.
        GammaClient::fetch_by_slug(self, slug)
    }
}

/// Process-scoped discovery/feed handles threaded through the slot loop,
/// bundled into one struct rather than passed as separate parameters (Task
/// 13 precedent: restructure around a clippy `too_many_arguments` lint
/// rather than suppress it — this crate carries no `#[allow(...)]`
/// anywhere). Generic over `G: Discover` (production: `GammaClient`, wired
/// in `run`) purely so `slot_loop`'s pipelined-discovery behavior is
/// testable against a scripted stub.
struct RuntimeHandles<G: Discover> {
    gamma: Arc<G>,
    tls: Option<tokio_tungstenite::Connector>,
    klines_tx: broadcast::Sender<SpotClose1s>,
    /// Binance `@aggTrade`, fanned out to each slot driver exactly like the
    /// klines are. `None` when `SNIPER_TICK_ENABLED=false` — in which
    /// case the stream is never subscribed at all, so this crate makes a
    /// single-stream socket.
    ///
    /// NOT `spawn_boundary_close_task`'s input. That task reconstructs bar
    /// CLOSES from the same trades and is DEFECTIVE (D2: it recomputes
    /// `sleep_for` under `biased` against a strictly-future `next_tick_ms`,
    /// so a trade preempting the boundary timer emits that second ~1000 ms
    /// late on 3-6% of bars) and its feature is separately graded NEGATIVE.
    /// The tick path shares only the ingest — it never touches the kline
    /// series, so `boundary_close_delay_ms` stays absent.
    trades_tx: Option<broadcast::Sender<SpotTrade>>,
    latch: KlineLatch,
}

/// O1: process-wide feed health, sourced from the ONE Binance kline ingest
/// this process runs, and the keeper of the p0 latch. This is deliberately
/// the only writer of `Rails::set_feed_healthy` in the crate — see the
/// module doc.
///
/// `cfg` is a parameter (Important 3, task-15 review) rather than an
/// internal `KillSwitchCfg::default()` — production still passes the
/// default (see `run`'s call site) unchanged. The staleness math below reads
/// real `SystemTime` via `now_ms()`, which `tokio::time::pause()` does NOT
/// control, so a test that needs to cross the staleness threshold has to
/// control the THRESHOLD, not virtual time — this parameter is what makes
/// that possible without waiting out the real 5s production default.
fn spawn_kline_watch_task(
    mut close_rx: broadcast::Receiver<SpotClose1s>,
    latch: KlineLatch,
    rails: Arc<Rails>,
    shutdown: CancellationToken,
    cfg: KillSwitchCfg,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_seen_ms: Option<u64> = None;
        let mut ticker = tokio::time::interval(Duration::from_millis(FEED_HEALTH_TICK_MS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                r = close_rx.recv() => match r {
                    Ok(k) => {
                        last_seen_ms = Some(now_ms());
                        *latch.lock().expect("kline latch lock") =
                            Some(LastClose { close_px: k.close_px, close_ts_ms: k.close_ts_ms });
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(n, "tickburst: kline latch watcher lagged (seconds dropped)");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                _ = ticker.tick() => {
                    // Never having seen a kline yet is not staleness — avoid
                    // a false-unhealthy at cold start, matching
                    // slot_driver.rs's seed_last_book_ms treatment of the
                    // same Option shape.
                    let unhealthy = last_seen_ms
                        .map(|last| check_ws_staleness(&cfg, now_ms().saturating_sub(last)).is_some())
                        .unwrap_or(false);
                    rails.set_feed_healthy(!unhealthy);
                }
            }
        }
    })
}

/// Bridges `ClobWsIngest`'s `mpsc::Sender<BookUpdate>` output into the
/// `broadcast::Receiver<BookUpdate>` `drive_slot` expects (verified
/// interface note: the two ingest transports use different channel kinds).
fn spawn_book_bridge(
    mut book_rx: mpsc::Receiver<BookUpdate>,
    tx: broadcast::Sender<BookUpdate>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                item = book_rx.recv() => match item {
                    Some(b) => {
                        let _ = tx.send(b);
                    }
                    None => break,
                }
            }
        }
    })
}

/// Sleeps for `secs`, or returns early (`true`) on shutdown. `secs = 0`
/// resolves immediately.
async fn sleep_or_shutdown(shutdown: &CancellationToken, secs: u64) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => true,
        _ = tokio::time::sleep(Duration::from_secs(secs)) => false,
    }
}

/// Gamma discovery for one slot (§15.4). A market not yet minted gets a
/// light retry (1s cadence) up to slot open; a genuine fetch error skips
/// the slot immediately — `GammaClient::fetch_by_slug` has already retried
/// 429/5xx internally by the time it returns `Err`. Never guesses at token
/// ids: the only outputs are "here is the real metadata" or "skip".
async fn discover_meta<G: Discover>(
    gamma: &G,
    slug: &str,
    target_open: u64,
    shutdown: &CancellationToken,
) -> Option<MarketMeta> {
    loop {
        match gamma.fetch_by_slug(slug).await {
            Ok(Some(m)) => return Some(m),
            Ok(None) => {
                if now_ms() / 1000 + 1 >= target_open {
                    tracing::warn!(
                        slug, "tickburst: slot not minted by open; skipping (§15.4 — never a \
                        guess at token ids)"
                    );
                    return None;
                }
                if sleep_or_shutdown(shutdown, 1).await {
                    return None;
                }
            }
            Err(e) => {
                tracing::warn!(
                    ?e, slug, "tickburst: gamma discovery failed; skipping this slot (§15.4 — \
                    never a guess at token ids)"
                );
                // M5 (task-15 review): back off before returning. Without
                // this, a fetch error far from `target_open` had the outer
                // loop re-target the SAME slot immediately (`next_target_open`
                // recomputes from an unchanged `now`), producing a tight
                // zero-wait retry burst instead of a genuine skip. Matches
                // the "not yet minted" cadence above.
                sleep_or_shutdown(shutdown, 1).await;
                return None;
            }
        }
    }
}

/// Waits until `target_open - DISCOVERY_LEAD_SECS` (if that moment hasn't
/// already passed), then discovers `target_open`'s metadata. Factored out of
/// the loop body so it can run either inline (the very first slot a process
/// targets, or the one right after a discovery failure) or as a PIPELINED
/// background task for the slot immediately following whichever one is
/// currently driving — see `advance_pipeline`.
async fn wait_then_discover<G: Discover>(
    gamma: &G,
    target_open: u64,
    shutdown: &CancellationToken,
) -> Option<MarketMeta> {
    let discover_at = target_open.saturating_sub(DISCOVERY_LEAD_SECS);
    let wait = discover_at.saturating_sub(now_ms() / 1000);
    if wait > 0 && sleep_or_shutdown(shutdown, wait).await {
        return None;
    }
    if shutdown.is_cancelled() {
        return None;
    }
    let slug = format!("btc-updown-5m-{target_open}");
    discover_meta(gamma, &slug, target_open, shutdown).await
}

/// The next slot boundary this process should target: the NEXT (not yet
/// open) slot boundary after `now_s`, never the one we're currently inside
/// of — this process cannot usefully join an already-open slot mid-flight
/// (`SlotSeries` requires contiguous klines starting at t=0), so the slot in
/// progress right now is never a candidate. If even the discovery lead for
/// that next boundary has already passed (we're within `DISCOVERY_LEAD_SECS`
/// of it, e.g. process startup at t=298 of the current slot), push out one
/// more slot rather than starting discovery too late.
///
/// Correct in isolation (pinned by the tests below) — it genuinely cannot
/// target a slot already in progress. `advance_pipeline` is what keeps a
/// SERIALIZED caller of this function from skipping every slot immediately
/// following one it just finished driving (Important 1, task-15 review):
/// called fresh, right after a slot's `drive_slot` returns at ~S+300, `now_s`
/// is already `>= S+300`, so this correctly (per its own contract) targets
/// `S+600`, not `S+300` — the very case a naive "just call this again" loop
/// gets wrong. Do not "fix" this function to somehow special-case that; keep
/// the fix at the caller (pipelining), not here.
fn next_target_open(now_s: u64) -> u64 {
    let cur_open = now_s / SLOT_SECS_U64 * SLOT_SECS_U64;
    let next_open = cur_open + SLOT_SECS_U64;
    if next_open.saturating_sub(now_s) >= DISCOVERY_LEAD_SECS {
        next_open
    } else {
        next_open + SLOT_SECS_U64
    }
}

/// Resolves the slot to run next — either draining an already-in-flight
/// PIPELINED discovery (spawned while the PREVIOUS slot's driver was still
/// running) or, if none is pending (process start, or the previous
/// iteration's discovery failed), discovering it inline exactly as a
/// strictly-serialized loop always did.
///
/// On a resolved slot, immediately spawns a NEW pipelined discovery for the
/// FOLLOWING boundary (`target_open + SLOT_SECS_U64`) as a detached task,
/// stored in `*pipeline` for the NEXT call to drain.
///
/// **Important 1 (task-15 review).** A serialized loop can never reach the
/// S+290 discovery lead for slot N+1 while slot N's driver is still running
/// to S+300 — `drive_slot` alone occupies the full ~300s, so by the time it
/// returns, `next_target_open(now)` (now `>= S+300`) correctly (per its own
/// contract) targets `S+600`, silently skipping N+1 every single time a full
/// slot is driven. Discovery for N+1 must therefore start CONCURRENTLY with
/// N's drive, not after it. `discover_meta`'s own never-a-guess retry/skip
/// semantics are unchanged — only WHEN the call happens moved earlier. Only
/// the gamma HTTP call overlaps with the caller's book-WS subscription +
/// `drive_slot` for `target_open`: this function never touches the book WS,
/// so §15.4's "exactly one driver and one book-subscription pair live" is
/// untouched — the pipelined task is detached and shutdown-aware (via
/// `wait_then_discover`'s own `sleep_or_shutdown`/`is_cancelled` checks), so
/// it self-terminates promptly on cancellation even though nothing joins it.
///
/// `None` means this slot never resolved (not minted / gamma error) — the
/// caller's loop must move on with nothing pipelined; the NEXT call
/// recomputes fresh via `next_target_open(now)`.
async fn advance_pipeline<G: Discover>(
    pipeline: &mut Option<(u64, oneshot::Receiver<Option<MarketMeta>>)>,
    gamma: &Arc<G>,
    shutdown: &CancellationToken,
) -> Option<(u64, MarketMeta)> {
    let (target_open, meta) = match pipeline.take() {
        Some((open, rx)) => {
            let meta = match rx.await {
                Ok(m) => m,
                Err(_) => {
                    tracing::warn!(
                        target_open = open,
                        "tickburst: pipelined discovery task vanished (panicked/dropped); \
                         treating as a discovery failure"
                    );
                    None
                }
            };
            (open, meta)
        }
        None => {
            let target_open = next_target_open(now_ms() / 1000);
            let meta = wait_then_discover(gamma.as_ref(), target_open, shutdown).await;
            (target_open, meta)
        }
    };
    let meta = meta?;

    let next_open = target_open + SLOT_SECS_U64;
    let (tx, rx) = oneshot::channel();
    let g = gamma.clone();
    let sd = shutdown.clone();
    tokio::spawn(async move {
        let m = wait_then_discover(g.as_ref(), next_open, &sd).await;
        let _ = tx.send(m);
    });
    *pipeline = Some((next_open, rx));

    Some((target_open, meta))
}

/// Drives boundary-close reconstruction: consumes `@aggTrade`, publishes a
/// synthetic `SpotClose1s` onto `klines_tx` at each bar boundary + `delay_ms`,
/// and pairs every one against the real `@kline_1s` for a live fidelity check.
///
/// This task OWNS `klines_tx` while enabled — `spawn_kline_watch_task` (O1)
/// therefore derives feed health from the reconstructed stream, which is the
/// correct source because it is the stream the engine actually consumes. The
/// `MAX_CARRY_MS` bound in `boundary_close` is what keeps that honest: a dead
/// aggTrade socket stops emitting rather than publishing a frozen price.
fn spawn_boundary_close_task(
    delay_ms: u64,
    mut trades: broadcast::Receiver<SpotTrade>,
    mut shadow_klines: broadcast::Receiver<SpotClose1s>,
    klines_tx: broadcast::Sender<SpotClose1s>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut bc = BoundaryClose::new(Coin::Btc, delay_ms);
        let mut shadow = ShadowCompare::default();
        let (mut paired, mut exact, mut worst) = (0u64, 0u64, 0f64);
        loop {
            let now = now_micros() / 1_000;
            let sleep_for = next_tick_ms(now, delay_ms).saturating_sub(now);
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                t = trades.recv() => match t {
                    Ok(t) => bc.on_trade(t.exch_ts_ms, t.price),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(n, "tickburst: boundary-close trade stream lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                k = shadow_klines.recv() => match k {
                    Ok(k) => {
                        if let Some(s) = shadow.on_kline(k.close_ts_ms, k.close_px) {
                            paired += 1;
                            if s.exact { exact += 1; }
                            worst = worst.max(s.delta_usd.abs());
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(n, "tickburst: boundary-close shadow kline lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                _ = tokio::time::sleep(Duration::from_millis(sleep_for)) => {
                    for c in bc.on_tick(now_micros() / 1_000) {
                        if let Some(s) = shadow.on_recon(c.close_ts_ms, c.close_px) {
                            paired += 1;
                            if s.exact { exact += 1; }
                            worst = worst.max(s.delta_usd.abs());
                        }
                        let _ = klines_tx.send(c);
                    }
                    // Fidelity is the whole safety story for this feature, so
                    // report it on a cadence an operator will actually see.
                    if paired > 0 && paired % 300 == 0 {
                        tracing::info!(
                            paired,
                            exact_pct = 100.0 * exact as f64 / paired as f64,
                            worst_abs_usd = worst,
                            "tickburst: boundary-close vs kline fidelity"
                        );
                    }
                }
            }
        }
        tracing::info!(paired, exact, "tickburst: boundary-close task exiting");
    })
}

/// Polls the kline latch for up to `P0_POLL_BUDGET_MS`, shutdown-aware
/// (Important 4, task-15 review — this was the one wait in `slot_loop` that
/// wasn't). See `P0_POLL_BUDGET_MS`'s doc for why polling longer is strictly
/// safer, never weaker, than a single fixed sleep.
async fn poll_for_p0(latch: &KlineLatch, slot_open: u64, shutdown: &CancellationToken) -> Option<f64> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(P0_POLL_BUDGET_MS);
    loop {
        if let Some(p0) = resolve_p0(*latch.lock().expect("kline latch lock"), slot_open) {
            return Some(p0);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::select! {
            _ = shutdown.cancelled() => return None,
            _ = tokio::time::sleep(Duration::from_millis(P0_POLL_INTERVAL_MS)) => {}
        }
    }
}

/// One slot at a time (§15.4): discover (pipelined ahead of need —
/// Important 1, see `advance_pipeline`), subscribe its own book-WS pair,
/// wait for open, resolve `p0`, drive it to completion (or skip it, logged,
/// if `p0` never resolves), tear the subscription down, repeat. Stops
/// spawning new slots — never merely drains the current one — the moment
/// `shutdown` fires (O2c).
async fn slot_loop<V: Venue, G: Discover>(
    venue: Arc<V>,
    rails: Arc<Rails>,
    settle: Arc<SettleLane<ClobHttpClient>>,
    tele: Writer,
    strat: &StrategySection,
    rt: &RuntimeHandles<G>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    // Important 1: the slot immediately following whichever one is about to
    // be (or currently being) driven, discovered concurrently — see
    // `advance_pipeline`'s doc. `None` only at the very first iteration of a
    // process's life, or right after a discovery failure.
    let mut pipeline: Option<(u64, oneshot::Receiver<Option<MarketMeta>>)> = None;

    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        let Some((target_open, meta)) = advance_pipeline(&mut pipeline, &rt.gamma, &shutdown).await
        else {
            continue;
        };
        // N4 (task-15 re-review): a second, standing cancellation check,
        // not folded into the top-of-loop one above. Today the top check
        // is the ONLY exit from the `else { continue }` path above — it
        // works, but only because that `continue` is the sole one in this
        // loop and `advance_pipeline`'s cancelled-path return is zero-wait
        // (no sleep, no yield to speak of). Any future `continue` added
        // below this point (a new discovery-failure arm, say) would
        // reintroduce a CPU-pegging spin against a cancelled `shutdown`
        // without ever passing back through the top check — this makes the
        // property hold regardless of how many `continue`s this loop grows.
        if shutdown.is_cancelled() {
            return Ok(());
        }

        let slug = format!("btc-updown-5m-{target_open}");
        let ctx = SlotCtx {
            slug: slug.clone(),
            slot_open: target_open,
            token_up: meta.token_id_up.clone(),
            token_dn: meta.token_id_down.clone(),
            condition_id: meta.condition_id.clone(),
        };

        // M6 (task-15 review): a fresh BookRegistry per slot, not one
        // process-wide instance reused forever — 288 slots/day x 2 sides
        // would otherwise accumulate entries keyed by slug without bound in
        // a 24/7 process. The earlier crates build a fresh
        // per-slot registry for the same reason; there is no prune/remove
        // method on the shared registry type to reach for instead.
        let reg = Arc::new(BookRegistry::new());
        let deps = Arc::new(SlotDeps {
            venue: venue.clone(),
            reg: reg.clone(),
            rails: rails.clone(),
            settle: settle.clone(),
            tele: tele.clone(),
        });

        // §15.4: exactly one book subscription pair live at a time. §2
        // deviation 2: TWO Subscriptions (Up and Dn), never a derived
        // mirror. This slot's own `ClobWsIngest` (with its own internal
        // reconnect loop, §15.3) lives only for the duration of this slot.
        let slot_shutdown = shutdown.child_token();
        let subs = vec![
            Subscription { asset_id: ctx.token_up.clone(), slug: ctx.slug.clone(), side: Side::Up },
            Subscription { asset_id: ctx.token_dn.clone(), slug: ctx.slug.clone(), side: Side::Down },
        ];
        let mut ws_ingest = ClobWsIngest::new_with_registry(CLOB_MARKET_WS, subs, reg.clone())
            .with_shutdown_token(slot_shutdown.clone())
            // M7 (task-15 review): the book legitimately goes sparse->silent
            // past close (earlier-crate precedent, 2026-07-06) —
            // this subscription lives to HARD_DEADLINE_SECS (330s), past the
            // point the default 11s stale-reconnect watchdog would otherwise
            // start churning a reconnect roughly every ~13s.
            .with_book_stale_reconnect_until((target_open + SLOT_SECS_U64) * 1_000_000);
        if let Some(c) = rt.tls.clone() {
            ws_ingest = ws_ingest.with_tls_connector(c);
        }
        let (book_mpsc_tx, book_mpsc_rx) = mpsc::channel(BOOK_MPSC_CAP);
        let ws_handle = tokio::spawn(async move {
            if let Err(e) = ws_ingest.run(book_mpsc_tx).await {
                tracing::warn!(?e, "tickburst: book ws ingest exited");
            }
        });
        let (book_bcast_tx, _book_bcast_rx0) = broadcast::channel::<BookUpdate>(BOOK_BCAST_CAP);
        let bridge_handle = spawn_book_bridge(book_mpsc_rx, book_bcast_tx.clone(), slot_shutdown.clone());

        // §15.2: populate the user-WS asset registry BEFORE the driver
        // starts, or TRADEs are silently dropped.
        deps.venue.prepare_slot(&ctx.slug, &ctx.token_up, &ctx.token_dn).await;

        // N1 (task-15 re-review): resolve p0 via the isolated helper below,
        // which subscribes to BOTH broadcast feeds BEFORE waiting for
        // `target_open` — never at the `drive_slot` call site. See
        // `subscribe_wait_and_resolve_p0`'s doc for why late subscription
        // can silently lose the t=0 kline.
        match subscribe_wait_and_resolve_p0(rt, &book_bcast_tx, target_open, &shutdown).await {
            OpenWaitOutcome::Resolved { feeds, p0 } => {
                let driver_cfg = DriverCfg {
                    engine: EngineCfg {
                        ask_price_min: strat.ask_price_min,
                        ask_price_max: strat.ask_price_max,
                        limit_price: strat.limit_price,
                        clip_usd: strat.clip_usd,
                        tick: tick_cfg(strat),
                    },
                    book_stale_disarm_ms: strat.book_stale_disarm_ms,
                    // M8 (task-15 review): sourced from live.rs's own `pub`
                    // constant rather than a hand-copied duplicate literal —
                    // the venue's synthetic `Terminal` timer and the
                    // driver's grace sweep are meant to line up, not drift
                    // apart from an edited-in-one-place-only constant.
                    fak_grace_ms: crate::execution::live::FAK_GRACE_MS,
                    hard_deadline_secs: HARD_DEADLINE_SECS,
                };
                // O2a/b: `shutdown` itself, unmodified — the same token an
                // auth halt cancels — never `slot_shutdown` (that one is
                // this loop's own per-slot WS teardown signal and must not
                // be conflated with the driver's lifetime).
                if let Err(e) =
                    drive_slot(ctx.clone(), p0, driver_cfg, deps.clone(), feeds, shutdown.clone()).await
                {
                    tracing::error!(?e, slug = %ctx.slug, "tickburst: drive_slot returned an error");
                }
            }
            OpenWaitOutcome::ShutdownDuringWait => {
                slot_shutdown.cancel();
                let _ = ws_handle.await;
                let _ = bridge_handle.await;
                return Ok(());
            }
            OpenWaitOutcome::P0Unresolved => {
                // Inherited fail-closed conservatism, NOT a POST-TWAP-SPEC
                // §3 requirement: no gate reads p0 any more (D/zD are
                // telemetry), so a missing kline latch costs this slot's
                // entry for feature-quality reasons alone. Kept because the
                // idiom of substituting a nearby close is a
                // $3.09-mean error, and skipping is the cheap side.
                tracing::warn!(
                    slug = %ctx.slug, slot_open = target_open,
                    "tickburst: p0 unresolved (missing or non-contiguous kline latch) — skipping \
                     this slot entirely rather than falling back to a guess"
                );
            }
        }

        slot_shutdown.cancel();
        let _ = ws_handle.await;
        let _ = bridge_handle.await;
    }
}

/// Translates the resolved trigger settings into the engine's own config.
/// `enabled = false` yields `None`, which makes the engine incapable of
/// firing at all — there is no second fire path (spec §6).
fn tick_cfg(strat: &StrategySection) -> Option<TickCfg> {
    strat.tick.enabled.then_some(TickCfg {
        mode: strat.tick.trigger,
        v_btc: strat.tick.v_btc,
        lean_min: strat.tick.lean_min,
        adverse_max_btc: strat.tick.adverse_max_btc,
        window_ms: strat.tick.window_ms,
        max_book_age_ms: strat.tick.max_book_age_ms,
    })
}

/// Outcome of `subscribe_wait_and_resolve_p0`.
enum OpenWaitOutcome {
    /// `p0` resolved; NONE of the broadcast receivers can have missed
    /// anything sent from the moment this function was called, because all
    /// were subscribed before its wait even started (N1, task-15 re-review).
    Resolved { feeds: SlotFeeds, p0: f64 },
    /// `shutdown` fired while waiting for `target_open` itself to arrive —
    /// the caller must stop the whole loop (matches the pre-N1 early
    /// `return Ok(())`), not merely skip this slot.
    ShutdownDuringWait,
    /// `target_open` arrived but `p0` never resolved — either the kline
    /// latch was missing/non-contiguous, or `shutdown` fired mid-poll (a
    /// genuine shutdown here is still caught promptly: `slot_loop`'s own
    /// top-of-iteration check catches it on the very next pass).
    P0Unresolved,
}

/// N1 fix (task-15 re-review), isolated from `slot_loop` so it is directly
/// unit-testable without a live gamma client or book WS: subscribes to
/// BOTH broadcast feeds, THEN waits for `target_open`, THEN polls for
/// `p0` — in that exact order. A `broadcast::Receiver` delivers nothing
/// sent before `subscribe()`; the `t=0` kline (close_ts_ms of `slot_open *
/// 1000 + 999`) publishes at ~`slot_open + 1.0s`, and `poll_for_p0`'s
/// budget is up to 1.2s, so subscribing only AFTER this wait can lose
/// `t=0` whenever the latch resolves late — precisely the case the
/// widened p0 budget exists to rescue. `SlotSeries` requires contiguity
/// from `t=0`, so losing it means the slot never advances, never arms,
/// and never fires for the whole 300s: a silent no-op, not a logged skip.
///
/// Subscribing this early is free and safe: `kline_to_t` (`slot_driver.rs`)
/// returns `None` for any close before `slot_open`, so pre-open klines
/// buffered on the returned receiver are inert once handed to
/// `drive_slot`, and `KLINE_BCAST_CAP` (4096) at 1 kline/s leaves enormous
/// headroom over the ~`DISCOVERY_LEAD_SECS` (10s) + `P0_POLL_BUDGET_MS`
/// (1.2s) this receiver can sit idle before `drive_slot` starts draining
/// it — nowhere near enough traffic to risk a `Lagged`.
/// `book_bcast_tx.subscribe()` is hoisted the same way for symmetry,
/// though it is not load-bearing the same way — `BookRegistry` carries
/// the book state a late subscriber would otherwise miss.
async fn subscribe_wait_and_resolve_p0<G: Discover>(
    rt: &RuntimeHandles<G>,
    book_bcast_tx: &broadcast::Sender<BookUpdate>,
    target_open: u64,
    shutdown: &CancellationToken,
) -> OpenWaitOutcome {
    // The trade feed is subscribed here for the same N1 reason as the other
    // two — it is the ONE place a receiver can be created early enough that
    // nothing sent during the wait is lost. It carries far more traffic than
    // the klines (tens of trades a second vs one bar), but it is also the one
    // feed whose backlog is harmless: `TickRing::push` prunes by TIMESTAMP,
    // so buffered pre-open ticks are dropped on the first in-window read
    // rather than shifting any reference price.
    let feeds = SlotFeeds {
        klines: rt.klines_tx.subscribe(),
        books: book_bcast_tx.subscribe(),
        trades: rt.trades_tx.as_ref().map(|tx| tx.subscribe()),
    };

    let wait = target_open.saturating_sub(now_ms() / 1000);
    if wait > 0 && sleep_or_shutdown(shutdown, wait).await {
        return OpenWaitOutcome::ShutdownDuringWait;
    }

    // Important 4: poll rather than a single fixed sleep — see
    // `poll_for_p0`'s doc. `resolve_p0`'s contiguity check is the hard
    // gate regardless of how long this waits.
    match poll_for_p0(&rt.latch, target_open, shutdown).await {
        Some(p0) => OpenWaitOutcome::Resolved { feeds, p0 },
        None => OpenWaitOutcome::P0Unresolved,
    }
}

/// Run the strategy against `venue` until `shutdown` fires. Generic over
/// `V: Venue` — `--dry-run` passes a `ShadowVenue`, `--live` a `LiveVenue`
/// (constructed by the CLI, AFTER `live::launch_gate` has passed — see
/// `main.rs`); this function is otherwise identical for both.
pub async fn run<V: Venue>(
    cfg: BotConfig,
    venue: Arc<V>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    crate::config::validate_strategy(&cfg.strategy)?;

    let epoch = now_ms() / 1000;
    let tele_path = std::path::PathBuf::from("data").join(format!("tickburst_telemetry_{epoch}.jsonl"));
    let (tele, tele_handle) = Writer::new_with_handle(&tele_path)?;
    tracing::info!(path = %tele_path.display(), "tickburst: telemetry sink");

    let rails = Arc::new(Rails::new(&cfg.strategy));

    let settle_fetch = ClobHttpClient::new(CLOB_HTTP_BASE)?;
    // §15.2: SettleLane is spawned ONCE at startup, not per slot — positions
    // outlive their slot by ~30s. `SettleLane` is cheap to clone (every
    // field is internally Arc-wrapped): `settle_for_task` drives the
    // background poll loop; `settle_arc` (M6: an `Arc` around the SAME
    // underlying SettleLane) is what every slot's `SlotDeps` clones from —
    // never a fresh `SettleLane` per slot, which would fragment `track()`
    // calls across independent position lists.
    let settle_lane = SettleLane::new(settle_fetch, rails.clone(), tele.clone());
    let settle_for_task = settle_lane.clone();
    let settle_arc = Arc::new(settle_lane);
    // O3: keep the handle and supervise it — a settle task that dies
    // otherwise leaves `concurrent_usd` ratcheting to the cap with no
    // alarm, silently vetoing every future fire (rails.rs's own doc on
    // `reconcile`/step 3).
    //
    // N3 (task-15 re-review): this supervisor DELIBERATELY only LOGS — it
    // never cancels `shutdown`, unlike `main.rs`'s `supervise_or_halt`
    // (used for the live user-WS ingest task). The two are similar in
    // shape — both await a `JoinHandle` and log loudly on panic — but they
    // are NOT the same policy, and the asymmetry is the point: a dead
    // settle lane fails CLOSED on its own. The exposure rail above
    // ratchets shut once roughly `concurrent_exposure_cap_usd / clip`
    // positions are stuck unsettled, which halts new fires without any
    // supervisor intervention. A dead user-WS instead fails OPEN — money
    // can still be spent, fills become invisible, and the CLOB-settled
    // loss kill (fed only by `settle()`'s output) goes blind — which is
    // why THAT supervisor cancels `shutdown` instead of merely logging.
    // Do not make this supervisor cancel too; see
    // `main.rs::supervise_or_halt`'s own doc for the other half of this
    // asymmetry.
    let settle_handle = tokio::spawn(settle_for_task.run(shutdown.clone()));
    let settle_supervisor = tokio::spawn(async move {
        if let Err(e) = settle_handle.await {
            tracing::error!(
                error = ?e,
                "tickburst: SettleLane task PANICKED — concurrent_usd will never be released for \
                 its still-tracked positions and the exposure rail will eventually veto every \
                 fire, with no other alarm. Restart the process."
            );
        }
    });

    // Shared TLS connector: one native-cert load for BOTH the Binance kline
    // WS and every per-slot Polymarket book WS.
    // Falls back to a per-connect load (acceptable, just slower) rather
    // than failing the whole run if this can't be built.
    let tls = tokio::task::spawn_blocking(build_shared_tls_connector)
        .await
        .context("join tls builder")?
        .map_err(|e| tracing::warn!(?e, "tickburst: no shared TLS connector; falling back per-connect"))
        .ok();

    // §15.2: ONE Binance @kline_1s ingest for the whole process, broadcast
    // to every slot driver via `klines_tx.subscribe()`.
    let (klines_tx, _klines_keepalive) = broadcast::channel::<SpotClose1s>(KLINE_BCAST_CAP);

    // Boundary-close reconstruction (the boundary-close probe, 2026-07-27).
    // OPT-IN. When enabled the ingest's @kline_1s is diverted to a SHADOW
    // channel and `klines_tx` is fed instead by `spawn_boundary_close_task`
    // from @aggTrade, ~152 ms earlier. Everything downstream — resolve_p0's
    // contiguity gate and every feature — is untouched either way, and under
    // spec §4.1 none of it gates anything.
    let boundary_delay = cfg.strategy.boundary_close_delay_ms;
    let tick_enabled = cfg.strategy.tick.enabled;
    let (trades_tx, _trades_keepalive) = broadcast::channel::<SpotTrade>(TRADE_BCAST_CAP);
    let (shadow_klines_tx, _shadow_keepalive) = broadcast::channel::<SpotClose1s>(KLINE_BCAST_CAP);
    let boundary_handle = boundary_delay.map(|d| {
        tracing::info!(
            delay_ms = d,
            "tickburst: boundary-close reconstruction ENABLED — klines rebuilt from @aggTrade; \
             @kline_1s retained as shadow"
        );
        spawn_boundary_close_task(
            d,
            trades_tx.subscribe(),
            shadow_klines_tx.subscribe(),
            klines_tx.clone(),
            shutdown.clone(),
        )
    });
    // When reconstruction is ON the ingest's kline goes to the shadow channel
    // and the boundary task owns `klines_tx`; when OFF the ingest feeds
    // `klines_tx` directly and nothing else changes.
    let ingest_kline_tx = match boundary_delay {
        None => klines_tx.clone(),
        Some(_) => shadow_klines_tx.clone(),
    };
    // `@aggTrade` rides the same socket whenever anything consumes it — the
    // tick path (the ONLY fire path) or the boundary-close reconstruction
    // (off, and staying off). Subscribed to NEITHER when both are off, in
    // which case this process cannot trade at all.
    let ingest_trades_tx = (tick_enabled || boundary_delay.is_some()).then(|| trades_tx.clone());
    if tick_enabled {
        tracing::info!(
            sniper_mode = cfg.strategy.mode.as_str(),
            trigger = cfg.strategy.tick.trigger.as_str(),
            ask_band = %format!("{}-{}", cfg.strategy.ask_price_min, cfg.strategy.ask_price_max),
            limit = cfg.strategy.limit_price,
            clip_usd = cfg.strategy.clip_usd,
            v_btc = cfg.strategy.tick.v_btc,
            lean_min = cfg.strategy.tick.lean_min,
            window_ms = cfg.strategy.tick.window_ms,
            max_book_age_ms = cfg.strategy.tick.max_book_age_ms,
            "tickburst: TICK fire path ENABLED — @aggTrade subscribed alongside @kline_1s"
        );
    } else {
        tracing::warn!(
            "tickburst: tick fire path DISABLED (SNIPER_TICK_ENABLED=false) — it is the ONLY \
             fire path, so this process cannot place an order at all"
        );
    }

    let ingest_handle = {
        let ktx = ingest_kline_tx;
        let btls = tls.clone();
        tokio::spawn(async move {
            let (spot_tx, _spot_rx) = broadcast::channel(1024);
            let (watch_txs, _watch_rxs) = build_spot_watch_channels();
            // Empty bookTicker coin list on purpose: this process consumes
            // ONLY @kline_1s (both the SpotTick broadcast and watch receivers
            // below are dropped immediately), and every bookTicker frame that
            // rides this socket delays the kline we are actually waiting on by
            // a measured +3.6 ms mean / +3 ms p50 — deployed VPS, 898 paired
            // bars, latency-squeeze probe of 2026-07-26 §2.
            let mut ingest = BinanceIngest::new(BinanceVenue::Spot.default_ws_base(), vec![])
                .with_kline_1s(vec![Coin::Btc], ktx);
            // The tick fire path (and, if it were ever enabled, the
            // boundary-close reconstruction) also carries @aggTrade on the
            // same socket. Same socket on purpose: a second connection would
            // put the ticks and the bars on independent latency paths, and
            // the tick gate pairs each tick with a specific closed bar.
            if let Some(tx) = ingest_trades_tx {
                ingest = ingest.with_agg_trades(vec![Coin::Btc], tx);
            }
            if let Some(c) = btls {
                ingest = ingest.with_tls_connector(c);
            }
            if let Err(e) = ingest.run(spot_tx, watch_txs).await {
                tracing::error!(?e, "tickburst: binance kline ingest exited");
            }
        })
    };

    let latch: KlineLatch = Arc::new(Mutex::new(None));
    // O1.
    let health_handle = spawn_kline_watch_task(
        klines_tx.subscribe(),
        latch.clone(),
        rails.clone(),
        shutdown.clone(),
        KillSwitchCfg::default(),
    );

    let gamma = Arc::new(GammaClient::new(GAMMA_BASE)?);
    let rt = RuntimeHandles {
        gamma,
        tls,
        klines_tx,
        trades_tx: tick_enabled.then(|| trades_tx.clone()),
        latch,
    };

    let tele_for_slots = tele.clone();
    drop(tele);

    let result = slot_loop(
        venue,
        rails.clone(),
        settle_arc.clone(),
        tele_for_slots,
        &cfg.strategy,
        &rt,
        shutdown.clone(),
    )
    .await;

    // Teardown, in dependency order. Cancel `shutdown` unconditionally
    // first: a `slot_loop` return that was NOT already a shutdown (an
    // internal error) must still stop the settle lane and drain telemetry
    // rather than leaking background tasks.
    shutdown.cancel();
    ingest_handle.abort();
    health_handle.abort();
    if let Some(h) = boundary_handle {
        h.abort();
    }
    let _ = settle_supervisor.await;
    // O4 / M6: releases the last Writer clone `settle_arc`'s SettleLane
    // instance holds (its `Arc<Writer>`, shared since construction with
    // `settle_for_task`'s — that one was already released above when
    // `settle_handle` finished and dropped `settle_for_task`). `slot_loop`'s
    // own `tele_for_slots` clone — and every per-slot `SlotDeps.tele` clone
    // it made along the way — is already gone: it was moved into `slot_loop`
    // by value and dropped when that call returned.
    drop(settle_arc);

    // O4: drain whatever telemetry is still queued rather than losing it at
    // process exit. By this point every `Writer` clone this function
    // created has been dropped, so the writer task's channel closes and its
    // loop runs to completion once it has flushed every already-queued row.
    match tokio::time::timeout(Duration::from_secs(5), tele_handle).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::error!(?e, "tickburst: telemetry writer task panicked while draining"),
        Err(_) => tracing::error!(
            "tickburst: telemetry writer did not drain within 5s at shutdown — some rows may be lost"
        ),
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk::rails::RailVeto;
    use crate::types::RewardsCfg;
    use crate::polymarket::phase::{PhaseClock, PhaseClockCfg};

    /// A real slot boundary (exactly divisible by `SLOT_SECS_U64`), unlike
    /// the arbitrary anchors other modules use for offset-only arithmetic.
    fn slot_boundary() -> u64 {
        1_000_000_000u64 / SLOT_SECS_U64 * SLOT_SECS_U64
    }

    #[test]
    fn next_target_open_targets_the_upcoming_boundary_with_lead_to_spare() {
        // `so` is the upcoming (not-yet-open) boundary; 20s of runway
        // remains before it, well clear of the 10s discovery lead.
        let so = slot_boundary();
        assert_eq!(next_target_open(so - 20), so);
    }

    #[test]
    fn next_target_open_never_targets_the_slot_already_in_progress() {
        // The instant a slot opens, it is already "in progress" as far as
        // this function is concerned — this process cannot join it
        // mid-flight (SlotSeries needs every kline from t=0), so the very
        // next call must target the FOLLOWING boundary, not `so` itself.
        let so = slot_boundary();
        assert_eq!(next_target_open(so), so + SLOT_SECS_U64);
        assert_eq!(next_target_open(so + 1), so + SLOT_SECS_U64);
    }

    #[test]
    fn next_target_open_skips_ahead_once_the_discovery_lead_has_passed() {
        let so = slot_boundary();
        // Only 5s of runway left before `so` opens — less than the 10s
        // discovery lead, so starting discovery now would already be late.
        // Must target the boundary AFTER `so` instead.
        assert_eq!(next_target_open(so - 5), so + SLOT_SECS_U64);
        // Exactly at the boundary: the discovery lead just barely still fits.
        assert_eq!(next_target_open(so - 10), so);
    }

    /// Always-resolved `Discover` stub — no network, no delay. Powers
    /// `advance_pipeline_targets_two_consecutive_slots_without_skipping`.
    struct StubGamma;

    fn stub_meta(slug: &str) -> MarketMeta {
        MarketMeta {
            slug: slug.to_string(),
            coin: Coin::Btc,
            market_start_ts: 0,
            market_end_ts: SLOT_SECS_U64,
            condition_id: format!("cond-{slug}"),
            token_id_up: format!("up-{slug}"),
            token_id_down: format!("dn-{slug}"),
            tick_size: 0.01,
            min_order_size: 5.0,
            neg_risk: false,
            maker_fee_bps: 0,
            taker_fee_bps: 0,
            rewards: RewardsCfg::default(),
            phase_clock: PhaseClock::new(0, PhaseClockCfg::default_5m()),
        }
    }

    impl Discover for StubGamma {
        fn fetch_by_slug(
            &self,
            slug: &str,
        ) -> impl Future<Output = anyhow::Result<Option<MarketMeta>>> + Send {
            let slug = slug.to_string();
            async move { Ok(Some(stub_meta(&slug))) }
        }
    }

    /// Important 1 (task-15 review): a serialized "discover only after
    /// driving" loop can never reach the S+290 discovery lead for slot N+1
    /// while slot N's driver is still running to S+300, so it silently
    /// SKIPS slot N+1 every time a full slot is driven. This pins
    /// `advance_pipeline`'s fix directly against the underlying mechanism —
    /// not against real elapsed timing, which a mutation could satisfy by
    /// coincidence: resolving one slot must immediately populate the
    /// pipeline with the VERY NEXT boundary, and the following call must
    /// resolve exactly that boundary rather than recomputing a fresh
    /// `next_target_open(now)` (which, at effectively-unchanged real `now` —
    /// this test performs no multi-hundred-second real sleep — would
    /// reproduce the SAME target as the first call instead of advancing
    /// past it).
    #[tokio::test(start_paused = true)]
    async fn advance_pipeline_targets_two_consecutive_slots_without_skipping() {
        let gamma = Arc::new(StubGamma);
        let shutdown = CancellationToken::new();
        let mut pipeline: Option<(u64, oneshot::Receiver<Option<MarketMeta>>)> = None;

        let (open1, _meta1) = advance_pipeline(&mut pipeline, &gamma, &shutdown)
            .await
            .expect("first slot must resolve (StubGamma always mints)");

        assert_eq!(
            pipeline.as_ref().map(|(open, _)| *open),
            Some(open1 + SLOT_SECS_U64),
            "resolving a slot must immediately kick off pipelined discovery for the slot right \
             after it, before the caller does anything else (subscribe the book WS, drive_slot)"
        );

        let (open2, _meta2) = advance_pipeline(&mut pipeline, &gamma, &shutdown)
            .await
            .expect("second slot must resolve from the pipeline");
        assert_eq!(
            open2,
            open1 + SLOT_SECS_U64,
            "the second call must drain the PIPELINED slot immediately following the first — a \
             reversion to the serialized shape (discovery kicked off only after this point, not \
             before) would instead recompute next_target_open(now) fresh here and land back on \
             open1 rather than advancing to open1 + SLOT_SECS_U64"
        );
    }

    /// N1 mutation guard (task-15 re-review): `subscribe_wait_and_resolve_p0`
    /// must subscribe to the kline broadcast feed BEFORE waiting for
    /// `target_open`, not after. A `broadcast::Receiver` delivers nothing
    /// sent before `subscribe()`, so this reproduces the exact rescued
    /// case: the kline latch only becomes contiguous LATE — partway
    /// through the wait, well before `target_open` itself arrives, mirroring
    /// a latch that resolves late relative to the widened p0 poll budget.
    /// A receiver subscribed only AFTER the wait (the pre-fix call-site
    /// shape, `rt.klines_tx.subscribe()` passed straight into `drive_slot`)
    /// would already have missed this exact send.
    ///
    /// Deliberately exercises real (unpaused) time: `now_ms()` reads real
    /// `SystemTime` (see `spawn_kline_watch_task`'s doc, same constraint),
    /// so `target_open` — and therefore the wait this function computes
    /// internally — cannot be driven by `tokio::time::pause()`.
    #[tokio::test]
    async fn subscribe_wait_and_resolve_p0_subscribes_before_the_wait_not_after() {
        let target_open = now_ms() / 1000 + 2;
        let (klines_tx, _keepalive) = broadcast::channel::<SpotClose1s>(KLINE_BCAST_CAP);
        let (book_tx, _book_keepalive) = broadcast::channel::<BookUpdate>(BOOK_BCAST_CAP);
        let (trades_tx, _trades_keepalive) = broadcast::channel::<SpotTrade>(TRADE_BCAST_CAP);
        let latch: KlineLatch = Arc::new(Mutex::new(None));
        let rt = RuntimeHandles {
            gamma: Arc::new(StubGamma),
            tls: None,
            klines_tx: klines_tx.clone(),
            trades_tx: Some(trades_tx.clone()),
            latch: latch.clone(),
        };
        let shutdown = CancellationToken::new();

        // Fires ~200ms into the ~2s wait: long after `subscribe_wait_and_
        // resolve_p0` must already have subscribed (it does so as its very
        // first act, before any wait — see the fn's own doc), long before
        // `target_open` arrives.
        let injector = {
            let klines_tx = klines_tx.clone();
            let trades_tx = trades_tx.clone();
            let latch = latch.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let close_ts_ms = (target_open - 1) * 1000 + 999;
                *latch.lock().expect("latch") =
                    Some(LastClose { close_px: 42.0, close_ts_ms });
                // The same sender both the process-wide latch watcher and
                // the per-slot driver subscribe from — this send is the
                // exact message a late-subscribing receiver would miss.
                let _ = klines_tx.send(SpotClose1s { coin: Coin::Btc, close_px: 42.0, close_ts_ms });
                // Same property for the trade feed: it is the tick path's
                // only input, so a late subscription silently blinds the gate
                // for the opening seconds of every slot.
                let _ = trades_tx.send(SpotTrade {
                    coin: Coin::Btc,
                    price: 64_000.0,
                    qty: 0.1,
                    buyer_is_maker: false,
                    exch_ts_ms: target_open * 1000,
                    recv_ts_us: target_open * 1_000_000,
                });
            })
        };

        let outcome = subscribe_wait_and_resolve_p0(&rt, &book_tx, target_open, &shutdown).await;
        injector.await.expect("injector must not panic");

        let OpenWaitOutcome::Resolved { feeds, p0 } = outcome else {
            panic!("p0 must resolve: the latch was made contiguous well before target_open arrived");
        };
        let SlotFeeds { mut klines, trades, .. } = feeds;
        assert_eq!(p0, 42.0);
        let mut trades = trades.expect("the tick path is enabled in this rt");
        assert!(
            trades.try_recv().is_ok(),
            "the trade receiver must have been subscribed before the wait too — a tick sent \
             during it is a fire the slot would never see"
        );

        // The N1 assertion proper: the receiver this function returns must
        // already hold the kline sent DURING the wait, proving `subscribe()`
        // ran before the wait started, not after it returned. Mutation-verify:
        // moving the two `subscribe()` calls in `subscribe_wait_and_resolve_p0`
        // down to just before constructing `OpenWaitOutcome::Resolved` (i.e.
        // back to "subscribe after the wait/poll", the pre-fix shape) makes
        // this `try_recv()` return `Empty` instead, since the injector's send
        // would already have happened on a channel state this receiver never
        // observed.
        match klines.try_recv() {
            Ok(k) => assert_eq!(k.close_px, 42.0),
            Err(e) => panic!(
                "subscribe() must happen before the wait for target_open — the receiver missed \
                 the t=0 kline sent during the wait: {e:?}"
            ),
        }
    }

    /// Important 3, "O1 both directions" (task-15 review):
    /// `spawn_kline_watch_task` must flip `Rails::feed_healthy` off the ONE
    /// Binance kline ingest's own staleness in BOTH directions — trips, and
    /// clears again on a fresh kline. `now_ms()` reads real `SystemTime`
    /// (`tokio::time::pause()` does not control it), so the THRESHOLD, not
    /// virtual time, is the lever this test controls — see
    /// `spawn_kline_watch_task`'s doc. Real (unpaused) sleeps, sized
    /// comfortably around the fixed `FEED_HEALTH_TICK_MS` (1s) ticker
    /// cadence, with a threshold chosen to sit strictly between one and two
    /// tick periods so "just ticked once" reads healthy and "two ticks with
    /// no kline" reads unhealthy, with slack on both sides.
    ///
    /// N5 (task-15 re-review): `THRESHOLD_MS` sits at the MIDPOINT between
    /// one tick (~1.0s) and two ticks (~2.0s), not off to one side — the
    /// prior 1_800 left the unhealthy assertion only ~0.2s of real-time
    /// slack (2.0s observed age vs an 1.8s threshold), so any delay in the
    /// first kline's delivery past ~200ms (a loaded box, a slow CI runner)
    /// shrunk the observed age below threshold and the trip never fired.
    /// 1_500 gives ~0.5s of slack on BOTH the healthy and unhealthy sides.
    #[tokio::test]
    async fn spawn_kline_watch_task_flips_feed_health_both_directions() {
        const THRESHOLD_MS: u64 = 1_500;
        const PHASE_SLEEP: Duration = Duration::from_millis(1_300);

        let (tx, rx) = broadcast::channel(16);
        let latch: KlineLatch = Arc::new(Mutex::new(None));
        let rails = Arc::new(Rails::new(&StrategySection::default()));
        let shutdown = CancellationToken::new();
        let cfg = KillSwitchCfg { ws_stale_threshold_ms: THRESHOLD_MS, ..KillSwitchCfg::default() };
        let handle = spawn_kline_watch_task(rx, latch.clone(), rails.clone(), shutdown.clone(), cfg);

        tx.send(SpotClose1s { coin: Coin::Btc, close_px: 100.0, close_ts_ms: now_ms() })
            .expect("send kline 1");
        tokio::time::sleep(PHASE_SLEEP).await;
        assert!(
            rails.check_fire(5.0, now_ms()).is_ok(),
            "must read healthy shortly after a fresh kline"
        );

        // No further kline: the NEXT tick observes staleness.
        tokio::time::sleep(PHASE_SLEEP).await;
        let trip = rails.check_fire(5.0, now_ms()).expect_err("must trip once stale");
        assert_eq!(trip.veto, RailVeto::FeedUnhealthy);

        // A fresh kline clears it again.
        tx.send(SpotClose1s { coin: Coin::Btc, close_px: 101.0, close_ts_ms: now_ms() })
            .expect("send kline 2");
        tokio::time::sleep(PHASE_SLEEP).await;
        assert!(rails.check_fire(5.0, now_ms()).is_ok(), "a fresh kline must clear the veto again");

        shutdown.cancel();
        handle.await.expect("watch task must exit cleanly on shutdown");
    }

    /// All `tickburst_telemetry_*.jsonl` file names currently present in
    /// `data/` (relative to the test binary's cwd — the crate root under
    /// `cargo test`). Used to detect what `run()` creates without ever
    /// touching the process-wide CWD (unsafe under parallel test execution).
    fn telemetry_file_names() -> std::collections::HashSet<std::ffi::OsString> {
        std::fs::read_dir("data")
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.file_name())
                    .filter(|n| n.to_string_lossy().starts_with("tickburst_telemetry_"))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Important 3, "O2c + O4 together" (task-15 review): `run()` with a
    /// PRE-CANCELLED token must return promptly (never spawn a slot —
    /// `slot_loop`'s very first check is `shutdown.is_cancelled()`, before
    /// any gamma/book-WS network touch), while still standing up and
    /// draining its telemetry writer (O4).
    #[tokio::test]
    async fn run_with_a_precancelled_token_returns_promptly_and_drains_telemetry() {
        let before = telemetry_file_names();

        let cfg = BotConfig { strategy: StrategySection::default(), live: None };
        let venue = Arc::new(crate::execution::venue::ShadowVenue::new());
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let started = std::time::Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(10), run(cfg, venue, shutdown)).await;
        let elapsed = started.elapsed();

        // Clean up whatever this run created BEFORE any assertion below can
        // panic and skip it — this test must never leave a stray file in
        // the crate's (non-gitignored, since only the workspace-root
        // `/data` is ignored) `data/` directory.
        let after = telemetry_file_names();
        let created: Vec<_> = after.difference(&before).cloned().collect();
        for name in &created {
            let _ = std::fs::remove_file(std::path::Path::new("data").join(name));
        }

        let result = outcome.expect("run() must return promptly on a pre-cancelled token, not hang");
        assert!(result.is_ok(), "a pre-cancelled shutdown is not itself an error: {result:?}");
        assert_eq!(created.len(), 1, "run() must still create exactly one telemetry sink file (O4)");
        // O4's drain step (`tokio::time::timeout(5s, tele_handle)` in `run`)
        // only takes its full budget when a `Writer` clone was left alive
        // past teardown (e.g. a dropped `drop(settle_arc)`/`drop(tele)` call
        // upstream) — nothing was ever emitted in this run, so a properly
        // draining process returns in milliseconds, not seconds.
        assert!(
            elapsed < Duration::from_secs(3),
            "run() took {elapsed:?} — O4's telemetry drain should be near-instant here (nothing \
             was ever emitted); this slow a return means some Writer clone was left alive past \
             teardown, forcing the drain into its full 5s fallback budget"
        );
    }
}
