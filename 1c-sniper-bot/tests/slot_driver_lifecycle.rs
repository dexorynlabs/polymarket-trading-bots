//! Lifecycle + boundary coverage for `slot_driver`: the pure `t`-derivation
//! helpers, then driver-level tests against `ShadowVenue` and a hand-driven
//! event sequence.
//!
//! Under POST-TWAP-SPEC §6 the stimulus changed but the money path did not:
//! every fire below is produced by a BOOK event (a standing 1c ask, from the
//! registry) followed by a burst of `@aggTrade` prints, because the tick path
//! is the only fire path. What each test then asserts — one post per fire,
//! the commit the rail books, reconcile-exactly-once, the FAK outcome
//! taxonomy, the settle handoff — is inherited unchanged.

use onecentbot::types::{now_micros, BookUpdate, Coin, Side, SpotClose1s, SpotTrade};
use onecentbot::polymarket::book_registry::{BookRegistry, BookSnapshot, Level};
use onecentbot::polymarket::clob_http::MarketFetch;
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use onecentbot::config::StrategySection;
use onecentbot::strategy::engine::EngineCfg;
use onecentbot::strategy::tick::TickCfg;
use onecentbot::risk::rails::Rails;
use onecentbot::bot::settle::SettleLane;
use onecentbot::bot::slot_driver::{
    book_to_t, drive_slot, kline_to_t, DriverCfg, SlotCtx, SlotDeps, SlotFeeds, HARD_DEADLINE_SECS,
};
use onecentbot::bot::telemetry::Writer;
use onecentbot::execution::venue::{FakOutcome, FakRequest, ShadowVenue, Venue, VenueEvent};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

#[test]
fn kline_t_is_derived_from_the_venue_close_time() {
    let so = 1_784_000_000u64;
    // The kline OPENING at slot_open closes at slot_open + 0.999s -> t = 0.
    assert_eq!(kline_to_t(so * 1000 + 999, so), Some(0));
    assert_eq!(kline_to_t((so + 214) * 1000 + 999, so), Some(214));
    assert_eq!(kline_to_t((so + 299) * 1000 + 999, so), Some(299));
}

#[test]
fn kline_t_rejects_out_of_slot_stamps() {
    let so = 1_784_000_000u64;
    assert_eq!(kline_to_t((so - 1) * 1000 + 999, so), None, "before slot open");
    assert_eq!(kline_to_t((so + 300) * 1000, so), None, "past slot end");
}

#[test]
fn book_t_comes_from_the_local_clock_and_is_bounded() {
    let so = 1_784_000_000u64;
    assert_eq!(book_to_t((so + 214) * 1000 + 1, so), Some(214));
    assert_eq!(book_to_t((so - 1) * 1000, so), None);
    assert_eq!(book_to_t((so + 300) * 1000, so), None);
}

#[test]
fn the_two_feeds_disagree_by_about_one_second_by_construction() {
    // This is the skew that produced the Task 5 book-path window defect, and
    // it is REAL, not an artifact: the kline for second t is published by the
    // venue only once second t has ended. So while the local clock reads
    // second 295, the newest kline available is the one for 294.
    let so = 1_784_000_000u64;
    let local_now_ms = (so + 295) * 1000 + 200;
    let newest_kline_close_ts = (so + 294) * 1000 + 999;
    assert_eq!(book_to_t(local_now_ms, so), Some(295));
    assert_eq!(kline_to_t(newest_kline_close_ts, so), Some(294));
}

// =============================================================================
// Driver-level tests (§13 step 5). Drive a real `SlotEngine` end to end
// through `drive_slot`, against `ShadowVenue` (or a small purpose-built
// double for the outcomes `ShadowVenue` can't express: post failure, a
// controllable Acked-then-silent order) plus a hand-built kline/book event
// sequence. No sockets.
// =============================================================================

const P0: f64 = 100_000.0;

/// The three feeds a live slot consumes. `trades` is what fires (spec §6), so
/// unlike the earlier version of this file it is never `None` for a test
/// that expects an order.
fn feeds(
    klines: broadcast::Receiver<SpotClose1s>,
    books: broadcast::Receiver<BookUpdate>,
    trades: broadcast::Receiver<SpotTrade>,
) -> SlotFeeds {
    SlotFeeds { klines, books, trades: Some(trades) }
}

/// Feeds with no trade stream at all — for the cases that must never fire.
fn no_trade_feeds(
    klines: broadcast::Receiver<SpotClose1s>,
    books: broadcast::Receiver<BookUpdate>,
) -> SlotFeeds {
    SlotFeeds { klines, books, trades: None }
}

/// The commit ONE fire books against the exposure rail: `size_shares *
/// limit_px` where the order is `clip_usd / limit_px` shares. Since the
/// 2026-08-10 sizing fix that is the CLIP, flat — the same at every ask in the
/// band, which is the point of sizing off the limit. Every headroom probe in
/// this file derives from it rather than a literal.
const COMMIT_USD: f64 = 500.0 * 0.02;

/// "Other" exposure the veto fixture pre-books, chosen to leave LESS headroom
/// than one fire's commit.
const OTHER_EXPOSURE_USD: f64 = 995.0;

/// Six 0.1 BTC taker-BUY prints inside second `t`, 10ms apart: 0.6 BTC of
/// trailing-1s volume, 100% leaning toward an UP token's recovery — over both
/// halves of the §3.5 gate.
fn send_recovery_burst(tx: &broadcast::Sender<SpotTrade>, slot_open: u64, t: usize) {
    send_burst(tx, slot_open, t, false)
}

/// The mirror image: taker SELLs, which push an UP token further behind.
fn send_adverse_burst(tx: &broadcast::Sender<SpotTrade>, slot_open: u64, t: usize) {
    send_burst(tx, slot_open, t, true)
}

fn send_burst(tx: &broadcast::Sender<SpotTrade>, slot_open: u64, t: usize, buyer_is_maker: bool) {
    for i in 0..6u64 {
        let ms = (slot_open + t as u64) * 1000 + i * 10;
        tx.send(SpotTrade {
            coin: Coin::Btc,
            price: P0,
            qty: 0.1,
            buyer_is_maker,
            exch_ts_ms: ms,
            recv_ts_us: ms * 1_000,
        })
        .expect("trade send");
    }
}

/// A `slot_open` anchored to real current time. Task 13 re-review
/// Important 2: `drive_slot` now returns immediately once
/// `slot_open + hard_deadline_secs <= now` (an already-expired slot has
/// nothing left to do — see `HARD_DEADLINE_SECS`'s doc in `slot_driver`),
/// so every test that exercises the driver's normal lifecycle needs a
/// `slot_open` close to real "now"; the frozen constant this file used to
/// share (`SLOT_OPEN = 1_784_000_000`, ~9 days in the past by the time of
/// the re-review) would trip that early return and turn every one of these
/// tests into an instant no-op. Call this ONCE per test into a local
/// `let so = fresh_slot_open();` and thread `so` through every helper below
/// — calling it again mid-test risks a (rare, but real) drift if a
/// wall-clock second boundary is crossed between calls.
fn fresh_slot_open() -> u64 {
    now_micros() / 1_000_000
}

/// The ONE exception: `a_fill_before_terminal_hands_off_to_settle` needs a
/// `slot_open` anchored in the real PAST (so `SettleLane`'s own
/// eligibility window — 330s past `slot_open` — already holds on its very
/// first poll tick), which is the opposite of what `fresh_slot_open()`
/// gives every other test. That test pairs this with
/// `cfg_with_hard_deadline`'s large override so the same real-time gap that
/// satisfies settle eligibility doesn't also trip `drive_slot`'s own
/// already-expired early return (Task 13 re-review Important 2).
const PAST_SLOT_OPEN: u64 = 1_784_000_000;

/// A plain drifting close series. Klines gate NOTHING under this spec (§4.1)
/// — they only feed `Feat` telemetry — so the load-bearing earlier fixture
/// (a $50 deficit closing $20 over three bars, with a ±$4 wobble tuned to put
/// zD under Cap20's ceiling) has no job left to do here.
fn close_at(i: usize, _t: usize) -> f64 {
    P0 - 50.0 + if i % 2 == 1 { 4.0 } else { 0.0 }
}

fn kline_at(slot_open: u64, t: usize, close_px: f64) -> SpotClose1s {
    SpotClose1s { coin: Coin::Btc, close_px, close_ts_ms: (slot_open + t as u64) * 1000 + 999 }
}

/// Sends the closes for `0..=t`. Nothing arms — this exists so `Feat` rows
/// and the engine's kline bookkeeping are exercised alongside the fire path.
fn send_klines(tx: &broadcast::Sender<SpotClose1s>, slot_open: u64, t: usize) {
    for i in 0..=t {
        tx.send(kline_at(slot_open, i, close_at(i, t))).expect("kline send");
    }
}

/// A `BookUpdate` whose `ts_micros` maps (via `book_to_t`) to second `t`,
/// `frac_of_second` (0.0..1.0) into it — lets a test construct two updates
/// mapping to the SAME `t` (the dedupe-bucket scenario) with distinct stamps.
fn book_update_at(slot_open: u64, t: usize, side: Side, frac_of_second: f64) -> BookUpdate {
    let ts_micros = (slot_open + t as u64) * 1_000_000 + (frac_of_second * 1_000_000.0) as u64;
    BookUpdate { slug: slug(slot_open), side, best_bid: None, best_ask: Some(0.01), ts_micros }
}

fn slug(slot_open: u64) -> String {
    format!("btc-updown-5m-{slot_open}")
}

fn ctx(slot_open: u64) -> SlotCtx {
    SlotCtx {
        slug: slug(slot_open),
        slot_open,
        token_up: "111".into(),
        token_dn: "222".into(),
        condition_id: "0xabc".into(),
    }
}

/// Wide-open health windows so the health tick never disarms anything during
/// a fast test — these tests are about the money/action path, not feed
/// health.
fn cfg(fak_grace_ms: u64) -> DriverCfg {
    cfg_with_hard_deadline(fak_grace_ms, HARD_DEADLINE_SECS)
}

/// Task 13 re-review Important 2: lets a test widen the hard-deadline
/// budget independently of `fak_grace_ms` and of the production default —
/// see `PAST_SLOT_OPEN`'s doc for the one test that needs it.
fn cfg_with_hard_deadline(fak_grace_ms: u64, hard_deadline_secs: u64) -> DriverCfg {
    DriverCfg {
        engine: engine_cfg(),
        book_stale_disarm_ms: 3_600_000,
        fak_grace_ms,
        hard_deadline_secs,
    }
}

/// Task 13 re-review Important 1 (mutation-verify round): lets a test
/// override `book_stale_disarm_ms` independently of the wide-open default
/// `cfg(...)` uses (3_600_000 on both health knobs, so `unhealthy` is false
/// on every tick in every other test — the exact gap this pass closes). See
/// the two `feed_unhealthy` tests below for why `0` is the right value: the
/// health ticker's staleness math reads real `SystemTime` (`now_ms()`), which
/// `tokio::time::pause` does not control, so a fixed nonzero threshold can't
/// be reliably breached under a paused clock — but at `0` the `match
/// last_book_ms` arm alone decides `book_stale`, with no elapsed-time race at
/// all (`None` needs no real time to pass; `Some` needs only >=1ms, which
/// normal test execution trivially clears).
fn cfg_with_book_stale(fak_grace_ms: u64, book_stale_disarm_ms: u64) -> DriverCfg {
    DriverCfg {
        engine: engine_cfg(),
        book_stale_disarm_ms,
        fak_grace_ms,
        hard_deadline_secs: HARD_DEADLINE_SECS,
    }
}

/// The shipped gate (spec §3), with the held-book bound wide enough that a
/// test's book event stays actionable for the whole test.
fn engine_cfg() -> EngineCfg {
    EngineCfg {
        ask_price_min: 0.009,
        ask_price_max: 0.011,
        limit_price: 0.02,
        clip_usd: 10.0,
        tick: Some(TickCfg {
            v_btc: 0.5,
            lean_min: 0.6,
            window_ms: 1_000,
            max_book_age_ms: 15_000,
        }),
    }
}

fn rails_with(cfg_overrides: StrategySection) -> Arc<Rails> {
    Arc::new(Rails::new(&cfg_overrides))
}

fn default_strategy_cfg() -> StrategySection {
    StrategySection {
        concurrent_exposure_cap_usd: 1_000.0,
        daily_spend_cap_usd: 10_000.0,
        realized_loss_kill_usd: 5_000.0,
        self_fire_rate_per_min: 1_000,
        ..StrategySection::default()
    }
}

/// `fetch_resolved` never resolves — these tests don't exercise settlement,
/// only the exposure-commit path, so what `SettleLane::run` would do with a
/// tracked position never matters here (and `run` is never even spawned).
struct NeverResolvedFetch;
impl MarketFetch for NeverResolvedFetch {
    async fn fetch_resolved(&self, _condition_id: &str) -> anyhow::Result<Option<Side>> {
        Ok(None)
    }
}

fn settle_lane(rails: Arc<Rails>, tele_path: &std::path::Path) -> Arc<SettleLane<NeverResolvedFetch>> {
    let tele = Writer::new(tele_path).unwrap();
    Arc::new(SettleLane::new(NeverResolvedFetch, rails, tele))
}

/// Populates the registry with the ladder spec §3.4 permits an entry against:
/// a standing 1c ask, with depth behind it. The DISPLAYED size is deliberately
/// smaller than the clip's 1,000 shares — v0 is depth-blind (sizing.rs), and
/// pinning that here means a re-introduced depth-aware walk cannot slip in
/// unnoticed.
fn seed_cheap_ask(reg: &BookRegistry, slot_open: u64) {
    reg.update(
        &slug(slot_open),
        Side::Up,
        BookSnapshot {
            bids: vec![],
            asks: vec![Level { price: 0.01, size: 400.0 }, Level { price: 0.18, size: 1000.0 }],
            ts_micros: now_micros(),
        },
    );
}

async fn read_telemetry(path: &std::path::Path) -> Vec<serde_json::Value> {
    // Under `start_paused = true` this sleep is virtual-clock time: it lets
    // the runtime run every other ready task (the driver, and the writer's
    // own spawned task) to quiescence before returning, per Task 12's
    // guidance (`sleep`, never a raw `time::advance()`).
    tokio::time::sleep(Duration::from_millis(200)).await;
    let contents = std::fs::read_to_string(path).unwrap_or_default();
    contents
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn count_kind(rows: &[serde_json::Value], kind: &str) -> usize {
    rows.iter().filter(|r| r["kind"] == kind).count()
}

#[tokio::test(start_paused = true)]
async fn fire_produces_exactly_one_shadow_post_from_the_action_values() {
    let dir = tempfile::tempdir().unwrap();
    let so = fresh_slot_open();
    let tele_path = dir.path().join("telemetry.jsonl");
    let venue = Arc::new(ShadowVenue::new());
    let reg = Arc::new(BookRegistry::new());
    let rails = rails_with(default_strategy_cfg());
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();

    seed_cheap_ask(&reg, so);
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });
    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (book_tx, book_rx) = broadcast::channel(64);
    let (trade_tx, trade_rx) = broadcast::channel(1024);
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn(drive_slot(
        ctx(so), P0, cfg(500), deps, feeds(kline_rx, book_rx, trade_rx), shutdown.clone(),
    ));

    send_klines(&kline_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    book_tx.send(book_update_at(so, 200, Side::Up, 0.5)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    send_recovery_burst(&trade_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let posts = venue.posts();
    assert_eq!(posts.len(), 1, "expected exactly one FAK post");
    let p = &posts[0];
    assert_eq!(p.side, Side::Up);
    assert_eq!(p.token_id, "111");
    assert!(
        (p.limit_px - 0.02).abs() < 1e-9,
        "limit_px must be the configured 2c FAK limit (spec §3.4), got {}", p.limit_px
    );
    assert!(
        (p.size_shares - 500.0).abs() < 1e-6,
        "size_shares must be the clip at the FAK limit ($10 / 2c), and must NOT be cut to \
         the 400 shares displayed — v0 is depth-blind, got {}", p.size_shares
    );
    assert!(
        (p.size_shares * p.limit_px - 10.0).abs() < 1e-9,
        "and the post must commit exactly the $10 clip, got ${}", p.size_shares * p.limit_px
    );

    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let rows = read_telemetry(&tele_path).await;
    let fire = rows.iter().find(|r| r["kind"] == "fire").expect("a fire row");
    assert!(
        (fire["commit_usd"].as_f64().unwrap() - COMMIT_USD).abs() < 1e-9,
        "commit_usd must be size_shares*limit_px ({COMMIT_USD}), got {}", fire["commit_usd"]
    );
    assert!(
        (fire["clip_usd"].as_f64().unwrap() - 10.0).abs() < 1e-9,
        "clip_usd must stay the $10 budget, not be aliased to the commit"
    );
    assert!((fire["best_ask"].as_f64().unwrap() - 0.01).abs() < 1e-9);
    assert_eq!(fire["card"], "burst");
    assert_eq!(count_kind(&rows, "order_ack"), 1);
}

#[tokio::test(start_paused = true)]
async fn rail_veto_from_the_real_commit_produces_zero_posts_and_one_rail_trip() {
    // per_order_cap_usd is its own field (default $100), comfortably above the
    // $10 commit, so it never masks the concurrent-exposure check below.
    // Pre-book $995 of "other" exposure, leaving $5 of headroom — less than
    // one fire's commit, so the concurrent rail must veto.
    //
    // NOTE (2026-08-10): this fixture used to discriminate "the driver passed
    // the CLIP where it should have passed the COMMIT" by leaving headroom
    // between the two ($10 vs $20). Sizing off the limit collapses that gap by
    // construction — commit == clip now, algebraically, for every config — so
    // no fixture can separate them any more. What is still worth pinning, and
    // is pinned here, is that a fire that does not fit is vetoed, emits
    // exactly one RailTrip carrying the rail's own value/threshold, and books
    // no exposure.
    let dir = tempfile::tempdir().unwrap();
    let so = fresh_slot_open();
    let tele_path = dir.path().join("telemetry.jsonl");
    let venue = Arc::new(ShadowVenue::new());
    let reg = Arc::new(BookRegistry::new());
    let rails = rails_with(StrategySection {
        concurrent_exposure_cap_usd: 1_000.0,
        daily_spend_cap_usd: 10_000.0,
        realized_loss_kill_usd: 5_000.0,
        self_fire_rate_per_min: 1_000,
        ..StrategySection::default()
    });
    rails.record_send(OTHER_EXPOSURE_USD, 1_000);
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();

    seed_cheap_ask(&reg, so);
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });
    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (book_tx, book_rx) = broadcast::channel(64);
    let (trade_tx, trade_rx) = broadcast::channel(1024);
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn(drive_slot(
        ctx(so), P0, cfg(500), deps, feeds(kline_rx, book_rx, trade_rx), shutdown.clone(),
    ));

    send_klines(&kline_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    book_tx.send(book_update_at(so, 200, Side::Up, 0.5)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    send_recovery_burst(&trade_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(venue.posts().is_empty(), "a vetoed fire must never reach the venue");

    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let rows = read_telemetry(&tele_path).await;
    assert_eq!(count_kind(&rows, "rail_trip"), 1, "exactly one RailTrip row");
    assert_eq!(count_kind(&rows, "fire"), 0, "a vetoed fire must not also emit a Fire row");
    let trip = rows.iter().find(|r| r["kind"] == "rail_trip").unwrap();
    assert_eq!(trip["rail"], "concurrent_exposure");
    assert!((trip["commit_usd"].as_f64().unwrap() - COMMIT_USD).abs() < 1e-9);
    assert_eq!(trip["side"], "up");
    assert_eq!(trip["card"], "burst");
    // The rail reports its own compared value and threshold — concurrent_usd
    // (995) + commit_usd (10) = 1005, against the configured cap (1000).
    assert!(
        (trip["value"].as_f64().unwrap() - (OTHER_EXPOSURE_USD + COMMIT_USD)).abs() < 1e-9,
        "value must be concurrent_usd + commit_usd, got {}", trip["value"]
    );
    assert_eq!(trip["threshold"], 1000.0, "threshold must be the rail's own concurrent_exposure_cap_usd");

    // Exposure was never booked for the vetoed fire: only the $995 "other"
    // position remains, so exactly $5 of headroom is left (per_order_cap
    // $100 is not the binding constraint here).
    assert!(rails.check_fire(5.0, 2_000).is_ok());
    assert_eq!(
        rails.check_fire(5.01, 2_000).unwrap_err().veto,
        onecentbot::risk::rails::RailVeto::ConcurrentExposure
    );
}

#[tokio::test(start_paused = true)]
async fn the_placebo_mirror_logs_an_adverse_burst_and_never_posts() {
    let dir = tempfile::tempdir().unwrap();
    let so = fresh_slot_open();
    let tele_path = dir.path().join("telemetry.jsonl");
    let venue = Arc::new(ShadowVenue::new());
    let reg = Arc::new(BookRegistry::new());
    let rails = rails_with(default_strategy_cfg());
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();

    seed_cheap_ask(&reg, so);
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });
    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (book_tx, book_rx) = broadcast::channel(64);
    let (trade_tx, trade_rx) = broadcast::channel(1024);
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn(drive_slot(
        ctx(so), P0, cfg(500), deps, feeds(kline_rx, book_rx, trade_rx), shutdown.clone(),
    ));

    send_klines(&kline_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    book_tx.send(book_update_at(so, 200, Side::Up, 0.5)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    // Taker SELLs against a cheap UP ask: the deficit-WIDENING mirror.
    send_adverse_burst(&trade_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(venue.posts().is_empty(), "the placebo mirror must never touch the venue");

    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let rows = read_telemetry(&tele_path).await;
    assert_eq!(count_kind(&rows, "placebo"), 1, "expected exactly one mirror row");
    assert_eq!(count_kind(&rows, "fire"), 0);
    assert_eq!(count_kind(&rows, "rail_trip"), 0);
    let p = rows.iter().find(|r| r["kind"] == "placebo").unwrap();
    assert_eq!(p["side"], "up");
    // The FIFTH print is the one that reaches 0.5 BTC, so the mirror fires
    // there — the row reports the burst it actually saw, not the whole six.
    assert!((p["vol_btc"].as_f64().unwrap() - 0.5).abs() < 1e-9, "got {}", p["vol_btc"]);
    assert!((p["lean"].as_f64().unwrap() - 0.0).abs() < 1e-9, "no recovery flow at all");
}

/// Spec §3.6: ONE shot per slot, and no re-arm. A second qualifying burst,
/// seconds later on a still-standing 1c ask, must not produce a second order —
/// the earlier answer here was a per-card `deduped_fire` row inside one
/// second, a row kind schema 4 no longer has.
#[tokio::test(start_paused = true)]
async fn a_second_qualifying_burst_in_the_same_slot_never_posts_again() {
    let dir = tempfile::tempdir().unwrap();
    let so = fresh_slot_open();
    let tele_path = dir.path().join("telemetry.jsonl");
    let venue = Arc::new(ShadowVenue::new());
    let reg = Arc::new(BookRegistry::new());
    let rails = rails_with(default_strategy_cfg());
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();

    seed_cheap_ask(&reg, so);
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });
    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (book_tx, book_rx) = broadcast::channel(64);
    let (trade_tx, trade_rx) = broadcast::channel(1024);
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn(drive_slot(
        ctx(so), P0, cfg(500), deps, feeds(kline_rx, book_rx, trade_rx), shutdown.clone(),
    ));

    send_klines(&kline_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    book_tx.send(book_update_at(so, 200, Side::Up, 0.5)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    send_recovery_burst(&trade_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(20)).await;
    // A fresh book event (the 1c ask is still standing) and a second, later
    // burst: nothing may fire again for the rest of the slot.
    book_tx.send(book_update_at(so, 240, Side::Up, 0.2)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    send_recovery_burst(&trade_tx, so, 240);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(venue.posts().len(), 1, "one shot per slot, no re-arm");

    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let rows = read_telemetry(&tele_path).await;
    assert_eq!(count_kind(&rows, "fire"), 1);
}

/// Returns `Err` on every send — the outcome `ShadowVenue` cannot express,
/// needed to prove the driver never retries a failed POST (§13.5).
struct FlakyVenue {
    calls: Arc<AtomicU32>,
    events_tx: broadcast::Sender<VenueEvent>,
}

impl FlakyVenue {
    fn new() -> Self {
        let (events_tx, _) = broadcast::channel(64);
        Self { calls: Arc::new(AtomicU32::new(0)), events_tx }
    }
}

impl Venue for FlakyVenue {
    fn send_fak(&self, _req: FakRequest) -> impl Future<Output = anyhow::Result<FakOutcome>> + Send {
        self.calls.fetch_add(1, Ordering::SeqCst);
        async { Err(anyhow::anyhow!("simulated POST failure")) }
    }

    fn events_rx(&self) -> broadcast::Receiver<VenueEvent> {
        self.events_tx.subscribe()
    }
}

/// Pre-books $950 of "other" exposure so the release is visible above
/// `release_exposure`'s `.max(0.0)` floor, then probes both sides of the exact
/// resulting boundary — a boundary probe that sat inside another cap would be
/// vacuous.
#[tokio::test(start_paused = true)]
async fn post_failed_is_attempted_exactly_once_and_never_retried() {
    let dir = tempfile::tempdir().unwrap();
    let so = fresh_slot_open();
    let tele_path = dir.path().join("telemetry.jsonl");
    let venue = Arc::new(FlakyVenue::new());
    let calls = venue.calls.clone();
    let reg = Arc::new(BookRegistry::new());
    let rails = rails_with(default_strategy_cfg());
    rails.record_send(950.0, 1_000);
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();

    seed_cheap_ask(&reg, so);
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });
    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (book_tx, book_rx) = broadcast::channel(64);
    let (trade_tx, trade_rx) = broadcast::channel(1024);
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn(drive_slot(
        ctx(so), P0, cfg(500), deps, feeds(kline_rx, book_rx, trade_rx), shutdown.clone(),
    ));

    send_klines(&kline_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    book_tx.send(book_update_at(so, 200, Side::Up, 0.5)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    send_recovery_burst(&trade_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(calls.load(Ordering::SeqCst), 1, "must attempt the POST exactly once");

    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let rows = read_telemetry(&tele_path).await;
    assert_eq!(count_kind(&rows, "miss"), 1);
    let miss = rows.iter().find(|r| r["kind"] == "miss").unwrap();
    assert_eq!(miss["reason"], "post_failed");

    // The commit was reconciled back to zero — never retried, and never left
    // stuck reserving exposure it never actually used. Headroom after the
    // single correct reconcile: cap(1000) - other(950) - this(0, fully
    // released) = 50 exactly.
    assert!(rails.check_fire(50.0, 2_000).is_ok(), "expected exactly $50 of headroom after the reconcile");
    assert_eq!(
        rails.check_fire(50.01, 2_000).unwrap_err().veto,
        onecentbot::risk::rails::RailVeto::ConcurrentExposure,
        "a skipped reconcile would leave only $30 of headroom (970 booked), vetoing this probe"
    );
}

/// Acks every send with a fixed order id, and never emits any `VenueEvent`
/// on its own — the test drives fill/terminal notifications manually via the
/// held `events_tx` handle, needed to exercise the grace-expiry ->
/// late-notification race that `ShadowVenue` (never emits any VenueEvent at
/// all) can't reach in bounded time.
struct AckOnlyVenue {
    order_id: String,
    events_tx: broadcast::Sender<VenueEvent>,
}

impl Venue for AckOnlyVenue {
    fn send_fak(&self, _req: FakRequest) -> impl Future<Output = anyhow::Result<FakOutcome>> + Send {
        let order_id = self.order_id.clone();
        async move { Ok(FakOutcome::Acked { order_id }) }
    }

    fn events_rx(&self) -> broadcast::Receiver<VenueEvent> {
        self.events_tx.subscribe()
    }
}

/// Task 13 resolution 6: `rails.reconcile` must run EXACTLY once per fire.
/// Drives a fire to Acked, lets `fak_grace_ms` expire with no notification
/// (the first reconcile, via the grace sweep), then delivers a LATE `Fill`
/// for the same order and proves it is observed (telemetry, `attributable:
/// false`) but does NOT reconcile a second time (the concurrent-exposure
/// headroom is bit-for-bit unchanged).
#[tokio::test(start_paused = true)]
async fn a_late_fill_after_grace_expiry_does_not_reconcile_a_second_time() {
    let dir = tempfile::tempdir().unwrap();
    let so = fresh_slot_open();
    let tele_path = dir.path().join("telemetry.jsonl");
    let (events_tx, _) = broadcast::channel(64);
    let venue = Arc::new(AckOnlyVenue { order_id: "order-1".into(), events_tx: events_tx.clone() });
    let reg = Arc::new(BookRegistry::new());
    // per_order_cap_usd is its own field (default $100), comfortably above the
    // $20 commit and every probe below, so only the CONCURRENT check is ever
    // exercised. $950 of "other" exposure is pre-booked so a phantom
    // double-release (which would free a further $20 - $0.10 = $19.90) shows
    // up as ADDITIONAL headroom rather than being swallowed by
    // release_exposure's 0.0 floor.
    let rails = rails_with(StrategySection {
        concurrent_exposure_cap_usd: 1_000.0,
        daily_spend_cap_usd: 10_000.0,
        realized_loss_kill_usd: 5_000.0,
        self_fire_rate_per_min: 1_000,
        ..StrategySection::default()
    });
    rails.record_send(950.0, 1_000);
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();

    seed_cheap_ask(&reg, so);
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });
    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (book_tx, book_rx) = broadcast::channel(64);
    let (trade_tx, trade_rx) = broadcast::channel(1024);
    let shutdown = CancellationToken::new();
    let fak_grace_ms = 500;

    let handle = tokio::spawn(drive_slot(
        ctx(so), P0, cfg(fak_grace_ms), deps, feeds(kline_rx, book_rx, trade_rx), shutdown.clone(),
    ));

    send_klines(&kline_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    book_tx.send(book_update_at(so, 200, Side::Up, 0.5)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    send_recovery_burst(&trade_tx, so, 200);
    // Let the fire go out (Acked) and the grace window fully expire, plus a
    // couple of health-tick cycles to guarantee the sweep has run.
    tokio::time::sleep(Duration::from_millis(fak_grace_ms + 3 * 250)).await;

    let rows = read_telemetry(&tele_path).await;
    assert_eq!(count_kind(&rows, "order_ack"), 1);
    assert_eq!(count_kind(&rows, "miss"), 1, "grace expiry must produce exactly one Miss");
    let miss = rows.iter().find(|r| r["kind"] == "miss").unwrap();
    assert_eq!(miss["reason"], "grace_expired");
    assert_eq!(count_kind(&rows, "fill"), 0, "no fill observed yet");

    // Headroom after the single correct reconcile: cap(1000) - other(950) -
    // this(0, fully released) = 50 exactly.
    assert!(rails.check_fire(50.0, 3_000).is_ok(), "expected exactly $50 of headroom after the single reconcile");
    assert_eq!(
        rails.check_fire(50.01, 3_000).unwrap_err().veto,
        onecentbot::risk::rails::RailVeto::ConcurrentExposure
    );

    // A late fill arrives for the SAME order, well after grace expiry.
    events_tx
        .send(VenueEvent::Fill(onecentbot::execution::venue::VenueFill {
            order_id: "order-1".into(),
            shares: 10.0,
            price: 0.01,
            venue_ts_ms: None,
        }))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let rows = read_telemetry(&tele_path).await;
    assert_eq!(count_kind(&rows, "miss"), 1, "the late fill must not produce a second Miss");
    assert_eq!(count_kind(&rows, "fill"), 1, "the late fill must still be observed/logged");
    let fill = rows.iter().find(|r| r["kind"] == "fill").unwrap();
    assert_eq!(fill["attributable"], false, "a fill after reconcile must be marked unattributable, not silently re-booked");

    // Headroom is BIT-FOR-BIT UNCHANGED — a double-reconcile would have
    // released a further $19.90 (20.0 - 10*0.01), moving the boundary to
    // $69.90 and making this probe wrongly pass.
    assert!(rails.check_fire(50.0, 4_000).is_ok());
    assert_eq!(
        rails.check_fire(50.01, 4_000).unwrap_err().veto,
        onecentbot::risk::rails::RailVeto::ConcurrentExposure
    );
}

/// Task 13 review B3: a replayed kline second (a reconnect burst re-sending
/// a `t` the engine already has) must produce AT MOST one `Feat` row for
/// that `t` — gating on `engine.last_kline_t()` rather than a driver-local
/// mirror of `SlotSeries::push`'s advance contract. Before this fix the gap
/// wasn't in the mirror's LOGIC (the reviewer traced both sides and
/// confirmed it can't diverge today) but in TEST COVERAGE: nothing exercised
/// this path at all. Re-introducing a driver-local mirror that ignores
/// contiguity (i.e. emitting `Feat` on every kline event whose `t` falls in
/// the window, regardless of whether it advanced the engine) must fail this
/// test with a second, mismatched `t=198` row.
#[tokio::test(start_paused = true)]
async fn a_replayed_kline_second_does_not_duplicate_its_feat_row() {
    let dir = tempfile::tempdir().unwrap();
    let so = fresh_slot_open();
    let tele_path = dir.path().join("telemetry.jsonl");
    let venue = Arc::new(ShadowVenue::new());
    let reg = Arc::new(BookRegistry::new());
    let rails = rails_with(default_strategy_cfg());
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });

    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (_book_tx, book_rx) = broadcast::channel::<BookUpdate>(64);
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn(drive_slot(ctx(so), P0, cfg(500), deps, no_trade_feeds(kline_rx, book_rx), shutdown.clone()));

    send_klines(&kline_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    // A reconnect replay burst re-delivering a second the engine already
    // has (§13.3, design §4.5): must be a no-op for `Feat` emission, not a
    // second row keyed `t=198` but paired with a LATER second's features.
    kline_tx.send(kline_at(so, 198, close_at(198, 200))).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let rows = read_telemetry(&tele_path).await;
    // One row per SIDE is correct (Feat is emitted for both Up and Down on
    // every advancing second) — the bug this test guards against is a
    // SECOND row for the SAME (t, side), not the normal two-sides-per-t
    // shape.
    let feat_198_up: Vec<_> =
        rows.iter().filter(|r| r["kind"] == "feat" && r["t"] == 198 && r["side"] == "up").collect();
    assert_eq!(
        feat_198_up.len(), 1,
        "a replayed kline second must not duplicate its Feat row, got {feat_198_up:?}"
    );
}

/// Task 13 review B1: the driver must exit ON ITS OWN once it has seen the
/// LAST valid in-slot second (§13.6's `t >= 300` termination), not rely
/// solely on the ~330s hard-deadline backstop (which exists for the
/// feed-goes-silent case, not the normal path). Discriminated by ELAPSED
/// VIRTUAL TIME: processing 300 buffered klines needs no timer at all, so a
/// correct exit returns near-instantly; falling through to the hard
/// deadline would require ~330s of (auto-advanced, but still real) virtual
/// time to elapse.
#[tokio::test(start_paused = true)]
async fn the_driver_exits_on_its_own_once_it_has_seen_the_last_in_slot_second() {
    let dir = tempfile::tempdir().unwrap();
    let so = fresh_slot_open();
    let tele_path = dir.path().join("telemetry.jsonl");
    let venue = Arc::new(ShadowVenue::new());
    let reg = Arc::new(BookRegistry::new());
    let rails = rails_with(default_strategy_cfg());
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });

    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (_book_tx, book_rx) = broadcast::channel::<BookUpdate>(64);
    // No shutdown.cancel() anywhere in this test — the driver must return
    // on its own.
    let shutdown = CancellationToken::new();

    let t0 = tokio::time::Instant::now();
    let handle = tokio::spawn(drive_slot(ctx(so), P0, cfg(500), deps, no_trade_feeds(kline_rx, book_rx), shutdown.clone()));

    send_klines(&kline_tx, so, 299); // SLOT_SECS - 1, the last valid in-slot second
    handle.await.unwrap().unwrap();
    let elapsed = tokio::time::Instant::now() - t0;
    assert!(
        elapsed < Duration::from_secs(60),
        "must exit at t=299 without waiting out the ~330s hard-deadline backstop, elapsed={elapsed:?}"
    );

    // No trade feed at all here, so nothing can fire (spec §6) and the
    // elapsed-time assertion above is the one that actually distinguishes
    // "exited at t=299" from "fell through to the ~330s hard-deadline
    // backstop". Confirm the kline pipeline itself ran to completion via the
    // `Feat` rows it produces every advancing second from t=145.
    let rows = read_telemetry(&tele_path).await;
    assert!(
        rows.iter().any(|r| r["kind"] == "feat" && r["t"] == 299),
        "the kline pipeline must have run all the way to the last in-slot second"
    );
}

/// §13.5: zero-fills every send — the outcome `ShadowVenue` (always Acks)
/// can never reach, needed to exercise the driver's `Ok(FakOutcome::
/// ZeroFill{..})` arm at all (Task 13 review C2).
struct ZeroFillVenue {
    calls: Arc<AtomicU32>,
    events_tx: broadcast::Sender<VenueEvent>,
}

impl ZeroFillVenue {
    fn new() -> Self {
        let (events_tx, _) = broadcast::channel(64);
        Self { calls: Arc::new(AtomicU32::new(0)), events_tx }
    }
}

impl Venue for ZeroFillVenue {
    fn send_fak(&self, _req: FakRequest) -> impl Future<Output = anyhow::Result<FakOutcome>> + Send {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        async move { Ok(FakOutcome::ZeroFill { order_id: format!("zero-{n}") }) }
    }

    fn events_rx(&self) -> broadcast::Receiver<VenueEvent> {
        self.events_tx.subscribe()
    }
}

/// Task 13 review C2: `slot_driver.rs`'s `ZeroFill` arm (§13.5's "clean,
/// free miss — do NOT re-arm") was entirely uncovered — `ShadowVenue`
/// always Acks and neither `FlakyVenue` nor `AckOnlyVenue` can express it.
/// Surviving mutations this test exists to catch: deleting the `reconcile`
/// call in that arm, changing the `reason` string, and adding a re-arm
/// (caught here as "zero additional posts" — exactly one send, ever, even
/// after the shot is consumed).
#[tokio::test(start_paused = true)]
async fn zero_fill_is_a_clean_miss_with_no_retry_and_no_re_arm() {
    let dir = tempfile::tempdir().unwrap();
    let so = fresh_slot_open();
    let tele_path = dir.path().join("telemetry.jsonl");
    let venue = Arc::new(ZeroFillVenue::new());
    let calls = venue.calls.clone();
    let reg = Arc::new(BookRegistry::new());
    // Same $950-pre-booked pattern as the post-failure reconcile probe: a
    // phantom double-release (or a skipped reconcile) shows up as
    // additional/missing headroom rather than vanishing into
    // release_exposure's 0.0 floor.
    let rails = rails_with(default_strategy_cfg());
    rails.record_send(950.0, 1_000);
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });

    seed_cheap_ask(&reg, so);
    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (book_tx, book_rx) = broadcast::channel(64);
    let (trade_tx, trade_rx) = broadcast::channel(1024);
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn(drive_slot(
        ctx(so), P0, cfg(500), deps, feeds(kline_rx, book_rx, trade_rx), shutdown.clone(),
    ));

    send_klines(&kline_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    book_tx.send(book_update_at(so, 200, Side::Up, 0.5)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    send_recovery_burst(&trade_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;

    shutdown.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 1, "no retry and no re-arm: exactly one send, ever");

    let rows = read_telemetry(&tele_path).await;
    assert_eq!(count_kind(&rows, "miss"), 1, "a ZeroFill must produce exactly one Miss row");
    let miss = rows.iter().find(|r| r["kind"] == "miss").unwrap();
    assert_eq!(miss["reason"], "fak_no_match");
    assert_eq!(count_kind(&rows, "order_ack"), 0, "ZeroFill is not an Ack — no order_ack row");

    // The commit was reconciled back to zero: with $950 "other" exposure
    // pre-booked, headroom is exactly cap(1000) - other(950) = 50.
    assert!(rails.check_fire(50.0, 3_000).is_ok());
    assert_eq!(
        rails.check_fire(50.01, 3_000).unwrap_err().veto,
        onecentbot::risk::rails::RailVeto::ConcurrentExposure
    );
}

/// Always resolves with a fixed winner, counting calls — needed (Task 13
/// review C3) to prove the fill -> `settle.track` handoff actually reaches
/// `SettleLane`, since `NeverResolvedFetch` (used everywhere else in this
/// file) deliberately never spawns `run()` at all.
struct WinsOnResolveFetch {
    winner: Side,
    calls: Arc<AtomicU32>,
}

impl MarketFetch for WinsOnResolveFetch {
    async fn fetch_resolved(&self, _condition_id: &str) -> anyhow::Result<Option<Side>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(self.winner))
    }
}

/// Task 13 review C3: `finalize_order`'s `filled_shares > 0.0` branch
/// (the `settle.track(...)` handoff) was never reached — no existing test
/// delivers a `Fill` before a `Terminal`. Deleting that whole block passes
/// every other test in this file. Extends `AckOnlyVenue`'s pattern to send
/// `Fill` then `Terminal`, then proves BOTH: (1) reconcile at the fill
/// point released only the commit-minus-actual overshoot (the position's
/// `cost_usd` stays booked, not yet released), and (2) the settle lane
/// actually observed the tracked position — proven by spinning up `run()`
/// against a fetch double that resolves immediately and counting its
/// calls, since a lost `track()` leaves the positions list empty forever
/// and `poll_once` has nothing to poll.
///
/// Task 13 re-review Important 2: this is the ONE test in this file that
/// needs a `slot_open` anchored in the real PAST (`PAST_SLOT_OPEN`) rather
/// than `fresh_slot_open()` — `SettleLane`'s own eligibility check (330s
/// past `slot_open`) must already hold on the very first poll tick below.
/// Paired with `cfg_with_hard_deadline`'s large override so `drive_slot`'s
/// own already-expired early return doesn't ALSO trip on that same
/// real-time gap (see `PAST_SLOT_OPEN`'s doc).
#[tokio::test(start_paused = true)]
async fn a_fill_before_terminal_hands_off_to_settle() {
    let dir = tempfile::tempdir().unwrap();
    let so = PAST_SLOT_OPEN;
    let tele_path = dir.path().join("telemetry.jsonl");
    let (events_tx, _) = broadcast::channel(64);
    let venue = Arc::new(AckOnlyVenue { order_id: "order-fill-1".into(), events_tx: events_tx.clone() });
    let reg = Arc::new(BookRegistry::new());
    let rails = rails_with(StrategySection {
        concurrent_exposure_cap_usd: 1_000.0,
        daily_spend_cap_usd: 10_000.0,
        realized_loss_kill_usd: 5_000.0,
        self_fire_rate_per_min: 1_000,
        ..StrategySection::default()
    });
    // $950 of "other" exposure pre-booked — same reasoning as the other
    // reconcile-boundary probes in this file: makes a wrong release amount
    // show up as different headroom instead of vanishing into the 0.0 floor.
    rails.record_send(950.0, 1_000);
    let resolve_calls = Arc::new(AtomicU32::new(0));
    let fetch = WinsOnResolveFetch { winner: Side::Up, calls: resolve_calls.clone() };
    let settle_tele = Writer::new(&dir.path().join("settle_internal.jsonl")).unwrap();
    let settle = Arc::new(SettleLane::new(fetch, rails.clone(), settle_tele));
    let tele = Writer::new(&tele_path).unwrap();
    let deps =
        Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle: settle.clone(), tele });

    seed_cheap_ask(&reg, so);
    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (book_tx, book_rx) = broadcast::channel(64);
    let (trade_tx, trade_rx) = broadcast::channel(1024);
    let shutdown = CancellationToken::new();
    // Wide enough that the grace sweep never fires before the Terminal
    // below arrives — this test is about the Fill-then-Terminal path, not
    // grace expiry. Task 13 re-review Minor 4: kept strictly below
    // `drive_slot`'s own `fak_grace_ms <= 5_000` bound (Important 2) rather
    // than sitting exactly on it — this value has no coupling to that bound
    // at all (it's picked solely to outlast this test's Fill/Terminal
    // timing), so sitting on the boundary was a coincidence that would trip
    // if the bound were ever tightened.
    let fak_grace_ms = 4_000;
    // Task 13 re-review Important 2: `so` (`PAST_SLOT_OPEN`) is far enough
    // in the real past that `so + HARD_DEADLINE_SECS` would already be
    // <= real "now" — the driver's own already-expired early return would
    // otherwise fire before this test gets to exercise anything. A huge
    // override keeps `drive_slot` running normally while `so` itself stays
    // old enough for `SettleLane`'s eligibility check below.
    let huge_hard_deadline_secs = 100 * 365 * 24 * 3_600;

    let handle = tokio::spawn(drive_slot(
        ctx(so), P0, cfg_with_hard_deadline(fak_grace_ms, huge_hard_deadline_secs),
        deps, feeds(kline_rx, book_rx, trade_rx), shutdown.clone(),
    ));

    send_klines(&kline_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    book_tx.send(book_update_at(so, 200, Side::Up, 0.5)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    send_recovery_burst(&trade_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Fill BEFORE Terminal: 400 of the 1,000 fired shares @ 1c, so cost_usd
    // ($4) is strictly less than commit_usd ($20) and the overshoot ($16) is
    // nonzero and distinguishable via headroom. This is also the realistic
    // shape — a FAK against a 400-share displayed level.
    events_tx
        .send(VenueEvent::Fill(onecentbot::execution::venue::VenueFill {
            order_id: "order-fill-1".into(),
            shares: 400.0,
            price: 0.01,
            venue_ts_ms: None,
        }))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    events_tx.send(VenueEvent::Terminal { order_id: "order-fill-1".into() }).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // finalize_order has now run with filled_shares=400 > 0: reconcile
    // released only the commit-vs-actual overshoot; cost_usd stays booked
    // until settlement. Headroom = cap(1000) - other(950) - cost(4).
    let cost_usd = 400.0 * 0.01;
    let expected_headroom = 1_000.0 - 950.0 - cost_usd;
    assert!(rails.check_fire(expected_headroom, 3_000).is_ok());
    assert_eq!(
        rails.check_fire(expected_headroom + 0.01, 3_000).unwrap_err().veto,
        onecentbot::risk::rails::RailVeto::ConcurrentExposure
    );

    shutdown.cancel();
    handle.await.unwrap().unwrap();

    // Drive settlement independently: `so` (`PAST_SLOT_OPEN`) is far enough
    // in the real wall-clock past that `SettleLane`'s eligibility check
    // already holds on the very first poll tick (the same trick settle.rs's
    // own tests use with `slot_open = 1`), and `tokio::time::interval`'s
    // first tick fires immediately.
    let settle_shutdown = CancellationToken::new();
    let settle_handle = tokio::spawn((*settle).clone().run(settle_shutdown.clone()));
    tokio::time::sleep(Duration::from_secs(1)).await;
    settle_shutdown.cancel();
    settle_handle.await.unwrap();

    assert_eq!(
        resolve_calls.load(Ordering::SeqCst), 1,
        "the settle lane must have observed and polled the tracked position — proves settle.track was called"
    );

    // Full release now that settlement has run: cap(1000) - other(950) = 50.
    assert!(rails.check_fire(50.0, 4_000).is_ok());
    assert_eq!(
        rails.check_fire(50.01, 4_000).unwrap_err().veto,
        onecentbot::risk::rails::RailVeto::ConcurrentExposure
    );
}

/// Task 13 re-review Minor 3: `ShadowVenue::prepare_slot` is the `Venue`
/// trait's default no-op (see its doc there) — it records nothing, so
/// `prepare_slot().await` could be hoisted above `drive_slot`'s
/// already-expired early return and every assertion in
/// `an_already_expired_slot_returns_immediately_without_prepare_or_posts`
/// would still pass. This double counts calls so that test can actually pin
/// what its name claims.
struct PrepareCountingVenue {
    inner: ShadowVenue,
    prepared: AtomicU32,
}

impl PrepareCountingVenue {
    fn new() -> Self {
        Self { inner: ShadowVenue::new(), prepared: AtomicU32::new(0) }
    }

    fn posts(&self) -> Vec<FakRequest> {
        self.inner.posts()
    }
}

impl Venue for PrepareCountingVenue {
    fn send_fak(&self, req: FakRequest) -> impl Future<Output = anyhow::Result<FakOutcome>> + Send {
        self.inner.send_fak(req)
    }

    fn events_rx(&self) -> broadcast::Receiver<VenueEvent> {
        self.inner.events_rx()
    }

    fn prepare_slot(&self, slug: &str, token_up: &str, token_dn: &str) -> impl Future<Output = ()> + Send {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        let _ = (slug, token_up, token_dn);
        async {}
    }
}

/// Task 13 re-review Important 2: an already-expired slot
/// (`slot_open + hard_deadline_secs <= now`) has, by construction, nothing
/// left to do. `drive_slot` must return immediately — never call
/// `prepare_slot`, never emit a `SlotOpen` telemetry row, never touch the
/// venue — rather than living out a full `hard_deadline_secs` budget (the
/// pre-fix behavior, which every OTHER test in this file exercised by
/// accident via the old frozen `SLOT_OPEN` constant). Uses `PAST_SLOT_OPEN`
/// with the PRODUCTION `cfg(...)` (not `cfg_with_hard_deadline`'s override)
/// so `PAST_SLOT_OPEN + HARD_DEADLINE_SECS` is guaranteed to already be in
/// the past.
#[tokio::test(start_paused = true)]
async fn an_already_expired_slot_returns_immediately_without_prepare_or_posts() {
    let dir = tempfile::tempdir().unwrap();
    let so = PAST_SLOT_OPEN;
    let tele_path = dir.path().join("telemetry.jsonl");
    let venue = Arc::new(PrepareCountingVenue::new());
    let reg = Arc::new(BookRegistry::new());
    let rails = rails_with(default_strategy_cfg());
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });

    let (_kline_tx, kline_rx) = broadcast::channel::<SpotClose1s>(1024);
    let (_book_tx, book_rx) = broadcast::channel::<BookUpdate>(64);
    // No shutdown.cancel() anywhere — the driver must return on its own,
    // and fast: nothing here ever sends a kline or book event, so if it did
    // NOT take the already-expired early return it would sit in the
    // `select!` loop until the ~330s hard-deadline backstop instead.
    let shutdown = CancellationToken::new();

    let t0 = tokio::time::Instant::now();
    let handle = tokio::spawn(drive_slot(ctx(so), P0, cfg(500), deps, no_trade_feeds(kline_rx, book_rx), shutdown.clone()));
    handle.await.unwrap().unwrap();
    let elapsed = tokio::time::Instant::now() - t0;
    assert!(
        elapsed < Duration::from_secs(1),
        "an already-expired slot must return immediately, not live out hard_deadline_secs, elapsed={elapsed:?}"
    );

    assert!(venue.posts().is_empty(), "an already-expired slot must never touch the venue");
    assert_eq!(
        venue.prepared.load(Ordering::SeqCst), 0,
        "an already-expired slot must never call prepare_slot"
    );
    let rows = read_telemetry(&tele_path).await;
    assert!(
        rows.is_empty(),
        "an already-expired slot must do nothing at all — not even emit SlotOpen, got {rows:?}"
    );
}

/// The health ticker's `Event::FeedUnhealthy` arm: deleting the
/// `if unhealthy { engine.on_event(Event::FeedUnhealthy) }` block passes every
/// other test in this crate (nothing else drives `unhealthy` true).
///
/// Under POST-TWAP-SPEC §6 the observable consequence changed with the state
/// machine: there are no armed cards left to disarm, so what a stale book now
/// costs is the HELD LADDER — the engine drops both sides' books, and a
/// qualifying burst that would otherwise have fired finds nothing to buy
/// against. Registry-seeds an OLD `ts_micros` so `last_book_ms` is `Some` from
/// the very first tick, with `book_stale_disarm_ms: 0` so the `Some` arm's
/// `now.saturating_sub(ms) > 0` needs only the real (non-virtual) wall-clock
/// time this test's own execution already spends.
///
/// Also re-asserts the process-global half: a re-added
/// `rails.set_feed_healthy(!unhealthy)` in the ticker would flip the
/// PROCESS-GLOBAL rail to `false` here, vetoing every other concurrently-
/// driven slot's fires — `check_fire` must stay `Ok` throughout.
#[tokio::test(start_paused = true)]
async fn a_stale_book_drops_the_held_ladder_without_touching_the_global_rail() {
    let dir = tempfile::tempdir().unwrap();
    let so = fresh_slot_open();
    let tele_path = dir.path().join("telemetry.jsonl");
    let venue = Arc::new(ShadowVenue::new());
    let reg = Arc::new(BookRegistry::new());
    let rails = rails_with(default_strategy_cfg());
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();

    seed_cheap_ask(&reg, so);
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });
    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (book_tx, book_rx) = broadcast::channel(64);
    let (trade_tx, trade_rx) = broadcast::channel(1024);
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn(drive_slot(
        ctx(so), P0, cfg_with_book_stale(500, 0), deps, feeds(kline_rx, book_rx, trade_rx),
        shutdown.clone(),
    ));

    send_klines(&kline_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    // The engine now HOLDS a 1c ladder — and the driver stamps `last_book_ms`
    // from the REAL clock as it processes this event.
    book_tx.send(book_update_at(so, 200, Side::Up, 0.5)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    // The health ticker's staleness math reads real `SystemTime` (`now_ms()`),
    // which `tokio::time::pause` does NOT control: the virtual sleep below
    // advances the ticker without advancing the clock it compares against, so
    // `now - last_book_ms > 0` would be a race on whether a real millisecond
    // happened to elapse. Burn real time explicitly — this is the only kind
    // the ticker can see, and 5ms of it makes the next tick's comparison
    // deterministic rather than usually-true.
    std::thread::sleep(Duration::from_millis(5));
    // Several health ticks at a zero staleness threshold now take the ladder.
    tokio::time::sleep(Duration::from_millis(3 * 250 + 50)).await;
    send_recovery_burst(&trade_tx, so, 210);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        venue.posts().is_empty(),
        "a burst against a book the feed-health ticker dropped must not reach the venue"
    );
    assert!(
        rails.check_fire(1.0, 0).is_ok(),
        "a per-slot FeedUnhealthy must not flip the process-global feed_healthy rail"
    );

    shutdown.cancel();
    handle.await.unwrap().unwrap();
}

/// The control for the test above, and the reason it is not vacuous: the SAME
/// sequence with a wide `book_stale_disarm_ms` DOES fire. Without this, a
/// mutation that simply broke the fire path would leave the stale-book test
/// green.
#[tokio::test(start_paused = true)]
async fn the_same_sequence_fires_when_the_book_is_not_stale() {
    let dir = tempfile::tempdir().unwrap();
    let so = fresh_slot_open();
    let tele_path = dir.path().join("telemetry.jsonl");
    let venue = Arc::new(ShadowVenue::new());
    let reg = Arc::new(BookRegistry::new());
    let rails = rails_with(default_strategy_cfg());
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();

    seed_cheap_ask(&reg, so);
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });
    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (book_tx, book_rx) = broadcast::channel(64);
    let (trade_tx, trade_rx) = broadcast::channel(1024);
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn(drive_slot(
        ctx(so), P0, cfg(500), deps, feeds(kline_rx, book_rx, trade_rx), shutdown.clone(),
    ));

    send_klines(&kline_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    book_tx.send(book_update_at(so, 200, Side::Up, 0.5)).unwrap();
    tokio::time::sleep(Duration::from_millis(3 * 250 + 50)).await;
    send_recovery_burst(&trade_tx, so, 210);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(venue.posts().len(), 1);

    shutdown.cancel();
    handle.await.unwrap().unwrap();
}

/// 2026-07-24 20:35 incident (probe doc §11): a one-tick kline-staleness flap
/// disarmed a card at burst onset, and the re-arm cost half the jackpot. Under
/// this spec klines gate NOTHING (§4.1), which makes the property stronger,
/// not weaker: a fully stalled kline feed must not stop a fire at all. Book
/// health is wide open here, so only a kline-gap term in the ticker could
/// manufacture a veto.
#[tokio::test(start_paused = true)]
async fn a_stalled_kline_feed_does_not_stop_a_fire() {
    let dir = tempfile::tempdir().unwrap();
    let so = fresh_slot_open();
    let tele_path = dir.path().join("telemetry.jsonl");
    let venue = Arc::new(ShadowVenue::new());
    let reg = Arc::new(BookRegistry::new());
    let rails = rails_with(default_strategy_cfg());
    let settle = settle_lane(rails.clone(), &dir.path().join("settle.jsonl"));
    let tele = Writer::new(&tele_path).unwrap();

    seed_cheap_ask(&reg, so);
    let deps = Arc::new(SlotDeps { venue: venue.clone(), reg: reg.clone(), rails: rails.clone(), settle, tele });
    let (kline_tx, kline_rx) = broadcast::channel(1024);
    let (book_tx, book_rx) = broadcast::channel(64);
    let (trade_tx, trade_rx) = broadcast::channel(1024);
    let shutdown = CancellationToken::new();

    let handle = tokio::spawn(drive_slot(
        ctx(so), P0, cfg(500), deps, feeds(kline_rx, book_rx, trade_rx), shutdown.clone(),
    ));

    send_klines(&kline_tx, so, 200);
    tokio::time::sleep(Duration::from_millis(50)).await;
    // The stall: several health-tick cycles with the klines silent.
    tokio::time::sleep(Duration::from_millis(3 * 250 + 50)).await;
    // The burst's cheap ask, one slot-second after the last kline.
    book_tx.send(book_update_at(so, 201, Side::Up, 0.5)).unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    send_recovery_burst(&trade_tx, so, 201);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(venue.posts().len(), 1, "a stalled kline feed must not stop a fire");

    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let rows = read_telemetry(&tele_path).await;
    assert_eq!(count_kind(&rows, "fire"), 1);
}

// RETIRED with the state machine it observed (POST-TWAP-SPEC §6):
// `a_never_seen_book_never_manufactures_feed_unhealthy_from_elapsed_time_alone`
// pinned the health ticker's `None => false` arm by asserting that no armed
// card was disarmed. Nothing arms any more, and the arm's only remaining
// effect — a spurious `Event::FeedUnhealthy` — is a NO-OP when
// `last_book_ms` is `None`, because `None` means no book was ever seen and
// the engine therefore holds no ladder to drop. The property is now
// unobservable at the driver level; `seed_last_book_ms`'s own unit tests in
// `src/slot_driver.rs` still pin the `Some`/`None` distinction directly.
