//! The clob-client boundary that can spend real money (Task 14). Everything
//! else in this crate is pure logic or log-only; every branch here is
//! specified in full because a wrong one costs dollars.
//!
//! **The governing rule (§14.2): unless we can PROVE the order did not land,
//! assume it did.** A retry that double-buys is unrecoverable; a missed fill
//! is free. Exactly ONE response proves a clean zero-fill — everything else
//! is `Err` and NEVER retried, which is why [`FakErrorClass::is_retryable`]
//! has no variant returning `true`: the rule lives in the type, pinned by
//! test, not in a comment a future edit can quietly contradict.
//!
//! Ported from the earlier taker's live module (§14.1), NOT re-derived — with one
//! deliberate divergence from that precedent: the earlier taker classifies a
//! zero-fill on the error string alone, falling back to a synthetic id when
//! no orderID is present. Here a clean zero-fill requires the no-match body
//! **and** an orderID; the no-orderID case is [`FakErrorClass::AmbiguousNoMatch`]
//! — still never-retryable, but logged at `warn!` with the raw body instead
//! of silently taking the free/quiet zero-fill path, because this crate's v0
//! job is measuring the race haircut (design §1) and that number must never
//! be quietly biased in either direction.
//!
//! `crate::execution::ClobClient::place_taker_buy` is **FOK**
//! (`live_clob.rs:750`) — all-or-nothing. Never use it here: against a thin
//! cheap ladder a FOK kills the whole order instead of taking what is
//! displayed, which is exactly the fill this strategy wants. FAK
//! (`OrderType::Fak`) is the only order type sent by [`LiveVenue`].
//!
//! **Task 15 obligation (auth halt: requested but NOT yet effected).** On a
//! proven 401/403 the venue calls `shutdown.cancel()` (see
//! [`LiveVenue::shutdown_token`]) and logs FATAL — but nothing observes that
//! token today, so the bot keeps firing, each fire failing identically and
//! burning `post_failed` misses until an operator notices. A raw
//! `std::process::exit` inside the spawned async task would kill the process
//! mid-telemetry-write, so a `CancellationToken` is the right mechanism, and
//! `drive_slot` already accepts one — it just isn't wired to this token yet.
//! Task 15 must:
//! (a) create ONE `CancellationToken`, pass it to `LiveVenue::new` **and**
//!     to every `drive_slot`;
//! (b) `select!` on `venue.shutdown_token().cancelled()` in `run()` alongside
//!     ctrl-c, and exit non-zero;
//! (c) stop **spawning new slots** on cancel, not merely drain the current
//!     one.
//!
//! Separately: `launch_gate`'s pUSD balance read (`require_pusd_balance`) is
//! an RPC `eth_call` against the collateral contract. It proves the wallet
//! is funded; it proves nothing about the CLOB API key/secret/passphrase or
//! HMAC signing. A stale API key passes this gate today and fails at the
//! first live fire — a candidate for a future gate step, not fixed here.

use crate::strategy::cards::CARDS;
use crate::config::BotConfig;
use crate::execution::venue::{FakOutcome, FakRequest, Venue, VenueEvent, VenueFill};
use anyhow::Context;
use crate::config::{ContractAddresses, LiveConfig, LiveReadinessCfg, MeasurementsCfg};
use crate::types::{Coin, Fill, Side};
use crate::execution::live_clob::{LiveClob, OrderType};
use crate::execution::live_readiness::verify_live_readiness_artifact;
use crate::execution::user_ws::{AssetIdRegistry, AssetMeta};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// The `POLY_*` live account must be set for a live launch. Split out from [`launch_gate`]
/// so it is independently testable without a socket (§14.3 step 2).
pub fn require_live_section(cfg: &BotConfig) -> anyhow::Result<&LiveConfig> {
    cfg.live.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "live launch requires the live account: set POLY_PRIVATE_KEY, POLY_PROXY_WALLET, \
             POLY_API_KEY, POLY_API_SECRET and POLY_API_PASSPHRASE (see .env.example)"
        )
    })
}

/// Fail-closed launch gate (§14.3), ordered, each step gating on the last:
/// 1. `RECON_GIT_SHA` present and non-empty (Inv 11).
/// 2. the `POLY_*` account present (`require_live_section`).
/// 3. `validate_live` (Inv 1-11 structural check).
/// 4. `validate_strategy` (strategy-local cap validation — design §2
///    deviation 6; `live_validator.rs` no longer exists).
/// 5. `verify_live_readiness_artifact` (Inv 11) — a fresh, sha-matching,
///    non-CI live-readiness artifact must exist. Its absence IS the
///    fail-closed signal (`readiness.rs` module doc); this is the step that
///    actually reads it.
/// 6. A pUSD balance read, at or above [`BALANCE_FLOOR_USD`] (NOT USDC.e — a
///    USDC.e `balanceOf` on this wallet reads $0 and would look like an
///    empty wallet). Reads the collateral address from
///    `ContractAddresses::v2_defaults()` rather than pasting the literal
///    here, so a future collateral cutover changes one place.
/// 7. A one-line summary log: sha, cap_usd, daily spend cap, loss kill, clip
///    mode, cards enabled, observed pUSD balance.
///
/// Constructs nothing live-capable — no signer, no HTTP client survives this
/// call. `LiveVenue` is built separately, only after this returns `Ok`.
pub async fn launch_gate(cfg: &BotConfig) -> anyhow::Result<()> {
    let git_sha = std::env::var("RECON_GIT_SHA")
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("Inv 11: RECON_GIT_SHA must be set for a live launch"))?;

    let live = require_live_section(cfg)?;
    crate::config::validate_live(live).context("Inv 1-11 (validate_live)")?;
    crate::config::validate_strategy(&cfg.strategy)
        .context("strategy-local cap validation (design §2 deviation 6)")?;
    verify_live_readiness_artifact(&MeasurementsCfg::default(), &LiveReadinessCfg::default(), &git_sha)
        .map_err(|e| anyhow::anyhow!("Inv 11 (live-readiness artifact): {e}"))?;
    let pusd_balance_usd =
        require_pusd_balance(live).await.context("pre-launch pUSD balance read")?;

    // Two DIFFERENT share counts, and the banner has to say which is which —
    // printing only one of them is how the sizing bug survived its first fires.
    // We SEND `clip/limit` shares; that commits exactly the clip; price
    // improvement returns `clip/fill_px` shares (`sizing::fak_shares`).
    let entry = format!(
        "${:.2} in [{:.3}, {:.3}] (limit {:.3}) = send {:.0} sh, commit ${:.2}, fill {:.0}-{:.0} sh",
        cfg.strategy.clip_usd,
        cfg.strategy.ask_price_min,
        cfg.strategy.ask_price_max,
        cfg.strategy.limit_price,
        crate::strategy::sizing::fak_shares(cfg.strategy.clip_usd, cfg.strategy.limit_price),
        crate::strategy::sizing::fak_shares(cfg.strategy.clip_usd, cfg.strategy.limit_price)
            * cfg.strategy.limit_price,
        crate::strategy::sizing::flat_shares(cfg.strategy.clip_usd, cfg.strategy.ask_price_max),
        crate::strategy::sizing::flat_shares(cfg.strategy.clip_usd, cfg.strategy.ask_price_min),
    );
    let cards_enabled: Vec<&'static str> = CARDS.iter().map(|c| c.id.as_str()).collect();
    tracing::info!(
        git_sha = %git_sha,
        cap_usd = live.safety.cap_usd,
        daily_spend_cap_usd = cfg.strategy.daily_spend_cap_usd,
        realized_loss_kill_usd = cfg.strategy.realized_loss_kill_usd,
        entry = %entry,
        sniper_mode = cfg.strategy.mode.as_str(),
        trigger = cfg.strategy.tick.trigger.as_str(),
        v_btc = cfg.strategy.tick.v_btc,
        lean_min = cfg.strategy.tick.lean_min,
        adverse_max_btc = cfg.strategy.tick.adverse_max_btc,
        cards_enabled = ?cards_enabled,
        pusd_balance_usd,
        "sniperbot: Inv 1-11 + strategy gate green — live launch"
    );
    Ok(())
}

/// Minimum pUSD balance the launch gate accepts — one clip's worth of
/// margin, not merely non-zero. Matches the earlier taker's `BALANCE_FLOOR_USD`.
/// Below this, every FAK the strategy sends
/// would come back a venue `Rejected` (classified as a sizing bug of OURS —
/// see [`FakErrorClass::Rejected`]) when the real cause is an empty wallet;
/// failing the gate here with the observed balance is the honest signal.
const BALANCE_FLOOR_USD: f64 = 10.0;

/// A throwaway `LiveClob` is cheap to construct (signer + HTTP client, no
/// network) — used here only for its `fetch_usdc_balance_micro` eth_call,
/// which reads `self.contracts.collateral_erc20`. `ContractAddresses::
/// v2_defaults()` is the same pUSD address the real venue will trade
/// against, so this is a config-sourced read, not a second hardcoded
/// literal. Fails below [`BALANCE_FLOOR_USD`] — a USDC.e balanceOf on this
/// wallet would read exactly zero, which is precisely the false-empty-wallet
/// trap this check exists to avoid. Returns the observed USD balance so the
/// caller can fold it into the gate's one-line summary log.
async fn require_pusd_balance(live: &LiveConfig) -> anyhow::Result<f64> {
    let clob = LiveClob::new(live, ContractAddresses::v2_defaults())
        .context("construct LiveClob for the pre-launch balance read")?;
    let micro = clob.fetch_usdc_balance_micro().await.context("pUSD balanceOf eth_call")?;
    let usd = micro as f64 / 1e6;
    anyhow::ensure!(
        usd >= BALANCE_FLOOR_USD,
        "pUSD balance is ${usd:.2}, below the ${BALANCE_FLOOR_USD:.2} floor — collateral is pUSD \
         ({}), not USDC.e; a USDC.e balanceOf on this wallet reads $0 and would look like an empty \
         wallet even when funded",
        ContractAddresses::v2_defaults().collateral_erc20
    );
    Ok(usd)
}

/// Every way `place_buy_with_order_type` can fail, and what each one means
/// for retrying (§14.2). No variant of [`is_retryable`](Self::is_retryable)
/// returns `true` — see the module doc.
#[derive(Clone, Debug, PartialEq)]
pub enum FakErrorClass {
    /// HTTP 400 "no orders found to match with FAK order" **with** an
    /// orderID — the only response that proves a clean zero-fill. Free,
    /// countable, logged at info — this IS the race-haircut measurement.
    CleanZeroFill { order_id: String },
    /// The no-match phrase, but with no extractable orderID. Looks like a
    /// zero-fill but is NOT proven — deviation from the earlier taker's
    /// precedent (module doc). Never retryable; logged at `warn!` with the
    /// raw body by the caller, distinct from an ordinary post_failed.
    AmbiguousNoMatch,
    /// HTTP 400, any other body: malformed request or below a venue
    /// minimum. A bug in OUR sizing/pricing, not a market condition.
    Rejected,
    /// HTTP 401 / 403: bad credentials. Every subsequent order fails
    /// identically, so this halts the process rather than burning the fire
    /// budget against a wall.
    AuthFailure,
    /// HTTP 429: rate limited. Never retry this order; back off before the
    /// next send.
    RateLimited,
    /// HTTP 5xx: venue error, landing unknown.
    VenueError,
    /// Timeout / connection reset: landing unknown.
    Timeout,
    /// Anything else — most commonly a 2xx whose body failed to parse (an
    /// order id may already exist live) or a `success:false` body. Landing
    /// unknown, the worst case: alarm loudly, never retry.
    LandingUnknown,
}

impl FakErrorClass {
    /// NOTHING is retryable. A retry that double-buys is unrecoverable; a
    /// missed fill is free — so the asymmetry always resolves toward "do
    /// not retry". Written as an exhaustive match, not a bare `false`, so a
    /// future variant added here without updating this method fails to
    /// compile instead of silently becoming retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            FakErrorClass::CleanZeroFill { .. }
            | FakErrorClass::AmbiguousNoMatch
            | FakErrorClass::Rejected
            | FakErrorClass::AuthFailure
            | FakErrorClass::RateLimited
            | FakErrorClass::VenueError
            | FakErrorClass::Timeout
            | FakErrorClass::LandingUnknown => false,
        }
    }

    /// Only a proven auth failure halts the process: bad creds will not
    /// fix themselves and every subsequent order fails identically.
    pub fn halts_process(&self) -> bool {
        matches!(self, FakErrorClass::AuthFailure)
    }
}

/// Pull the `orderID` out of a venue error body embedded in an error
/// string (ported from the earlier taker's live module). An empty `"orderID":""` is
/// a *documented* behaviour of this endpoint (`live_clob.rs:441-444`), not
/// proof of anything — filtered out here so the caller sees `None` and
/// routes to [`FakErrorClass::AmbiguousNoMatch`] instead of treating an
/// empty string as a proven clean zero-fill.
fn extract_order_id(msg: &str) -> Option<String> {
    let start = msg.find("\"orderID\":\"")? + "\"orderID\":\"".len();
    let rest = &msg[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string()).filter(|s| !s.is_empty())
}

/// The literal marker `LiveClob` emits directly before a non-2xx HTTP status
/// code (`live_clob.rs`: `"POST {url} -> HTTP {status}: {txt} ..."`).
const HTTP_STATUS_MARKER: &str = " -> HTTP ";

/// The 3-digit HTTP status following [`HTTP_STATUS_MARKER`], e.g. `400` out
/// of `"POST ... -> HTTP 400: {...}"`. Anchored to that exact marker — not a
/// bare `"HTTP "` anywhere in the string — so an unrelated occurrence of the
/// word "HTTP" inside a response body can never be mistaken for the status
/// line. The digit run is bounded to exactly 3 digits: a 4-or-more-digit run
/// (or a shorter one) is rejected rather than silently truncated/accepted.
/// `None` when the string carries no such marker at all (a transport-level
/// error, or a 2xx body that failed to parse before a status could be
/// attributed in the message).
fn extract_http_status(msg: &str) -> Option<u16> {
    let idx = msg.find(HTTP_STATUS_MARKER)?;
    let rest = &msg[idx + HTTP_STATUS_MARKER.len()..];
    let mut chars = rest.chars();
    let code: String = chars.by_ref().take(3).take_while(|c| c.is_ascii_digit()).collect();
    if code.len() != 3 || chars.next().is_some_and(|c| c.is_ascii_digit()) {
        return None;
    }
    code.parse().ok()
}

/// The FAK outcome taxonomy (§14.2), as a pure function over the error
/// string — testable without a socket, which is where most of this module's
/// safety work happens (real HTTP/venue code has the least test grip).
///
/// The no-match phrase alone is NOT sufficient for
/// [`FakErrorClass::CleanZeroFill`]/[`FakErrorClass::AmbiguousNoMatch`] — the
/// status must independently be exactly HTTP 400 first. Without that gate, a
/// gateway 5xx that echoes an origin body containing the phrase (or a 200
/// `success:false` whose `errorMsg` quotes it) would classify as a proven
/// zero-fill it never was.
pub fn classify_fak_error(msg: &str) -> FakErrorClass {
    let status = extract_http_status(msg);
    let class = if status == Some(400) && msg.contains("no orders found to match with FAK order")
    {
        match extract_order_id(msg) {
            Some(order_id) => FakErrorClass::CleanZeroFill { order_id },
            None => FakErrorClass::AmbiguousNoMatch,
        }
    } else {
        match status {
            Some(400) => FakErrorClass::Rejected,
            Some(401) | Some(403) => FakErrorClass::AuthFailure,
            Some(429) => FakErrorClass::RateLimited,
            Some(s) if (500..600).contains(&s) => FakErrorClass::VenueError,
            Some(_) => FakErrorClass::LandingUnknown,
            None => {
                let lower = msg.to_ascii_lowercase();
                if lower.contains("timed out")
                    || lower.contains("timeout")
                    || lower.contains("connection reset")
                {
                    FakErrorClass::Timeout
                } else {
                    FakErrorClass::LandingUnknown
                }
            }
        }
    };
    // Staleness canary: the no-match phrase is only trustworthy inside a
    // parsed HTTP 400 (see the module doc above). If it shows up anywhere
    // else, extract_http_status's marker or the phrase itself may have
    // drifted from what live_clob.rs now emits — alarm distinctly so the
    // operator diagnoses that instead of a generic unattributable error.
    if !matches!(class, FakErrorClass::CleanZeroFill { .. } | FakErrorClass::AmbiguousNoMatch)
        && msg.contains("no orders found to match with FAK order")
    {
        tracing::error!(
            body = %msg,
            classified_as = ?class,
            "tickburst: STALE CLASSIFIER — the FAK no-match phrase appeared outside a parsed HTTP \
             400; classify_fak_error's assumptions about the venue or upstream message shape may \
             be stale"
        );
    }
    class
}

/// Classify-and-react on a `send_fak` failure that was NOT the proven clean
/// zero-fill (the caller handles that case before this runs). A free
/// function over an explicit `shutdown` token — rather than a `&self`
/// method — so the auth-failure-cancels-shutdown wiring is unit-testable
/// without constructing a live signer/HTTP client at all.
fn react_to_send_error(shutdown: &CancellationToken, class: &FakErrorClass, raw: &str) {
    match class {
        FakErrorClass::CleanZeroFill { .. } => {
            // Genuinely unreachable given the caller's match today, but a
            // panic here would happen inside the awaited `send_fak` future
            // and abort the slot driver — a future refactor must not be
            // able to turn a classification bug into a process crash on the
            // money path. Warn loudly and fall through to the LandingUnknown
            // treatment (the safest, most alarming outcome) instead.
            tracing::warn!(
                body = %raw,
                "tickburst: BUG — react_to_send_error saw CleanZeroFill; the caller is supposed to \
                 handle that case before calling in. Treating as LandingUnknown rather than \
                 panicking inside the awaited send_fak future."
            );
            tracing::error!(
                body = %raw,
                "tickburst: FAK POST response unattributable — worst case, an order may already be \
                 live and unaccounted for. Never retrying; alarm."
            );
        }
        FakErrorClass::AmbiguousNoMatch => {
            tracing::warn!(
                body = %raw,
                "tickburst: FAK no-match phrase present but no orderID extractable — NOT a proven \
                 clean zero-fill; treating as a non-retryable error, not a free miss"
            );
        }
        FakErrorClass::AuthFailure => {
            tracing::error!(
                body = %raw,
                "tickburst: FATAL — HTTP 401/403 on a FAK POST; credentials will not self-heal and \
                 every subsequent order fails identically. Halting."
            );
            shutdown.cancel();
        }
        FakErrorClass::Rejected => {
            tracing::error!(
                body = %raw,
                "tickburst: FAK rejected by the venue for a reason other than no-match — likely a \
                 bug in OUR sizing/pricing, not a market condition. Never retrying."
            );
        }
        FakErrorClass::RateLimited => {
            tracing::warn!(
                body = %raw,
                "tickburst: HTTP 429 rate limited — not retrying this order; back off before the next send"
            );
        }
        FakErrorClass::VenueError | FakErrorClass::Timeout => {
            tracing::error!(
                body = %raw,
                "tickburst: FAK POST failed with the order's landing unknown — never retrying"
            );
        }
        FakErrorClass::LandingUnknown => {
            tracing::error!(
                body = %raw,
                "tickburst: FAK POST response unattributable — worst case, an order may already be \
                 live and unaccounted for. Never retrying; alarm."
            );
        }
    }
}

/// Per-order dedup between the provisional (TRADE MATCHED) and settled
/// (TRADE MINED/CONFIRMED) fill streams — ported verbatim from
/// the earlier takers' orphaned-fill fix. Never settled-only: a
/// `FAK_GRACE_MS` grace can never observe a MINED fill.
pub struct FillDedup {
    per_order: std::collections::HashMap<String, DedupRec>,
    fifo: std::collections::VecDeque<String>,
}

#[derive(Default)]
struct DedupRec {
    provisional: f64,
    settled: f64,
    forwarded: f64,
}

const DEDUP_ORDER_CAP: usize = 1024;

impl Default for FillDedup {
    fn default() -> Self {
        Self::new()
    }
}

impl FillDedup {
    pub fn new() -> Self {
        Self { per_order: std::collections::HashMap::new(), fifo: std::collections::VecDeque::new() }
    }

    /// Incremental shares to forward for this fill (0.0 = duplicate).
    pub fn incremental(&mut self, order_id: &str, sh: f64, provisional: bool) -> f64 {
        if !self.per_order.contains_key(order_id) {
            self.per_order.insert(order_id.to_string(), DedupRec::default());
            self.fifo.push_back(order_id.to_string());
            while self.fifo.len() > DEDUP_ORDER_CAP {
                if let Some(old) = self.fifo.pop_front() {
                    self.per_order.remove(&old);
                }
            }
        }
        let rec = self.per_order.get_mut(order_id).expect("just inserted");
        if provisional {
            rec.provisional += sh;
        } else {
            rec.settled += sh;
        }
        let total = rec.provisional.max(rec.settled);
        let inc = total - rec.forwarded;
        if inc > 1e-9 {
            rec.forwarded = total;
            inc
        } else {
            0.0
        }
    }
}

/// How long after the ack a FAK waits for its user-WS fill before the
/// driver sees `Terminal`. The fill normally lands within tens of ms; the
/// window covers WS jitter. A fill that still misses it is absorbed by the
/// slot driver's own `sweep_grace_expired` backstop — never dropped.
///
/// `pub` (M8, task-15 review): `runtime.rs`'s `DriverCfg::fak_grace_ms`
/// reads THIS constant directly rather than carrying its own hand-copied
/// duplicate literal — the venue's synthetic `Terminal` timer here and the
/// driver's grace sweep are meant to line up, not drift apart from an
/// edit made in only one of the two places.
pub const FAK_GRACE_MS: u64 = 1500;

/// Real-money venue: FAK marketable limits via
/// `LiveClob::place_buy_with_order_type`. This strategy never rests an
/// order — no GTC, no cancel path — so unlike the earlier taker's `LiveVenue`
/// there is no open-order/cancel-set bookkeeping here at all; a FAK is
/// always venue-terminal at POST time and its fills (if any) arrive on the
/// user WS during the grace window.
pub struct LiveVenue {
    clob: Arc<LiveClob>,
    asset_registry: AssetIdRegistry,
    events_tx: broadcast::Sender<VenueEvent>,
    /// Cancelled on a proven auth failure (§14.2: "Err + halt the
    /// process" — bad creds never self-heal and every subsequent order
    /// fails identically). The runtime observes this alongside its own
    /// ctrl-c/shutdown token and propagates it to every `drive_slot`, so
    /// the process stops cleanly (telemetry flushed) rather than via a
    /// raw `std::process::exit` deep inside an async task.
    shutdown: CancellationToken,
}

impl LiveVenue {
    /// Spawns the fill-dedup background task, so this must be called from
    /// inside a running tokio runtime — `tokio::spawn` panics otherwise.
    pub fn new(
        clob: Arc<LiveClob>,
        asset_registry: AssetIdRegistry,
        mut fills_rx: broadcast::Receiver<Fill>,
        mut provisional_rx: broadcast::Receiver<Fill>,
        shutdown: CancellationToken,
    ) -> Self {
        let (events_tx, _) = broadcast::channel(256);
        let tx = events_tx.clone();
        tokio::spawn(async move {
            let mut dedup = FillDedup::new();
            let (mut settled_open, mut provisional_open) = (true, true);
            while settled_open || provisional_open {
                let (fill, provisional) = tokio::select! {
                    f = fills_rx.recv(), if settled_open => match f {
                        Ok(f) => (f, false),
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(n, "tickburst: live venue lagged the settled fills feed");
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            settled_open = false;
                            continue;
                        }
                    },
                    f = provisional_rx.recv(), if provisional_open => match f {
                        Ok(f) => (f, true),
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(n, "tickburst: live venue lagged the provisional fills feed");
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            provisional_open = false;
                            continue;
                        }
                    },
                };
                let inc = dedup.incremental(fill.id.as_str(), fill.shares, provisional);
                if inc > 1e-9 {
                    let _ = tx.send(VenueEvent::Fill(VenueFill {
                        order_id: fill.id.as_str().to_string(),
                        shares: inc,
                        price: fill.price,
                        venue_ts_ms: Some(fill.ts_micros / 1000),
                    }));
                }
            }
        });
        Self { clob, asset_registry, events_tx, shutdown }
    }

    /// Observed by the runtime alongside its own shutdown signal so a
    /// proven auth failure stops the process cleanly (§14.2). NOT YET
    /// wired to `run()` — see the module doc's Task 15 obligation.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }
}

impl Venue for LiveVenue {
    fn send_fak(
        &self,
        req: FakRequest,
    ) -> impl Future<Output = anyhow::Result<FakOutcome>> + Send {
        let clob = self.clob.clone();
        let events_tx = self.events_tx.clone();
        let shutdown = self.shutdown.clone();
        async move {
            // Resolution 2: checked BEFORE the call so a sizing bug fails as
            // ours and diagnosable, not as a venue-helper bail. The budget
            // walk guarantees >= 1.0/0.20 = 5 sh at the widest cap, so this
            // cannot bind in practice.
            anyhow::ensure!(
                req.size_shares >= 5.0,
                "tickburst: refusing to send {} sh on {} ({:?}) — below the venue's 5-share LIMIT \
                 floor; the budget walk should guarantee >= 5 sh at the widest cap (1.0/0.20), so \
                 this indicates a sizing bug upstream, not a venue condition",
                req.size_shares,
                req.token_id,
                req.side
            );
            let order_type = OrderType::Fak;
            tracing::info!(
                token_id = %req.token_id,
                limit_px = req.limit_px,
                size_shares = req.size_shares,
                order_type = order_type.as_str(),
                "tickburst: sending FAK order"
            );
            let post = clob
                .place_buy_with_order_type(
                    &req.token_id,
                    req.limit_px,
                    req.size_shares,
                    order_type,
                    false,
                )
                .await;
            let id = match post {
                Ok(id) => id,
                Err(e) => {
                    // {:#} (alternate Display) chains anyhow's causes with
                    // ": " — e.to_string() prints only the outermost
                    // .with_context() layer, which strands the reqwest
                    // timeout/connection-reset text the Timeout classification
                    // depends on (live_clob.rs wraps the send with
                    // with_context). The 400 path is a leaf bail!, so this is
                    // byte-identical there — strictly more information, never
                    // less.
                    let msg = format!("{e:#}");
                    return match classify_fak_error(&msg) {
                        FakErrorClass::CleanZeroFill { order_id } => {
                            tracing::info!(
                                order_id = %order_id,
                                limit_px = req.limit_px,
                                "tickburst: FAK killed — no match at limit (clean zero-fill; the \
                                 race-haircut measurement)"
                            );
                            Ok(FakOutcome::ZeroFill { order_id })
                        }
                        other => {
                            react_to_send_error(&shutdown, &other, &msg);
                            Err(e)
                        }
                    };
                }
            };
            let order_id = id.as_str().to_string();
            let oid = order_id.clone();
            let tx = events_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(FAK_GRACE_MS)).await;
                let _ = tx.send(VenueEvent::Terminal { order_id: oid });
            });
            Ok(FakOutcome::Acked { order_id })
        }
    }

    fn events_rx(&self) -> broadcast::Receiver<VenueEvent> {
        self.events_tx.subscribe()
    }

    fn prepare_slot(
        &self,
        slug: &str,
        token_up: &str,
        token_dn: &str,
    ) -> impl Future<Output = ()> + Send {
        let registry = self.asset_registry.clone();
        let slug = slug.to_string();
        let up = token_up.to_string();
        let dn = token_dn.to_string();
        async move {
            // populate-before-fills invariant (feedback_user_ws_registry_unpopulated)
            let coin = Coin::from_slug_prefix(slug.split('-').next().unwrap_or("")).unwrap_or_else(|| {
                tracing::error!(slug, "tickburst: unknown coin prefix — labeling fills btc");
                Coin::Btc
            });
            let mut m = registry.lock().await;
            m.insert(up, AssetMeta { slug: slug.clone(), coin, outcome_side: Side::Up });
            m.insert(dn, AssetMeta { slug, coin, outcome_side: Side::Down });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_order_id_finds_the_json_field() {
        let msg = "HTTP 400: {\"error\":\"no orders found to match with FAK order\",\"orderID\":\"0xabc\"}";
        assert_eq!(extract_order_id(msg), Some("0xabc".to_string()));
        assert_eq!(extract_order_id("no field here"), None);
    }

    #[test]
    fn extract_order_id_rejects_an_empty_value_as_unproven() {
        // A2: "orderID":"" is a documented empty-orderID behaviour of this
        // endpoint (live_clob.rs:441-444), not proof of anything.
        let msg = "HTTP 400: {\"error\":\"no orders found to match with FAK order\",\"orderID\":\"\"}";
        assert_eq!(extract_order_id(msg), None);
    }

    #[test]
    fn extract_http_status_requires_the_arrow_marker() {
        assert_eq!(extract_http_status("POST https://x -> HTTP 400: bad"), Some(400));
        assert_eq!(
            extract_http_status("POST https://x -> HTTP 502: bad gateway [elapsed_ms=12]"),
            Some(502)
        );
        // D2: a bare "HTTP " without the " -> " marker must not match — the
        // marker anchors to the exact shape LiveClob emits, so a response
        // body that merely mentions the word "HTTP" can't be confused for
        // the status line.
        assert_eq!(extract_http_status("HTTP 400: bad"), None);
        assert_eq!(extract_http_status("no status here"), None);
        // D2: a 4+-digit run must not be truncated into a false 3-digit
        // status.
        assert_eq!(extract_http_status("POST https://x -> HTTP 4000: weird"), None);
        assert_eq!(extract_http_status("POST https://x -> HTTP 42: too short"), None);
    }

    #[test]
    fn auth_failure_cancels_the_shutdown_token() {
        let token = CancellationToken::new();
        react_to_send_error(&token, &FakErrorClass::AuthFailure, "HTTP 401: unauthorized");
        assert!(token.is_cancelled(), "an auth failure must halt the process");
    }

    #[test]
    fn non_auth_failures_never_cancel_the_shutdown_token() {
        for class in [
            FakErrorClass::CleanZeroFill { order_id: "x".into() },
            FakErrorClass::AmbiguousNoMatch,
            FakErrorClass::Rejected,
            FakErrorClass::RateLimited,
            FakErrorClass::VenueError,
            FakErrorClass::Timeout,
            FakErrorClass::LandingUnknown,
        ] {
            let token = CancellationToken::new();
            react_to_send_error(&token, &class, "irrelevant body");
            assert!(!token.is_cancelled(), "{class:?} must not halt the process");
        }
    }
}
