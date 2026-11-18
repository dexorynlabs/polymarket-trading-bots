//! The bot's configuration. Everything comes from the environment (or a
//! `.env` file) — see `config::env` for every variable:
//!
//! - `SNIPER_MODE=1c|99c` + optional `SNIPER_*` overrides: WHAT to trade.
//! - `SNIPER_*_CAP_USD` etc.: the spend rails (all have defaults).
//! - `POLY_*`: the live account (wallet key, CLOB API key). `--live` and
//!   `--preflight` only; a dry run needs none of it.
//!
//! Strict: an unknown `SNIPER_*` / `POLY_*` variable is a hard error, so a
//! typo fails loudly instead of silently trading on a default.

pub mod env;

use crate::strategy::mode::SniperMode;
use crate::strategy::tick::TriggerMode;

/// The resolved configuration every mode of the binary runs on.
#[derive(Debug)]
pub struct BotConfig {
    pub strategy: StrategySection,
    /// `None` when no `POLY_*` variable is set (a dry run).
    pub live: Option<crate::config::LiveConfig>,
}

/// Account-level spend limits that do not change with the mode.
#[derive(Clone, Debug)]
pub struct RailsSection {
    /// Rolling-24h cap on COMMITTED spend (`size_shares * limit_price` per
    /// fire, never released by the ledger).
    pub daily_spend_cap_usd: f64,
    pub concurrent_exposure_cap_usd: f64,
    /// Max committed exposure a SINGLE fire may carry.
    pub per_order_cap_usd: f64,
    /// A LATCH, not a rate limiter: the automaton fires at most ONE order per
    /// 300s slot, so a trip is proof of a state-machine fault.
    pub self_fire_rate_per_min: u32,
    /// Drop the held ladders when the book WS has been silent this many ms.
    pub book_stale_disarm_ms: u64,
    /// Reconstruct the Binance 1s close at the bar boundary from `@aggTrade`,
    /// this many ms after it. TELEMETRY only. `None` (default) = off.
    pub boundary_close_delay_ms: Option<u64>,
}

impl Default for RailsSection {
    fn default() -> Self {
        Self {
            daily_spend_cap_usd: 2_500.0,
            concurrent_exposure_cap_usd: 150.0,
            per_order_cap_usd: 100.0,
            self_fire_rate_per_min: 20,
            book_stale_disarm_ms: 5_000,
            boundary_close_delay_ms: None,
        }
    }
}

impl BotConfig {
    /// Resolves the whole config from `SNIPER_*` / `POLY_*` variables.
    pub fn from_env(env: &env::EnvVars) -> anyhow::Result<Self> {
        let rails = env::resolve_rails(env)?;
        let strategy = env::resolve_strategy(env, &rails)?;
        let live = env::resolve_live(env)?;
        Ok(Self { strategy, live })
    }
}

/// The trigger — the ONLY fire path.
#[derive(Clone, Copy, Debug)]
pub struct TickSection {
    /// OFF makes this process incapable of firing at all (it still ingests,
    /// records books and writes `Feat` telemetry).
    pub enabled: bool,
    /// `burst`: flow toward the bought token. `calm`: no flow against it
    /// (`99c` only).
    pub trigger: TriggerMode,
    /// Trailing-window Binance-spot volume, in BTC, that counts as a burst —
    /// the `burst` entry floor, and the placebo mirror's floor for both.
    pub v_btc: f64,
    /// Minimum share of a burst's volume in the direction under test, (0.5, 1].
    pub lean_min: f64,
    /// `calm` only: veto the entry while trailing-window volume AGAINST the
    /// token is >= this many BTC.
    pub adverse_max_btc: f64,
    /// Width of the trailing window, in ms.
    pub window_ms: u64,
    /// A held book older than this is not acted on.
    pub max_book_age_ms: u64,
}

/// The resolved strategy: the mode preset, `SNIPER_*` overrides, and the
/// spend rails.
#[derive(Clone, Debug)]
pub struct StrategySection {
    /// `1c` (buy the trailer) or `99c` (buy the leader).
    pub mode: SniperMode,
    /// v0 is btc-only.
    pub coin: String,
    /// The entry BAND, inclusive: the token's standing best ask must sit
    /// here. This band is what makes the mode.
    pub ask_price_min: f64,
    pub ask_price_max: f64,
    /// The FAK's limit price. A BUY limit fills at ANY lower ask, so the
    /// limit bounds what we pay but never protects against a collapse.
    pub limit_price: f64,
    /// Flat per-fire budget, USD: a fire commits EXACTLY this, because the
    /// order is sized `clip_usd / limit_price` (`sizing::fak_shares`).
    pub clip_usd: f64,
    pub boundary_close_delay_ms: Option<u64>,
    pub daily_spend_cap_usd: f64,
    pub concurrent_exposure_cap_usd: f64,
    pub per_order_cap_usd: f64,
    /// Positive magnitude; the rail trips when CLOB-settled realized loss
    /// reaches -this.
    pub realized_loss_kill_usd: f64,
    pub self_fire_rate_per_min: u32,
    pub book_stale_disarm_ms: u64,
    pub tick: TickSection,
}

impl StrategySection {
    /// The mode's preset with default rails and no env overrides.
    pub fn for_mode(mode: SniperMode) -> Self {
        Self::from_preset(mode, &RailsSection::default())
    }

    pub fn from_preset(mode: SniperMode, rails: &RailsSection) -> Self {
        let p = mode.preset();
        Self {
            mode,
            coin: "btc".into(),
            ask_price_min: p.ask_price_min,
            ask_price_max: p.ask_price_max,
            limit_price: p.limit_price,
            clip_usd: p.clip_usd,
            boundary_close_delay_ms: rails.boundary_close_delay_ms,
            daily_spend_cap_usd: rails.daily_spend_cap_usd,
            concurrent_exposure_cap_usd: rails.concurrent_exposure_cap_usd,
            per_order_cap_usd: rails.per_order_cap_usd,
            realized_loss_kill_usd: p.realized_loss_kill_usd,
            self_fire_rate_per_min: rails.self_fire_rate_per_min,
            book_stale_disarm_ms: rails.book_stale_disarm_ms,
            tick: TickSection {
                enabled: true,
                trigger: p.trigger,
                v_btc: p.v_btc,
                lean_min: p.lean_min,
                adverse_max_btc: p.adverse_max_btc,
                window_ms: p.window_ms,
                max_book_age_ms: p.max_book_age_ms,
            },
        }
    }
}

/// The `99c` preset. Used by unit tests that do not care about the mode;
/// real runs always go through `BotConfig::from_env`.
impl Default for StrategySection {
    fn default() -> Self {
        Self::for_mode(SniperMode::NinetyNineCent)
    }
}

/// Past this the reconstruction is slower than simply waiting for Binance's
/// own pushed kline (p50 152 / p90 176 ms after the bar close), so it can only
/// be an operator mistake.
const BOUNDARY_CLOSE_MAX_DELAY_MS: u64 = 200;

/// `99c`: highest FAK limit that can still pay off after fees.
const MAX_LIMIT_PRICE: f64 = 0.999;
/// `99c`: most the limit may sit above the band's low edge.
const MAX_LIMIT_CHASE: f64 = 0.03;
/// `1c`: most the limit may sit above the band's low edge, as a multiple.
const MAX_LIMIT_MULTIPLE_1C: f64 = 4.0;

fn ensure_band_ordered(s: &StrategySection) -> anyhow::Result<()> {
    anyhow::ensure!(
        s.ask_price_min <= s.ask_price_max,
        "SNIPER_ASK_MIN ({}) must be <= SNIPER_ASK_MAX ({}) — an inverted band admits nothing \
         and the strategy would never fire",
        s.ask_price_min,
        s.ask_price_max
    );
    anyhow::ensure!(
        s.limit_price >= s.ask_price_max,
        "SNIPER_LIMIT ({}) must be >= SNIPER_ASK_MAX ({}) — the FAK limit is the bound on what \
         we will pay, never below the dearest ask we are willing to enter at",
        s.limit_price,
        s.ask_price_max
    );
    Ok(())
}

/// `1c`: cheap asks only. The economics (win ~99c, lose ~1c, break-even ~1%)
/// are a cheap-ask fact.
fn validate_one_cent_band(s: &StrategySection) -> anyhow::Result<()> {
    for (name, px) in [("SNIPER_ASK_MIN", s.ask_price_min), ("SNIPER_ASK_MAX", s.ask_price_max)] {
        anyhow::ensure!(
            (0.005..=0.10).contains(&px),
            "{name} ({px}) must be in [0.005, 0.10] in SNIPER_MODE=1c — for dear asks use \
             SNIPER_MODE=99c"
        );
    }
    ensure_band_ordered(s)?;
    anyhow::ensure!(
        s.limit_price <= MAX_LIMIT_MULTIPLE_1C * s.ask_price_min,
        "SNIPER_LIMIT ({}) must be <= {MAX_LIMIT_MULTIPLE_1C}x SNIPER_ASK_MIN ({}) in \
         SNIPER_MODE=1c — a wider limit stops being race-tail slack and starts buying the sweep \
         that just took the cheap level",
        s.limit_price,
        s.ask_price_min
    );
    anyhow::ensure!(
        s.tick.trigger == TriggerMode::Burst,
        "SNIPER_TRIGGER=calm is not allowed in SNIPER_MODE=1c — entering a standing 1c ask with \
         no burst graded -$563 to -$2,954 in the 1c census"
    );
    Ok(())
}

/// `99c`: dear asks only. The economics (win ~1c, lose ~99c, break-even ~99%)
/// are a dear-ask fact.
fn validate_ninety_nine_cent_band(s: &StrategySection) -> anyhow::Result<()> {
    for (name, px) in [("SNIPER_ASK_MIN", s.ask_price_min), ("SNIPER_ASK_MAX", s.ask_price_max)] {
        anyhow::ensure!(
            (0.90..=0.999).contains(&px),
            "{name} ({px}) must be in [0.90, 0.999] in SNIPER_MODE=99c — for cheap asks use \
             SNIPER_MODE=1c"
        );
    }
    ensure_band_ordered(s)?;
    anyhow::ensure!(
        s.limit_price <= MAX_LIMIT_PRICE,
        "SNIPER_LIMIT ({}) must be <= {MAX_LIMIT_PRICE} — a share pays at most $1.00, so a limit \
         at or above it buys a guaranteed loss after fees",
        s.limit_price
    );
    anyhow::ensure!(
        s.limit_price - s.ask_price_min <= MAX_LIMIT_CHASE + 1e-9,
        "SNIPER_LIMIT ({}) must be within {MAX_LIMIT_CHASE} of SNIPER_ASK_MIN ({}) in \
         SNIPER_MODE=99c — at 99c every cent of chase is a full win's worth of edge",
        s.limit_price,
        s.ask_price_min
    );
    Ok(())
}

/// Strategy-local invariants. Called by every mode that can send an order.
pub fn validate_strategy(s: &StrategySection) -> anyhow::Result<()> {
    anyhow::ensure!(s.coin == "btc", "v0 supports coin = \"btc\" only, got {:?}", s.coin);
    match s.mode {
        SniperMode::OneCent => validate_one_cent_band(s)?,
        SniperMode::NinetyNineCent => validate_ninety_nine_cent_band(s)?,
    }
    anyhow::ensure!(
        s.clip_usd >= 1.0,
        "SNIPER_CLIP_USD ({}) must be >= 1.0 — the venue's $1 marketable minimum",
        s.clip_usd
    );
    // The venue's LIMIT floor is 5 SHARES (not $5 of notional). Checked on the
    // count we actually SEND — `clip_usd / limit_price` (`sizing::fak_shares`)
    // — which is the smallest count in play, since `limit_price` is by
    // definition at or above every ask we will enter at.
    let sent_shares = s.clip_usd / s.limit_price;
    anyhow::ensure!(
        sent_shares >= 5.0,
        "SNIPER_CLIP_USD {} at SNIPER_LIMIT {} is {sent_shares} shares — under the venue's 5-share \
         LIMIT minimum",
        s.clip_usd,
        s.limit_price
    );
    if let Some(d) = s.boundary_close_delay_ms {
        anyhow::ensure!(
            d <= BOUNDARY_CLOSE_MAX_DELAY_MS,
            "SNIPER_BOUNDARY_CLOSE_DELAY_MS ({d}) must be <= {BOUNDARY_CLOSE_MAX_DELAY_MS} — the pushed \
             @kline_1s itself lands p50 152 / p90 176 ms after the bar closes, so a larger delay \
             is strictly slower than doing nothing"
        );
    }
    // ALL THREE spend rails are checked against one fire's own committed
    // exposure. A cap below it does not BOUND the strategy, it turns it off —
    // silently, one `RailTrip` row at a time — which is exactly the
    // "a rail must not bind silently" failure of design §5.0. Raise a rail;
    // never let one sit under the clip.
    // Since 2026-08-10 the commit is `clip_usd` FLAT: the order is sized off
    // the limit, so there is no longer a worse case at the band's cheap edge.
    let shares = sent_shares;
    let commit = shares * s.limit_price;
    for (name, cap) in [
        ("SNIPER_PER_ORDER_CAP_USD", s.per_order_cap_usd),
        ("SNIPER_EXPOSURE_CAP_USD", s.concurrent_exposure_cap_usd),
        ("SNIPER_DAILY_CAP_USD", s.daily_spend_cap_usd),
    ] {
        anyhow::ensure!(cap > 0.0, "{name} must be > 0");
        anyhow::ensure!(
            cap >= commit,
            "{name} ({cap}) is below a single fire's committed exposure ({shares} shares x limit \
             {} = ${commit}) — every fire would be vetoed, so this rail would turn the strategy \
             off rather than bound it",
            s.limit_price
        );
    }
    anyhow::ensure!(
        s.realized_loss_kill_usd > 0.0 && s.realized_loss_kill_usd < s.daily_spend_cap_usd,
        "SNIPER_LOSS_KILL_USD ({}) must be > 0 and < SNIPER_DAILY_CAP_USD ({}) — otherwise the \
         spend cap always bites first and the loss kill is inert (the maker-lineage bug)",
        s.realized_loss_kill_usd,
        s.daily_spend_cap_usd
    );
    anyhow::ensure!(s.self_fire_rate_per_min > 0, "SNIPER_FIRE_RATE_PER_MIN must be > 0");
    validate_tick(&s.tick)?;
    Ok(())
}

/// Past this the "trailing window" reaches back before the burst it is meant
/// to measure — the mechanism is a sub-second micro-burst, so anything near a
/// slot's length is an operator mistake, not a wider arm.
const TICK_MAX_WINDOW_MS: u64 = 5_000;

fn validate_tick(t: &TickSection) -> anyhow::Result<()> {
    if !t.enabled {
        return Ok(());
    }
    anyhow::ensure!(
        t.v_btc > 0.0,
        "SNIPER_V_BTC ({}) must be > 0 — it is the burst floor of the burst entry AND of the \
         placebo mirror; for a no-burst entry use SNIPER_TRIGGER=calm (99c only)",
        t.v_btc
    );
    anyhow::ensure!(
        t.adverse_max_btc > 0.0 && t.adverse_max_btc.is_finite(),
        "SNIPER_ADVERSE_MAX_BTC ({}) must be finite and > 0 — at 0 calm can never fire",
        t.adverse_max_btc
    );
    anyhow::ensure!(
        t.lean_min > 0.5 && t.lean_min <= 1.0,
        "SNIPER_LEAN_MIN ({}) must be in (0.5, 1.0] — at or below 0.5 the 'lean' is not a direction \
         at all (both sides of one print can pass at once, which would also let the placebo \
         mirror and the real gate fire on the same tick), the shipped value is 0.6",
        t.lean_min
    );
    anyhow::ensure!(
        (1..=TICK_MAX_WINDOW_MS).contains(&t.window_ms),
        "SNIPER_WINDOW_MS ({}) must be in [1, {TICK_MAX_WINDOW_MS}]",
        t.window_ms
    );
    anyhow::ensure!(
        t.max_book_age_ms > 0,
        "SNIPER_MAX_BOOK_AGE_MS must be > 0 — at 0 no held book is ever fresh enough and nothing \
         ever fires"
    );
    Ok(())
}

// ── Shared venue / wallet config (was polymarket-client::config) ──
// Polymarket-client configuration.
//
// Leaf cfg types in [`types`]; consumers can import them via
// `polymarket_client::config::Mode`, etc.
//
// The umbrella `Config` and strategy-specific presets live in the
// strategy crate (`recon::config`).

pub mod live;
pub mod types;

pub use live::*;
pub use types::*;
