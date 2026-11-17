//! Operator TOML. Same shape-safety contract as the earlier taker: everything we own
//! is `deny_unknown_fields`, so a misspelled key is a hard parse error rather
//! than a silent default. `[live.*]` deserializes into the shared
//! `crate::config::LiveConfig`.
//!
//! The earlier knobs are GONE with the machine they configured
//! (POST-TWAP-SPEC §6) — `clip_mode`/`flat_clip_usd` (the tiered/dose clip
//! laws), `slip_mult` (the REV 2 bounded-slippage FAK price),
//! `max_entry_ask_usd` (the cap ceiling), `tick.dose_usd`, and
//! `kline_gap_disarm_ms` (nothing arms any more). Because this section is
//! `deny_unknown_fields`, an old TOML fails loudly at parse rather than
//! silently trading the new strategy under stale numbers — which is the point.
//!
//! Cap validation is STRATEGY-LOCAL: `polymarket-client`'s config validator is
//! generic, and the Inv-12 cap whitelist is retired in this lineage (the
//! SpendLedger enforces the TOML).

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TickburstToml {
    pub strategy: StrategySection,
    pub live: Option<crate::config::LiveConfig>,
}

/// `[strategy.tick]` — the micro-burst trigger (spec §3.5), the ONLY fire
/// path. The shipped values ARE the frozen gate; spec §7 says do not tune
/// them while grading them.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TickSection {
    /// OFF makes this process incapable of firing at all (it still ingests,
    /// records books and writes `Feat` telemetry). ON is the default — there
    /// is no other fire path.
    pub enabled: bool,
    /// Trailing-window Binance-spot volume, in BTC, required to fire.
    pub v_btc: f64,
    /// Minimum share of that volume in the recovery direction, in [0, 1].
    pub lean_min: f64,
    /// Width of the trailing window, in ms. Also the window the `_1s` flow
    /// telemetry sums are taken over (`flow.rs`) — moving it off 1000 makes
    /// those key names approximate.
    pub window_ms: u64,
    /// A held book older than this is not acted on. Default 15s = the
    /// census's own staleness bound (`census_surface.py`'s `STALE_MS`); see
    /// `tick::TickCfg` for why the earlier 500ms would suppress nearly
    /// every measured entry.
    pub max_book_age_ms: u64,
}

impl Default for TickSection {
    fn default() -> Self {
        Self { enabled: true, v_btc: 0.5, lean_min: 0.6, window_ms: 1_000, max_book_age_ms: 15_000 }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StrategySection {
    /// v0 is btc-only; `validate_strategy` rejects anything else.
    pub coin: String,
    /// The entry BAND, inclusive. Spec §3.4 is the equality 1c; the shipped
    /// 0.9c-1.1c is an OPERATOR WIDENING on top of it (2026-08-09), measured
    /// over Aug 7-9 as: one entry moves (a loss either way), nothing added,
    /// nothing lost, the same 5 wins, and no 1.1c ask qualifying at all.
    /// `cards::ask_qualifies` carries the full rationale, including why 2c
    /// and sub-0.9c stay out.
    pub ask_price_min: f64,
    pub ask_price_max: f64,
    /// The FAK's limit price (spec §3.4). Price priority fills at the
    /// standing 1c; the wider limit is race-tail optionality only and is NOT
    /// permission to enter at 2c.
    pub limit_price: f64,
    /// Flat per-fire budget, USD (spec §3.7) — and since the 2026-08-10 sizing
    /// fix it is a budget in the literal sense: a fire commits EXACTLY this
    /// many dollars, because the order is sized `clip_usd / limit_price`
    /// (`sizing::fak_shares`). The ask decides what those dollars come back as
    /// — 1,111 shares at 0.9c, 1,000 at 1c, 909 at 1.1c — never how many go
    /// out. A win at 1c pays ~+$989 net of the C=0.07 taker fee (break-even
    /// 1.07% vs a measured 5/98 in-band win rate).
    pub clip_usd: f64,
    /// Reconstruct the Binance 1s close AT THE BAR BOUNDARY from `@aggTrade`
    /// instead of waiting for the pushed `@kline_1s` (~152ms later), firing
    /// `this` ms after the boundary. `None` (default) keeps the kline path
    /// unchanged. Under this spec klines feed TELEMETRY only, so this knob no
    /// longer touches any decision — it only makes `Feat` rows land earlier.
    pub boundary_close_delay_ms: Option<u64>,
    /// Rolling-24h cap on COMMITTED spend (`size_shares * limit_price` per
    /// fire, never released by the ledger). Re-derived for this gate when a
    /// fire still committed $20: at the census's ~50 entries/day the
    /// earlier $1,200 would have bound at 60 entries — inside one busy
    /// day — so this was set at ~2.5x that. Since the 2026-08-10 sizing fix a
    /// fire commits $10, so the same $2,500 now buys 250 fires/day, i.e. ~5x
    /// the measured rate. Left where it is deliberately: design §5.0 says a
    /// rail must not bind silently, and a LOOSER rail cannot.
    pub daily_spend_cap_usd: f64,
    pub concurrent_exposure_cap_usd: f64,
    /// Max committed exposure (`size_shares * limit_price`) a SINGLE fire may
    /// carry. DECOUPLED from `concurrent_exposure_cap_usd`.
    pub per_order_cap_usd: f64,
    /// Positive magnitude; the rail trips when realized loss reaches -this.
    /// CLOB-settled PnL only — never gross spend (the maker-lineage bug).
    pub realized_loss_kill_usd: f64,
    /// A LATCH, not a rate limiter. The automaton fires at most ONE order per
    /// 300s slot (spec §3.6), so no 60s window can hold more than one send —
    /// a trip is proof of a state-machine fault, not a market condition.
    pub self_fire_rate_per_min: u32,
    /// Drop the held ladders when the book WS has been silent this many ms.
    pub book_stale_disarm_ms: u64,
    /// The trigger (`[strategy.tick]`). A TOML sub-table, so it must be
    /// written after every scalar key in `[strategy]`.
    pub tick: TickSection,
}

impl Default for StrategySection {
    fn default() -> Self {
        Self {
            coin: "btc".into(),
            // The operator band (2026-08-09), NOT the graded 1c equality —
            // `tests/posttwap_parity.rs` pins the graded config explicitly and
            // measures this one as its own cell.
            ask_price_min: 0.009,
            ask_price_max: 0.011,
            limit_price: 0.02,
            clip_usd: 10.0,
            // OFF by default: the kline path stays unchanged unless an
            // operator opts in, and it gates nothing either way.
            boundary_close_delay_ms: None,
            daily_spend_cap_usd: 2_500.0,
            concurrent_exposure_cap_usd: 150.0,
            per_order_cap_usd: 100.0,
            realized_loss_kill_usd: 400.0,
            self_fire_rate_per_min: 20,
            book_stale_disarm_ms: 5_000,
            tick: TickSection::default(),
        }
    }
}

/// Past this the reconstruction is slower than simply waiting for Binance's
/// own pushed kline (p50 152 / p90 176 ms after the bar close), so it can only
/// be an operator mistake.
const BOUNDARY_CLOSE_MAX_DELAY_MS: u64 = 200;

/// Strategy-local invariants. Called by every mode that can send an order.
pub fn validate_strategy(s: &StrategySection) -> anyhow::Result<()> {
    anyhow::ensure!(s.coin == "btc", "v0 supports coin = \"btc\" only, got {:?}", s.coin);
    for (name, px) in [("ask_price_min", s.ask_price_min), ("ask_price_max", s.ask_price_max)] {
        anyhow::ensure!(
            (0.005..=0.10).contains(&px),
            "{name} ({px}) must be in [0.005, 0.10] — spec §3.4 ships the 1c equality and the \
             operator band is 0.9c-1.1c; this strategy's whole economics (break-even ~1% at a \
             ~5% measured win rate) are a cheap-ask fact"
        );
    }
    anyhow::ensure!(
        s.ask_price_min <= s.ask_price_max,
        "ask_price_min ({}) must be <= ask_price_max ({}) — an inverted band admits nothing and \
         the strategy would never fire",
        s.ask_price_min,
        s.ask_price_max
    );
    anyhow::ensure!(
        s.limit_price >= s.ask_price_max,
        "limit_price ({}) must be >= ask_price_max ({}) — the FAK limit is the race-tail bound \
         on what we will pay, never below the dearest ask we are willing to enter at",
        s.limit_price,
        s.ask_price_max
    );
    // Bounded against the CHEAP edge, deliberately: that is the strictest
    // reading, so widening the band cannot silently license a wider limit too.
    anyhow::ensure!(
        s.limit_price <= 4.0 * s.ask_price_min,
        "limit_price ({}) must be <= 4x ask_price_min ({}) — spec §3.4 ships 2c against a 1c \
         ask; a wider limit stops being race-tail optionality and starts buying the sweep that \
         just killed the cheap level",
        s.limit_price,
        s.ask_price_min
    );
    anyhow::ensure!(
        s.clip_usd >= 1.0,
        "clip_usd ({}) must be >= 1.0 — the venue's $1 marketable minimum",
        s.clip_usd
    );
    // The venue's LIMIT floor is 5 SHARES (not $5 of notional). Checked on the
    // count we actually SEND — `clip_usd / limit_price` (`sizing::fak_shares`)
    // — which is the smallest count in play, since `limit_price` is by
    // definition at or above every ask we will enter at.
    let sent_shares = s.clip_usd / s.limit_price;
    anyhow::ensure!(
        sent_shares >= 5.0,
        "clip_usd {} at limit_price {} is {sent_shares} shares — under the venue's 5-share \
         LIMIT minimum",
        s.clip_usd,
        s.limit_price
    );
    if let Some(d) = s.boundary_close_delay_ms {
        anyhow::ensure!(
            d <= BOUNDARY_CLOSE_MAX_DELAY_MS,
            "boundary_close_delay_ms ({d}) must be <= {BOUNDARY_CLOSE_MAX_DELAY_MS} — the pushed \
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
        ("per_order_cap_usd", s.per_order_cap_usd),
        ("concurrent_exposure_cap_usd", s.concurrent_exposure_cap_usd),
        ("daily_spend_cap_usd", s.daily_spend_cap_usd),
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
        "realized_loss_kill_usd ({}) must be > 0 and < daily_spend_cap_usd ({}) — otherwise the \
         spend cap always bites first and the loss kill is inert (the maker-lineage bug)",
        s.realized_loss_kill_usd,
        s.daily_spend_cap_usd
    );
    anyhow::ensure!(s.self_fire_rate_per_min > 0, "self_fire_rate_per_min must be > 0");
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
        "tick.v_btc ({}) must be > 0 — a non-positive volume floor fires on every print inside \
         the band, which is the blanket quiet-channel entry the census measured at −$563 to \
         −$2,954",
        t.v_btc
    );
    anyhow::ensure!(
        t.lean_min > 0.5 && t.lean_min <= 1.0,
        "tick.lean_min ({}) must be in (0.5, 1.0] — at or below 0.5 the 'lean' is not a direction \
         at all (both sides of one print can pass at once, which would also let the placebo \
         mirror and the real gate fire on the same tick), and the graded gate is 0.6",
        t.lean_min
    );
    anyhow::ensure!(
        (1..=TICK_MAX_WINDOW_MS).contains(&t.window_ms),
        "tick.window_ms ({}) must be in [1, {TICK_MAX_WINDOW_MS}]",
        t.window_ms
    );
    anyhow::ensure!(
        t.max_book_age_ms > 0,
        "tick.max_book_age_ms must be > 0 — at 0 no held book is ever fresh enough and nothing \
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
