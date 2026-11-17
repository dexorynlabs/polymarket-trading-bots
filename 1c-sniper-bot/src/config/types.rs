//! Leaf configuration types shared by polymarket-client modules and the
//! strategy crates that consume them.
//!
//! These types are pure data (only depend on `std`, `serde`, and
//! [`crate::types::PerCoin`]) — no behaviour beyond `Default` and
//! a handful of constants/helpers. The umbrella `Config` struct, the
//! strategy-specific `preset_951bd_*`, and the TOML overlay machinery
//! all stay in the strategy crate (`recon::config`).
//!
//! See `docs/superpowers/specs/2026-05-20-polymarket-client-workspace-extraction-design.md`
//! §4.1 for the rationale behind splitting only the leaf types.

use crate::types::PerCoin;
use serde::{Deserialize, Serialize};

/// Run mode: paper-mode replays/simulation vs. live trading.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Paper,
    Live,
}

/// The four crypto-updown market families recon supports today.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MarketFamily {
    Fivem,
    Fifteenm,
    OnehourEt,
}

impl MarketFamily {
    pub const ALL: [MarketFamily; 3] = [
        MarketFamily::Fivem,
        MarketFamily::Fifteenm,
        MarketFamily::OnehourEt,
    ];
}

/// σ estimator source.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SigmaSource {
    /// Pin σ to a per-coin constant. Units: bps/√sec = dimensionless per √sec;
    /// the fair-value model multiplies by √τ (τ in seconds) to get a unitless
    /// `d`. See `crate::polymarket::fair`.
    /// Used by `preset_951bd` with 0.74 bps/√sec = 7.4e-5 for BTC.
    Constant(PerCoin<f64>),
    /// Legacy: realised 1-s stdev × per-coin uplift. Ring-buffer estimator
    /// with `window_samples` samples.
    RealizedUplift {
        window_samples: usize,
        uplift: PerCoin<f64>,
    },
    /// Paper-mode replay: synthesizes SpotTicks from a kline
    /// JSONL file at recorded timestamps, then feeds the SigmaEstimator
    /// identically to the live path.
    Fixture {
        klines_path: std::path::PathBuf,
        window_samples: usize,
        uplift: PerCoin<f64>,
    },
}

/// Collateral asset backing the bot's Polymarket positions. V1 CLOB uses
/// `USDC.e` (bridged USDC at `0x2791…4174`). V2 CLOB (Apr 28 2026 ~11:00
/// UTC, unix 1777374000) uses `pUSD`. Operators flip this field at cutover
/// as part of the V2 runbook (`docs/runbooks/clob-v2-cutover.md`).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CollateralAsset {
    UsdcE,
    Pusd,
}

impl Default for CollateralAsset {
    fn default() -> Self {
        Self::UsdcE
    }
}

/// Polygon mainnet addresses used for EIP-712 signing and ERC-1155 tracking.
/// V1 defaults are pinned; V2 addresses will be published by Polymarket
/// around the cutover — operators set them via `Config::contracts` when the
/// runbook fires.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ContractAddresses {
    pub exchange: String,
    pub ctf: String,
    pub collateral_erc20: String,
}

impl ContractAddresses {
    /// V2 mainnet defaults (post-2026-04-28 cutover).
    /// Standard exchange — recon does not trade neg-risk markets so the
    /// neg-risk exchange address is not hardcoded here. CTF and NegRiskAdapter
    /// are unchanged from V1 per Polymarket's V2 migration doc.
    pub fn v2_defaults() -> Self {
        Self {
            exchange: "0xE111180000d2663C0091e4f400237545B87B996B".into(), // V2 std Exchange
            ctf: "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045".into(), // ConditionalTokens (unchanged)
            collateral_erc20: "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB".into(), // pUSD
        }
    }
}

/// Resolution-poll configuration (post-market_end oracle wait).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolveCfg {
    /// Delay between `market_end_ts` and first fetch attempt.
    pub resolve_delay_secs: u64,
    /// Interval between retries when the market isn't closed yet.
    pub poll_interval_secs: u64,
    /// Max fetch attempts before giving up.
    pub max_retries: u32,
}

impl Default for ResolveCfg {
    fn default() -> Self {
        // v2 §7.2.6 p90 REDEEM latency = 926 s post-market_end. Oracle
        // typically posts 300-500 s post-end; delay 300 s avoids the futile
        // 0-300 s polling window while keeping the full 900 s retry budget
        // (30 s × 30 retries).
        Self {
            resolve_delay_secs: 300,
            poll_interval_secs: 30,
            max_retries: 30,
        }
    }
}

/// Paper-mode gas-cost calibration. Polygon mainnet average per Polymarket-
/// class tx is ~$0.002 (v2 §7.9). Applied to: order place, order cancel,
/// taker buy, MERGE tx, REDEEM tx. `usd_per_tx = 0.0` disables (legacy
/// behaviour).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct GasCfg {
    pub usd_per_tx: f64,
}

impl Default for GasCfg {
    fn default() -> Self {
        Self { usd_per_tx: 0.002 }
    }
}

/// Paper-mode oracle-disagreement model. Polymarket's updown oracle is
/// Binance-close by construction, but observed agreement is 88-97% (see
/// memory:project_oracle_is_binance). Paper rolls a per-condition-id PRNG
/// seeded from `seed`; with probability `1 - binance_agreement_rate` the
/// winner is flipped before settlement.
///
/// `binance_agreement_rate = 1.0` disables the flip (legacy behaviour).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct OracleCfg {
    pub binance_agreement_rate: f64,
    pub seed: u64,
}

impl Default for OracleCfg {
    fn default() -> Self {
        // Stable hex sentinel (0xC0FFEEBEEF) so per-cid flip verdicts are
        // reproducible across paper reruns.
        Self {
            binance_agreement_rate: 0.92,
            seed: 0x00C0_FFEE_BEEF,
        }
    }
}

/// Submitter-side latency model. Mimics Polygon RPC + Polymarket API
/// round-trip time so paper mode surfaces race conditions the current
/// zero-latency model masks (`CancelBySlugSide`, fill-before-id, etc).
///
/// - `latency_ms`: central sleep before every CLOB call.
/// - `jitter_ms`: uniform jitter width; actual sleep is
///   `latency_ms + uniform(-jitter_ms, +jitter_ms)`.
/// - `seed`: RNG seed for reproducibility across reruns.
///
/// `latency_ms = 0, jitter_ms = 0` disables the sleep entirely.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubmitterCfg {
    pub latency_ms: u64,
    pub jitter_ms: u64,
    pub seed: u64,
}

impl Default for SubmitterCfg {
    fn default() -> Self {
        Self {
            latency_ms: 150,
            jitter_ms: 50,
            seed: 0x5EED_7A37,
        }
    }
}

/// Phase 3 measurements config — controls the `MeasurementsLayer` aggregator
/// + bilateral-imbalance emission cadence (spec §6.5).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MeasurementsCfg {
    #[serde(default = "default_measurements_enabled")]
    pub enabled: bool,
    #[serde(default = "default_bilateral_imbalance_cadence_secs")]
    pub bilateral_imbalance_cadence_secs: u64,
    #[serde(default = "default_measurements_out_dir")]
    pub out_dir: String,
    #[serde(default = "default_unresolved_slug_warn")]
    pub unresolved_slug_warn: bool,
    #[serde(default = "default_unresolved_slug_retention_secs")]
    pub unresolved_slug_retention_secs: u64,
    #[serde(default = "default_sigma_clamp_hits_paper_threshold")]
    pub sigma_clamp_hits_paper_threshold: u64,
    #[serde(default = "default_reservoir_seed")]
    pub reservoir_seed: u64,
}

fn default_measurements_enabled() -> bool {
    true
}
fn default_bilateral_imbalance_cadence_secs() -> u64 {
    5
}
fn default_measurements_out_dir() -> String {
    "data".to_string()
}
fn default_unresolved_slug_warn() -> bool {
    true
}
fn default_unresolved_slug_retention_secs() -> u64 {
    86_400
}
fn default_sigma_clamp_hits_paper_threshold() -> u64 {
    10
}
fn default_reservoir_seed() -> u64 {
    0x52454e9a
}

impl Default for MeasurementsCfg {
    fn default() -> Self {
        Self {
            enabled: default_measurements_enabled(),
            bilateral_imbalance_cadence_secs: default_bilateral_imbalance_cadence_secs(),
            out_dir: default_measurements_out_dir(),
            unresolved_slug_warn: default_unresolved_slug_warn(),
            unresolved_slug_retention_secs: default_unresolved_slug_retention_secs(),
            sigma_clamp_hits_paper_threshold: default_sigma_clamp_hits_paper_threshold(),
            reservoir_seed: default_reservoir_seed(),
        }
    }
}

/// Live-readiness gate configuration (spec §F-LR).
///
/// Used by `verify_live_readiness_artifact` to bound artifact freshness and
/// retention. `clob_rate_limit_safety_factor` is the scaling applied to
/// the operator-measured CLOB rate-limit observation when computing the
/// runtime's safe submit cadence (Task 24's `validate_live_config`).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LiveReadinessCfg {
    #[serde(default = "default_artifact_max_age_secs")]
    pub artifact_max_age_secs: u64,
    #[serde(default = "default_clob_rate_limit_safety_factor")]
    pub clob_rate_limit_safety_factor: f64,
    #[serde(default = "default_artifact_retention_days")]
    pub artifact_retention_days: u64,
}

fn default_artifact_max_age_secs() -> u64 {
    86_400
}
fn default_clob_rate_limit_safety_factor() -> f64 {
    0.5
}
fn default_artifact_retention_days() -> u64 {
    30
}

impl Default for LiveReadinessCfg {
    fn default() -> Self {
        Self {
            artifact_max_age_secs: default_artifact_max_age_secs(),
            clob_rate_limit_safety_factor: default_clob_rate_limit_safety_factor(),
            artifact_retention_days: default_artifact_retention_days(),
        }
    }
}

/// G6 (Phase 1 cutover): rolling spot-move kill-switch config.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct SpotMoveKillCfg {
    /// G6 (Phase 1 cutover): kill-all on fast spot moves.
    /// Default disabled; explicitly enabled in live overlays.
    pub enabled: bool,
    pub threshold_bps: u32,
    pub window_secs: u64,
    pub pause_secs: u64,
}

impl Default for SpotMoveKillCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            threshold_bps: 30,
            window_secs: 10,
            pause_secs: 60,
        }
    }
}

/// `exec::guards` configuration: §7.1 exposure cap, §7.2 adverse-selection
/// streak, §7.3 balance floor, §7.4 velocity kill. See spec
/// docs/superpowers/specs/2026-04-27-recon-951bd-alignment-design.md §4.
///
/// Phase 4 of the recon ↔ 951bd alignment plan implements §7.1 + §7.3 +
/// `clear_slug` only; the §7.2 / §7.4 fields are present so Phase 5 can extend
/// without refactoring callers or callers' configs.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct GuardsCfg {
    /// Global runtime cap on total in-flight bid notional, summed across
    /// all (slug, side) pairs in `committed_per_side`. Mirrors
    /// `live.safety.cap_usd` — `runtime::build_inner` copies that value
    /// here in live mode. Paper-mode default `1e9` (effectively
    /// unbounded) so existing tests preserve their expectations. The
    /// per-side cap (`exposure_cap_per_side_usd`) and this global cap are
    /// both enforced at admission; the *tighter* of the two binds for
    /// any given placement.
    #[serde(default = "default_cap_usd")]
    pub cap_usd: f64,
    pub exposure_cap_per_side_usd: f64,
    /// **F17 #6 (strategy doc §16 cap #1, 2026-05-11):** per-slot directional
    /// inventory cap in USD — sum of reservations across both `Side::Up` and
    /// `Side::Down` for any single `slug`. Distinct from `cap_usd` (global
    /// cross-slot) and `exposure_cap_per_side_usd` (per-(slug, side)).
    ///
    /// `None` (default) disables the per-slot cap — preserves legacy
    /// behaviour where only `cap_usd` + `exposure_cap_per_side_usd` bind.
    /// V1 multi-coin deployment sets this to `$50` while `cap_usd = $300`
    /// (matches strategy §16 row 1 + row 2).
    #[serde(default)]
    pub max_per_slot_total_usd: Option<f64>,
    /// Optional, price-independent per-`(slug, side)` SHARE-COUNT cap (units =
    /// shares), mirroring `exposure_cap_per_side_usd` (which is USD). Caps each
    /// side independently; the lagging/repair side stays below the cap while the
    /// slot is imbalanced, so repair is never blocked (coverage-safe). Distinct
    /// from the USD caps — equal $ ≠ equal shares on a directional slot — and
    /// from a both-sides-summed share cap (which would freeze repair).
    ///
    /// `None` (default) disables the cap — no behaviour change.
    #[serde(default)]
    pub exposure_cap_per_side_shares: Option<f64>,
    pub balance_floor_usd: f64,
    pub adverse_selection_threshold: u32,
    pub velocity_window_secs: u64,
    pub velocity_drop_threshold_usd: f64,
    pub balance_poll_interval_secs: u64,
    /// F-E4: AdverseSelection / Merge halts auto-clear after this many
    /// seconds with no further fills against the halted slug. Default 60.
    /// Conservative — short enough to recover within a 5m slot, long
    /// enough to ride out the original adverse-selection trigger.
    /// Set to 0 to disable auto-recovery (legacy behaviour: halt persists
    /// until market reap).
    #[serde(default = "default_unhalt_after_secs")]
    pub unhalt_after_secs: u64,
    /// F6 (Phase 1 cutover): CLOB API failure circuit-breaker.
    /// Trips when failed submitter calls exceed `clob_failure_threshold_per_min`
    /// in any 60 s rolling window. Pause holds for `clob_failure_pause_secs`.
    /// 0 disables the gate.
    #[serde(default = "default_clob_failure_threshold")]
    pub clob_failure_threshold_per_min: u32,
    #[serde(default = "default_clob_failure_pause_secs")]
    pub clob_failure_pause_secs: u64,
    /// F5 (Phase 1 cutover): rolling 24 h cumulative PnL drawdown.
    /// `daily_drawdown_threshold_usd` is positive; trip when cumulative
    /// loss exceeds it (i.e. cum_pnl < -threshold). Pause holds
    /// `daily_drawdown_pause_secs`. 0 disables.
    #[serde(default = "default_daily_drawdown_threshold")]
    pub daily_drawdown_threshold_usd: f64,
    #[serde(default = "default_daily_drawdown_pause_secs")]
    pub daily_drawdown_pause_secs: u64,
    /// Per-cid position cap (spec §5.1). Defaults at Phase 1 thresholds.
    #[serde(default)]
    pub per_cid_cap: PerCidCapCfg,
    /// Safety rail: hard cap on order count per (slug, side)
    /// within a slot. `None` = unbounded (recon default — quoter posts many
    /// rungs). `Some(n)` = block admission once `n` orders admitted for this
    /// (slug, side). Counter resets on `clear_slug`.
    #[serde(default)]
    pub max_orders_per_slot: Option<u32>,
    /// Safety rail: hard cap on individual order USD notional
    /// (`price × size_shares` for resting; `max_ask × max_shares` for taker).
    /// `None` = unbounded. Operator-facing — Inv-S3 sanity-gates this against
    /// `cap_usd`.
    #[serde(default)]
    pub per_order_size_cap_usd: Option<f64>,
}

fn default_unhalt_after_secs() -> u64 {
    60
}
fn default_cap_usd() -> f64 {
    1e9
}
fn default_clob_failure_threshold() -> u32 {
    10
}
fn default_clob_failure_pause_secs() -> u64 {
    30
}
fn default_daily_drawdown_threshold() -> f64 {
    30.0
}
fn default_daily_drawdown_pause_secs() -> u64 {
    3600
}

impl Default for GuardsCfg {
    fn default() -> Self {
        Self {
            cap_usd: 1e9,
            exposure_cap_per_side_usd: 75.0,
            max_per_slot_total_usd: None,
            exposure_cap_per_side_shares: None,
            balance_floor_usd: 30.0,
            adverse_selection_threshold: 5,
            velocity_window_secs: 60,
            velocity_drop_threshold_usd: 50.0,
            balance_poll_interval_secs: 5,
            unhalt_after_secs: 60,
            clob_failure_threshold_per_min: 10,
            clob_failure_pause_secs: 30,
            daily_drawdown_threshold_usd: 30.0,
            daily_drawdown_pause_secs: 3600,
            per_cid_cap: PerCidCapCfg::default(),
            max_orders_per_slot: None,
            per_order_size_cap_usd: None,
        }
    }
}

/// Per-cid position cap thresholds (spec §5.1). Two independent axes:
///
/// - **Imbalance** (`|up_shares − down_shares|`): trip → pause new posts on
///   the *larger* side; bilateral guard exempted (asymmetric drain).
/// - **Concentration** (`committed_$` per cid): trip → pause new posts on
///   the *whole* cid; bilateral guard cancels both sides naturally.
///
/// Both axes use trip/resume hysteresis to prevent oscillation when the
/// underlying measure flickers near the threshold.
///
/// Phase 1 defaults are calibrated to `cap_usd = $30`. The Phase 3 overlay
/// (`config/phase3.toml`) overrides at `cap_usd = $250`.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct PerCidCapCfg {
    /// Imbalance trip threshold (shares). When `|up_shares − down_shares| >
    /// imbalance_trip_sh`, the larger side enters the imbalance-pause state.
    /// Default 50.0 (Phase 1: max single-rung-1 fill).
    #[serde(default = "default_per_cid_imbalance_trip_sh")]
    pub imbalance_trip_sh: f64,
    /// Imbalance resume threshold (shares). When the larger side is paused
    /// and `|up_shares − down_shares| < imbalance_resume_sh`, pause clears.
    /// Default 25.0 (= 0.5 × trip).
    #[serde(default = "default_per_cid_imbalance_resume_sh")]
    pub imbalance_resume_sh: f64,
    /// Concentration trip threshold (USD). When the cid's total committed
    /// notional (sum of both sides in `committed_per_side`) exceeds this,
    /// the whole cid enters the concentration-pause state.
    /// Default 24.0 (Phase 1: 80 % of `cap_usd = $30`).
    #[serde(default = "default_per_cid_concentration_trip_usd")]
    pub concentration_trip_usd: f64,
    /// Concentration resume threshold (USD). When the cid is paused and
    /// committed total drops below this, pause clears.
    /// Default 18.0 (= 0.75 × trip).
    #[serde(default = "default_per_cid_concentration_resume_usd")]
    pub concentration_resume_usd: f64,
}

fn default_per_cid_imbalance_trip_sh() -> f64 {
    50.0
}
fn default_per_cid_imbalance_resume_sh() -> f64 {
    25.0
}
fn default_per_cid_concentration_trip_usd() -> f64 {
    24.0
}
fn default_per_cid_concentration_resume_usd() -> f64 {
    18.0
}

impl Default for PerCidCapCfg {
    fn default() -> Self {
        Self {
            imbalance_trip_sh: 50.0,
            imbalance_resume_sh: 25.0,
            concentration_trip_usd: 24.0,
            concentration_resume_usd: 18.0,
        }
    }
}
