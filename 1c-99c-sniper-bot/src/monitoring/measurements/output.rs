//! `MeasurementsOutputV1` — schema-locked JSON drain output (Phase 3 §5.1, §7.4).

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MeasurementsOutputV1 {
    pub schema_version: u32,
    pub git_sha: String,
    pub started_at_utc: String,
    pub ended_at_utc: String,
    pub opp_cross: OppCrossSection,
    pub conservative_gate: ConservativeGateSection,
    pub cooldown_gate: CooldownGateSection,
    pub bilateral_imbalance: BilateralImbalanceSection,
    pub sigma_clamp_hits: u64,
    pub drain_failures: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OppCrossSection {
    pub per_slot: Vec<OppCrossPerSlot>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OppCrossPerSlot {
    pub slot_id: String,
    pub attempts: u64,
    pub suppressed: u64,
    pub pnl_p50_usd: Option<f64>,
    pub pnl_p99_usd: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConservativeGateSection {
    pub global: ConservativeGateGlobal,
    pub redeem_join: Vec<ConservativeGateRedeemJoin>,
    pub unresolved_slugs: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConservativeGateGlobal {
    pub blocked_count: u64,
    pub counterfactual_pnl_p50_usd: Option<f64>,
    pub counterfactual_pnl_p99_usd: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConservativeGateRedeemJoin {
    pub slug: String,
    pub blocked_merges: u64,
    pub residual_pnl_p50_usd: Option<f64>,
    pub residual_pnl_p99_usd: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CooldownGateSection {
    pub blocked_count: u64,
    pub remaining_secs_p50: Option<f64>,
    pub remaining_secs_p99: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BilateralImbalanceSection {
    pub samples: u64,
    pub abs_p50_sh: Option<f64>,
    pub abs_p99_sh: Option<f64>,
}
