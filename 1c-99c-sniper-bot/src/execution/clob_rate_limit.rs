//! Operator-supplied CLOB rate-limit observation file format (spec §5.4).

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClobRateLimitObservationV1 {
    pub schema_version: u32,
    pub measured_per_sec: f64,
    pub measured_at_utc: String,
    pub documented_limit_per_sec: f64,
    pub endpoint: String,
    #[serde(default)]
    pub notes: String,
}
