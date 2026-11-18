//! `MeasurementsLayer` — `tracing_subscriber::Layer` subscribed to
//! `recon::measurements.*` events. Maintains in-memory bounded sketches
//! and drains to JSON.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, SystemTime};

use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

use crate::monitoring::measurements::output::*;
use crate::monitoring::measurements::sketch::BoundedReservoir;

struct LayerState {
    opp_cross: HashMap<String, OppCrossPerSlot>, // keyed by slot_id
    conservative_gate_global_counterfactual: BoundedReservoir,
    conservative_gate_blocked_total: u64,
    conservative_gate_per_slug: HashMap<String, BlockedMergeBuffer>,
    cooldown_gate_blocked_total: u64,
    cooldown_gate_remaining: BoundedReservoir,
    bilateral_imbalance_abs: BoundedReservoir,
    sigma_clamp_hits: u64,
    redeem_seen: HashSet<String>,
    started_at: SystemTime,
}

impl Default for LayerState {
    fn default() -> Self {
        Self {
            opp_cross: HashMap::new(),
            conservative_gate_global_counterfactual: BoundedReservoir::default(),
            conservative_gate_blocked_total: 0,
            conservative_gate_per_slug: HashMap::new(),
            cooldown_gate_blocked_total: 0,
            cooldown_gate_remaining: BoundedReservoir::default(),
            bilateral_imbalance_abs: BoundedReservoir::default(),
            sigma_clamp_hits: 0,
            redeem_seen: HashSet::new(),
            started_at: SystemTime::now(),
        }
    }
}

#[derive(Default)]
struct BlockedMergeBuffer {
    paired_sh: Vec<f64>,
    sum_p_at_block: Vec<f64>,
    last_event_at: Option<SystemTime>,
}

struct Inner {
    out_path: PathBuf,
    git_sha: String,
    state: Mutex<LayerState>,
    drain_failed_counter: AtomicU64,
    #[allow(dead_code)]
    seed: u64,
    /// F-RDM aging window: per-slug `BlockedMergeBuffer` entries with no
    /// `redeem_seen` mark are dropped after `retention` of inactivity.
    retention: Duration,
}

/// Default retention for unresolved-slug entries (24 h). Matches the
/// expected upper bound for a market to settle (oracle p99 ~926 s).
const DEFAULT_RETENTION_SECS: u64 = 86_400;

#[derive(Clone)]
pub struct MeasurementsLayer {
    inner: Arc<Inner>,
}

impl MeasurementsLayer {
    pub fn new(out_path: PathBuf, git_sha: String, seed: u64) -> Self {
        Self::new_with_retention(out_path, git_sha, seed, DEFAULT_RETENTION_SECS)
    }

    /// Variant of [`new`] with a configurable retention window for
    /// unresolved-slug entries (F-RDM memory aging). Mostly useful for
    /// tests; production should call [`new`] which uses the 24 h default.
    pub fn new_with_retention(
        out_path: PathBuf,
        git_sha: String,
        seed: u64,
        retention_secs: u64,
    ) -> Self {
        let state = LayerState {
            conservative_gate_global_counterfactual: BoundedReservoir::new(seed),
            cooldown_gate_remaining: BoundedReservoir::new(seed.wrapping_add(1)),
            bilateral_imbalance_abs: BoundedReservoir::new(seed.wrapping_add(2)),
            started_at: SystemTime::now(),
            ..Default::default()
        };
        Self {
            inner: Arc::new(Inner {
                out_path,
                git_sha,
                state: Mutex::new(state),
                drain_failed_counter: AtomicU64::new(0),
                seed,
                retention: Duration::from_secs(retention_secs),
            }),
        }
    }

    /// F-RDM memory aging — drop `conservative_gate_per_slug` entries
    /// that haven't been settled (no REDEEM seen) and have been idle
    /// longer than `retention`. Caller must NOT hold the state lock when
    /// invoking this; the function takes the lock itself, collects
    /// candidates, releases the lock, and only then emits warns to avoid
    /// re-entrant deadlock through the `recon::measurements.*` Layer.
    fn age_unresolved_slugs(&self) {
        let aged_out: Vec<String> = {
            let mut state = self.inner.state.lock().unwrap();
            let now = SystemTime::now();
            let retention = self.inner.retention;
            let candidates: Vec<String> = state
                .conservative_gate_per_slug
                .iter()
                .filter(|(slug, buf)| {
                    !state.redeem_seen.contains(slug.as_str())
                        && buf
                            .last_event_at
                            .map(|t| {
                                now.duration_since(t)
                                    .map(|d| d > retention)
                                    .unwrap_or(false)
                            })
                            .unwrap_or(false)
                })
                .map(|(s, _)| s.clone())
                .collect();
            for slug in &candidates {
                state.conservative_gate_per_slug.remove(slug);
            }
            candidates
        };

        // Lock dropped — safe to emit warns. Re-entry through on_event
        // for the new target acquires a fresh lock.
        for slug in &aged_out {
            tracing::warn!(
                target: "recon::measurements.aged_unresolved_slug",
                slug = %slug,
                "aged out unresolved slug after retention window"
            );
        }
    }

    /// Drain to JSON. On disk-full / IO error, logs an `error!` and
    /// increments `drain_failed_counter`; never panics.
    pub fn drain(&self) {
        let snapshot = self.snapshot();
        match serde_json::to_string_pretty(&snapshot) {
            Ok(s) => {
                if let Err(e) = std::fs::write(&self.inner.out_path, s) {
                    tracing::error!(
                        target: "recon::measurements.drain_failed",
                        error = %e,
                        path = %self.inner.out_path.display(),
                        "drain failed (IO error); continuing without panic"
                    );
                    self.inner
                        .drain_failed_counter
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => {
                tracing::error!(
                    target: "recon::measurements.drain_failed",
                    error = %e,
                    "drain failed (serialization error)"
                );
                self.inner
                    .drain_failed_counter
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> MeasurementsOutputV1 {
        let s = self.inner.state.lock().unwrap();

        let now = SystemTime::now();
        let started_at_utc = system_time_to_utc(s.started_at);
        let ended_at_utc = system_time_to_utc(now);

        let (cf_p50, cf_p99) = s
            .conservative_gate_global_counterfactual
            .percentiles()
            .map(|(a, b)| (Some(a), Some(b)))
            .unwrap_or((None, None));

        let (cd_p50, cd_p99) = s
            .cooldown_gate_remaining
            .percentiles()
            .map(|(a, b)| (Some(a), Some(b)))
            .unwrap_or((None, None));

        let (bi_p50, bi_p99) = s
            .bilateral_imbalance_abs
            .percentiles()
            .map(|(a, b)| (Some(a), Some(b)))
            .unwrap_or((None, None));

        // Compute redeem_join: per-slug residual_pnl from blocked merges
        // that have seen a REDEEM.
        let mut redeem_join = vec![];
        let mut unresolved_slugs = vec![];
        for (slug, buf) in &s.conservative_gate_per_slug {
            if s.redeem_seen.contains(slug) {
                let mut residuals: Vec<f64> = buf
                    .paired_sh
                    .iter()
                    .zip(&buf.sum_p_at_block)
                    .map(|(&sh, &sp)| sh * (1.0 - sp))
                    .collect();
                residuals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let p50 = if residuals.is_empty() {
                    None
                } else {
                    Some(residuals[residuals.len() / 2])
                };
                let p99_idx =
                    ((residuals.len() as f64) * 0.99).min(residuals.len() as f64 - 1.0) as usize;
                let p99 = residuals.get(p99_idx).copied();
                redeem_join.push(ConservativeGateRedeemJoin {
                    slug: slug.clone(),
                    blocked_merges: buf.paired_sh.len() as u64,
                    residual_pnl_p50_usd: p50,
                    residual_pnl_p99_usd: p99,
                });
            } else {
                unresolved_slugs.push(slug.clone());
            }
        }

        MeasurementsOutputV1 {
            schema_version: 1,
            git_sha: self.inner.git_sha.clone(),
            started_at_utc,
            ended_at_utc,
            opp_cross: OppCrossSection {
                per_slot: s.opp_cross.values().cloned().collect(),
            },
            conservative_gate: ConservativeGateSection {
                global: ConservativeGateGlobal {
                    blocked_count: s.conservative_gate_blocked_total,
                    counterfactual_pnl_p50_usd: cf_p50,
                    counterfactual_pnl_p99_usd: cf_p99,
                },
                redeem_join,
                unresolved_slugs,
            },
            cooldown_gate: CooldownGateSection {
                blocked_count: s.cooldown_gate_blocked_total,
                remaining_secs_p50: cd_p50,
                remaining_secs_p99: cd_p99,
            },
            bilateral_imbalance: BilateralImbalanceSection {
                samples: s.bilateral_imbalance_abs.count(),
                abs_p50_sh: bi_p50,
                abs_p99_sh: bi_p99,
            },
            sigma_clamp_hits: s.sigma_clamp_hits,
            drain_failures: self.inner.drain_failed_counter.load(Ordering::Relaxed),
        }
    }
}

fn system_time_to_utc(t: SystemTime) -> String {
    use chrono::{DateTime, Utc};
    let dt: DateTime<Utc> = t.into();
    dt.to_rfc3339()
}

impl<S> Layer<S> for MeasurementsLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let target = event.metadata().target();
        if !target.starts_with("recon::measurements.") {
            return;
        }

        // F-RDM memory aging: prune stale unresolved-slug buffers before
        // taking the state lock. `age_unresolved_slugs` manages its own
        // lock and emits any warns AFTER releasing the guard, so the
        // re-entrant warn (`aged_unresolved_slug` matches the prefix)
        // doesn't deadlock when this Layer re-enters via `on_event`.
        self.age_unresolved_slugs();

        let mut visitor = FieldExtractor::default();
        event.record(&mut visitor);

        let mut s = self.inner.state.lock().unwrap();

        match target {
            "recon::measurements.opp_cross_summary" => {
                let slot_id = visitor.slot_id.clone().unwrap_or_default();
                let attempts = visitor.attempts.unwrap_or(0);
                let suppressed = visitor.suppressed.unwrap_or(0);
                s.opp_cross
                    .entry(slot_id.clone())
                    .or_insert(OppCrossPerSlot {
                        slot_id: slot_id.clone(),
                        attempts: 0,
                        suppressed: 0,
                        pnl_p50_usd: None,
                        pnl_p99_usd: None,
                    });
                if let Some(slot) = s.opp_cross.get_mut(&slot_id) {
                    slot.attempts = attempts;
                    slot.suppressed = suppressed;
                }
            }
            "recon::measurements.conservative_gate" => {
                if let (Some(slug), Some(paired_sh), Some(sum_p)) =
                    (visitor.slug.as_deref(), visitor.paired_sh, visitor.sum_p)
                {
                    s.conservative_gate_blocked_total += 1;
                    let counterfactual = paired_sh * (1.0 - sum_p);
                    s.conservative_gate_global_counterfactual
                        .push(counterfactual);
                    let buf = s
                        .conservative_gate_per_slug
                        .entry(slug.to_string())
                        .or_insert(BlockedMergeBuffer::default());
                    buf.paired_sh.push(paired_sh);
                    buf.sum_p_at_block.push(sum_p);
                    buf.last_event_at = Some(SystemTime::now());
                }
            }
            "recon::measurements.cooldown_gate" => {
                if let Some(remaining) = visitor.remaining_secs {
                    s.cooldown_gate_blocked_total += 1;
                    s.cooldown_gate_remaining.push(remaining);
                }
            }
            "recon::measurements.bilateral_imbalance" => {
                if let Some(value) = visitor.running_imbalance_sh {
                    s.bilateral_imbalance_abs.push(value.abs());
                }
            }
            "recon::measurements.sigma_clamp" => {
                s.sigma_clamp_hits += 1;
            }
            "recon::measurements.redeem" => {
                if let Some(slug) = visitor.slug.as_deref() {
                    s.redeem_seen.insert(slug.to_string());
                }
            }
            _ => {} // unknown target — drop silently
        }
    }
}

#[derive(Default)]
struct FieldExtractor {
    slot_id: Option<String>,
    slug: Option<String>,
    attempts: Option<u64>,
    suppressed: Option<u64>,
    paired_sh: Option<f64>,
    sum_p: Option<f64>,
    remaining_secs: Option<f64>,
    running_imbalance_sh: Option<f64>,
}

impl FieldExtractor {
    fn assign_f64(&mut self, name: &str, value: f64) {
        match name {
            "paired_sh" => self.paired_sh = Some(value),
            "sum_p" => self.sum_p = Some(value),
            "remaining_secs" => self.remaining_secs = Some(value),
            "running_imbalance_sh" => self.running_imbalance_sh = Some(value),
            _ => {}
        }
    }
}

impl tracing::field::Visit for FieldExtractor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "slot_id" => self.slot_id = Some(value.to_string()),
            "slug" => self.slug = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match field.name() {
            "attempts" => self.attempts = Some(value),
            "suppressed" => self.suppressed = Some(value),
            // Bridge to f64 fields if a u64 literal was used.
            other => self.assign_f64(other, value as f64),
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        match field.name() {
            "attempts" if value >= 0 => self.attempts = Some(value as u64),
            "suppressed" if value >= 0 => self.suppressed = Some(value as u64),
            other => self.assign_f64(other, value as f64),
        }
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.assign_f64(field.name(), value);
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        // Defensive fallback: tracing macros sometimes route numeric values
        // through Debug. Try to parse as f64 for the f64 fields.
        let name = field.name();
        match name {
            "paired_sh" | "sum_p" | "remaining_secs" | "running_imbalance_sh" => {
                let s = format!("{:?}", value);
                if let Ok(parsed) = s.parse::<f64>() {
                    self.assign_f64(name, parsed);
                }
            }
            _ => {}
        }
    }
}
