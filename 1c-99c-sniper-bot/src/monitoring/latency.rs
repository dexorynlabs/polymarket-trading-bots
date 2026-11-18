//! Stage-timer instrumentation for the hot path.
//!
//! Purpose: measure before/after for latency optimizations without polluting
//! the signal the way `tracing::info!` would. Each stage owns an
//! `hdrhistogram::Histogram<u64>` in nanoseconds. Recording is a single
//! `record_saturating` call behind a `Mutex<_>`; the dump task snapshots
//! every `dump_every` and emits a structured line via `tracing::info!`.
//!
//! The stage set is fixed at startup (no dynamic insert on the hot path) so
//! `record_ns` is a hashmap lookup of a `&'static str` against a prebuilt map.
//!
//! Intended stages (wire sites):
//!   • `ws_parse`       — time in `parse_events` + `apply_snapshot/change` (clob_ws)
//!   • `registry_update`— `BookRegistry::update` including broadcast send
//!   • `quoter_wake`    — from `BookUpdate.ts_micros` to start of quoter arm body
//!   • `opp_cross_scan` — quoter opp-cross predicate + sizer (pre-submit)
//!   • `reconcile`      — end-to-end reconcile call (no cmd_tx send inside)
//!   • `cmd_dispatch`   — submitter `process_command` including ClobClient call
//!   • `fill_process`   — runtime fills-consumer `process_resting_fill`
//!
//! Non-hot-path callers (health, gamma poll) intentionally NOT instrumented.

use hdrhistogram::Histogram;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// Fixed stage set. New stages → add here, rebuild. Keeps `record_ns` lookup
/// deterministic and avoids lock-contention on a growable map.
pub const STAGES: &[&str] = &[
    "ws_parse",
    "registry_update",
    "ladder_fetch",
    "direct_scan",
    "quoter_wake",
    "opp_cross_scan",
    "reconcile",
    "cmd_dispatch",
    "fill_process",
    "bs_decision_total_us",
    "bs_fair_compute_us",
    "gap_threshold_eval_us",
    "safety_rail_eval_us",
    "signer_sign_us",
    "clob_post_rtt_us",
];

static REGISTRY: OnceLock<HashMap<&'static str, Mutex<Histogram<u64>>>> = OnceLock::new();

fn registry() -> &'static HashMap<&'static str, Mutex<Histogram<u64>>> {
    REGISTRY.get_or_init(|| {
        let mut m = HashMap::with_capacity(STAGES.len());
        for &s in STAGES {
            // 1 ns .. 60 s, 3 sig figs. ~2 MB per histogram, fine for 7 stages.
            let h = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
                .expect("valid histogram bounds");
            m.insert(s, Mutex::new(h));
        }
        m
    })
}

/// Record a sample. `stage` must be one of `STAGES`; unknown names are dropped
/// silently (prefer compile-time constants at call sites).
#[inline]
pub fn record_ns(stage: &'static str, ns: u64) {
    if let Some(h) = registry().get(stage) {
        // `record_saturating` clamps overlong values to the top bucket rather
        // than returning an error — the right choice on a hot path.
        if let Ok(mut g) = h.lock() {
            g.saturating_record(ns);
        }
    }
}

/// Convenience: record the elapsed time since `start` (a `std::time::Instant`).
#[inline]
pub fn record_elapsed(stage: &'static str, start: std::time::Instant) {
    record_ns(
        stage,
        start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
    );
}

/// RAII scope timer. Records on Drop — handy for functions with many early
/// returns. Usage: `let _t = ScopeTimer::new("reconcile");` at the top of
/// the function.
pub struct ScopeTimer {
    stage: &'static str,
    start: std::time::Instant,
}

impl ScopeTimer {
    #[inline]
    pub fn new(stage: &'static str) -> Self {
        Self {
            stage,
            start: std::time::Instant::now(),
        }
    }
}

impl Drop for ScopeTimer {
    #[inline]
    fn drop(&mut self) {
        record_elapsed(self.stage, self.start);
    }
}

/// Snapshot of one stage for logging. ns, so human-readable formatter can
/// divide as it sees fit.
#[derive(Clone, Copy, Debug)]
pub struct StageSnapshot {
    pub count: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

pub fn snapshot_stage(stage: &'static str) -> Option<StageSnapshot> {
    let h = registry().get(stage)?.lock().ok()?;
    #[allow(clippy::len_zero)]
    if h.len() == 0 {
        // `Histogram` has no `is_empty()`; `len()` returns sample count.
        return None;
    }
    Some(StageSnapshot {
        count: h.len(),
        p50_ns: h.value_at_quantile(0.50),
        p90_ns: h.value_at_quantile(0.90),
        p99_ns: h.value_at_quantile(0.99),
        max_ns: h.max(),
    })
}

/// Clear every stage. Useful for scoped benchmarks (baseline vs fix).
pub fn reset_all() {
    for &s in STAGES {
        if let Some(h) = registry().get(s) {
            if let Ok(mut g) = h.lock() {
                g.reset();
            }
        }
    }
}

/// Periodic dump task. Logs one `latency_snapshot` event per interval with
/// fields per stage. Runs until the tokio runtime is shut down (abort-safe).
pub async fn run_dump_task(dump_every: Duration, reset_on_dump: bool) {
    let mut tick = tokio::time::interval(dump_every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick so the first dump has real samples.
    tick.tick().await;
    loop {
        tick.tick().await;
        for &stage in STAGES {
            if let Some(snap) = snapshot_stage(stage) {
                tracing::info!(
                    target: "latency",
                    stage,
                    count = snap.count,
                    p50_us = snap.p50_ns as f64 / 1000.0,
                    p90_us = snap.p90_ns as f64 / 1000.0,
                    p99_us = snap.p99_ns as f64 / 1000.0,
                    max_us = snap.max_ns as f64 / 1000.0,
                    "latency_snapshot",
                );
            }
        }
        if reset_on_dump {
            reset_all();
        }
    }
}

#[cfg(test)]
mod tests {
    //! The histograms are global static state, so these tests serialize
    //! against each other and against any other test in the binary that
    //! records to the same stages (e.g. `handle_text_message` unit tests
    //! writing into `ws_parse`). Each test uses its own stage to avoid
    //! cross-test races, and we pick a stage no production call site
    //! touches.
    use super::*;
    use std::sync::Mutex;
    use std::time::Instant;

    // Held for the full test body so two latency tests never run concurrently.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn record_and_snapshot_roundtrip() {
        let _g = TEST_LOCK.lock().unwrap();
        // `quoter_wake` is recorded only in prod code paths the unit tests
        // don't touch, so it's safe to isolate on.
        reset_all();
        for i in 0..1000 {
            record_ns("quoter_wake", 100 + i);
        }
        let s = snapshot_stage("quoter_wake").expect("samples");
        assert!(s.count >= 1000);
        assert!(s.p50_ns >= 100);
        assert!(s.p99_ns >= s.p50_ns);
        assert!(s.max_ns >= s.p99_ns);
    }

    #[test]
    fn unknown_stage_is_silent_drop() {
        let _g = TEST_LOCK.lock().unwrap();
        record_ns("definitely_not_a_stage", 42);
        record_ns("reconcile", 500);
        assert!(snapshot_stage("definitely_not_a_stage").is_none());
    }

    #[test]
    fn record_elapsed_captures_nonzero() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let t0 = Instant::now();
        std::thread::sleep(std::time::Duration::from_micros(50));
        record_elapsed("opp_cross_scan", t0);
        let s = snapshot_stage("opp_cross_scan").expect("sample");
        assert!(s.max_ns >= 1_000);
    }

    #[test]
    fn snapshot_none_when_stage_has_no_samples() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        assert!(snapshot_stage("fill_process").is_none());
    }

    #[test]
    fn stages_const_includes_all_stage_names() {
        for s in &[
            "bs_decision_total_us",
            "bs_fair_compute_us",
            "gap_threshold_eval_us",
            "safety_rail_eval_us",
            "signer_sign_us",
            "clob_post_rtt_us",
        ] {
            assert!(STAGES.contains(s), "STAGES missing {}", s);
        }
    }
}
