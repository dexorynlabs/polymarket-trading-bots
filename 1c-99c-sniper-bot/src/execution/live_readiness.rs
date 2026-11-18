//! LiveReadinessArtifactV1 — operator-/CI-emitted JSON describing whether
//! every gate to enable live trading has been observed-green for the build
//! identified by `git_sha`. The runtime's `--live` enable path requires a
//! fresh, matching, ci_mode=false artifact (see spec §F-LR / runbook
//! §"live readiness gate").

use crate::config::{LiveReadinessCfg, MeasurementsCfg};
use crate::execution::clob_rate_limit::ClobRateLimitObservationV1;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::PathBuf;
use std::time::SystemTime;

/// Result of one named live-readiness check.
///
/// Schema matches Phase 3 spec §5.4 literal:
/// * `status` — `"pass"` / `"fail"` / `"skip"`. The verifier rejects an
///   artifact unless every check is `"pass"`.
/// * `duration_secs` — wall-clock time of the check (None when not
///   measured, e.g. ci_mode skipped checks).
/// * `detail` — structured JSON payload (None when no extra detail).
///
/// Both `duration_secs` and `detail` are `#[serde(default)]` so older
/// artifacts that lack the fields parse to `None` instead of failing
/// `deny_unknown_fields` shape checks. They serialise out only when
/// populated (`skip_serializing_if = "Option::is_none"`), so the
/// happy-path JSON keeps the spec-listed compact form.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CheckResult {
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

/// All live-readiness checks the operator/CI pipeline must observe-green
/// before enabling live trading. Names track spec §F-LR.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LiveReadinessChecks {
    pub cargo_test: CheckResult,
    pub paper_mode_3x: CheckResult,
    pub clob_rate_limit: CheckResult,
}

/// Top-level artifact. Filename convention:
/// `<measurements.out_dir>/live-readiness-<git_sha>-<unix>.json`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LiveReadinessArtifactV1 {
    pub schema_version: u32,
    pub git_sha: String,
    pub git_branch: String,
    pub generated_at_utc: String,
    pub ci_mode: bool,
    pub checks: LiveReadinessChecks,
}

#[derive(Debug, thiserror::Error)]
pub enum LiveReadinessError {
    #[error(
        "no live-readiness artifact found for git_sha={git_sha} in {dir} \
         (see runbook §live-readiness-gate)"
    )]
    Missing { git_sha: String, dir: String },

    #[error(
        "live-readiness artifact stale: age_secs={age_secs} > max={max_secs} \
         (see runbook §live-readiness-gate)"
    )]
    Stale { age_secs: i64, max_secs: u64 },

    #[error(
        "live-readiness check failed: {check} status={status} detail={detail} \
         (see runbook §live-readiness-gate)"
    )]
    CheckFailed {
        check: String,
        status: String,
        detail: String,
    },

    #[error(
        "live-readiness artifact has ci_mode=true; refusing to enable live \
         (see runbook §live-readiness-gate)"
    )]
    CiMode,

    #[error(
        "live-readiness artifact malformed at {path}: {source} \
         (see runbook §live-readiness-gate)"
    )]
    Malformed {
        path: String,
        #[source]
        source: anyhow::Error,
    },
}

/// Locates the newest `live-readiness-<git_sha>-*.json` under
/// `measurements.out_dir`, parses it, and validates:
///  * `schema_version == 1`
///  * `git_sha` matches
///  * `ci_mode == false`
///  * artifact age < `live_readiness.artifact_max_age_secs`
///  * every `checks.*.status == "pass"`
///
/// Returns the parsed artifact on success.
pub fn verify_live_readiness_artifact(
    measurements: &MeasurementsCfg,
    live_readiness: &LiveReadinessCfg,
    git_sha: &str,
) -> Result<LiveReadinessArtifactV1, LiveReadinessError> {
    let dir = PathBuf::from(&measurements.out_dir);
    let prefix = format!("live-readiness-{}-", git_sha);

    let entries = std::fs::read_dir(&dir).map_err(|_| LiveReadinessError::Missing {
        git_sha: git_sha.to_string(),
        dir: measurements.out_dir.clone(),
    })?;

    let mut newest: Option<(PathBuf, std::time::SystemTime)> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.starts_with(&prefix) || !name.ends_with(".json") {
            continue;
        }
        let mtime = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::UNIX_EPOCH);
        match &newest {
            Some((_, t)) if *t >= mtime => {}
            _ => newest = Some((path, mtime)),
        }
    }

    let path = newest
        .map(|(p, _)| p)
        .ok_or_else(|| LiveReadinessError::Missing {
            git_sha: git_sha.to_string(),
            dir: measurements.out_dir.clone(),
        })?;

    let bytes = std::fs::read(&path).map_err(|e| LiveReadinessError::Malformed {
        path: path.display().to_string(),
        source: e.into(),
    })?;

    let artifact: LiveReadinessArtifactV1 =
        serde_json::from_slice(&bytes).map_err(|e| LiveReadinessError::Malformed {
            path: path.display().to_string(),
            source: e.into(),
        })?;

    if artifact.schema_version != 1 {
        return Err(LiveReadinessError::Malformed {
            path: path.display().to_string(),
            source: anyhow::anyhow!("unsupported schema_version {}", artifact.schema_version),
        });
    }

    if artifact.git_sha != git_sha {
        return Err(LiveReadinessError::Missing {
            git_sha: git_sha.to_string(),
            dir: measurements.out_dir.clone(),
        });
    }

    if artifact.ci_mode {
        return Err(LiveReadinessError::CiMode);
    }

    let generated = DateTime::parse_from_rfc3339(&artifact.generated_at_utc)
        .map_err(|e| LiveReadinessError::Malformed {
            path: path.display().to_string(),
            source: anyhow::anyhow!("generated_at_utc: {e}"),
        })?
        .with_timezone(&Utc);

    let age_secs = Utc::now().signed_duration_since(generated).num_seconds();
    let max_secs = live_readiness.artifact_max_age_secs;
    if age_secs < 0 || (age_secs as u64) > max_secs {
        return Err(LiveReadinessError::Stale { age_secs, max_secs });
    }

    let checks = [
        ("cargo_test", &artifact.checks.cargo_test),
        ("paper_mode_3x", &artifact.checks.paper_mode_3x),
        ("clob_rate_limit", &artifact.checks.clob_rate_limit),
    ];
    for (name, c) in checks.iter() {
        if c.status != "pass" {
            return Err(LiveReadinessError::CheckFailed {
                check: (*name).to_string(),
                status: c.status.clone(),
                detail: c.detail.as_ref().map(|v| v.to_string()).unwrap_or_default(),
            });
        }
    }

    Ok(artifact)
}

/// Arguments for `run_subcommand` (the `--live-readiness` CLI mode).
///
/// `ci_mode=true` skips the cargo-test and paper-mode-3x runs (which would
/// otherwise spawn `cargo test` / `cargo run` recursively under a CI test
/// invocation) and emits an artifact with `ci_mode: true`. The runtime's
/// `verify_live_readiness_artifact` rejects ci_mode=true artifacts for the
/// live-enable gate, so CI-emitted artifacts can never authorise live.
///
/// `skip_cargo_test=true` is a local-dev iteration flag: short-circuits the
/// cargo-test step (records `cargo_test.detail = {"skipped":
/// "skip_cargo_test_flag"}`) but still runs `paper_mode_3x` and
/// `clob_rate_limit`. The artifact's top-level `ci_mode` flag tracks
/// `args.ci_mode` only — so `skip_cargo_test=true, ci_mode=false` still
/// produces a live-eligible artifact. Only set when `cargo test --release`
/// has been verified out-of-band against the same checkout. Precedence:
/// when both flags are set, the cargo_test detail records
/// `skip_cargo_test_flag` (the more specific cause).
///
/// `out_dir` is optional — when `None`, the function uses
/// `Config::default().measurements.out_dir` (`"data"`). Tests that want
/// isolation pass a tempdir.
pub struct SubcommandArgs {
    pub clob_rate_limit_file: PathBuf,
    pub ci_mode: bool,
    pub skip_cargo_test: bool,
    pub config: PathBuf,
    pub fixture: PathBuf,
    pub out_dir: Option<PathBuf>,
}

/// Drive a live-readiness run end-to-end.
///
/// Returns Ok(()) only if every check passes; on any failure (rate-limit
/// breach, malformed observation, cargo/paper-mode failure) returns an
/// error and writes NO artifact (the absence is itself a signal).
///
/// On success writes
/// `<out_dir>/live-readiness-<git_sha>-<unix>.json` and prunes any
/// older `live-readiness-*.json` whose mtime is past
/// `live_readiness.artifact_retention_days`.
pub fn run_subcommand(
    args: SubcommandArgs,
    measurements: &MeasurementsCfg,
    live_readiness: &LiveReadinessCfg,
    binary_name: &str,
) -> anyhow::Result<()> {
    let git_sha = run_git("git rev-parse HEAD")?.trim().to_string();
    let git_branch = run_git("git rev-parse --abbrev-ref HEAD")?
        .trim()
        .to_string();
    let started = Utc::now();

    let cargo_test = if args.skip_cargo_test {
        CheckResult {
            status: "pass".into(),
            duration_secs: None,
            detail: Some(json!({"skipped": "skip_cargo_test_flag"})),
        }
    } else if args.ci_mode {
        CheckResult {
            status: "pass".into(),
            duration_secs: None,
            detail: Some(json!({"skipped": "ci_mode"})),
        }
    } else {
        let t0 = std::time::Instant::now();
        let status = std::process::Command::new("cargo")
            .args(["test", "--release", "--quiet"])
            .status()
            .map_err(|e| anyhow::anyhow!("cargo test spawn: {}", e))?;
        let secs = t0.elapsed().as_secs();
        if status.success() {
            CheckResult {
                status: "pass".into(),
                duration_secs: Some(secs),
                detail: None,
            }
        } else {
            CheckResult {
                status: "fail".into(),
                duration_secs: Some(secs),
                detail: Some(json!({"exit": status.code()})),
            }
        }
    };

    let out_dir_path = args
        .out_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from(&measurements.out_dir));

    let paper_mode_3x = if args.ci_mode {
        CheckResult {
            status: "pass".into(),
            duration_secs: None,
            detail: Some(json!({"skipped": "ci_mode"})),
        }
    } else {
        run_paper_mode_3x(&args, measurements, live_readiness, binary_name)?
    };

    let clob_rate_limit = run_clob_rate_limit_check(&args, live_readiness)?;

    let any_fail = [&cargo_test, &paper_mode_3x, &clob_rate_limit]
        .iter()
        .any(|c| c.status != "pass");
    if any_fail {
        anyhow::bail!(
            "live-readiness checks failed: cargo_test={}, paper_mode_3x={}, clob_rate_limit={}",
            cargo_test.status,
            paper_mode_3x.status,
            clob_rate_limit.status
        );
    }

    let artifact = LiveReadinessArtifactV1 {
        schema_version: 1,
        git_sha: git_sha.clone(),
        git_branch,
        generated_at_utc: started.to_rfc3339(),
        ci_mode: args.ci_mode,
        checks: LiveReadinessChecks {
            cargo_test,
            paper_mode_3x,
            clob_rate_limit,
        },
    };

    std::fs::create_dir_all(&out_dir_path)?;
    let out = out_dir_path.join(format!(
        "live-readiness-{}-{}.json",
        git_sha,
        started.timestamp(),
    ));
    std::fs::write(&out, serde_json::to_string_pretty(&artifact)?)?;
    println!("wrote {}", out.display());

    // Retention: drop artifacts older than `artifact_retention_days`.
    let retention = chrono::Duration::days(live_readiness.artifact_retention_days as i64);
    let cutoff = Utc::now() - retention;
    if let Ok(read_dir) = std::fs::read_dir(&out_dir_path) {
        for entry in read_dir.filter_map(Result::ok) {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("live-readiness-") && name_str.ends_with(".json") {
                if let Ok(meta) = entry.metadata() {
                    if let Ok(modified) = meta.modified() {
                        let modified_dt: DateTime<Utc> = modified.into();
                        if modified_dt < cutoff {
                            let _ = std::fs::remove_file(entry.path());
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn run_clob_rate_limit_check(
    args: &SubcommandArgs,
    live_readiness: &LiveReadinessCfg,
) -> anyhow::Result<CheckResult> {
    let bytes = std::fs::read(&args.clob_rate_limit_file)
        .map_err(|e| anyhow::anyhow!("read {}: {}", args.clob_rate_limit_file.display(), e))?;
    let obs: ClobRateLimitObservationV1 = serde_json::from_slice(&bytes)
        .map_err(|e| anyhow::anyhow!("parse {}: {}", args.clob_rate_limit_file.display(), e))?;
    let safety_factor = live_readiness.clob_rate_limit_safety_factor;
    if obs.measured_per_sec >= obs.documented_limit_per_sec * safety_factor {
        return Ok(CheckResult {
            status: "fail".into(),
            duration_secs: None,
            detail: Some(json!({
                "reason": "measured_per_sec >= documented * safety_factor",
                "measured": obs.measured_per_sec,
                "documented": obs.documented_limit_per_sec,
                "safety_factor": safety_factor,
            })),
        });
    }
    let measured: DateTime<Utc> = DateTime::parse_from_rfc3339(&obs.measured_at_utc)
        .map_err(|e| anyhow::anyhow!("measured_at_utc parse: {}", e))?
        .with_timezone(&Utc);
    if (Utc::now() - measured).num_days() > 7 {
        return Ok(CheckResult {
            status: "fail".into(),
            duration_secs: None,
            detail: Some(json!({"reason": "measured_at_utc > 7d"})),
        });
    }
    Ok(CheckResult {
        status: "pass".into(),
        duration_secs: None,
        detail: Some(json!({
            "measured": obs.measured_per_sec,
            "documented": obs.documented_limit_per_sec,
        })),
    })
}

fn run_paper_mode_3x(
    args: &SubcommandArgs,
    measurements: &MeasurementsCfg,
    _live_readiness: &LiveReadinessCfg,
    binary_name: &str,
) -> anyhow::Result<CheckResult> {
    let mut runs = vec![];
    for i in 0..3 {
        let status = std::process::Command::new("cargo")
            .args([
                "run",
                "--release",
                "--quiet",
                "-p",
                binary_name,
                "--",
                "--paper-mode-replay",
                args.fixture.to_str().unwrap(),
            ])
            .status()
            .map_err(|e| anyhow::anyhow!("paper-mode spawn: {}", e))?;
        if !status.success() {
            return Ok(CheckResult {
                status: "fail".into(),
                duration_secs: None,
                detail: Some(json!({"run": i, "exit": status.code()})),
            });
        }
        let latest = find_latest_measurements_json(&measurements.out_dir)?;
        let m: crate::monitoring::measurements::output::MeasurementsOutputV1 =
            serde_json::from_slice(&std::fs::read(&latest)?)?;
        if !m.conservative_gate.unresolved_slugs.is_empty() {
            return Ok(CheckResult {
                status: "fail".into(),
                duration_secs: None,
                detail: Some(json!({"reason": "unresolved_slugs", "run": i})),
            });
        }
        if m.sigma_clamp_hits > measurements.sigma_clamp_hits_paper_threshold {
            return Ok(CheckResult {
                status: "fail".into(),
                duration_secs: None,
                detail: Some(json!({
                    "reason": "sigma_clamp_hits",
                    "value": m.sigma_clamp_hits,
                })),
            });
        }
        runs.push(json!({
            "jsonl_path": latest.display().to_string(),
            "summary": {"sigma_clamp_hits": m.sigma_clamp_hits},
        }));
    }
    Ok(CheckResult {
        status: "pass".into(),
        duration_secs: None,
        detail: Some(json!({"runs": runs})),
    })
}

fn find_latest_measurements_json(dir: &str) -> anyhow::Result<PathBuf> {
    let read_dir =
        std::fs::read_dir(dir).map_err(|e| anyhow::anyhow!("read_dir {}: {}", dir, e))?;
    let entries: Vec<_> = read_dir
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().starts_with("measurements_"))
        .collect();
    let latest = entries
        .into_iter()
        .max_by_key(|e| {
            e.metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH)
        })
        .ok_or_else(|| anyhow::anyhow!("no measurements_*.json found in {}", dir))?;
    Ok(latest.path())
}

fn run_git(cmd: &str) -> anyhow::Result<String> {
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .output()
        .map_err(|e| anyhow::anyhow!("spawn {}: {}", cmd, e))?;
    if !output.status.success() {
        anyhow::bail!("{} failed (status {})", cmd, output.status);
    }
    Ok(String::from_utf8(output.stdout)?)
}
