//! CLI entry. Before anything else, `.env` (if present) is loaded into the
//! process environment; then the whole config is resolved from `SNIPER_*`
//! (strategy + rails) and `POLY_*` (live account) — see `config::env`.
//!
//! Modes, dispatched on `args[0]`:
//!
//! - `--live` — real money. `live::launch_gate` runs BEFORE any live-capable
//!   object is constructed, so a gate failure can never leave a signer
//!   alive. Builds ONE `CancellationToken`, hands it to both `LiveVenue::new`
//!   and `runtime::run` (Task 15 obligation O2a) — see `build_live_venue`
//!   and the ctrl-c-vs-auth-halt distinction below.
//! - `--dry-run` — `ShadowVenue`, which owns no HTTP client/signer/creds BY
//!   TYPE. `cfg.live` IS resolved here (`load_config` builds the
//!   whole `BotConfig`, `POLY_*` included, and the whole struct is passed
//!   into `runtime::run`) — the property that actually holds is narrower
//!   than "never reads it": nothing on this path ever constructs a signer or
//!   a credentialed transport FROM it, by type (M10, task-15 review).
//! - `--preflight` — read-only diagnostics against the `POLY_*` account.
//! - `--live-readiness` — writes the Inv-11 artifact (no rate-limit file needed).
//!
//! The gating tests are `tests/posttwap_parity.rs` (the 1c census replay) and
//! `tests/leader_trigger.rs` (99c behaviour; no 99c census exists).

use sniperbot::config::env::EnvVars;
use sniperbot::config::BotConfig;
use sniperbot::HELP;
use tokio_util::sync::CancellationToken;

const DOTENV_PATH: &str = ".env";
/// The pre-env config file. No longer read; its presence is an error so the
/// keys in it are never silently ignored.
const LEGACY_TOML: &str = "config/tickburst.toml";

/// Resolves the whole config from the `SNIPER_*` / `POLY_*` environment.
fn load_config() -> anyhow::Result<BotConfig> {
    anyhow::ensure!(
        !std::path::Path::new(LEGACY_TOML).exists(),
        "{LEGACY_TOML} is no longer read — everything is set in .env now. Move [rails] to \
         SNIPER_DAILY_CAP_USD / SNIPER_EXPOSURE_CAP_USD / SNIPER_PER_ORDER_CAP_USD and \
         [live.*] to POLY_PRIVATE_KEY / POLY_PROXY_WALLET / POLY_API_* (see .env.example), \
         then delete {LEGACY_TOML}"
    );
    let cfg = BotConfig::from_env(&EnvVars::from_process())
        .map_err(|e| anyhow::anyhow!("environment / .env: {e:#}"))?;
    sniperbot::config::validate_strategy(&cfg.strategy)?;
    Ok(cfg)
}

/// Spawns the ctrl-c handler. `user_requested` is stamped `true` before
/// `shutdown` is cancelled, so a caller that also observes an
/// auth-triggered cancel on the SAME token (`--live`'s `LiveVenue`, per
/// Task 15 obligation O2a) can tell the two apart afterward — see the
/// `--live` arm below for why that distinction is the whole point.
fn spawn_ctrl_c(shutdown: CancellationToken, user_requested: std::sync::Arc<std::sync::atomic::AtomicBool>) {
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("tickburst: ctrl-c — shutting down");
        user_requested.store(true, std::sync::atomic::Ordering::SeqCst);
        shutdown.cancel();
    });
}

/// Critical 1 (task-15 review): supervises a background task this process
/// cannot safely run without — awaits its `JoinHandle` and cancels
/// `shutdown` on `Err` (a panic). Similar in SHAPE to `runtime.rs`'s
/// `settle_supervisor` pattern for `SettleLane`'s `JoinHandle` (Task 15
/// obligation O3) — both await a `JoinHandle` and log loudly on panic —
/// but the two are DELIBERATELY NOT the same policy (N3, task-15
/// re-review): this one cancels `shutdown` on top of logging;
/// `settle_supervisor` only logs. A dead live user-WS ingest (the caller
/// below) fails OPEN — orders can still be sent, fills become invisible,
/// and the CLOB-settled loss kill (fed only by `settle.rs`'s poll) goes
/// blind — so halting the whole process is the only safe response. A dead
/// settle lane instead fails CLOSED on its own: `rails.rs`'s exposure rail
/// ratchets shut once roughly `concurrent_exposure_cap_usd / clip`
/// positions are stuck unsettled, which stops new fires without any
/// supervisor intervention. Do not "fix" that asymmetry by making
/// `settle_supervisor` cancel too — see its own doc in `runtime.rs`.
async fn supervise_or_halt(handle: tokio::task::JoinHandle<()>, shutdown: CancellationToken, what: &'static str) {
    if let Err(e) = handle.await {
        tracing::error!(
            error = ?e, task = what,
            "tickburst: background task PANICKED; this process can no longer rely on it. Halting."
        );
        shutdown.cancel();
    }
}

/// Builds the real `LiveClob` + user-WS fills stack and the `LiveVenue`
/// (Task 14) behind it. Mirrors the earlier crates' live-stack wiring.
/// `shutdown` is the SAME token
/// the caller passes into `runtime::run` (obligation O2a) — `LiveVenue`
/// cancels it on a proven 401/403 (`live.rs`'s module doc).
async fn build_live_venue(
    live: &sniperbot::config::LiveConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<sniperbot::execution::live::LiveVenue> {
    use sniperbot::config::ContractAddresses;
    use sniperbot::execution::live_clob::LiveClob;
    use sniperbot::execution::maker_rebate_ledger::MakerRebateLedger;
    use sniperbot::execution::position::PositionStore;
    use sniperbot::execution::user_ws::run_with_provisional_fills;
    use std::sync::Arc;
    use tokio::sync::broadcast;

    let clob = Arc::new(LiveClob::new(live, ContractAddresses::v2_defaults())?);
    let asset_registry: sniperbot::execution::user_ws::AssetIdRegistry =
        Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
    let (fills_tx, _fills_keepalive) = broadcast::channel(256);
    // Provisional MATCHED stream: the only sub-second fill signal the venue
    // emits (settled MINED/CONFIRMED trails by seconds) — required for the
    // 1500ms FAK grace window to ever see a fill in time.
    let (provisional_tx, _provisional_keepalive) = broadcast::channel(256);
    let positions = Arc::new(std::sync::Mutex::new(PositionStore::new()));
    let rebates = Arc::new(MakerRebateLedger::default());
    let tls = tokio::task::spawn_blocking(sniperbot::polymarket::clob_ws::build_shared_tls_connector)
        .await?
        .ok();
    {
        let ws_url = live.clob.ws_url.clone();
        let creds = clob.creds();
        let registry = asset_registry.clone();
        let fills_tx = fills_tx.clone();
        let provisional_tx = provisional_tx.clone();
        let user_ws_handle = tokio::spawn(async move {
            if let Err(e) = run_with_provisional_fills(
                ws_url,
                creds,
                registry,
                fills_tx,
                Some(provisional_tx),
                positions,
                rebates,
                tls,
                None,
            )
            .await
            {
                tracing::error!(?e, "tickburst: user_ws exited with error");
            }
        });
        // CRITICAL 1 (task-15 review): `run_with_provisional_fills` is an
        // INFINITE reconnect loop (polymarket-client/src/exec/user_ws.rs:
        // `loop { match run_once(..).await { .. } ... }` never returns), so
        // the `if let Err` arm above is unreachable in practice — the ONLY
        // way this task ends is a PANIC, which a dropped `JoinHandle` would
        // never surface. If it dies: `fills_tx`/`provisional_tx` drop ->
        // `LiveVenue`'s dedup loop (live.rs) sees both fill channels closed
        // and exits -> `events_tx` drops -> `drive_slot`'s `venue_events`
        // channel closes -> every outstanding FAK finalizes as a free
        // `GraceExpired` Miss even though real money was spent and the
        // shares are actually held, and `rails.reconcile` releases the full
        // exposure while the position is still open. Real positions would
        // never settle, never reach `record_settled_pnl`, and the loss kill
        // would be blind — the exact O3 failure mode Task 15 fixed for
        // `SettleLane`, one file over. A bot that cannot observe its own
        // fills must not keep firing, so this cancels `shutdown` rather
        // than merely logging.
        tokio::spawn(supervise_or_halt(user_ws_handle, shutdown.clone(), "user_ws"));
    }
    Ok(sniperbot::execution::live::LiveVenue::new(
        clob,
        asset_registry,
        fills_tx.subscribe(),
        provisional_tx.subscribe(),
        shutdown,
    ))
}

fn main() -> anyhow::Result<()> {
    // Load `.env` while this is still the only thread: mutating the process
    // environment is not thread-safe, and the tokio runtime starts below.
    let loaded = sniperbot::config::env::load_dotenv(std::path::Path::new(DOTENV_PATH))?;
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(run_cli(loaded))
}

async fn run_cli(dotenv_vars: usize) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    if dotenv_vars > 0 {
        tracing::info!(count = dotenv_vars, path = DOTENV_PATH, "loaded environment variables from .env");
    }

    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str).unwrap_or("") {
        "--dry-run" => {
            // ShadowVenue owns no HTTP client/signer/creds BY TYPE. `cfg.live`
            // IS deserialized and threaded into `runtime::run` below (the
            // whole `BotConfig` goes in) — the property that holds is
            // narrower than "never reads cfg.live": nothing on this path
            // ever constructs a signer or a credentialed transport from it,
            // by type (M10, task-15 review).
            let cfg = load_config()?;
            let shutdown = CancellationToken::new();
            let user_requested = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            spawn_ctrl_c(shutdown.clone(), user_requested);
            let venue = std::sync::Arc::new(sniperbot::execution::venue::ShadowVenue::new());
            sniperbot::bot::runtime::run(cfg, venue, shutdown).await?;
        }
        "--preflight" => {
            let cfg = load_config()?;
            let Some(live) = &cfg.live else {
                eprintln!("--preflight requires the live account: set the POLY_* variables in .env");
                std::process::exit(2);
            };
            let report = sniperbot::monitoring::preflight::run(live).await?;
            report.print();
            if !report.all_green() {
                std::process::exit(1);
            }
        }
        "--live-readiness" => {
            // clob_rate_limit is no longer a gating check (readiness.rs), so no
            // rate-limit observation file is required. Any extra path arg is
            // accepted and ignored for back-compat with the deploy script.
            match sniperbot::bot::readiness::write_readiness_artifact().await {
                Ok(()) => println!("live-readiness artifact written under data/"),
                Err(e) => {
                    eprintln!("live-readiness FAILED (no artifact written): {e:#}");
                    std::process::exit(1);
                }
            }
        }
        "--live" => {
            let cfg = load_config()?;
            // MUST run BEFORE constructing any live-capable object — a gate
            // failure must never leave a signer alive.
            sniperbot::execution::live::launch_gate(&cfg).await?;
            let live_cfg = sniperbot::execution::live::require_live_section(&cfg)?;

            // O2a: ONE CancellationToken, passed to LiveVenue::new AND into
            // runtime::run (which threads it, unmodified, into every
            // drive_slot call).
            let shutdown = CancellationToken::new();
            let user_requested = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            spawn_ctrl_c(shutdown.clone(), user_requested.clone());

            let venue = std::sync::Arc::new(build_live_venue(live_cfg, shutdown.clone()).await?);
            let result = sniperbot::bot::runtime::run(cfg, venue, shutdown.clone()).await;

            // O2b: distinguish an operator ctrl-c from an internal halt
            // (LiveVenue cancelling the SAME token on a proven 401/403,
            // live.rs's module doc) — `shutdown.is_cancelled()` alone can't
            // tell them apart since both flip the same token; the ctrl-c
            // handler marks `user_requested` first. Anything else that
            // reaches here with the token cancelled but NOT operator-marked
            // is exactly the auth-halt case (or another internal fault) —
            // exit non-zero so an operator/monitor is paged instead of the
            // process quietly going idle.
            if shutdown.is_cancelled() && !user_requested.load(std::sync::atomic::Ordering::SeqCst) {
                tracing::error!(
                    "tickburst: shutdown was NOT operator-requested (auth halt or an internal \
                     fault) — exiting non-zero"
                );
                result?;
                std::process::exit(1);
            }
            result?;
        }
        _ => println!("{HELP}"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Critical 1, mutation-verify (task-15 review): a supervised task that
    /// PANICS must cancel `shutdown` — this is the entire fix for the
    /// fire-and-forget live user-WS ingest task in `build_live_venue`,
    /// pinned directly rather than only through a live socket.
    #[tokio::test]
    async fn a_panicking_supervised_task_cancels_shutdown() {
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(async { panic!("boom") });
        supervise_or_halt(handle, shutdown.clone(), "test").await;
        assert!(shutdown.is_cancelled(), "a panicking supervised task must cancel shutdown");
    }

    /// The other direction: a task that exits CLEANLY (returns `Ok`-shaped,
    /// i.e. no panic) must never trip `shutdown` — `supervise_or_halt` only
    /// reacts to a `JoinError`, not to the task's own return value.
    #[tokio::test]
    async fn a_cleanly_exiting_supervised_task_does_not_cancel_shutdown() {
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(async {});
        supervise_or_halt(handle, shutdown.clone(), "test").await;
        assert!(!shutdown.is_cancelled(), "a clean task exit must not cancel shutdown");
    }
}
