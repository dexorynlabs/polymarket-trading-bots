//! Live-mode safety task: kill-switch + velocity check.
//!
//! Both gates trip the same `halt_flag: Arc<AtomicBool>`. Submitter
//! shards check the flag before dispatching new placements. Cancels
//! are NOT halted — the operator wants existing orders to drain.

use crate::execution::ledger::SpendLedger;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct RiskCfg {
    pub kill_switch_loss_usd: f64,
    pub velocity_loss_per_hour: f64,
    pub velocity_consecutive_hrs: u32,
    /// Tick interval. Production is 30s; tests can shorten.
    pub tick_secs: u64,
}

impl RiskCfg {
    pub fn from_safety(s: &crate::config::SafetyCfg) -> Self {
        Self {
            kill_switch_loss_usd: s.kill_switch_loss_usd,
            velocity_loss_per_hour: s.velocity_loss_per_hour,
            velocity_consecutive_hrs: s.velocity_consecutive_hrs,
            tick_secs: 30,
        }
    }
}

/// Spawn the kill-switch + velocity task. Returns the JoinHandle so
/// `runtime::build_live` can store it for clean shutdown.
///
/// `shutdown_token` (§7.5 Step F 2026-05-04 incident remediation): when
/// the kill switch or velocity guard trips, the task cancels this token
/// (in addition to setting `halt_flag`) so the runtime drives a graceful
/// shutdown of all per-market submitters and ancillary tasks. Pass
/// `None` from tests / callers that don't want shutdown plumbing.
pub fn spawn(
    cfg: RiskCfg,
    ledger: Arc<Mutex<SpendLedger>>,
    halt_flag: Arc<AtomicBool>,
    shutdown_token: Option<tokio_util::sync::CancellationToken>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run(cfg, ledger, halt_flag, shutdown_token))
}

async fn run(
    cfg: RiskCfg,
    ledger: Arc<Mutex<SpendLedger>>,
    halt_flag: Arc<AtomicBool>,
    shutdown_token: Option<tokio_util::sync::CancellationToken>,
) {
    let mut history: VecDeque<(u64, f64)> = VecDeque::new();
    let mut interval = tokio::time::interval(Duration::from_secs(cfg.tick_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let cancel_shutdown = |reason: &'static str| {
        if let Some(t) = &shutdown_token {
            tracing::error!(
                reason,
                "RISK: cancelling shutdown_token; bot will exit gracefully"
            );
            t.cancel();
        }
    };
    loop {
        interval.tick().await;
        if halt_flag.load(Ordering::SeqCst) {
            continue;
        }
        let now = unix_now();
        // F16 fix (2026-05-11): the daily kill predicate consumes the
        // *trade-leg* net P&L view — Lip credits are excluded so subsidy
        // income cannot delay the kill when trade-leg drifts below the
        // threshold (strategy doc §17.5 / §16). Rebates stay on the trade
        // side per spec. Reporting paths can still read `net_pnl_24h_at`.
        let trade_pnl = {
            let g = ledger.lock().unwrap();
            g.net_trade_pnl_24h_at(micros_at(now))
        };
        if trade_pnl < -cfg.kill_switch_loss_usd {
            tracing::error!(
                trade_pnl,
                threshold = -cfg.kill_switch_loss_usd,
                "KILL SWITCH TRIPPED: net_trade_pnl_24h fell below threshold (LIP excluded)"
            );
            halt_flag.store(true, Ordering::SeqCst);
            cancel_shutdown("kill_switch_loss");
            continue;
        }
        let hourly_loss = {
            let g = ledger.lock().unwrap();
            hourly_loss_from_ledger(&g, now)
        };
        history.push_back((now, hourly_loss));
        let max_samples = (cfg.velocity_consecutive_hrs as u64 * 3600 / cfg.tick_secs) as usize;
        while history.len() > max_samples {
            history.pop_front();
        }
        if history.len() == max_samples
            && history.iter().all(|(_, l)| *l > cfg.velocity_loss_per_hour)
        {
            tracing::error!(
                window_hrs = cfg.velocity_consecutive_hrs,
                loss_per_hour = cfg.velocity_loss_per_hour,
                "VELOCITY GUARD TRIPPED: sustained loss above threshold for window"
            );
            halt_flag.store(true, Ordering::SeqCst);
            cancel_shutdown("velocity_loss_per_hour");
        }
    }
}

fn hourly_loss_from_ledger(ledger: &SpendLedger, now_unix: u64) -> f64 {
    // F16 fix (2026-05-11): velocity guard runs on the same trade-leg view as
    // the daily kill so they have consistent semantics. A sustained trade-leg
    // bleed should trip the velocity guard even if LIP income masks the gross
    // P&L number.
    let now_us = micros_at(now_unix);
    let then_us = micros_at(now_unix.saturating_sub(3600));
    let trade_now = ledger.net_trade_pnl_24h_at(now_us);
    let trade_then = ledger.net_trade_pnl_24h_at(then_us);
    -(trade_now - trade_then) // positive number = loss
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn micros_at(unix_secs: u64) -> u64 {
    unix_secs.saturating_mul(1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::ledger::{EventClass, SpendLedger};

    #[tokio::test]
    async fn kill_switch_trips_when_pnl_drops_below_threshold() {
        let halt = Arc::new(AtomicBool::new(false));
        let ledger = Arc::new(Mutex::new(SpendLedger::new(1000.0)));
        {
            let mut g = ledger.lock().unwrap();
            g.record_class(EventClass::TradeSpend, 100.0, micros_at(unix_now()));
            g.record_class(EventClass::TradeCredit, 40.0, micros_at(unix_now()));
        }
        let cfg = RiskCfg {
            kill_switch_loss_usd: 50.0,
            velocity_loss_per_hour: 10.0,
            velocity_consecutive_hrs: 2,
            tick_secs: 1,
        };
        let h = spawn(cfg, ledger.clone(), halt.clone(), None);
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert!(
            halt.load(Ordering::SeqCst),
            "expected halt after $-60 net pnl"
        );
        h.abort();
    }

    #[tokio::test]
    async fn no_trip_when_pnl_within_threshold() {
        let halt = Arc::new(AtomicBool::new(false));
        let ledger = Arc::new(Mutex::new(SpendLedger::new(1000.0)));
        {
            let mut g = ledger.lock().unwrap();
            g.record_class(EventClass::TradeSpend, 50.0, micros_at(unix_now()));
            g.record_class(EventClass::TradeCredit, 20.0, micros_at(unix_now()));
        }
        let cfg = RiskCfg {
            kill_switch_loss_usd: 50.0,
            velocity_loss_per_hour: 100.0,
            velocity_consecutive_hrs: 2,
            tick_secs: 1,
        };
        let h = spawn(cfg, ledger.clone(), halt.clone(), None);
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert!(!halt.load(Ordering::SeqCst));
        h.abort();
    }

    /// §7.5 Step F 2026-05-04 incident remediation: when the kill switch
    /// trips, in addition to setting `halt_flag`, the task must cancel
    /// the optional `shutdown_token` so the runtime exits gracefully.
    /// Pinning this prevents a regression where the bot keeps spinning
    /// after a kill — see `docs/incidents/2026-05-04-step-f-loss.log`.
    #[tokio::test]
    async fn kill_switch_cancels_shutdown_token_when_tripped() {
        let halt = Arc::new(AtomicBool::new(false));
        let ledger = Arc::new(Mutex::new(SpendLedger::new(1000.0)));
        {
            let mut g = ledger.lock().unwrap();
            g.record_class(EventClass::TradeSpend, 100.0, micros_at(unix_now()));
            g.record_class(EventClass::TradeCredit, 40.0, micros_at(unix_now()));
        }
        let cfg = RiskCfg {
            kill_switch_loss_usd: 50.0,
            velocity_loss_per_hour: 10.0,
            velocity_consecutive_hrs: 2,
            tick_secs: 1,
        };
        let token = tokio_util::sync::CancellationToken::new();
        let h = spawn(cfg, ledger.clone(), halt.clone(), Some(token.clone()));
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert!(halt.load(Ordering::SeqCst), "halt_flag must trip");
        assert!(
            token.is_cancelled(),
            "shutdown_token must be cancelled when kill switch trips"
        );
        h.abort();
    }
}
