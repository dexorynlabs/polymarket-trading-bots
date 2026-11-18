//! Strategy-agnostic safety primitives.
//!
//! - `kill_switches`: seven detection functions consumed by the quoter and
//!   lifecycle (MidJump / TradeRate / DepthCollapse / BookFlip /
//!   WsStaleness / DailyPnl / Velocity).
//! - `spot_move_kill`: rolling Binance-spot deviation detector that the
//!   runtime watcher consults.
//! - `risk_cfg`: the live-mode safety task body — `RiskCfg`, `spawn`,
//!   `hourly_loss_from_ledger`. Lifted from `recon::risk` in step 9
//!   alongside the exec extraction (the task consumes
//!   `crate::execution::ledger::SpendLedger`).
//!
//! `risk_cfg`'s public surface is re-exported here so callers can write
//! `crate::risk::{RiskCfg, spawn}` directly.

pub mod kill_switches;
pub mod risk_cfg;
pub mod rails;

pub use risk_cfg::{spawn, RiskCfg};
