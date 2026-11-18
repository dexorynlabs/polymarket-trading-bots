//! sniperbot — a late-slot sniper for Polymarket `btc-updown-5m`, with one
//! engine and two strategies picked by `SNIPER_MODE`:
//!
//! - `1c`: buy the TRAILING token when its standing ask sits at ~1c
//!   (0.9c–1.1c) and Binance prints a burst toward its recovery. Win ~+99c /
//!   lose ~1c per share. Graded on the 1c census (`tests/posttwap_parity.rs`).
//! - `99c`: buy the LEADING token when its standing ask sits at 97c–99c and
//!   the trigger passes (`burst` toward the leader, or `calm` = no burst
//!   against it). Win ~+1c / lose ~99c per share. UNMEASURED: break-even is
//!   a ~99.07% win rate.
//!
//! In a binary market the two token prices sum to ~$1, so both strategies
//! find their moment with the same machinery: from t+150 to t+291 of each
//! 300s slot, watch the held order book, and on a qualifying Binance
//! `@aggTrade` fire ONE fill-and-kill buy for a flat dollar clip, then hold
//! to resolution. One shot per slot.
//!
//! Everything is configured from the environment or a `.env` file
//! (`config::env`): the strategy and spend rails as `SNIPER_*`, the live
//! account (wallet, API keys) as `POLY_*`. There is no config file.
//!
//! The decision core (`strategy::{features, flow, cards, tick, sizing,
//! engine, mode}`) is PURE — no I/O, no clock, no async — so the tests can
//! pin its behaviour exactly.
//!
//! Module map: `config` (env) → `strategy` (the pure decision core) →
//! `venue` (the `Venue` trait + credential-free `ShadowVenue`) → `telemetry`
//! (JSONL sink) → `rails` (risk vetoes) → `settle` (CLOB-settled PnL, feeds
//! the loss kill) → `slot_driver` (the per-slot async shell around one
//! `SlotEngine`) → `live` (the launch gate + `LiveVenue` FAK path) →
//! `readiness` (the readiness artifact) → `runtime` (process assembly) →
//! `main` (CLI dispatch).

pub mod binance;
pub mod bot;
pub mod config;
pub mod execution;
pub mod monitoring;
pub mod polymarket;
pub mod risk;
pub mod strategy;
pub mod types;

pub const HELP: &str = "\
sniperbot — late-slot 1c / 99c sniper for Polymarket btc-updown-5m

  Pick the strategy in the environment (or a .env file):
    SNIPER_MODE=1c    buy the trailing token at ~1c on a recovery burst
    SNIPER_MODE=99c   buy the leading token at ~99c (UNMEASURED; dry-run first)
  Optional overrides: SNIPER_TRIGGER, SNIPER_CLIP_USD, SNIPER_ASK_MIN/MAX,
  SNIPER_LIMIT, ... — every key is listed in .env.example.

  Spend rails: SNIPER_DAILY_CAP_USD, SNIPER_EXPOSURE_CAP_USD, ... (defaults).
  Live account (--live / --preflight only): POLY_PRIVATE_KEY, POLY_PROXY_WALLET,
  POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE.

  --dry-run            feeds + decisions via ShadowVenue; sends NOTHING
  --preflight          read-only checks of the POLY_* account
  --live-readiness     write the readiness artifact (runs the test suite)
  --live               real money; requires RECON_GIT_SHA + the launch gate

  Gating tests:
    cargo test --profile release-test --test posttwap_parity   (1c census replay)
    cargo test --profile release-test --test leader_trigger    (99c behaviour)
";
