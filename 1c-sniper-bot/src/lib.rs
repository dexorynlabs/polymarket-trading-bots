//! Late-slot cheap-side taker on btc-updown-5m — the POST-TWAP re-gating.
//!
//! From t+150 to t+291 it watches the trailing side's standing cheap ask —
//! 0.9c-1.1c, the shipped operator band around spec §3.4's graded 1c equality.
//! When Binance spot prints a trailing-1s micro-burst of >= 0.5 BTC with >= 60%
//! of that volume leaning toward the trailing side's recovery, it fires ONE FAK
//! for a $10 clip at that ask (limit 2c — price priority fills at the standing
//! ask, the wider limit is race-tail optionality only) and holds to
//! resolution. One shot per slot. A missed race costs $0; the risk rails bound
//! the loss side.
//!
//! **The earlier machine this crate was built from is DEAD on this era** —
//! the D-band cards graded 4/4 arms killed (base+B1 0-for-48, −100% ROS).
//! What replaced it, and every alternative that
//! was measured and graded OFF (near-tie D, depth, the quiet availability
//! channel, 2c entries, re-arm, perp/multi-venue feeds), is in
//! [POST-TWAP-SPEC.md](../POST-TWAP-SPEC.md) — read §4 before resurrecting
//! any of them.
//!
//! Design: taker-tickburst/POST-TWAP-SPEC.md
//! Measurement record: the perp early-fire grade §§6a-6c, 7
//! Census artifacts: data/tickburst_posttwap_census/
//!
//! The decision core (`features`/`flow`/`cards`/`tick`/`sizing`/`engine`) is
//! PURE — no I/O, no clock, no async. That is deliberate: v0 ships live with
//! no shadow phase, so `tests/posttwap_parity.rs` — replaying the graded
//! Aug 7-9 census through `SlotEngine` and reproducing its 98-entry/5-win 1c
//! cell exactly — is the correctness proof, and it can only exist if the core
//! is pure.
//!
//! Module map: `config` (operator TOML) → `features`/`flow`/`cards`/`tick`/
//! `sizing`/`engine` (the pure decision core) → `venue` (the `Venue` trait +
//! credential-free `ShadowVenue`) → `telemetry` (JSONL sink) → `rails` (risk
//! vetoes) → `settle` (CLOB-settled PnL, feeds the loss-kill) → `slot_driver`
//! (the per-slot async shell around one `SlotEngine`) → `live` (the launch
//! gate + `LiveVenue` FAK path) → `readiness` (the Inv-11 artifact producer)
//! → `runtime` (process assembly) → `main` (CLI dispatch).

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
1centbot — late-slot cheap-side taker on btc-updown-5m (POST-TWAP-SPEC)

  0.9-1.1c standing ask + a 0.5 BTC/1s 60%-recovery burst, one shot per slot.
  Never co-run with another cheap-ask taker on the same market: they race for the same asks.

  --live               real money; requires RECON_GIT_SHA + launch gate
  --dry-run            feeds + discovery + decisions via ShadowVenue; sends NOTHING
  --preflight          read-only diagnostics
  --live-readiness     write the Inv-11 readiness artifact (no rate-limit file)

  The gating harness is a test, not a CLI mode (it needs a merged book+tick
  stream):  cargo test --profile release-test --test posttwap_parity
";
