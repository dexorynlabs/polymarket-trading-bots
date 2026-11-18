//! Live-mode configuration shared by all strategies.
//!
//! [`LiveConfig`] is the operator-config consumed at boot — wallet creds,
//! chain RPC, CLOB endpoints, API auth, and the safety caps/timers Inv 1-11
//! gate against. The structural validator [`validate_live`] enforces the
//! invariants common to every strategy. Strategy-specific overlays (e.g.
//! recon's Inv 12 cap-tier whitelist) live in the strategy crate and run
//! alongside this validator at startup.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LiveConfig {
    pub wallet: WalletCfg,
    pub chain: ChainCfg,
    pub clob: ClobCfg,
    pub api: ApiCfg,
    pub safety: SafetyCfg,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WalletCfg {
    pub proxy_wallet: String,
    pub private_key: String,
    /// Wallet signing variant: 1 = Magic.link PROXY, 2 = Safe v1.3.0.
    /// Defaults to 1 for backward compatibility with existing recon configs
    /// that omit this field. A Safe-proxy wallet sets this to 2.
    #[serde(default = "default_wallet_signature_type")]
    pub signature_type: u8,
    /// Chain ID for Safe v1.3.0 signing (used when signature_type = 2).
    /// Defaults to 137 (Polygon mainnet). Ignored for Magic.link wallets.
    #[serde(default = "default_safe_chain_id")]
    pub safe_chain_id: u64,
}

fn default_wallet_signature_type() -> u8 {
    1
}
fn default_safe_chain_id() -> u64 {
    137
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ChainCfg {
    pub rpc_url: String,
    pub chain_id: u64,
    /// Optional Polygon WS RPC URL for `eth_subscribe(logs)`. When set,
    /// `--preflight` validates a one-shot subscribe/unsubscribe round-trip.
    /// Omit (TOML default `None`) to skip the WS connectivity check.
    #[serde(default)]
    pub rpc_ws_url: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClobCfg {
    pub http_base: String,
    pub ws_url: String,
    pub signature_type: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ApiCfg {
    pub key: String,
    pub secret: String,
    pub passphrase: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SafetyCfg {
    pub cap_usd: f64,
    pub kill_switch_loss_usd: f64,
    pub velocity_loss_per_hour: f64,
    pub velocity_consecutive_hrs: u32,
    pub skip_alternate_cids: bool,
    pub cutover_at_unix: Option<u64>,
    /// Reject markets running longer than this many seconds at spawn time.
    /// **Default `Some(60)`** — TOML files that omit the key get a 60-second
    /// freshness gate. To effectively disable the gate (NOT RECOMMENDED for
    /// live), set `max_market_age_at_spawn_secs = 999999` (or any value
    /// large enough that no real market will exceed it). TOML has no null
    /// literal, so setting this to actual `None` is only possible via
    /// direct Rust struct construction (used in tests).
    /// Audit gap fix: previously `None` by default, which silently disabled
    /// the gate when an operator forgot to copy the line.
    #[serde(default = "default_max_market_age_at_spawn_secs")]
    pub max_market_age_at_spawn_secs: Option<u64>,
    /// **F17 #6 (2026-05-11):** strategy doc §16 cap #1 — per-slot directional
    /// inventory cap in USD (sum of reservations across both sides on a
    /// single slug). Distinct from `cap_usd` (global cross-slot). When set,
    /// admission rejects placements that would push a single slug's combined
    /// notional above this value. Omit (TOML default `None`) to disable —
    /// only the global `cap_usd` + per-side `exposure_cap_per_side_usd` bind.
    ///
    /// V1 multi-coin deployment sets `cap_usd = 300`, `max_per_slot_total_usd = 50`
    /// (matches strategy §16 row 1 + row 2). Phase 8 step 1 (single-slot)
    /// leaves this `None` since `cap_usd = 50` already bounds the single slot.
    #[serde(default)]
    pub max_per_slot_total_usd: Option<f64>,
    /// Optional, price-independent per-`(slug, side)` SHARE-COUNT cap (units =
    /// shares). Composes with the USD caps — the tighter binds. Caps each side
    /// independently, so the lagging/repair side keeps filling while the slot
    /// is imbalanced (coverage-safe). Omit (TOML default `None`) to disable —
    /// no behaviour change; only the USD caps bind. `validate_live` rejects a
    /// non-finite or non-positive value when set. Maps into
    /// `GuardsCfg.exposure_cap_per_side_shares` via `build_guards`.
    #[serde(default)]
    pub exposure_cap_per_side_shares: Option<f64>,
}

fn default_max_market_age_at_spawn_secs() -> Option<u64> {
    Some(60)
}

/// Structural validator for `LiveConfig`. Runs at startup (caller composes
/// this with strategy-specific invariants like recon's Inv 12 cap-tier
/// whitelist). Returns `Err` with a contextual message on the first
/// failing field — boot is the only safe time to fail closed on shape.
pub fn validate_live(live: &LiveConfig) -> Result<()> {
    if !is_eth_address(&live.wallet.proxy_wallet) {
        bail!("POLY_PROXY_WALLET must match 0x[0-9a-fA-F]{{40}}");
    }
    if !is_private_key(&live.wallet.private_key) {
        bail!("POLY_PRIVATE_KEY must be 0x + 64 hex chars");
    }
    if live.chain.rpc_url.is_empty() {
        bail!("POLY_RPC_URL must be non-empty");
    }
    if live.chain.chain_id == 0 {
        bail!("POLY_CHAIN_ID must be non-zero");
    }
    if live.clob.http_base.is_empty() || live.clob.ws_url.is_empty() {
        bail!("POLY_CLOB_HTTP and POLY_CLOB_WS must be non-empty");
    }
    // 3 = the wallet generation Polymarket issues to accounts created around
    // 2026-08 (146-byte ERC-7760 proxy, owner EOA in the immutable args).
    // py_order_utils only defines 0-2, but the venue accepts 3: probed live
    // via GET /balance-allowance?signature_type=N, which reports the funded
    // balance at 3 and `error resolving address` at 4+.
    // Signing is unaffected — sign_order carries signature_type as a plain
    // EIP-712 field and always signs with the EOA.
    if live.clob.signature_type > 3 {
        bail!(
            "POLY_SIGNATURE_TYPE must be 0, 1, 2, or 3 (got {})",
            live.clob.signature_type
        );
    }
    if live.api.key.is_empty() || live.api.secret.is_empty() || live.api.passphrase.is_empty() {
        bail!("POLY_API_KEY, POLY_API_SECRET and POLY_API_PASSPHRASE must all be non-empty");
    }
    if live.safety.cap_usd <= 0.0 {
        bail!("SNIPER_LIVE_BUY_CAP_USD must be > 0");
    }
    // 2026-05-11 phase-8-step1 incident: the prior upper bound
    // (`kill_switch_loss_usd < cap_usd`) presumed the daily-P&L methodology
    // counted only realized losses. In fact `risk::run` reads
    // `ledger.net_pnl_24h_at(now)`, which marks BUY fills as `TradeSpend`
    // immediately and credits back only on `MergeBurn` / `TradeCredit` /
    // `RedeemCredit` events. For an LIP-farming bot that posts paired BUY-
    // BUY makers, deployed paired inventory is counted as 100% loss until
    // a merge or redeem posts. With cap_usd=50 and 50sh sizing, normal
    // single-slot deployment hits $34-38 in fills (well under cap, far over
    // any threshold < cap). The threshold must therefore exceed cap_usd
    // (typically ≥ 2× cap_usd to accommodate multi-slot 24h cumulative
    // spend before the next merge cycle).
    if live.safety.kill_switch_loss_usd <= 0.0 {
        bail!("SNIPER_LIVE_DAILY_LOSS_USD must be > 0");
    }
    if let Some(n) = live.safety.exposure_cap_per_side_shares {
        if !(n.is_finite() && n > 0.0) {
            bail!("exposure_cap_per_side_shares must be finite and > 0 when set (got {n})");
        }
    }
    Ok(())
}

pub fn is_eth_address(s: &str) -> bool {
    s.len() == 42 && s.starts_with("0x") && s[2..].chars().all(|c| c.is_ascii_hexdigit())
}

pub fn is_private_key(s: &str) -> bool {
    s.len() == 66 && s.starts_with("0x") && s[2..].chars().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_live() -> LiveConfig {
        LiveConfig {
            wallet: WalletCfg {
                proxy_wallet: "0x365f0ca36ae1f641e02fe3b7743673da42a13a70".into(),
                private_key: "0x1111111111111111111111111111111111111111111111111111111111111111"
                    .into(),
                signature_type: 1,
                safe_chain_id: 137,
            },
            chain: ChainCfg {
                rpc_url: "https://polygon-rpc.com".into(),
                chain_id: 137,
                rpc_ws_url: None,
            },
            clob: ClobCfg {
                http_base: "https://clob.polymarket.com".into(),
                ws_url: "wss://ws-subscriptions-clob.polymarket.com".into(),
                signature_type: 1,
            },
            api: ApiCfg {
                key: "k".into(),
                secret: "s".into(),
                passphrase: "p".into(),
            },
            safety: SafetyCfg {
                cap_usd: 50.0,
                kill_switch_loss_usd: 100.0,
                velocity_loss_per_hour: 50.0,
                velocity_consecutive_hrs: 2,
                skip_alternate_cids: false,
                cutover_at_unix: None,
                max_market_age_at_spawn_secs: Some(60),
                max_per_slot_total_usd: None,
                exposure_cap_per_side_shares: None,
            },
        }
    }

    #[test]
    fn validate_live_accepts_valid_baseline_with_none_share_cap() {
        assert!(validate_live(&valid_live()).is_ok());
    }

    #[test]
    fn validate_live_accepts_positive_share_cap() {
        let mut c = valid_live();
        c.safety.exposure_cap_per_side_shares = Some(150.0);
        assert!(validate_live(&c).is_ok());
    }

    /// `exposure_cap_per_side_shares`, when set, must be finite and > 0.
    /// Mirrors the `cap_usd <= 0` validator. Clones the per-field shape so a
    /// regression that drops the check is caught.
    #[test]
    fn validate_live_rejects_nonpositive_or_nonfinite_share_cap() {
        for bad in [0.0_f64, -1.0, f64::NAN, f64::INFINITY] {
            let mut c = valid_live();
            c.safety.exposure_cap_per_side_shares = Some(bad);
            assert!(
                validate_live(&c).is_err(),
                "exposure_cap_per_side_shares = {bad} must be rejected"
            );
        }
    }
}
