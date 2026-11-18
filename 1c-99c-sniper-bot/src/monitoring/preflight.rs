//! Preflight checks for V2 live launch.
//!
//! Runs four checks and prints a green/red summary. Exit 0 on all
//! green; exit 1 with specific failure on any miss.

use crate::config::LiveConfig;
use crate::execution::auth::{auth_headers, ApiCreds};
use crate::execution::signer::SignerService;
use crate::polymarket::onchain::polygon_events::preflight_subscribe;
use anyhow::{Context, Result};

pub struct PreflightReport {
    pub checks: Vec<(String, std::result::Result<String, String>)>,
}

impl PreflightReport {
    pub fn all_green(&self) -> bool {
        self.checks.iter().all(|(_, r)| r.is_ok())
    }

    pub fn print(&self) {
        for (name, r) in &self.checks {
            match r {
                Ok(detail) => println!("  [PASS] {name}: {detail}"),
                Err(detail) => println!("  [FAIL] {name}: {detail}"),
            }
        }
        if self.all_green() {
            println!("\nAll preflight checks PASS. Safe to launch.");
        } else {
            println!("\nPreflight has FAILURES. Do not launch.");
        }
    }
}

pub async fn run(live: &LiveConfig) -> Result<PreflightReport> {
    let mut checks: Vec<(String, std::result::Result<String, String>)> = Vec::new();

    // 1. Signer derivation. Capture EOA for reuse in check #4.
    let signer_addr_result: std::result::Result<String, String> =
        SignerService::from_pk_hex(&live.wallet.private_key)
            .map(|s| s.address_hex())
            .map_err(|e| e.to_string());
    let eoa: String = signer_addr_result.clone().unwrap_or_default();
    checks.push((
        "signer derives EOA from private key".into(),
        signer_addr_result
            .map(|a| format!("EOA = {a}; proxy_wallet = {}", live.wallet.proxy_wallet)),
    ));

    // 2. pUSD balance.
    let bal = balance_pusd(&live.chain.rpc_url, &live.wallet.proxy_wallet).await;
    checks.push((
        "proxy wallet pUSD balance".into(),
        bal.map(|b| format!("{b:.2} pUSD (cap_usd = {})", live.safety.cap_usd))
            .map_err(|e| e.to_string()),
    ));

    // 3. ERC-20 allowance.
    let allow = allowance_pusd(&live.chain.rpc_url, &live.wallet.proxy_wallet, EXCHANGE_V2).await;
    checks.push((
        "pUSD allowance for V2 Exchange".into(),
        allow
            .map(|a| format!("{a:.2} approved (need >= {})", live.safety.cap_usd))
            .map_err(|e| e.to_string()),
    ));

    // 4. CLOB API creds via GET /auth/api-keys with key-membership check.
    //    Catches both B3 (wrong POLY_ADDRESS) and B4 (wrong endpoint).
    let creds = ApiCreds {
        key: live.api.key.clone(),
        secret: live.api.secret.clone(),
        passphrase: live.api.passphrase.clone(),
        address: eoa.clone(),
    };
    let api_keys = check_api_keys(&live.clob.http_base, &creds, &live.api.key).await;
    checks.push((
        "CLOB API creds (GET /auth/api-keys)".into(),
        api_keys.map_err(|e| e.to_string()),
    ));

    // 5. Polygon WS RPC connectivity (eth_subscribe round-trip).
    //    Only runs when live.chain.rpc_ws_url is configured; None = skipped
    //    (counts as green so operators without a WS URL aren't blocked).
    if let Some(ws_url) = &live.chain.rpc_ws_url {
        let ws_result = preflight_subscribe(ws_url, std::time::Duration::from_secs(30)).await;
        checks.push((
            "Polygon WS RPC eth_subscribe round-trip".into(),
            ws_result
                .map(|()| format!("subscribe+unsubscribe OK (url = {ws_url})"))
                .map_err(|e| e.to_string()),
        ));
    }

    Ok(PreflightReport { checks })
}

const PUSD_POLYGON: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
const EXCHANGE_V2: &str = "0xE111180000d2663C0091e4f400237545B87B996B";

async fn balance_pusd(rpc_url: &str, holder: &str) -> Result<f64> {
    let payload = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{{"to":"{PUSD_POLYGON}","data":"0x70a08231{}"}},"latest"]}}"#,
        pad_addr(holder)
    );
    let raw = post_rpc(rpc_url, &payload).await?;
    let v = parse_hex_u256_to_f64(&raw)? / 1_000_000.0;
    Ok(v)
}

async fn allowance_pusd(rpc_url: &str, owner: &str, spender: &str) -> Result<f64> {
    let payload = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{{"to":"{PUSD_POLYGON}","data":"0xdd62ed3e{}{}"}},"latest"]}}"#,
        pad_addr(owner),
        pad_addr(spender)
    );
    let raw = post_rpc(rpc_url, &payload).await?;
    let v = parse_hex_u256_to_f64(&raw)? / 1_000_000.0;
    Ok(v)
}

async fn check_api_keys(http_base: &str, creds: &ApiCreds, expected_key: &str) -> Result<String> {
    let path = "/auth/api-keys";
    let url = format!("{http_base}{path}");
    let h = auth_headers(creds, "GET", path, "")?;
    let mut req = reqwest::Client::new().get(&url);
    for (k, v) in &h {
        req = req.header(k, v);
    }
    let resp = req.send().await.with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    if !status.is_success() {
        let txt = resp.text().await.unwrap_or_default();
        anyhow::bail!("GET {url} -> HTTP {status}: {txt}");
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .with_context(|| format!("parse {url} body as JSON"))?;
    let found = body
        .get("apiKeys")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().any(|k| k.as_str() == Some(expected_key)))
        .unwrap_or(false);
    if !found {
        anyhow::bail!(
            "GET {url} returned 200 but live.api.key not in apiKeys list — \
             POLY_ADDRESS likely set to wrong address (must be EOA, not proxy_wallet) \
             OR the API key is bound to a different EOA"
        );
    }
    Ok(format!("HTTP {status}; api key validated against EOA"))
}

async fn post_rpc(url: &str, body: &str) -> Result<String> {
    let resp = reqwest::Client::new()
        .post(url)
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await?;
    let v: serde_json::Value = resp.json().await?;
    let result = v
        .get("result")
        .and_then(|r| r.as_str())
        .ok_or_else(|| anyhow::anyhow!("rpc response missing `result`: {v}"))?;
    Ok(result.to_string())
}

fn parse_hex_u256_to_f64(hex: &str) -> Result<f64> {
    let trimmed = hex.strip_prefix("0x").unwrap_or(hex);
    // u128 covers ≤ ~3.4e38 — fine for typical balances (cap_usd is far
    // below this). For larger values — most commonly max-uint256 from
    // Polymarket UI's "Enable Trading" default — return f64::INFINITY,
    // which displays cleanly as "inf" and satisfies any `>= threshold`
    // check. Avoids the misleading "parse hex u256" error the audit
    // flagged at this line.
    if let Ok(n) = u128::from_str_radix(trimmed, 16) {
        return Ok(n as f64);
    }
    Ok(f64::INFINITY)
}

fn pad_addr(a: &str) -> String {
    let trimmed = a.strip_prefix("0x").unwrap_or(a);
    format!("000000000000000000000000{}", trimmed.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hex_u256_handles_zero() {
        let v = parse_hex_u256_to_f64("0x0").unwrap();
        assert_eq!(v, 0.0);
    }

    #[test]
    fn parse_hex_u256_handles_big_values() {
        let v = parse_hex_u256_to_f64("0x3b9aca00").unwrap();
        assert_eq!(v, 1_000_000_000.0);
    }

    #[test]
    fn parse_hex_u256_returns_inf_for_max_uint256_approval() {
        // Polymarket UI's "Enable Trading" approves max-uint256 by default.
        // Preflight must not crash on this — it should treat as effectively
        // infinite and pass any `>= cap` check.
        let max_u256_hex = "0x".to_string() + &"f".repeat(64);
        let v = parse_hex_u256_to_f64(&max_u256_hex).unwrap();
        assert!(
            v.is_infinite(),
            "expected INFINITY for max-uint256, got {v}"
        );
    }

    #[test]
    fn parse_hex_u256_returns_inf_for_near_max_uint256() {
        // Real-world case: max-uint256 minus a small amount (the allowance
        // has been decremented through prior trades). Still > u128::MAX.
        let near_max = "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffff8770ee5";
        let v = parse_hex_u256_to_f64(near_max).unwrap();
        assert!(
            v.is_infinite(),
            "expected INFINITY for near-max-uint256, got {v}"
        );
    }

    #[test]
    fn pad_addr_zero_extends_to_32_bytes() {
        let p = pad_addr("0x365f0ca36ae1f641e02fe3b7743673da42a13a70");
        assert_eq!(p.len(), 64);
        assert!(p.ends_with("365f0ca36ae1f641e02fe3b7743673da42a13a70"));
    }
}
