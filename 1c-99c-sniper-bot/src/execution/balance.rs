//! pUSD balance manager for live mode (post-V2 cutover; was USDC.e in V1).
//!
//! Polls the operator's proxy wallet for ERC-20 balance via Polygon
//! RPC. Caches for 5s. Rejects placements that would exceed cap;
//! fail-closed after 3 consecutive cache-refresh failures.
//!
//! Type and method names retain the `Usdc*` prefix as a deliberate
//! cross-version stability anchor — the address-of-record now points
//! to pUSD via `ContractAddresses::v2_defaults().collateral_erc20`.

use alloy_primitives::U256;
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Calldata for ERC-1155 `balanceOf(address account, uint256 id)` —
/// selector `0x00fdd58e`. `token_id_dec` is the CLOB asset id as the
/// decimal string gamma's `clobTokenIds` carries (same id the CLOB and
/// user-WS speak). The contract to call is `ContractAddresses::ctf`.
pub fn erc1155_balance_of_calldata(holder: &str, token_id_dec: &str) -> Result<String> {
    let addr = holder.strip_prefix("0x").unwrap_or(holder);
    if addr.len() != 40 || !addr.chars().all(|c| c.is_ascii_hexdigit()) {
        anyhow::bail!("holder must be a 20-byte hex address, got {holder:?}");
    }
    let id = U256::from_str_radix(token_id_dec, 10)
        .with_context(|| format!("token id is not a decimal uint256: {token_id_dec:?}"))?;
    Ok(format!(
        "0x00fdd58e000000000000000000000000{}{:0>64}",
        addr.to_lowercase(),
        format!("{id:x}"),
    ))
}

/// Strict u64 parse of an `eth_call` hex result — deliberately DIVERGES
/// from `live_clob::parse_balance_hex_to_micro`'s saturate-on-anything
/// behaviour. That sibling feeds a balance-poll task that must never halt;
/// this one feeds §4.7 inventory recovery, which is EXACT by contract — a
/// silently saturated u64::MAX would pass the §4.12 monotone-max check in
/// the inflating direction instead of routing to the cold path.
fn parse_eth_call_hex_u64_strict(raw: &str) -> Result<u64> {
    let t = raw.strip_prefix("0x").unwrap_or(raw);
    if t.is_empty() {
        anyhow::bail!("empty eth_call result");
    }
    let v = U256::from_str_radix(t, 16)
        .with_context(|| format!("non-hex eth_call result {raw:?}"))?;
    u64::try_from(v).map_err(|_| {
        anyhow::anyhow!(
            "eth_call balance 0x{v:x} exceeds u64 — implausible for a CTF position; \
             refusing to saturate"
        )
    })
}

/// One-shot ERC-1155 `balanceOf` read via `eth_call`, returning base-1e6
/// units (CTF positions inherit the collateral's 6 decimals: 5 shares =
/// 5_000_000). Inventory recovery: BUY-only + never-sell
/// + within-slot means the slot tokens' balances ARE the cumulative fills,
/// journal-independent.
pub async fn fetch_erc1155_balance_micro(
    http: &reqwest::Client,
    rpc_url: &str,
    ctf: &str,
    holder: &str,
    token_id_dec: &str,
) -> Result<u64> {
    let calldata = erc1155_balance_of_calldata(holder, token_id_dec)?;
    let payload = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{{"to":"{ctf}","data":"{calldata}"}},"latest"]}}"#
    );
    let resp = http
        .post(rpc_url)
        .header("content-type", "application/json")
        .body(payload)
        .send()
        .await
        .with_context(|| format!("POST {rpc_url} (eth_call 1155 balanceOf)"))?;
    let v: serde_json::Value = resp.json().await.context("parse JSON-RPC response body")?;
    let raw = v
        .get("result")
        .and_then(|r| r.as_str())
        .ok_or_else(|| anyhow::anyhow!("rpc response missing `result`: {v}"))?;
    parse_eth_call_hex_u64_strict(raw)
}

#[derive(Clone, Copy, Debug)]
pub enum CollateralDecimals {
    Six,
}

#[async_trait]
pub trait ErcBalanceRpc: Send + Sync + 'static {
    async fn balance_of(&self, erc20: &str, holder: &str) -> Result<u128>;
}

pub struct UsdcBalanceManager {
    rpc: Arc<dyn ErcBalanceRpc>,
    erc20: String,
    holder: String,
    decimals: CollateralDecimals,
    cap_usd: f64,
    cache: Mutex<CacheState>,
    ttl: Duration,
}

struct CacheState {
    last_value: Option<u128>,
    last_fresh_at: Option<Instant>,
    consecutive_failures: u32,
}

impl UsdcBalanceManager {
    pub fn new(
        rpc: Arc<dyn ErcBalanceRpc>,
        erc20: String,
        holder: String,
        decimals: CollateralDecimals,
        cap_usd: f64,
    ) -> Self {
        Self {
            rpc,
            erc20,
            holder,
            decimals,
            cap_usd,
            cache: Mutex::new(CacheState {
                last_value: None,
                last_fresh_at: None,
                consecutive_failures: 0,
            }),
            ttl: Duration::from_secs(5),
        }
    }

    /// Returns the current balance in USD. Refreshes cache if stale.
    /// Returns the last known value if cache is fresh; refreshes RPC
    /// otherwise. After 3 consecutive RPC failures, returns Err.
    pub async fn current_usd(&self) -> Result<f64> {
        let mut cache = self.cache.lock().await;
        if let (Some(v), Some(t)) = (cache.last_value, cache.last_fresh_at) {
            if t.elapsed() < self.ttl {
                return Ok(self.to_usd(v));
            }
        }
        match self.rpc.balance_of(&self.erc20, &self.holder).await {
            Ok(v) => {
                cache.last_value = Some(v);
                cache.last_fresh_at = Some(Instant::now());
                cache.consecutive_failures = 0;
                Ok(self.to_usd(v))
            }
            Err(e) => {
                cache.consecutive_failures += 1;
                if cache.consecutive_failures >= 3 {
                    anyhow::bail!(
                        "balance RPC failed {} consecutive times: {e}",
                        cache.consecutive_failures
                    );
                }
                if let Some(v) = cache.last_value {
                    tracing::warn!(
                        ?e,
                        consecutive = cache.consecutive_failures,
                        "balance RPC failed; using stale cache"
                    );
                    Ok(self.to_usd(v))
                } else {
                    Err(e).context("first balance RPC failure has no cache to fall back on")
                }
            }
        }
    }

    /// Returns true if a placement of `usd` would still leave the
    /// wallet at or above zero. Uses current cached balance.
    pub async fn can_afford(&self, usd: f64) -> Result<bool> {
        let bal = self.current_usd().await?;
        Ok(bal + 1e-9 >= usd)
    }

    fn to_usd(&self, v: u128) -> f64 {
        let scale = match self.decimals {
            CollateralDecimals::Six => 1_000_000.0,
        };
        v as f64 / scale
    }

    pub fn cap_usd(&self) -> f64 {
        self.cap_usd
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct StubRpc {
        calls: AtomicU32,
        value: u128,
        fail_first: u32,
    }

    #[async_trait]
    impl ErcBalanceRpc for StubRpc {
        async fn balance_of(&self, _: &str, _: &str) -> Result<u128> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_first {
                anyhow::bail!("stubbed failure #{n}");
            }
            Ok(self.value)
        }
    }

    #[tokio::test]
    async fn balance_caches_for_ttl() {
        let stub = Arc::new(StubRpc {
            calls: AtomicU32::new(0),
            value: 150_000_000, // $150 at 6 decimals
            fail_first: 0,
        });
        let m = UsdcBalanceManager::new(
            stub.clone(),
            "0xerc20".into(),
            "0xholder".into(),
            CollateralDecimals::Six,
            150.0,
        );
        let a = m.current_usd().await.unwrap();
        let b = m.current_usd().await.unwrap();
        assert!((a - 150.0).abs() < 1e-9);
        assert_eq!(a, b);
        assert_eq!(stub.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn can_afford_predicate() {
        let stub = Arc::new(StubRpc {
            calls: AtomicU32::new(0),
            value: 50_000_000, // $50
            fail_first: 0,
        });
        let m = UsdcBalanceManager::new(
            stub.clone(),
            "0xerc20".into(),
            "0xholder".into(),
            CollateralDecimals::Six,
            150.0,
        );
        assert!(m.can_afford(10.0).await.unwrap());
        assert!(m.can_afford(50.0).await.unwrap());
        assert!(!m.can_afford(50.01).await.unwrap());
    }

    // --- ERC-1155 balanceOf variant ---

    /// Known-good vector: selector keccak256("balanceOf(address,uint256)")[..4]
    /// = 0x00fdd58e, holder left-padded to 32 bytes, id as uint256. Expected
    /// strings computed independently (python, 2026-08-01).
    #[test]
    fn erc1155_calldata_known_vector_small_id() {
        let got = erc1155_balance_of_calldata(
            "0x365f0ca36ae1f641e02fe3b7743673da42a13a70",
            "5",
        )
        .unwrap();
        assert_eq!(
            got,
            "0x00fdd58e000000000000000000000000365f0ca36ae1f641e02fe3b7743673da42a13a70\
             0000000000000000000000000000000000000000000000000000000000000005"
                .replace(char::is_whitespace, "")
        );
    }

    /// A realistic 77-digit CLOB asset id must fill the full 32-byte word.
    #[test]
    fn erc1155_calldata_known_vector_full_width_id() {
        let got = erc1155_balance_of_calldata(
            "0x365f0ca36ae1f641e02fe3b7743673da42a13a70",
            "21742633143463906290569050155826241533067272736897614950488156847949938836455",
        )
        .unwrap();
        assert_eq!(
            got,
            "0x00fdd58e000000000000000000000000365f0ca36ae1f641e02fe3b7743673da42a13a70\
             3011e4ede0f6befa0ad3f571001d3e1ffeef3d4af78c3112aaac90416e3a43e7"
                .replace(char::is_whitespace, "")
        );
        // 4-byte selector + 2 words = 2 + 8 + 64 + 64 chars.
        assert_eq!(got.len(), 138);
    }

    #[test]
    fn erc1155_calldata_rejects_bad_holder_and_bad_id() {
        assert!(erc1155_balance_of_calldata("0x1234", "5").is_err());
        assert!(erc1155_balance_of_calldata(
            "0x365f0ca36ae1f641e02fe3b7743673da42a13a70",
            "0xdeadbeef"
        )
        .is_err(), "hex-form ids must be rejected — the CLOB asset id is decimal");
        assert!(erc1155_balance_of_calldata(
            "0x365f0ca36ae1f641e02fe3b7743673da42a13a70",
            "not-a-number"
        )
        .is_err());
    }

    #[test]
    fn strict_hex_parse_reads_padded_word_and_zero() {
        assert_eq!(parse_eth_call_hex_u64_strict("0x0").unwrap(), 0);
        // 5 shares in base-1e6 as a full 32-byte eth_call word.
        assert_eq!(
            parse_eth_call_hex_u64_strict(
                "0x00000000000000000000000000000000000000000000000000000000004c4b40"
            )
            .unwrap(),
            5_000_000
        );
    }

    /// Divergence from the ERC-20 sibling is deliberate: §4.7 is an EXACT
    /// read, so garbage and over-u64 values must ERROR, never saturate.
    #[test]
    fn strict_hex_parse_errors_on_garbage_and_overflow() {
        assert!(parse_eth_call_hex_u64_strict("").is_err());
        assert!(parse_eth_call_hex_u64_strict("0x").is_err());
        assert!(parse_eth_call_hex_u64_strict("0xzz").is_err());
        let max_u256 = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        assert!(parse_eth_call_hex_u64_strict(max_u256).is_err());
    }

    #[tokio::test]
    async fn fetch_erc1155_balance_micro_round_trips_mocked_rpc() {
        use wiremock::matchers::{body_string_contains, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            // Pin the wire shape: eth_call to the CTF with the 1155 selector.
            .and(body_string_contains("eth_call"))
            .and(body_string_contains("0x00fdd58e"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "result": "0x00000000000000000000000000000000000000000000000000000000004c4b40"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let micro = fetch_erc1155_balance_micro(
            &reqwest::Client::new(),
            &server.uri(),
            "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
            "0x365f0ca36ae1f641e02fe3b7743673da42a13a70",
            "5",
        )
        .await
        .unwrap();
        assert_eq!(micro, 5_000_000, "5 shares at 6 decimals");
    }

    #[tokio::test]
    async fn fetch_erc1155_balance_micro_errors_on_rpc_error_body() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": 1,
                "error": {"code": -32000, "message": "execution reverted"}
            })))
            .mount(&server)
            .await;
        let err = fetch_erc1155_balance_micro(
            &reqwest::Client::new(),
            &server.uri(),
            "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
            "0x365f0ca36ae1f641e02fe3b7743673da42a13a70",
            "5",
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("missing `result`"), "{err:#}");
    }

    #[tokio::test]
    async fn fails_closed_after_three_consecutive_failures_with_no_cache() {
        let stub = Arc::new(StubRpc {
            calls: AtomicU32::new(0),
            value: 0,
            fail_first: 999,
        });
        let m = UsdcBalanceManager::new(
            stub.clone(),
            "0xerc20".into(),
            "0xholder".into(),
            CollateralDecimals::Six,
            150.0,
        );
        let _ = m.current_usd().await;
        let _ = m.current_usd().await;
        let r = m.current_usd().await;
        assert!(r.is_err());
    }
}
