//! `LiveClob::cancel_market_orders` — the per-market cancel-all primitive
//! (`DELETE /cancel-market-orders {"market": cid}`, venue fact pinned
//! 2026-08-01 L21.28). Consumers: the §4.4 close sweep, the §4.7 cold
//! restart, and kill-switch halts.
//!
//! Pinned here: the body shape the venue expects, the L2-signed DELETE, and
//! that an HTTP failure is an ERROR rather than a silent "swept clean" — an
//! operator who trusts a false clean sweep leaves live bids resting.

use onecentbot::config::{ContractAddresses, LiveConfig};
use onecentbot::execution::live_clob::LiveClob;
use wiremock::matchers::{body_json, header_exists, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn live_cfg(base: &str) -> LiveConfig {
    let toml = format!(
        r#"
[wallet]
proxy_wallet = "0x365f0ca36ae1f641e02fe3b7743673da42a13a70"
private_key = "0x4c0883a69102937d6231471b5dbb6204fe512961708279feb1be6ae5538da033"
signature_type = 1

[chain]
rpc_url = "http://127.0.0.1:1"
chain_id = 137

[clob]
http_base = "{base}"
ws_url = "wss://example.invalid"
signature_type = 1

[api]
key = "00000000-0000-0000-0000-000000000001"
secret = "c2VjcmV0LXNlY3JldC1zZWNyZXQt"
passphrase = "pp"

[safety]
cap_usd = 150.0
kill_switch_loss_usd = 200.0
velocity_loss_per_hour = 50.0
velocity_consecutive_hrs = 2
skip_alternate_cids = false
"#
    );
    toml::from_str(&toml).expect("test LiveConfig parses")
}

fn clob(base: &str) -> LiveClob {
    LiveClob::new(&live_cfg(base), ContractAddresses::v2_defaults()).unwrap()
}

#[tokio::test]
async fn cancel_market_orders_sends_the_market_body_and_l2_headers() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/cancel-market-orders"))
        .and(body_json(serde_json::json!({"market": "0xcid"})))
        .and(header_exists("poly_signature"))
        .and(header_exists("poly_timestamp"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "canceled": ["0xorder1", "0xorder2"],
            "not_canceled": {}
        })))
        .mount(&server)
        .await;

    let n = clob(&server.uri())
        .cancel_market_orders("0xcid")
        .await
        .expect("sweep succeeds");
    assert_eq!(n, 2, "the caller wants to know how many were swept");
}

#[tokio::test]
async fn an_http_failure_is_an_error_not_a_false_clean_sweep() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/cancel-market-orders"))
        .respond_with(ResponseTemplate::new(500).set_body_string("upstream boom"))
        .mount(&server)
        .await;

    assert!(clob(&server.uri()).cancel_market_orders("0xcid").await.is_err());
}

/// Sweeping a market with nothing resting is a legitimate no-op, not a fault.
#[tokio::test]
async fn an_empty_sweep_is_a_success_with_zero() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/cancel-market-orders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "canceled": [], "not_canceled": {}
        })))
        .mount(&server)
        .await;

    assert_eq!(
        clob(&server.uri())
            .cancel_market_orders("0xcid")
            .await
            .unwrap(),
        0
    );
}
