//! T4: order time-in-force plumbing.
//! Pins that `place_buy_with_order_type` puts the requested orderType +
//! postOnly on the wire (FAK marketable limit; GTC-with-cancel fallback is
//! marketable too, so postOnly must be false), and that the venue
//! LIMIT-order floor (≥ 5 shares) fails closed before any HTTP call.

use onecentbot::config::{ContractAddresses, LiveConfig};
use onecentbot::execution::live_clob::{LiveClob, OrderType};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

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

fn body_json(req: &Request) -> serde_json::Value {
    serde_json::from_slice(&req.body).expect("order POST body is JSON")
}

#[tokio::test]
async fn fak_order_type_and_post_only_false_reach_the_wire() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "orderID": "0xdeadbeef"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let clob = LiveClob::new(&live_cfg(&server.uri()), ContractAddresses::v2_defaults())
        .expect("LiveClob");
    let id = clob
        .place_buy_with_order_type("1234567890", 0.61, 8.0, OrderType::Fak, false)
        .await
        .expect("post ok");
    assert_eq!(id.as_str(), "0xdeadbeef");

    let reqs = server.received_requests().await.expect("requests recorded");
    let body = body_json(&reqs[0]);
    assert_eq!(body["orderType"], "FAK");
    assert_eq!(body["postOnly"], false);
    assert_eq!(body["order"]["side"], "BUY");
    assert_eq!(body["order"]["tokenId"], "1234567890");
}

#[tokio::test]
async fn gtc_fallback_is_marketable_not_post_only() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "orderID": "0xfeedface"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let clob = LiveClob::new(&live_cfg(&server.uri()), ContractAddresses::v2_defaults())
        .expect("LiveClob");
    clob.place_buy_with_order_type("42", 0.38, 8.0, OrderType::Gtc, false)
        .await
        .expect("post ok");

    let reqs = server.received_requests().await.expect("requests recorded");
    let body = body_json(&reqs[0]);
    assert_eq!(body["orderType"], "GTC");
    assert_eq!(
        body["postOnly"], false,
        "marketable GTC fallback must NOT be postOnly (would be venue-rejected)"
    );
}

#[tokio::test]
async fn venue_limit_floor_rejects_below_5_shares_before_any_http() {
    let server = MockServer::start().await;
    // no mounted mock: any HTTP call would 404 — but none must happen
    let clob = LiveClob::new(&live_cfg(&server.uri()), ContractAddresses::v2_defaults())
        .expect("LiveClob");
    let err = clob
        .place_buy_with_order_type("42", 0.50, 4.99, OrderType::Fak, false)
        .await
        .expect_err("sub-floor size must fail closed");
    assert!(
        err.to_string().contains("5"),
        "error should name the 5-share floor: {err}"
    );
    assert!(
        server.received_requests().await.expect("recorded").is_empty(),
        "floor check must run before any HTTP"
    );
}
