//! Phase-9 Tier 2-5 HTTP stack, ported 2026-06-13 from branch
//! worktree-phase9-tier1-reaction-budget (@ 4f0a373 + 273ebb5) after the
//! live canary measured post→ack p50 591ms with 1.2-1.5s
//! cold-connection tails (canary leg-1 fill-failure probe,
//! 2026-06-12, §6) vs the branch's
//! measured 104ms warm floor. Pins: production defaults unchanged
//! (ALPN-negotiated, shared pool), per-coin isolation is opt-in, the
//! keepalive heartbeat actually hits /time and stops on shutdown, and the
//! FAK no-match error contract survives the Tier-4 diagnostics suffix.

use sniperbot::config::{ContractAddresses, LiveConfig};
use sniperbot::execution::live_clob::{LiveClob, LiveClobOptions, OrderType};
use std::sync::Arc;
use wiremock::matchers::{method, path};
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

#[test]
fn default_options_are_the_production_configuration() {
    let opts = LiveClobOptions::default();
    assert!(!opts.force_http1_only, "ALPN negotiates by default");
    assert!(!opts.isolated_clients_per_coin, "shared pool by default");
}

#[tokio::test]
async fn tuned_client_constructs_under_every_option_combination() {
    for (h1, iso) in [(false, false), (true, false), (false, true), (true, true)] {
        let opts = LiveClobOptions { force_http1_only: h1, isolated_clients_per_coin: iso };
        LiveClob::new_with_options(&live_cfg("http://127.0.0.1:1"), ContractAddresses::v2_defaults(), opts)
            .unwrap_or_else(|e| panic!("h1={h1} iso={iso}: {e:#}"));
    }
}

/// Per-coin isolation must not change wire behavior — a post still lands on
/// /order and parses the same response (it just rides a dedicated pool).
#[tokio::test]
async fn isolated_clients_post_orders_identically() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "success": true,
            "orderID": "0xabc"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let clob = LiveClob::new_with_options(
        &live_cfg(&server.uri()),
        ContractAddresses::v2_defaults(),
        LiveClobOptions { isolated_clients_per_coin: true, ..Default::default() },
    )
    .unwrap();
    let id = clob
        .place_buy_with_order_type("42", 0.61, 8.0, OrderType::Fak, false)
        .await
        .expect("post ok");
    assert_eq!(id.as_str(), "0xabc");
}

/// 2026-06-12 contract: a zero-match FAK is HTTP 400 with an orderID in the
/// body. The Tier-4 diagnostics suffix on the bail message must not break
/// the no-match detection nor the orderID extraction downstream
/// (the strategy-side `extract_order_id`).
#[tokio::test]
async fn fak_no_match_error_text_keeps_body_and_order_id_with_diagnostics_suffix() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/order"))
        .respond_with(
            ResponseTemplate::new(400)
                .insert_header("cf-ray", "8a3e7b1c-BEG")
                .set_body_json(serde_json::json!({
                    "error": "no orders found to match with FAK order",
                    "orderID": "0xfak1"
                })),
        )
        .mount(&server)
        .await;
    let clob =
        LiveClob::new(&live_cfg(&server.uri()), ContractAddresses::v2_defaults()).unwrap();
    let err = clob
        .place_buy_with_order_type("42", 0.61, 8.0, OrderType::Fak, false)
        .await
        .expect_err("400 must error");
    let msg = err.to_string();
    assert!(msg.contains("no orders found to match with FAK order"), "{msg}");
    assert!(msg.contains("\"orderID\":\"0xfak1\""), "orderID extractable: {msg}");
    assert!(msg.contains("cf_ray=8a3e7b1c-BEG"), "diagnostics on error path: {msg}");
}

#[tokio::test]
async fn keepalive_heartbeat_hits_time_and_stops_on_shutdown() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/time"))
        .respond_with(ResponseTemplate::new(200).set_body_string("1781300000"))
        .mount(&server)
        .await;
    let clob = Arc::new(
        LiveClob::new(&live_cfg(&server.uri()), ContractAddresses::v2_defaults()).unwrap(),
    );
    let shutdown = tokio_util::sync::CancellationToken::new();
    // every_secs is clamped to >=5 inside; use tokio's paused time to avoid
    // a slow test: first tick fires immediately on interval creation.
    clob.spawn_keepalive_heartbeat(5, shutdown.clone());
    // first interval tick is immediate; give the task a moment to GET
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if !server.received_requests().await.unwrap_or_default().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("heartbeat must hit /time at least once");
    shutdown.cancel();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let n_after_cancel = server.received_requests().await.unwrap_or_default().len();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let n_later = server.received_requests().await.unwrap_or_default().len();
    assert_eq!(n_after_cancel, n_later, "no heartbeats after shutdown");
}
