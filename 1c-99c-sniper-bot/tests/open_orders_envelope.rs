//! `LiveClob::open_orders` — the open-orders getter
//! (2026-08-01). The venue docs claim `GET /data/orders` returns a
//! flat array; the live response is a cursor ENVELOPE
//! `{data, next_cursor, limit, count}` with terminal cursor `"LTE="`
//! (doc lie caught live 2026-08-01, venue-facts probe
//! §5.5). These tests pin the
//! end-to-end wire behaviour: envelope walk across pages, the `market`
//! filter riding in the query string, and error (never "no orders") on a
//! non-envelope body or HTTP failure.

use sniperbot::config::{ContractAddresses, LiveConfig};
use sniperbot::execution::live_clob::LiveClob;
use wiremock::matchers::{method, path, query_param};
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

/// Two-page walk: page 1 hands back cursor "NTAw", page 2 is terminal.
/// A flat-array reading of this endpoint would have returned page 1 only.
#[tokio::test]
async fn open_orders_follows_next_cursor_across_pages() {
    let server = MockServer::start().await;
    // Mount the page-2 mock first: wiremock picks the first mock whose
    // matchers all pass, and only page 2 carries next_cursor=NTAw.
    Mock::given(method("GET"))
        .and(path("/data/orders"))
        .and(query_param("next_cursor", "NTAw"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"id": "0xpage2", "side": "BUY", "size_matched": "0"}],
            "next_cursor": "LTE=", "limit": 500, "count": 1
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/data/orders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [
                {"id": "0xpage1a", "side": "BUY", "size_matched": "0"},
                {"id": "0xpage1b", "side": "BUY", "size_matched": "5"}
            ],
            "next_cursor": "NTAw", "limit": 500, "count": 2
        })))
        .expect(1)
        .mount(&server)
        .await;
    let orders = clob(&server.uri()).open_orders(None).await.expect("walk ok");
    let ids: Vec<&str> = orders.iter().map(|o| o.id.as_str()).collect();
    assert_eq!(ids, ["0xpage1a", "0xpage1b", "0xpage2"]);
    assert_eq!(orders[1].size_matched, "5");
}

/// The `market` (condition id) filter rides in the query string; the L2
/// signature covers only the bare path, so the filtered call must still be
/// accepted by a server matching on the query.
#[tokio::test]
async fn open_orders_sends_market_filter_in_query() {
    let server = MockServer::start().await;
    let cid = "0x0a1aaa0fdead";
    Mock::given(method("GET"))
        .and(path("/data/orders"))
        .and(query_param("market", cid))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [], "next_cursor": "LTE=", "limit": 500, "count": 0
        })))
        .expect(1)
        .mount(&server)
        .await;
    let orders = clob(&server.uri()).open_orders(Some(cid)).await.expect("filtered ok");
    assert!(orders.is_empty());
}

/// The documented-but-wrong flat array must surface as an ERROR — §4.7
/// reconciliation treating it as "no orders" would cancel-all a healthy
/// book on every restart.
#[tokio::test]
async fn open_orders_flat_array_body_errors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data/orders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .mount(&server)
        .await;
    let err = clob(&server.uri()).open_orders(None).await.unwrap_err();
    assert!(err.to_string().contains("not the cursor envelope"), "{err:#}");
}

#[tokio::test]
async fn open_orders_http_error_is_an_error_not_empty() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data/orders"))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(serde_json::json!({"error": "invalid signature"})),
        )
        .mount(&server)
        .await;
    let err = clob(&server.uri()).open_orders(None).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("HTTP 401"), "{msg}");
    assert!(msg.contains("invalid signature"), "{msg}");
}
