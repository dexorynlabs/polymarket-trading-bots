//! Preflight one-shot eth_subscribe round-trip. Live-network dependent;
//! the round-trip test is ignored by default. Compile-check is always
//! enforced so refactors that break the export get caught.

use sniperbot::polymarket::onchain::polygon_events::preflight_subscribe;

#[tokio::test]
#[ignore]
async fn preflight_subscribe_round_trip_against_live_rpc() {
    let ws = std::env::var("POLYGON_WS_URL")
        .expect("POLYGON_WS_URL not set; this test is ignored by default — set the env var to run");
    let r = preflight_subscribe(&ws, std::time::Duration::from_secs(30)).await;
    assert!(r.is_ok(), "preflight RPC-WS round-trip failed: {:?}", r);
}

#[test]
fn preflight_subscribe_fn_exists_and_compiles() {
    // Compile-only check — guarantees the helper is exported and callable.
    let _ = preflight_subscribe;
}
