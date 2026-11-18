//! Read-only LIVE smoke for the ERC-1155 CTF `balanceOf` read (inventory
//! recovery, 2026-08-01). No secrets, no orders, no
//! state changes: public data-api positions rows for `PROXY` (set it to any proxy
//! wallet that currently holds positions) are
//! cross-checked against `eth_call balanceOf(proxy, asset_id)` on a public
//! Polygon RPC. A wrong selector / padding / id-decode returns 0 or garbage
//! for every row, so one exact match proves the encoding end-to-end.
//!
//! Run: cargo test -p polymarket-client --profile release-test \
//!        --test erc1155_balance_live_smoke -- --ignored --nocapture

use sniperbot::config::ContractAddresses;
use sniperbot::execution::balance::fetch_erc1155_balance_micro;

const PROXY: &str = "0x365f0ca36ae1f641e02fe3b7743673da42a13a70";
// polygon-rpc.com now answers "tenant disabled" (2026-08-01); these are the
// public RPCs the on-chain scripts already use.
const RPCS: [&str; 2] = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
];

#[tokio::test]
#[ignore = "live network: public Polygon RPC + Polymarket data-api"]
async fn erc1155_balance_matches_data_api_positions() {
    let http = reqwest::Client::new();
    let url = format!("https://data-api.polymarket.com/positions?user={PROXY}&limit=20");
    let rows: serde_json::Value = http
        .get(&url)
        .send()
        .await
        .expect("GET data-api positions")
        .json()
        .await
        .expect("positions JSON");
    let rows = rows.as_array().expect("positions is an array");
    let ctf = ContractAddresses::v2_defaults().ctf;

    let mut tried = 0usize;
    let mut matched = 0usize;
    for row in rows {
        let Some(asset) = row.get("asset").and_then(|a| a.as_str()) else { continue };
        let Some(size) = row.get("size").and_then(|s| s.as_f64()) else { continue };
        if size <= 0.0 {
            continue;
        }
        tried += 1;
        let mut read = None;
        for rpc in RPCS {
            match fetch_erc1155_balance_micro(&http, rpc, &ctf, PROXY, asset).await {
                Ok(m) => {
                    read = Some(m);
                    break;
                }
                Err(e) => println!("rpc {rpc} failed ({e:#}); trying next"),
            }
        }
        let micro = read.expect("every public RPC failed the eth_call");
        let chain_sh = micro as f64 / 1e6;
        let diff = (chain_sh - size).abs();
        println!("asset {}… data-api {size} sh, chain {chain_sh} sh (diff {diff:.6})", &asset[..12]);
        // Exact-agreement tolerance: data-api sizes are decimal shares; the
        // chain is base-1e6. Fills between the two reads can skew a row, so
        // one exact row among several is the pass condition.
        if diff <= 0.01 {
            matched += 1;
        }
        if tried >= 5 {
            break;
        }
    }
    assert!(
        tried > 0,
        "proxy holds no data-api positions to cross-check — rerun when it holds tokens"
    );
    assert!(
        matched >= 1,
        "0/{tried} positions matched the chain read — encoding or endpoint wrong"
    );
}
