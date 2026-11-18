//! Bench the PRE-SEND order-construction path — the one "UNMEASURED" row in
//! the taker latency budget (latency-options probe, 2026-07-26, §7;
//! handoff §3d).
//!
//! `LiveClob::post_order` does, on the hot path, before the socket is touched:
//!   sign_order (EIP-712 hash + secp256k1 ECDSA) -> order_to_json
//!   -> serde_json::to_string -> auth_headers (base64 decode + HMAC-SHA256)
//! and only then `req.send()`. The question the handoff asks is whether that
//! is worth pre-signing during the ARMED window. This answers it with numbers
//! instead of a guess.
//!
//! Ignored by default (it is a benchmark, not an assertion). Run:
//!   cargo test -p polymarket-client --release --test order_construction_bench -- --ignored --nocapture
//!
//! Uses a well-known throwaway test private key and fake API creds — reads no
//! config, holds no funds, sends nothing.

use std::time::Instant;

use sniperbot::execution::auth::{auth_headers, ApiCreds};
use sniperbot::execution::signer::{PolymarketOrder, PolymarketOrderDomain, SignerService};

/// hardhat/anvil account #0 — public, throwaway, holds nothing.
const TEST_PK: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

fn pct(xs: &[f64], p: f64) -> f64 {
    let mut s = xs.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[((p / 100.0 * s.len() as f64) as usize).min(s.len() - 1)]
}

fn stat(label: &str, us: &[f64]) {
    println!(
        "  {label:<34} p50 {:>8.1}  p90 {:>8.1}  p99 {:>8.1}  max {:>8.1}  (us)",
        pct(us, 50.0),
        pct(us, 90.0),
        pct(us, 99.0),
        pct(us, 100.0),
    );
}

fn sample_order() -> PolymarketOrder {
    PolymarketOrder {
        salt: 123_456_789,
        maker: "0x365f0ca36ae1f641e02fe3b7743673da42a13a70".into(),
        signer: "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".into(),
        taker: "0x0000000000000000000000000000000000000000".into(),
        // a real btc-updown-5m token id (77 decimal digits) — length matters
        // for the U256 radix parse inside sign_order.
        token_id: "71321045679252212594626385532706912750332728571942532289631379312455583992563"
            .into(),
        maker_amount: 11_000_000,
        taker_amount: 366_666_666,
        expiration: 0,
        timestamp_ms: 1_785_069_900_000,
        metadata: [0u8; 32],
        builder: [0u8; 32],
        side: 0,
        signature_type: 1,
    }
}

fn sample_domain() -> PolymarketOrderDomain {
    PolymarketOrderDomain {
        name: "Polymarket CTF Exchange".into(),
        version: "2".into(),
        chain_id: 137,
        verifying_contract: "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E".into(),
    }
}

fn sample_creds() -> ApiCreds {
    ApiCreds {
        key: "00000000-0000-0000-0000-000000000000".into(),
        secret: "dGVzdC1zZWNyZXQtdGhhdC1pcy1sb25nZXItdGhhbi1zaG9ydA==".into(),
        passphrase: "0000000000000000000000000000000000000000000000000000000000000000".into(),
        address: "0x365f0ca36ae1f641e02fe3b7743673da42a13a70".into(),
    }
}

#[test]
#[ignore = "benchmark; run with --ignored --nocapture"]
fn bench_pre_send_construction() {
    const WARMUP: usize = 200;
    const N: usize = 3_000;

    let signer = SignerService::from_pk_hex(TEST_PK).expect("signer");
    let order = sample_order();
    let domain = sample_domain();
    let creds = sample_creds();

    // A representative POST /order body: the signed order plus the four
    // envelope fields. Serialised once to size the HMAC input honestly.
    let sig0 = signer.sign_order(&order, &domain).expect("sign");
    let body_value = serde_json::json!({
        "order": {
            "salt": order.salt,
            "maker": order.maker,
            "signer": order.signer,
            "taker": order.taker,
            "tokenId": order.token_id,
            "makerAmount": order.maker_amount.to_string(),
            "takerAmount": order.taker_amount.to_string(),
            "side": "BUY",
            "expiration": "0",
            "signatureType": order.signature_type,
            "timestamp": order.timestamp_ms.to_string(),
            "metadata": format!("0x{}", hex::encode(order.metadata)),
            "builder": format!("0x{}", hex::encode(order.builder)),
            "signature": sig0,
        },
        "owner": creds.key,
        "orderType": "FAK",
        "postOnly": false,
    });
    let body_str = serde_json::to_string(&body_value).expect("serialise");

    for _ in 0..WARMUP {
        let s = signer.sign_order(&order, &domain).unwrap();
        std::hint::black_box(s);
        std::hint::black_box(auth_headers(&creds, "POST", "/order", &body_str).unwrap());
    }

    let (mut sign_us, mut json_us, mut hmac_us, mut total_us) =
        (vec![], vec![], vec![], vec![]);
    for _ in 0..N {
        let t_all = Instant::now();

        let t = Instant::now();
        let sig = signer.sign_order(&order, &domain).expect("sign");
        sign_us.push(t.elapsed().as_secs_f64() * 1e6);

        let t = Instant::now();
        let s = serde_json::to_string(&body_value).expect("serialise");
        json_us.push(t.elapsed().as_secs_f64() * 1e6);

        let t = Instant::now();
        let h = auth_headers(&creds, "POST", "/order", &s).expect("auth");
        hmac_us.push(t.elapsed().as_secs_f64() * 1e6);

        total_us.push(t_all.elapsed().as_secs_f64() * 1e6);
        std::hint::black_box((sig, h));
    }

    println!("\n  pre-send order construction, n={N}, body {} bytes", body_str.len());
    stat("sign_order (EIP-712 + ECDSA)", &sign_us);
    stat("serde_json::to_string(body)", &json_us);
    stat("auth_headers (b64 + HMAC-SHA256)", &hmac_us);
    stat("TOTAL pre-send", &total_us);
    println!(
        "\n  => TOTAL p50 {:.3} ms / p99 {:.3} ms of the ~560 ms pipeline\n",
        pct(&total_us, 50.0) / 1000.0,
        pct(&total_us, 99.0) / 1000.0,
    );
}
