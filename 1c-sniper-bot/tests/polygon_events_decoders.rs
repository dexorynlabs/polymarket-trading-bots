//! Unit tests for Polygon adapter event decoders. Topic0 hexes verified
//! against captured on-chain logs (4h capture, 2026-05-17).
//! Synthetic-log inputs mirror the real on-chain layout
//! (redeemer at topics[1], cid at topics[2]; Merged.data = 1 uint256,
//! Redeemed.data = (uint256[] X, uint256 amount) head-tail-encoded).

use onecentbot::polymarket::onchain::polygon_events::{
    decode_merged_log, decode_redeemed_log, MergedEvent, RawLog, RedeemedEvent, MERGED_TOPIC0,
    REDEEMED_TOPIC0,
};

const ADAPTER_ADDR: [u8; 20] = [
    0xad, 0xa1, 0x00, 0xdb, 0x00, 0xca, 0x00, 0x07, 0x38, 0x11, 0x82, 0x06, 0x92, 0x00, 0x54, 0x00,
    0x21, 0x8f, 0xce, 0x1f,
];

#[test]
fn topic0_merged_pins_0x08c15f31() {
    assert_eq!(&MERGED_TOPIC0[..4], &[0x08, 0xc1, 0x5f, 0x31]);
    assert_eq!(
        hex::encode(MERGED_TOPIC0),
        "08c15f31e413dbd9bbd3b4c01b191db840e1657ab4f8d1d7d448a9a5083ba255"
    );
}

#[test]
fn topic0_redeemed_pins_0x1daec0b1() {
    assert_eq!(&REDEEMED_TOPIC0[..4], &[0x1d, 0xae, 0xc0, 0xb1]);
    assert_eq!(
        hex::encode(REDEEMED_TOPIC0),
        "1daec0b1f629de29b749a189499f43104a80df46a9f34d9ccf73aa59b85fc0c0"
    );
}

fn padded_address(addr: [u8; 20]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[12..32].copy_from_slice(&addr);
    out
}

fn u256_be(v: u128) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[16..32].copy_from_slice(&v.to_be_bytes());
    out
}

#[test]
fn decode_merged_log_extracts_cid_redeemer_amount() {
    let redeemer = [0xA6u8; 20];
    let cid = [0xABu8; 32];
    let amount: u128 = 155_000_000; // 155 sh × $1 × 1e6 pUSD decimals

    let log = RawLog {
        address: ADAPTER_ADDR,
        topics: vec![MERGED_TOPIC0, padded_address(redeemer), cid],
        data: u256_be(amount).to_vec(),
        block_number: 100,
        tx_hash: [0xEEu8; 32],
        log_index: 0,
    };
    let m = decode_merged_log(&log).expect("decode succeeds");
    assert_eq!(
        m,
        MergedEvent {
            condition_id: cid,
            redeemer,
            amount_pusd_units: amount,
            tx_hash: [0xEEu8; 32],
            block_number: 100,
        }
    );
}

#[test]
fn decode_redeemed_log_extracts_cid_redeemer_payout() {
    let redeemer = [0xA6u8; 20];
    let cid = [0xBCu8; 32];
    let amount: u128 = 1_015_914_070; // matches captured payout_usd 1015.91407

    // ABI-encode (uint256[] X, uint256 amount) where X = [a, b]:
    //   head[0] = offset to X = 0x40 (64)
    //   head[1] = amount
    //   tail: length(=2), X[0], X[1]
    let mut data = Vec::with_capacity(5 * 32);
    data.extend_from_slice(&u256_be(0x40)); // offset
    data.extend_from_slice(&u256_be(amount)); // amount
    data.extend_from_slice(&u256_be(2)); // length
    data.extend_from_slice(&u256_be(amount)); // X[0]
    data.extend_from_slice(&u256_be(961_843_344)); // X[1]

    let log = RawLog {
        address: ADAPTER_ADDR,
        topics: vec![REDEEMED_TOPIC0, padded_address(redeemer), cid],
        data,
        block_number: 200,
        tx_hash: [0xFFu8; 32],
        log_index: 3,
    };
    let r = decode_redeemed_log(&log).expect("decode succeeds");
    assert_eq!(
        r,
        RedeemedEvent {
            condition_id: cid,
            redeemer,
            payout_pusd_units: amount,
            tx_hash: [0xFFu8; 32],
            block_number: 200,
        }
    );
}

#[test]
fn decode_merged_log_rejects_wrong_topic0() {
    let log = RawLog {
        address: ADAPTER_ADDR,
        topics: vec![[0x00u8; 32], padded_address([0xA6u8; 20]), [0xABu8; 32]],
        data: u256_be(123).to_vec(),
        block_number: 0,
        tx_hash: [0u8; 32],
        log_index: 0,
    };
    assert!(decode_merged_log(&log).is_err());
}

#[test]
fn decode_redeemed_log_rejects_short_data() {
    let log = RawLog {
        address: ADAPTER_ADDR,
        topics: vec![REDEEMED_TOPIC0, padded_address([0xA6u8; 20]), [0xBCu8; 32]],
        data: vec![0u8; 32], // only 32 bytes; needs >= 64
        block_number: 0,
        tx_hash: [0u8; 32],
        log_index: 0,
    };
    assert!(decode_redeemed_log(&log).is_err());
}

// --- recover_gap tests ---

use onecentbot::polymarket::onchain::polygon_events::{GapRecoveryRpc, PolygonEventsSubscriber};
use std::sync::Mutex;

struct MockGapRpc {
    head: Mutex<u64>,
    queued: Mutex<Vec<((u64, u64), Vec<RawLog>)>>,
    calls: Mutex<Vec<String>>,
}

impl MockGapRpc {
    fn new() -> Self {
        Self {
            head: Mutex::new(0),
            queued: Mutex::new(Vec::new()),
            calls: Mutex::new(Vec::new()),
        }
    }
    fn set_chain_head(&self, h: u64) {
        *self.head.lock().unwrap() = h;
    }
    fn queue_logs_for_range(&self, from: u64, to: u64, logs: Vec<RawLog>) {
        self.queued.lock().unwrap().push(((from, to), logs));
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl GapRecoveryRpc for MockGapRpc {
    async fn eth_block_number(&self) -> anyhow::Result<u64> {
        self.calls
            .lock()
            .unwrap()
            .push("eth_blockNumber".to_string());
        Ok(*self.head.lock().unwrap())
    }
    async fn eth_get_logs(
        &self,
        from_block: u64,
        to_block: u64,
        _address: [u8; 20],
        _topics: Vec<[u8; 32]>,
    ) -> anyhow::Result<Vec<RawLog>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("eth_getLogs(from={from_block},to={to_block})"));
        let mut q = self.queued.lock().unwrap();
        if let Some(pos) = q
            .iter()
            .position(|((f, t), _)| *f == from_block && *t == to_block)
        {
            Ok(q.remove(pos).1)
        } else {
            Ok(vec![])
        }
    }
}

#[tokio::test]
async fn reconnect_uses_eth_get_logs_for_gap_block_range() {
    // Simulate: subscriber's last_seen_block = 100; chain head jumps to 250.
    // On recover_gap, subscriber must call eth_getLogs(from=101, to=250).
    let rpc = std::sync::Arc::new(MockGapRpc::new());
    rpc.set_chain_head(250);
    let synth_log = RawLog {
        address: ADAPTER_ADDR,
        topics: vec![MERGED_TOPIC0, [0u8; 32], [0u8; 32]],
        data: vec![0u8; 32],
        block_number: 150,
        tx_hash: [0xAA; 32],
        log_index: 0,
    };
    rpc.queue_logs_for_range(
        101,
        250,
        vec![synth_log.clone(), synth_log.clone(), synth_log],
    );

    let mut sub = PolygonEventsSubscriber::new(std::sync::Arc::clone(&rpc));
    sub.set_last_seen_block(100);
    let recovered = sub.recover_gap().await.expect("recover_gap");
    assert_eq!(recovered.len(), 3, "expected 3 logs recovered");

    let calls = rpc.calls();
    assert!(
        calls.iter().any(|c| c == "eth_blockNumber"),
        "expected eth_blockNumber call; got: {:?}",
        calls
    );
    assert!(
        calls.iter().any(|c| c == "eth_getLogs(from=101,to=250)"),
        "expected eth_getLogs(from=101,to=250); got: {:?}",
        calls
    );
    assert_eq!(
        sub.last_seen_block(),
        250,
        "last_seen_block must advance to chain head after recovery"
    );
}

#[tokio::test]
async fn recover_gap_no_op_when_already_at_head() {
    let rpc = std::sync::Arc::new(MockGapRpc::new());
    rpc.set_chain_head(100);
    let mut sub = PolygonEventsSubscriber::new(std::sync::Arc::clone(&rpc));
    sub.set_last_seen_block(100);
    let recovered = sub.recover_gap().await.expect("recover_gap");
    assert!(recovered.is_empty(), "no logs when head == last_seen");
}
