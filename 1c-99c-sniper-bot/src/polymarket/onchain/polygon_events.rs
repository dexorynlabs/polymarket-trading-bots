//! Polygon `eth_subscribe(logs)` + `eth_getLogs` for the CTF adapter's
//! `PositionsMerged` and `Redeemed` events. Topic0 constants live in
//! `crate::polymarket::onchain::constants`.
//!
//! ## Event layouts (verified against captured Polygon logs)
//!
//! `PositionsMerged(address indexed redeemer, bytes32 indexed conditionId,
//!                  uint256 amount)`:
//! - topics[0] = MERGED_TOPIC0
//! - topics[1] = indexed redeemer (our address, left-padded to 32 bytes)
//! - topics[2] = indexed conditionId
//! - data      = 32 bytes = uint256 amount (in pUSD units, 6 decimals)
//!
//! `Redeemed(address indexed redeemer, bytes32 indexed conditionId,
//!           uint256[] X, uint256 amount)`:
//! - topics[0] = REDEEMED_TOPIC0
//! - topics[1] = indexed redeemer
//! - topics[2] = indexed conditionId
//! - data      = 5 × 32 bytes:
//!     bytes[0..32]   = offset to X (= 0x40)
//!     bytes[32..64]  = amount (payout in pUSD units)
//!     bytes[64..96]  = X.length
//!     bytes[96..128] = X[0]
//!     bytes[128..160]= X[1]
//!
//! Note: the strategy code only consumes `condition_id` and `amount`;
//! the partition array X is decoded but not surfaced through the event
//! structs.

pub use crate::polymarket::onchain::constants::{MERGED_TOPIC0, REDEEMED_TOPIC0};
use anyhow::{anyhow, Result};

#[derive(Clone, Debug, PartialEq)]
pub struct RawLog {
    pub address: [u8; 20],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
    pub block_number: u64,
    pub tx_hash: [u8; 32],
    pub log_index: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MergedEvent {
    pub condition_id: [u8; 32],
    pub redeemer: [u8; 20],
    /// Merged amount in pUSD units (6 decimals). Divide by 1_000_000 for
    /// shares (at $1 parity, 1 share = $1 = 1_000_000 pUSD units).
    pub amount_pusd_units: u128,
    pub tx_hash: [u8; 32],
    pub block_number: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RedeemedEvent {
    pub condition_id: [u8; 32],
    pub redeemer: [u8; 20],
    /// Total payout in pUSD units.
    pub payout_pusd_units: u128,
    pub tx_hash: [u8; 32],
    pub block_number: u64,
}

pub fn decode_merged_log(log: &RawLog) -> Result<MergedEvent> {
    if log.topics.first() != Some(&MERGED_TOPIC0) {
        return Err(anyhow!("topic0 mismatch for Merged"));
    }
    let redeemer = decode_indexed_address(
        log.topics
            .get(1)
            .ok_or_else(|| anyhow!("Merged log missing indexed redeemer"))?,
    );
    let cid = *log
        .topics
        .get(2)
        .ok_or_else(|| anyhow!("Merged log missing indexed conditionId"))?;
    if log.data.len() < 32 {
        return Err(anyhow!("Merged data too short: {} bytes", log.data.len()));
    }
    let amount = u256_low_u128(&log.data[0..32]);
    Ok(MergedEvent {
        condition_id: cid,
        redeemer,
        amount_pusd_units: amount,
        tx_hash: log.tx_hash,
        block_number: log.block_number,
    })
}

pub fn decode_redeemed_log(log: &RawLog) -> Result<RedeemedEvent> {
    if log.topics.first() != Some(&REDEEMED_TOPIC0) {
        return Err(anyhow!("topic0 mismatch for Redeemed"));
    }
    let redeemer = decode_indexed_address(
        log.topics
            .get(1)
            .ok_or_else(|| anyhow!("Redeemed log missing indexed redeemer"))?,
    );
    let cid = *log
        .topics
        .get(2)
        .ok_or_else(|| anyhow!("Redeemed log missing indexed conditionId"))?;
    if log.data.len() < 64 {
        return Err(anyhow!("Redeemed data too short: {} bytes", log.data.len()));
    }
    // Layout is (uint256[] X, uint256 amount); the amount sits in head slot 1
    // (bytes[32..64]) per ABI head-tail encoding (X is dynamic, its head is
    // an offset; amount is static, inlined in head).
    let amount = u256_low_u128(&log.data[32..64]);
    Ok(RedeemedEvent {
        condition_id: cid,
        redeemer,
        payout_pusd_units: amount,
        tx_hash: log.tx_hash,
        block_number: log.block_number,
    })
}

fn decode_indexed_address(topic: &[u8; 32]) -> [u8; 20] {
    // Indexed address topics are left-padded with 12 zero bytes.
    let mut out = [0u8; 20];
    out.copy_from_slice(&topic[12..32]);
    out
}

fn u256_low_u128(slice32: &[u8]) -> u128 {
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&slice32[16..32]);
    u128::from_be_bytes(buf)
}

// --- PolygonEvent + GapRecoveryRpc scaffold ---

use std::sync::Arc;

/// Decoded adapter event surfaced to consumers. Idempotency is the
/// consumer's responsibility (cf. simplification S-E plan findings
/// line 86); this subscriber does NOT deduplicate.
#[derive(Clone, Debug, PartialEq)]
pub enum PolygonEvent {
    Merged(MergedEvent),
    Redeemed(RedeemedEvent),
}

/// Polygon HTTP RPC operations used by gap recovery. The async WS
/// subscriber depends on this for the eth_blockNumber + eth_getLogs path
/// during reconnect-and-fill. Lifted out as a trait so the scaffold is
/// testable without a live RPC.
#[async_trait::async_trait]
pub trait GapRecoveryRpc: Send + Sync + 'static {
    async fn eth_block_number(&self) -> anyhow::Result<u64>;
    async fn eth_get_logs(
        &self,
        from_block: u64,
        to_block: u64,
        address: [u8; 20],
        topics: Vec<[u8; 32]>,
    ) -> anyhow::Result<Vec<RawLog>>;
}

/// Polygon `eth_subscribe(logs)` WS subscriber for the CTF adapter's
/// Merged + Redeemed events. The full WS `run` loop is deferred to
/// Task 1.4a's `preflight_subscribe` (per S-D simplification). This
/// scaffold owns the gap-recovery state machine + the per-tx
/// last_seen_block bookkeeping.
pub struct PolygonEventsSubscriber<R: GapRecoveryRpc> {
    rpc: Arc<R>,
    last_seen_block: u64,
}

impl<R: GapRecoveryRpc> PolygonEventsSubscriber<R> {
    pub fn new(rpc: Arc<R>) -> Self {
        Self {
            rpc,
            last_seen_block: 0,
        }
    }

    pub fn last_seen_block(&self) -> u64 {
        self.last_seen_block
    }

    pub fn set_last_seen_block(&mut self, b: u64) {
        self.last_seen_block = b;
    }

    /// Drain logs from `last_seen_block + 1` up to the current chain
    /// head via `eth_getLogs`. Advances `last_seen_block` to the head
    /// after a successful fetch. No-op when head ≤ last_seen_block.
    pub async fn recover_gap(&mut self) -> anyhow::Result<Vec<RawLog>> {
        let head = self.rpc.eth_block_number().await?;
        if head <= self.last_seen_block {
            return Ok(vec![]);
        }
        let logs = self
            .rpc
            .eth_get_logs(
                self.last_seen_block + 1,
                head,
                crate::polymarket::onchain::constants::CTF_COLLATERAL_ADAPTER_BYTES,
                vec![MERGED_TOPIC0, REDEEMED_TOPIC0],
            )
            .await?;
        self.last_seen_block = head;
        Ok(logs)
    }
}

/// One-shot `eth_subscribe(logs)` + immediate unsubscribe against the
/// supplied WS URL. Returns `Ok(())` on round-trip success within `timeout`,
/// `Err` on connection failure, subscription rejection, or timeout. Used
/// by `--preflight` to give the operator a green/red signal before live
/// launch. NOT used in the hot path.
pub async fn preflight_subscribe(ws_url: &str, timeout: std::time::Duration) -> anyhow::Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let fut = async {
        let (mut ws, _resp) = tokio_tungstenite::connect_async(ws_url)
            .await
            .map_err(|e| anyhow::anyhow!("WS connect failed: {e}"))?;

        // Send eth_subscribe(logs, {address: CTF_COLLATERAL_ADAPTER,
        // topics: [[MERGED_TOPIC0, REDEEMED_TOPIC0]]}).
        let adapter_hex = format!(
            "0x{}",
            hex::encode(crate::polymarket::onchain::constants::CTF_COLLATERAL_ADAPTER)
        );
        let merged_hex = format!("0x{}", hex::encode(MERGED_TOPIC0));
        let redeemed_hex = format!("0x{}", hex::encode(REDEEMED_TOPIC0));
        let subscribe_req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": [
                "logs",
                { "address": adapter_hex, "topics": [[merged_hex, redeemed_hex]] }
            ]
        });
        ws.send(Message::Text(subscribe_req.to_string().into()))
            .await
            .map_err(|e| anyhow::anyhow!("WS send subscribe failed: {e}"))?;

        // Wait for the subscription confirmation (response with id=1,
        // has a "result" field that is the subscription id as a hex string).
        let subscription_id = loop {
            let msg = ws
                .next()
                .await
                .ok_or_else(|| anyhow::anyhow!("WS stream ended before subscription confirm"))?
                .map_err(|e| anyhow::anyhow!("WS recv failed: {e}"))?;
            if let Message::Text(text) = msg {
                let v: serde_json::Value = serde_json::from_str(text.as_str())
                    .map_err(|e| anyhow::anyhow!("WS subscribe response not JSON: {e}"))?;
                if v.get("id").and_then(|i| i.as_u64()) == Some(1) {
                    if let Some(err) = v.get("error") {
                        return Err(anyhow::anyhow!("eth_subscribe rejected: {err}"));
                    }
                    let id = v.get("result").and_then(|r| r.as_str()).ok_or_else(|| {
                        anyhow::anyhow!("eth_subscribe response missing 'result'")
                    })?;
                    break id.to_string();
                }
                // If the server pushes a log before the subscription confirm
                // (shouldn't happen but defensive), ignore and keep waiting.
            }
        };

        // Send eth_unsubscribe(subscription_id).
        let unsub_req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "eth_unsubscribe",
            "params": [subscription_id]
        });
        ws.send(Message::Text(unsub_req.to_string().into()))
            .await
            .ok();
        let _ = ws.close(None).await;
        anyhow::Ok(())
    };

    tokio::time::timeout(timeout, fut)
        .await
        .map_err(|_| anyhow::anyhow!("preflight eth_subscribe timed out after {:?}", timeout))?
}
