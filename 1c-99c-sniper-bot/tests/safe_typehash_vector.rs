//! Sanity-check that the Safe v1.3.0 SafeTx hash includes chain_id in the
//! domain separator (so the same SafeTx params on chain 137 vs chain 1
//! produce DIFFERENT hashes). Load-bearing correctness is verified by
//! the byte-equality replay against 27 captured on-chain merge
//! transactions in `safe_encoding_replay.rs` (Task 0a.4).
//!
//! Per simplification S-C (plan findings table line 84): the
//! "find a published reference vector" test was dropped — its purpose
//! is redundantly covered by the replay test, which is the authoritative
//! gate.

use alloy_primitives::{Address, U256};
use sniperbot::polymarket::onchain::safe;

#[test]
fn safe_tx_hash_chain_id_137_pinned() {
    // Same Safe address + same SafeTx params, only chain_id differs.
    // Hash MUST differ — chain_id is in the EIP712Domain typehash.
    let to: Address = "0x0000000000000000000000000000000000000001"
        .parse()
        .unwrap();
    let safe_address: Address = "0x0000000000000000000000000000000000000002"
        .parse()
        .unwrap();

    let h137 = safe::safe_tx_hash(
        safe_address,
        137,
        to,
        U256::ZERO,
        &[0xDE, 0xAD],
        /* operation = */ 0,
        U256::ZERO,
        U256::ZERO,
        U256::ZERO,
        Address::ZERO,
        Address::ZERO,
        U256::ZERO,
    );
    let h1 = safe::safe_tx_hash(
        safe_address,
        1,
        to,
        U256::ZERO,
        &[0xDE, 0xAD],
        0,
        U256::ZERO,
        U256::ZERO,
        U256::ZERO,
        Address::ZERO,
        Address::ZERO,
        U256::ZERO,
    );
    assert_ne!(h137, h1, "domain separator must depend on chain_id");
}
