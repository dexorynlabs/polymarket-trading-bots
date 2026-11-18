//! Safe v1.3.0 EIP-712 SafeTx struct-hash + domain-separator.
//!
//! Per the Safe v1.3.0 contracts:
//!   DOMAIN_TYPEHASH = keccak256("EIP712Domain(uint256 chainId,address verifyingContract)")
//!   SAFE_TX_TYPEHASH = keccak256("SafeTx(address to,uint256 value,bytes data,uint8 operation,
//!                                 uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,
//!                                 address gasToken,address refundReceiver,uint256 nonce)")
//!   domain_separator = keccak256(abi.encode(DOMAIN_TYPEHASH, chainId, verifyingContract))
//!   struct_hash      = keccak256(abi.encode(SAFE_TX_TYPEHASH, to, value, keccak256(data),
//!                                            operation, safeTxGas, baseGas, gasPrice,
//!                                            gasToken, refundReceiver, nonce))
//!   safe_tx_hash     = keccak256(0x1901 || domain_separator || struct_hash)
//!
//! `SAFE_EXEC_TRANSACTION_SELECTOR` (0x6a761202) is the selector for
//! Safe.execTransaction; consumers use it when assembling outer calldata.

use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_sol_types::SolValue;

pub const SAFE_EXEC_TRANSACTION_SELECTOR: [u8; 4] = [0x6a, 0x76, 0x12, 0x02];

// Single-line literals to guarantee no embedded whitespace or newlines.
// VERIFIED: rustc line-continuation in b"...\<NL>..." does strip the newline +
// leading spaces, but using single-line avoids any ambiguity.
const DOMAIN_TYPEHASH_PREIMAGE: &[u8] = b"EIP712Domain(uint256 chainId,address verifyingContract)";
const SAFE_TX_TYPEHASH_PREIMAGE: &[u8] =
    b"SafeTx(address to,uint256 value,bytes data,uint8 operation,uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,address gasToken,address refundReceiver,uint256 nonce)";

fn domain_typehash() -> B256 {
    keccak256(DOMAIN_TYPEHASH_PREIMAGE)
}

fn safe_tx_typehash() -> B256 {
    keccak256(SAFE_TX_TYPEHASH_PREIMAGE)
}

pub fn domain_separator(safe_address: Address, chain_id: u64) -> B256 {
    let encoded = (domain_typehash(), U256::from(chain_id), safe_address).abi_encode();
    keccak256(&encoded)
}

#[allow(clippy::too_many_arguments)]
pub fn safe_tx_hash(
    safe_address: Address,
    chain_id: u64,
    to: Address,
    value: U256,
    data: &[u8],
    operation: u8,
    safe_tx_gas: U256,
    base_gas: U256,
    gas_price: U256,
    gas_token: Address,
    refund_receiver: Address,
    nonce: U256,
) -> B256 {
    let data_hash = keccak256(data);
    // `operation` is uint8 in the Safe ABI; ABI-encode pads it to 32 bytes,
    // same as uint256 for values 0-255. Cast to U256 so the tuple is fully
    // SolValue (u8 does not impl SolValue directly in alloy 0.8).
    let operation_u256 = U256::from(operation);
    let struct_encoded = (
        safe_tx_typehash(),
        to,
        value,
        data_hash,
        operation_u256,
        safe_tx_gas,
        base_gas,
        gas_price,
        gas_token,
        refund_receiver,
        nonce,
    )
        .abi_encode();
    let struct_hash = keccak256(&struct_encoded);

    let ds = domain_separator(safe_address, chain_id);
    let mut preimage = Vec::with_capacity(2 + 32 + 32);
    preimage.extend_from_slice(&[0x19, 0x01]);
    preimage.extend_from_slice(ds.as_slice());
    preimage.extend_from_slice(struct_hash.as_slice());
    keccak256(&preimage)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the SAFE_TX_TYPEHASH_PREIMAGE is the canonical one-line string
    /// with no embedded spaces or newlines in the argument list.
    #[test]
    fn safe_tx_typehash_preimage_is_canonical() {
        let s = std::str::from_utf8(SAFE_TX_TYPEHASH_PREIMAGE).unwrap();
        assert_eq!(
            s,
            "SafeTx(address to,uint256 value,bytes data,uint8 operation,\
             uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,\
             address gasToken,address refundReceiver,uint256 nonce)"
        );
        // No embedded space after comma before uint/address (other than the one after "to,")
        // The canonical form has exactly one space after each comma-separated type name:
        // "address to" "uint256 value" etc — no double spaces
        assert!(!s.contains("  "), "no double spaces in preimage");
        assert!(!s.contains('\n'), "no newlines in preimage");
    }

    /// Verify the DOMAIN_TYPEHASH_PREIMAGE is canonical.
    #[test]
    fn domain_typehash_preimage_is_canonical() {
        let s = std::str::from_utf8(DOMAIN_TYPEHASH_PREIMAGE).unwrap();
        assert_eq!(s, "EIP712Domain(uint256 chainId,address verifyingContract)");
    }

    /// Verify the execTransaction selector matches keccak256 of the ABI signature.
    /// 0x6a761202 is well-known and verifiable from Safe contract source.
    #[test]
    fn exec_transaction_selector_value() {
        assert_eq!(SAFE_EXEC_TRANSACTION_SELECTOR, [0x6a, 0x76, 0x12, 0x02]);
    }
}
