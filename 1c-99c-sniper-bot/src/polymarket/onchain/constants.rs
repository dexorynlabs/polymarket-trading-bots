//! Polygon mainnet (chain 137) Polymarket contract addresses.
//!
//! These are protocol constants, not operator config. They are
//! cross-confirmed against:
//! - `py-builder-relayer-client` (`config.py`, `CONFIG[137]`)
//! - `magic-proxy-builder-example` (`constants/proxyWallet.ts`)
//! - On-chain `eth_getCode` probes (PROXY_FACTORY and RELAY_HUB are
//!   deployed contracts; CTF and PUSD are well-known recon constants)

// TODO(consolidation): the same four addresses also exist as hex strings
// in src/config.rs (Default::default()), src/preflight.rs (PUSD_POLYGON),
// src/main.rs, and src/live_config.rs (example TOML strings). Those forms
// serve TOML parsing and config-validation surfaces and are kept separately
// for now. If any address rotates, all of those sites must be updated
// alongside this file. A future refactor could derive the strings from
// these constants at boot to prevent silent drift.

/// Polymarket Magic.link proxy factory. Used as the `to:`
/// field in PROXY-type relayer submissions and as one of the static
/// 20-byte fields in the `"rlx:"` proxy struct hash.
pub const PROXY_FACTORY: [u8; 20] = [
    0xab, 0x45, 0xc5, 0xa4, 0xb0, 0xc9, 0x41, 0xa2, 0xf2, 0x31, 0xc0, 0x4c, 0x3f, 0x49, 0x18, 0x2e,
    0x1a, 0x25, 0x40, 0x52,
];

/// Polymarket proxy CREATE2 init code hash. Used by
/// `crate::polymarket::onchain::derive_proxy::derive_proxy_wallet` to compute an
/// EOA's proxy address deterministically.
///
/// Cross-confirmed against:
/// - `py-builder-relayer-client::constants/constants.py::PROXY_INIT_CODE_HASH`
/// - `magic-proxy-builder-example::constants/proxyWallet.ts::PROXY_INIT_CODE_HASH`
pub const PROXY_INIT_CODE_HASH: [u8; 32] = [
    0xd2, 0x1d, 0xf8, 0xdc, 0x65, 0x88, 0x0a, 0x86, 0x06, 0xf0, 0x9f, 0xe0, 0xce, 0x3d, 0xf9, 0xb8,
    0x86, 0x92, 0x87, 0xab, 0x0b, 0x05, 0x8b, 0xe0, 0x5a, 0xa9, 0xe8, 0xaf, 0x63, 0x30, 0xa0, 0x0b,
];

/// Polymarket relay hub. Static contract address embedded in the
/// proxy struct hash AND surfaced in `signatureParams.relayHub`. The
/// hub itself never appears in the `to:` field — that's PROXY_FACTORY.
pub const RELAY_HUB: [u8; 20] = [
    0xd2, 0x16, 0x15, 0x3c, 0x06, 0xe8, 0x57, 0xcd, 0x7f, 0x72, 0x66, 0x5e, 0x0a, 0xf1, 0xd7, 0xd8,
    0x21, 0x72, 0xf4, 0x94,
];

/// Conditional Tokens Framework (CTF) contract. The inner `data` of a
/// MERGE submission is a `mergePositions(...)` call to this address.
pub const CTF: [u8; 20] = [
    0x4d, 0x97, 0xdc, 0xd9, 0x7e, 0xc9, 0x45, 0xf4, 0x0c, 0xf6, 0x5f, 0x87, 0x09, 0x7a, 0xce, 0x5e,
    0xa0, 0x47, 0x60, 0x45,
];

/// Polymarket USD (pUSD) — the V2 collateral token. Passed as the
/// first arg to `mergePositions(collateralToken, ...)`.
pub const PUSD: [u8; 20] = [
    0xc0, 0x11, 0xa7, 0xe1, 0x2a, 0x19, 0xf7, 0xb1, 0xf6, 0x70, 0xd4, 0x6f, 0x03, 0xb0, 0x3f, 0x33,
    0x42, 0xe8, 0x2d, 0xfb,
];

/// CTF Collateral Adapter — the merge / redeem dispatcher contract on
/// Polygon mainnet.
/// Alias for any callers expecting the verbose CTF_COLLATERAL_ADAPTER
/// name; the byte-array form CTF_COLLATERAL_ADAPTER_BYTES is identical
/// (kept for code paths that accept `&[u8; 20]` explicitly).
///
/// Call chain: relayer EOA → Safe → CtfCollateralAdapter (this address)
/// → CTF.mergePositions / CTF.redeemPositions (the CTF main contract at
/// `CTF`). These are distinct contracts; do NOT confuse with `CTF`.
pub const CTF_COLLATERAL_ADAPTER: [u8; 20] = [
    0xad, 0xa1, 0x00, 0xdb, 0x00, 0xca, 0x00, 0x07, 0x38, 0x11, 0x82, 0x06, 0x92, 0x00, 0x54, 0x00,
    0x21, 0x8f, 0xce, 0x1f,
];

/// Byte-array alias of `CTF_COLLATERAL_ADAPTER`. Kept for code paths
/// that accept `&[u8; 20]` explicitly.
pub const CTF_COLLATERAL_ADAPTER_BYTES: [u8; 20] = CTF_COLLATERAL_ADAPTER;

/// CTF collateral token. The CTF Collateral Adapter wraps USDC.e ↔ pUSD
/// internally; `redeemPositions`/`mergePositions` calls use pUSD as the
/// collateral arg.
pub const CTF_COLLATERAL_TOKEN: [u8; 20] = PUSD;

/// CTF Collateral Adapter `PositionsMerged` event topic0. Verified
/// against captured Polygon logs.
/// Signature: keccak256("PositionsMerged(address,bytes32,uint256)") per
/// the adapter at `CTF_COLLATERAL_ADAPTER`.
pub const MERGED_TOPIC0: [u8; 32] = [
    0x08, 0xc1, 0x5f, 0x31, 0xe4, 0x13, 0xdb, 0xd9, 0xbb, 0xd3, 0xb4, 0xc0, 0x1b, 0x19, 0x1d, 0xb8,
    0x40, 0xe1, 0x65, 0x7a, 0xb4, 0xf8, 0xd1, 0xd7, 0xd4, 0x48, 0xa9, 0xa5, 0x08, 0x3b, 0xa2, 0x55,
];

/// CTF Collateral Adapter `Redeemed` event topic0. Verified against
/// captured Polygon logs; all 32 bytes observed directly from captured
/// topic0.
pub const REDEEMED_TOPIC0: [u8; 32] = [
    0x1d, 0xae, 0xc0, 0xb1, 0xf6, 0x29, 0xde, 0x29, 0xb7, 0x49, 0xa1, 0x89, 0x49, 0x9f, 0x43, 0x10,
    0x4a, 0x80, 0xdf, 0x46, 0xa9, 0xf3, 0x4d, 0x9c, 0xcf, 0x73, 0xaa, 0x59, 0xb8, 0x5f, 0xc0, 0xc0,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ctf_collateral_adapter_alias_matches_canonical_adapter_address() {
        // The adapter is the CTF Collateral Adapter at
        // 0xada100db00ca00073811820692005400218fce1f. If the existing
        // CTF constant points elsewhere, this test catches the drift.
        assert_eq!(
            CTF_COLLATERAL_ADAPTER,
            hex_literal::hex!("ada100db00ca00073811820692005400218fce1f")
        );
        assert_eq!(CTF_COLLATERAL_ADAPTER_BYTES, CTF_COLLATERAL_ADAPTER);
    }

    #[test]
    fn ctf_collateral_token_alias_matches_pusd() {
        // pUSD is the collateral token for the CTF adapter.
        assert_eq!(CTF_COLLATERAL_TOKEN, PUSD);
    }

    #[test]
    fn proxy_factory_matches_polygon_mainnet_constant() {
        assert_eq!(
            hex::encode(PROXY_FACTORY),
            "ab45c5a4b0c941a2f231c04c3f49182e1a254052"
        );
    }

    #[test]
    fn relay_hub_matches_polygon_mainnet_constant() {
        assert_eq!(
            hex::encode(RELAY_HUB),
            "d216153c06e857cd7f72665e0af1d7d82172f494"
        );
    }

    #[test]
    fn ctf_matches_polygon_mainnet_constant() {
        assert_eq!(hex::encode(CTF), "4d97dcd97ec945f40cf65f87097ace5ea0476045");
    }

    #[test]
    fn pusd_matches_polygon_mainnet_constant() {
        assert_eq!(
            hex::encode(PUSD),
            "c011a7e12a19f7b1f670d46f03b03f3342e82dfb"
        );
    }

    #[test]
    fn proxy_init_code_hash_matches_py_client_constant() {
        assert_eq!(
            hex::encode(PROXY_INIT_CODE_HASH),
            "d21df8dc65880a8606f09fe0ce3df9b8869287ab0b058be05aa9e8af6330a00b"
        );
    }
}
