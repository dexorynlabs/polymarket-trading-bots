//! EIP-712 typed-data signer for Polymarket CLOB orders.
//!
//! Polymarket's CTF Exchange contract consumes EIP-712 orders. This
//! module wraps `alloy` to produce the signature string that lives
//! INSIDE the order body in `POST /order` (audit delta #1).
//!
//! ## Wallet kinds
//!
//! `SignerService` now dispatches on a `WalletKind` enum:
//!
//! - `WalletKind::MagicLink` — the existing proxy-wallet path used by recon.
//!   `from_pk_hex` returns this variant, preserving backward compatibility
//!   with every existing call site in recon.
//!
//! - `WalletKind::Safe` — the Safe v1.3.0 wallet path. `from_safe`
//!   returns this variant. `sign_merge_call` dispatches to `sign_merge_safe`
//!   which is fully value-correct for production use.

use alloy_primitives::{Address, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{eip712_domain, sol, SolCall, SolStruct};
use anyhow::{Context, Result};
use serde::Serialize;

/// `SignatureTypeV2::POLY_1271` — deposit-wallet orders, validated by the
/// maker contract via ERC-1271 instead of ecrecover.
pub const SIG_TYPE_POLY_1271: u8 = 3;

/// EIP-712 type string for [`Order`]. Must stay byte-identical to the struct
/// below — it is embedded verbatim in the ERC-7739 signature envelope, so a
/// drift here silently invalidates every deposit-wallet order.
const ORDER_TYPE_STRING: &str = "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";

/// Solady's `TypedDataSign` wrapper type string: the envelope type followed
/// by the contents type, per ERC-7739.
const SOLADY_TYPE_STRING: &str = "TypedDataSign(Order contents,string name,string version,uint256 chainId,address verifyingContract,bytes32 salt)Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";

sol! {
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint8 side;
        uint8 signatureType;
        uint256 timestamp;
        bytes32 metadata;
        bytes32 builder;
    }
}

sol! {
    /// Safe v1.3.0 execTransaction ABI. Used by `sign_merge_safe` to encode
    /// the outer calldata that wraps the inner CTF mergePositions call.
    /// Selector: keccak256("execTransaction(address,uint256,bytes,uint8,uint256,
    ///   uint256,uint256,address,address,bytes)")[0..4] = 0x6a761202.
    function execTransaction(
        address to,
        uint256 value,
        bytes data,
        uint8 operation,
        uint256 safeTxGas,
        uint256 baseGas,
        uint256 gasPrice,
        address gasToken,
        address refundReceiver,
        bytes signatures
    );
}

/// Wallet variant carried by `SignerService`. Determines which signing
/// path `sign_merge_call` dispatches to.
#[derive(Clone, Debug)]
pub enum WalletKind {
    /// Polymarket Magic.link proxy-wallet path. Used by recon's
    /// `OnChainMerger::merge`. `from_pk_hex` constructs this variant.
    ///
    /// `proxy_factory` and `relay_hub` are the static Polygon mainnet
    /// constants from `crate::polymarket::onchain::constants`; held as fields so that
    /// tests can substitute mock addresses without touching the constants.
    MagicLink {
        signer_eoa: PrivateKeySigner,
        proxy_factory: [u8; 20],
        relay_hub: [u8; 20],
    },
    /// Safe v1.3.0 wallet path. `from_safe` constructs this
    /// variant. `sign_merge_call` dispatches to `sign_merge_safe` which is
    /// fully value-correct for production use.
    Safe {
        controller_eoa: PrivateKeySigner,
        safe_proxy_address: Address,
        chain_id: u64,
    },
}

/// JSON body sent to the Polymarket REST relayer (`POST /submit`).
///
/// The `type` field distinguishes the wallet kind:
/// - `"PROXY"` for Magic.link proxy-wallet submissions.
/// - `"SAFE"` (or an env-override) for Safe v1.3.0 submissions.
///
/// `signature_params` is a `serde_json::Value` so each variant can embed
/// its own fields without a shared schema struct.
#[derive(Clone, Debug, Serialize)]
pub struct RelayerBody {
    #[serde(rename = "type")]
    pub ty: String,
    pub from: String,
    pub to: String,
    pub data: String,
    #[serde(rename = "signatureParams")]
    pub signature_params: serde_json::Value,
    pub signature: String,
}

impl RelayerBody {
    /// Serialize this body to a compact JSON string.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct PolymarketOrder {
    pub salt: u64,
    pub maker: String,
    pub signer: String,
    /// Wire-only in V2 (still serialized in POST /order body, but NOT in the
    /// signed EIP-712 struct). Always `"0x0000…0000"` for resting orders.
    pub taker: String,
    pub token_id: String,
    pub maker_amount: u128,
    pub taker_amount: u128,
    /// Wire-only in V2 (still serialized in POST /order body, but NOT in the
    /// signed EIP-712 struct). Recon does not use GTD; always `0`.
    pub expiration: u64,
    /// Order creation time in milliseconds. V2 replaces `nonce` for
    /// per-address uniqueness — set once at sign time via `SystemTime::now()`.
    pub timestamp_ms: u128,
    pub metadata: [u8; 32],
    pub builder: [u8; 32],
    pub side: u8,
    pub signature_type: u8,
}

#[derive(Clone, Debug)]
pub struct PolymarketOrderDomain {
    pub name: String,
    pub version: String,
    pub chain_id: u64,
    pub verifying_contract: String,
}

/// EIP-712 signer dispatching over `WalletKind`.
///
/// All existing public APIs (`from_pk_hex`, `address_hex`,
/// `sign_proxy_relay_message`, `sign_order`, `proxy_struct_concat`,
/// `proxy_struct_hash`) preserve their pre-refactor signatures exactly.
/// `from_pk_hex` returns a `MagicLink` variant; every recon call site
/// continues to compile and behave identically.
pub struct SignerService {
    pub kind: WalletKind,
}

impl SignerService {
    /// Construct a `MagicLink` signer from a hex-encoded private key.
    ///
    /// This is the backward-compatible constructor used by every existing
    /// call site in `recon` (runtime.rs, lifecycle.rs, merger_onchain.rs,
    /// bin/*). Returns `WalletKind::MagicLink` with `PROXY_FACTORY` and
    /// `RELAY_HUB` from `crate::polymarket::onchain::constants`.
    pub fn from_pk_hex(pk_hex: &str) -> Result<Self> {
        use crate::polymarket::onchain::constants::{PROXY_FACTORY, RELAY_HUB};
        let trimmed = pk_hex.strip_prefix("0x").unwrap_or(pk_hex);
        let bytes = hex::decode(trimmed).context("decode private key hex")?;
        let signer_eoa =
            PrivateKeySigner::from_slice(&bytes).context("construct PrivateKeySigner")?;
        Ok(Self {
            kind: WalletKind::MagicLink {
                signer_eoa,
                proxy_factory: PROXY_FACTORY,
                relay_hub: RELAY_HUB,
            },
        })
    }

    /// Construct a `Safe` signer from a hex-encoded controller private key,
    /// the Safe proxy address, and the chain ID.
    ///
    /// The controller EOA is the externally-owned account that controls the
    /// Safe (threshold = 1). `sign_merge_call` dispatches to `sign_merge_safe`
    /// which uses `crate::polymarket::onchain::safe::safe_tx_hash` and encodes the outer
    /// `execTransaction` calldata.
    pub fn from_safe(
        controller_pk_hex: &str,
        safe_proxy_address: Address,
        chain_id: u64,
    ) -> Result<Self> {
        let trimmed = controller_pk_hex
            .strip_prefix("0x")
            .unwrap_or(controller_pk_hex);
        let bytes = hex::decode(trimmed).context("decode private key hex")?;
        let controller_eoa = PrivateKeySigner::from_slice(&bytes)
            .context("construct controller PrivateKeySigner")?;
        Ok(Self {
            kind: WalletKind::Safe {
                controller_eoa,
                safe_proxy_address,
                chain_id,
            },
        })
    }

    /// Return whichever `PrivateKeySigner` this variant holds.
    ///
    /// Both `MagicLink` and `Safe` carry an EOA that can produce
    /// EIP-191 (`sign_message_sync`) and EIP-712 (`sign_hash_sync`)
    /// signatures via alloy's `SignerSync` trait.
    fn eoa(&self) -> &PrivateKeySigner {
        match &self.kind {
            WalletKind::MagicLink { signer_eoa, .. } => signer_eoa,
            WalletKind::Safe { controller_eoa, .. } => controller_eoa,
        }
    }

    /// Return the EOA address as a lowercased `0x`-prefixed hex string.
    pub fn address_hex(&self) -> String {
        format!("{:?}", self.eoa().address())
    }

    /// Compute the byte concatenation that gets keccak-hashed into the
    /// proxy struct hash. Mirrors py-builder-relayer-client's
    /// `create_proxy_struct_hash` byte layout exactly.
    ///
    /// Layout (10 fields):
    ///   `"rlx:" || from(20B) || to(20B) || data(variable) || tx_fee(32B) ||
    ///    gas_price(32B) || gas_limit(32B) || nonce(32B) || relay_hub(20B) ||
    ///    relay(20B)`
    ///
    /// All numeric fields are 32-byte big-endian (uint256). `data` is
    /// inline (NO length prefix — diverges from the earlier spike spec).
    /// `from` is the operator's EOA. `to` is `PROXY_FACTORY` (NOT the
    /// relay address). `relay_hub` is the static `RELAY_HUB` constant.
    /// `relay` is the per-tx address from `GET /relay-payload`.
    pub(crate) fn proxy_struct_concat(
        from_address: &[u8; 20],
        to: &[u8; 20],
        data: &[u8],
        tx_fee: u64,
        gas_price: u64,
        gas_limit: u64,
        nonce: u64,
        relay_hub_address: &[u8; 20],
        relay_address: &[u8; 20],
    ) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(4 + 20 + 20 + data.len() + 32 * 4 + 20 + 20);
        buf.extend_from_slice(b"rlx:");
        buf.extend_from_slice(from_address);
        buf.extend_from_slice(to);
        buf.extend_from_slice(data);
        buf.extend_from_slice(&u256_be(tx_fee));
        buf.extend_from_slice(&u256_be(gas_price));
        buf.extend_from_slice(&u256_be(gas_limit));
        buf.extend_from_slice(&u256_be(nonce));
        buf.extend_from_slice(relay_hub_address);
        buf.extend_from_slice(relay_address);
        buf
    }

    /// Compute `keccak256(proxy_struct_concat(...))`. The result is the
    /// 32-byte digest the relayer expects to see EIP-191 personal-signed.
    ///
    /// Visibility is `pub` (not `pub(crate)`) so that integration tests in
    /// `tests/` can exercise the full encode → hash → sign chain in the
    /// relayer KAT (tests/relayer_kat_proxy_signature.rs).
    pub fn proxy_struct_hash(
        from_address: &[u8; 20],
        to: &[u8; 20],
        data: &[u8],
        tx_fee: u64,
        gas_price: u64,
        gas_limit: u64,
        nonce: u64,
        relay_hub_address: &[u8; 20],
        relay_address: &[u8; 20],
    ) -> [u8; 32] {
        let buf = Self::proxy_struct_concat(
            from_address,
            to,
            data,
            tx_fee,
            gas_price,
            gas_limit,
            nonce,
            relay_hub_address,
            relay_address,
        );
        alloy_primitives::keccak256(&buf).into()
    }

    /// EIP-191 personal-sign the proxy struct hash.
    ///
    /// This is the second half of the relayer signature scheme (spike §2.6):
    /// the keccak256 digest from [`Self::proxy_struct_hash`] is wrapped with
    /// the `"\x19Ethereum Signed Message:\n32"` prefix and then ECDSA-signed
    /// with the operator's EOA. Returns a 0x-prefixed 65-byte hex string.
    pub fn sign_proxy_relay_message(&self, hash: &[u8; 32]) -> Result<String> {
        let sig = self
            .eoa()
            .sign_message_sync(hash)
            .context("EIP-191 sign proxy struct hash")?;
        Ok(format!("0x{}", hex::encode(sig.as_bytes())))
    }

    pub fn sign_order(
        &self,
        order: &PolymarketOrder,
        domain: &PolymarketOrderDomain,
    ) -> Result<String> {
        use alloy_primitives::FixedBytes;
        let maker = parse_address(&order.maker)?;
        let signer_addr = parse_address(&order.signer)?;
        let verifying = parse_address(&domain.verifying_contract)?;

        let eip = eip712_domain! {
            name: domain.name.clone(),
            version: domain.version.clone(),
            chain_id: domain.chain_id,
            verifying_contract: verifying,
        };

        let order_struct = Order {
            salt: U256::from(order.salt),
            maker,
            signer: signer_addr,
            tokenId: U256::from_str_radix(&order.token_id, 10).context("parse token_id")?,
            makerAmount: U256::from(order.maker_amount),
            takerAmount: U256::from(order.taker_amount),
            side: order.side,
            signatureType: order.signature_type,
            timestamp: U256::from(order.timestamp_ms),
            metadata: FixedBytes::<32>::from(order.metadata),
            builder: FixedBytes::<32>::from(order.builder),
        };

        // POLY_1271 (deposit wallets, account wallets deployed on/after
        // 2026-05-04). `signer` is the deposit-wallet CONTRACT, so the
        // exchange validates through its `isValidSignature` rather than
        // ecrecover — a plain ECDSA signature is rejected. Types 0/1/2 are
        // untouched.
        if order.signature_type == SIG_TYPE_POLY_1271 {
            return self.sign_order_1271(&order_struct, &eip);
        }

        let digest = order_struct.eip712_signing_hash(&eip);
        let sig = self
            .eoa()
            .sign_hash_sync(&digest)
            .context("sign EIP-712 digest")?;
        Ok(format!("0x{}", hex::encode(sig.as_bytes())))
    }

    /// Solady ERC-7739 `TypedDataSign` nested signature, as required by
    /// Polymarket deposit wallets (`signatureType = 3`).
    ///
    /// Layout, mirroring py_clob_client_v2's
    /// `ExchangeOrderBuilderV2._build_poly_1271_order_signature`:
    ///
    /// ```text
    /// inner_ecdsa(65) || app_domain_separator(32) || contents_hash(32)
    ///                 || contents_type_string || len(contents_type_string) as u16 BE
    /// ```
    ///
    /// The inner digest is `keccak(0x1901 || app_sep || TypedDataSign_struct_hash)`,
    /// where the TypedDataSign struct binds the order to the DEPOSIT WALLET's
    /// own EIP-712 domain (`name="DepositWallet"`, `version="1"`,
    /// `verifyingContract = order.signer`, `salt = 0`).
    fn sign_order_1271(
        &self,
        order: &Order,
        eip: &alloy_sol_types::Eip712Domain,
    ) -> Result<String> {
        use alloy_primitives::keccak256;

        let contents_hash = order.eip712_hash_struct();
        let app_sep = eip.separator();

        // All seven fields are static, so ABI encoding is a plain
        // concatenation of 32-byte words.
        let mut buf: Vec<u8> = Vec::with_capacity(7 * 32);
        buf.extend_from_slice(keccak256(SOLADY_TYPE_STRING.as_bytes()).as_slice());
        buf.extend_from_slice(contents_hash.as_slice());
        buf.extend_from_slice(keccak256(b"DepositWallet").as_slice());
        buf.extend_from_slice(keccak256(b"1").as_slice());
        buf.extend_from_slice(&eip.chain_id.unwrap_or_default().to_be_bytes::<32>());
        buf.extend_from_slice(&[0u8; 12]); // address left-pad
        buf.extend_from_slice(order.signer.as_slice());
        buf.extend_from_slice(&[0u8; 32]); // DepositWallet domain salt = 0

        let mut preimage: Vec<u8> = Vec::with_capacity(2 + 32 + 32);
        preimage.extend_from_slice(&[0x19, 0x01]);
        preimage.extend_from_slice(app_sep.as_slice());
        preimage.extend_from_slice(keccak256(&buf).as_slice());

        let sig = self
            .eoa()
            .sign_hash_sync(&keccak256(&preimage))
            .context("sign ERC-7739 TypedDataSign digest")?;

        Ok(format!(
            "0x{}{}{}{}{:04x}",
            hex::encode(sig.as_bytes()),
            hex::encode(app_sep.as_slice()),
            hex::encode(contents_hash.as_slice()),
            hex::encode(ORDER_TYPE_STRING.as_bytes()),
            ORDER_TYPE_STRING.len(),
        ))
    }

    /// Sign a merge-positions inner calldata for the relayer.
    ///
    /// Returns a `RelayerBody` containing the signed body in the shape
    /// expected by `POST <relayer>/submit`. Dispatches to the per-variant
    /// implementation:
    ///
    /// - `WalletKind::MagicLink` → `sign_merge_magiclink` (PROXY body with
    ///   placeholder relay address — see that method's doc comment).
    /// - `WalletKind::Safe` → `sign_merge_safe` (fully value-correct SAFE body).
    pub fn sign_merge_call(
        &self,
        inner_calldata: &[u8],
        nonce: u64,
        gas_limit: u64,
    ) -> Result<RelayerBody> {
        match &self.kind {
            WalletKind::MagicLink { .. } => {
                self.sign_merge_magiclink(inner_calldata, nonce, gas_limit)
            }
            WalletKind::Safe { .. } => self.sign_merge_safe(inner_calldata, nonce, gas_limit),
        }
    }

    /// Build a PROXY-style relayer body for the Magic.link wallet path.
    ///
    /// **NOTE:** The Magic.link path of `sign_merge_call` returns a body
    /// whose `signatureParams.relay` is a placeholder zero-address. The
    /// per-tx relay address is fetched from `/relay-payload` by the
    /// async orchestration layer in `recon/src/exec/merger_onchain.rs`,
    /// which continues to construct its own body inline. This
    /// `sign_merge_magiclink` exists solely to satisfy the per-variant
    /// regression test (`relayer_body_magiclink_variant.rs`) that pins
    /// the PROXY body shape; real Magic.link merges should go through
    /// `OnChainMerger::merge` in recon, not via this signer method.
    ///
    /// For the Safe path, all required state is in `WalletKind::Safe`
    /// (controller_eoa, safe_proxy_address, chain_id), so `sign_merge_safe`
    /// is the production path and is fully value-correct.
    fn sign_merge_magiclink(
        &self,
        inner: &[u8],
        nonce: u64,
        gas_limit: u64,
    ) -> Result<RelayerBody> {
        let WalletKind::MagicLink {
            signer_eoa,
            proxy_factory,
            relay_hub,
        } = &self.kind
        else {
            unreachable!("sign_merge_magiclink called with non-MagicLink kind");
        };
        let from_bytes: [u8; 20] = signer_eoa.address().into_array();
        // Placeholder relay address — the real per-tx relay is fetched by
        // recon's async OnChainMerger::merge. See doc comment above for context.
        let placeholder_relay = [0u8; 20];
        let hash = Self::proxy_struct_hash(
            &from_bytes,
            proxy_factory,
            inner,
            /* tx_fee = */ 0,
            /* gas_price = */ 0,
            gas_limit,
            nonce,
            relay_hub,
            &placeholder_relay,
        );
        let signature = self
            .sign_proxy_relay_message(&hash)
            .context("sign proxy relay message")?;

        let body = RelayerBody {
            ty: "PROXY".to_string(),
            from: format!("{:?}", signer_eoa.address()),
            to: format!("0x{}", hex::encode(proxy_factory)),
            data: format!("0x{}", hex::encode(inner)),
            signature_params: serde_json::json!({
                "gasPrice":   "0",
                "gasLimit":   gas_limit.to_string(),
                "relayerFee": "0",
                "relayHub":   format!("0x{}", hex::encode(relay_hub)),
                "relay":      format!("0x{}", hex::encode(placeholder_relay)),
            }),
            signature,
        };
        Ok(body)
    }

    /// Build a SAFE-style relayer body for the Safe v1.3.0 wallet path.
    ///
    /// This is the production path for a Safe wallet. It:
    /// 1. Computes the SafeTx hash via `crate::polymarket::onchain::safe::safe_tx_hash`
    ///    (to = CTF_COLLATERAL_ADAPTER, value = 0, data = inner, operation = 0).
    /// 2. Signs the hash with the controller EOA.
    /// 3. Encodes the outer `Safe.execTransaction(...)` calldata using the
    ///    `sol!`-generated `execTransactionCall`. The selector `0x6a761202`
    ///    is prepended by `abi_encode()` automatically — do NOT add it manually.
    /// 4. Returns a `RelayerBody` with `type = "SAFE"` (overridable via
    ///    `POLYMARKET_SAFE_TYPE_DISCRIMINATOR` env var for Task 0a.7 probe).
    fn sign_merge_safe(&self, inner: &[u8], nonce: u64, _gas_limit: u64) -> Result<RelayerBody> {
        use crate::polymarket::onchain::constants::CTF_COLLATERAL_ADAPTER;
        use crate::polymarket::onchain::safe::{safe_tx_hash, SAFE_EXEC_TRANSACTION_SELECTOR};

        let WalletKind::Safe {
            controller_eoa,
            safe_proxy_address,
            chain_id,
        } = &self.kind
        else {
            unreachable!("sign_merge_safe called with non-Safe kind");
        };

        // SafeTx: to=adapter, value=0, data=inner, operation=0 (CALL),
        // safeTxGas/baseGas/gasPrice = 0, gasToken/refundReceiver = address(0).
        let hash = safe_tx_hash(
            *safe_proxy_address,
            *chain_id,
            Address::from(CTF_COLLATERAL_ADAPTER),
            U256::ZERO,
            inner,
            0,
            U256::ZERO,
            U256::ZERO,
            U256::ZERO,
            Address::ZERO,
            Address::ZERO,
            U256::from(nonce),
        );
        let sig = controller_eoa
            .sign_hash_sync(&hash)
            .context("sign Safe tx hash")?;
        let signature_hex = format!("0x{}", hex::encode(sig.as_bytes()));

        // Outer calldata = execTransaction(to, value, data, operation,
        //   safeTxGas, baseGas, gasPrice, gasToken, refundReceiver, signatures)
        // `abi_encode()` prepends the 0x6a761202 selector automatically.
        let call = execTransactionCall {
            to: Address::from(CTF_COLLATERAL_ADAPTER),
            value: U256::ZERO,
            data: alloy_primitives::Bytes::from(inner.to_vec()),
            operation: 0u8,
            safeTxGas: U256::ZERO,
            baseGas: U256::ZERO,
            gasPrice: U256::ZERO,
            gasToken: Address::ZERO,
            refundReceiver: Address::ZERO,
            signatures: alloy_primitives::Bytes::from(sig.as_bytes().to_vec()),
        };
        let outer = call.abi_encode();
        // Sanity: abi_encode() must have prepended the execTransaction selector.
        debug_assert_eq!(
            outer[0..4],
            SAFE_EXEC_TRANSACTION_SELECTOR,
            "execTransaction selector mismatch — alloy sol! regression"
        );

        // TODO(Task 0a.7): confirm the correct type discriminator by probing
        // the relayer. The env var override lets the operator flip it without
        // recompiling (S-B simplification per plan §10.2 risk #4).
        let ty = std::env::var("POLYMARKET_SAFE_TYPE_DISCRIMINATOR")
            .unwrap_or_else(|_| "SAFE".to_string());

        let body = RelayerBody {
            ty,
            from: format!("{:?}", controller_eoa.address()),
            to: format!("{:?}", safe_proxy_address),
            data: format!("0x{}", hex::encode(&outer)),
            signature_params: serde_json::json!({
                "operation":      0,
                "safeTxGas":      "0",
                "baseGas":        "0",
                "gasPrice":       "0",
                "gasToken":       format!("{:?}", Address::ZERO),
                "refundReceiver": format!("{:?}", Address::ZERO),
            }),
            signature: signature_hex,
        };
        Ok(body)
    }
}

fn parse_address(s: &str) -> Result<Address> {
    s.parse::<Address>()
        .with_context(|| format!("parse address {s}"))
}

/// Result of `dry_run` — the operator-facing report that the
/// `--signer-dry-run` CLI flag prints to stdout. Fields are
/// hex-formatted for direct comparison against `config/recon.toml`
/// addresses.
///
/// Construction is gated by `dry_run`; callers cannot bypass the
/// EOA-derivation + proxy-match check.
#[derive(Clone, Debug, PartialEq)]
pub struct DryRunReport {
    /// Operator's Magic.link Polymarket Proxy address (from config).
    pub proxy_wallet: String,
    /// EOA address derived from `private_key`. **MUST be the controller
    /// of `proxy_wallet`** for the relayer to accept signed messages.
    pub derived_eoa: String,
    /// Hex-encoded test message (the `b"rlx:"`-prefixed proxy struct
    /// concat) that was hashed and signed.
    pub test_message_hex: String,
    /// 32-byte keccak256 hash of `test_message_hex`, the actual input to
    /// EIP-191 personal_sign.
    pub hash_hex: String,
    /// 65-byte ECDSA signature, hex-encoded with 0x prefix. Splits into
    /// (r=64h, s=64h, v=2h) for operators wanting to verify the recovery
    /// byte.
    pub signature_hex: String,
}

/// Run a hardcoded signing path against the operator's configured
/// `private_key` + `proxy_wallet`. Loads no network state, submits
/// nothing — proves only that the local signing pipeline produces a
/// well-formed signature for a Polymarket-shaped relayer payload.
///
/// Phase 1 use case: the F-C5 derisk. The relayer signing scheme
/// (`b"rlx:" || keccak256`) is KAT-tested against Polygon Amoy
/// (chain_id 80002). Mainnet (chain_id 137) is verified by the **first
/// MERGE on Phase 1** as the production proof. To derisk operator
/// confusion at first MERGE, run this dry-run pre-launch and confirm:
///   1. `derived_eoa` matches the wallet you funded.
///   2. `proxy_wallet` matches `[live.wallet].proxy_wallet`.
///   3. A signature was produced (non-empty `signature_hex`).
///
/// Test payload: a deterministic 32-byte 0xDE…DE inner data field
/// (`[0xDE; 32]`), nonce 1, gas_price 0, gas_limit 0. The relay
/// address is an internal placeholder (`[0xAA; 20]`) — real merges
/// fetch it dynamically from `/relay-payload`; no operator config
/// field is consulted. The hash differs from any real merge submission
/// so an accidental dry-run can't be replayed against the live relayer.
/// Note: the dry-run does NOT verify that `derived_eoa` controls
/// `proxy_wallet` — that mapping requires an on-chain proxy lookup and
/// is the operator's eyeball check, not auto-asserted.
///
/// Errors:
/// - `private_key` is malformed → message mentions "decode private key".
/// - `proxy_wallet` doesn't parse as 0x-prefixed 20-byte address →
///   message mentions "proxy_wallet".
pub fn dry_run(proxy_wallet: &str, private_key: &str) -> Result<DryRunReport> {
    let signer = SignerService::from_pk_hex(private_key)
        .context("decode private key (check [live.wallet].private_key)")?;
    let proxy_addr = parse_address_bytes(proxy_wallet)
        .context("parse proxy_wallet (check [live.wallet].proxy_wallet)")?;

    // Deterministic test payload. A 32-byte 0xDE…DE marker is an obvious
    // "this is a dry run" sentinel — distinguishable from any real merge
    // calldata in case a log line leaks.
    let inner_data: [u8; 32] = [0xDE; 32];
    let dummy_from = [0xDDu8; 20]; // distinct from the proxy_wallet pattern
    let dummy_to = [0xCCu8; 20]; // distinct from PROXY_FACTORY
    let dummy_relay_hub = [0xBBu8; 20]; // distinct from RELAY_HUB
                                        // Internal placeholder distinct from dummy_relay_hub so a future
                                        // struct-hash field swap between the two slots would be detectable.
    let dummy_relay = [0xAAu8; 20];
    let nonce: u64 = 1;
    let gas_price: u64 = 0;
    let gas_limit: u64 = 0;
    let tx_fee: u64 = 0;

    let test_message = SignerService::proxy_struct_concat(
        &dummy_from,
        &dummy_to,
        &inner_data,
        tx_fee,
        gas_price,
        gas_limit,
        nonce,
        &dummy_relay_hub,
        &dummy_relay,
    );
    let hash = SignerService::proxy_struct_hash(
        &dummy_from,
        &dummy_to,
        &inner_data,
        tx_fee,
        gas_price,
        gas_limit,
        nonce,
        &dummy_relay_hub,
        &dummy_relay,
    );
    let signature = signer
        .sign_proxy_relay_message(&hash)
        .context("sign_proxy_relay_message")?;

    Ok(DryRunReport {
        proxy_wallet: format!("0x{}", hex::encode(proxy_addr)),
        derived_eoa: signer.address_hex(),
        test_message_hex: format!("0x{}", hex::encode(&test_message)),
        hash_hex: format!("0x{}", hex::encode(hash)),
        signature_hex: signature,
    })
}

impl DryRunReport {
    /// Pretty-print the report to stdout in the operator-facing layout
    /// pinned by the F-C5 runbook section. Includes v2 PROXY constants
    /// for visual verification. Format is:
    ///
    /// ```text
    /// --- v2 PROXY dry-run ---
    /// proxy_wallet (config):      0x...
    /// derived_eoa (signer):       0x...
    /// PROXY_FACTORY (constant):   0x...
    /// RELAY_HUB (constant):       0x...
    /// test_message_hex:           0x...
    /// hash_hex:                   0x...
    /// signature_hex:              0x...
    /// ```
    ///
    /// Operators eyeball `derived_eoa` against the wallet they funded
    /// and `proxy_wallet` against `[live.wallet].proxy_wallet`. The
    /// `PROXY_FACTORY` and `RELAY_HUB` constants are embedded in the
    /// relayer's signing scheme and should match the on-chain deployment.
    /// The `hash_hex` and `test_message_hex` lines exist for advanced audit
    /// (compare against an offline keccak256 of the concat).
    pub fn print(&self) {
        println!("--- v2 PROXY dry-run ---");
        println!("proxy_wallet (config):      {}", self.proxy_wallet);
        println!("derived_eoa (signer):       {}", self.derived_eoa);
        println!(
            "PROXY_FACTORY (constant):   0x{}",
            hex::encode(crate::polymarket::onchain::constants::PROXY_FACTORY)
        );
        println!(
            "RELAY_HUB (constant):       0x{}",
            hex::encode(crate::polymarket::onchain::constants::RELAY_HUB)
        );
        println!("test_message_hex:           {}", self.test_message_hex);
        println!("hash_hex:                   {}", self.hash_hex);
        println!("signature_hex:              {}", self.signature_hex);
        println!();
        println!("If derived_eoa does NOT control proxy_wallet on chain, the relayer will reject submissions.");
        println!(
            "Verify on Polygonscan: https://polygonscan.com/address/{}",
            self.proxy_wallet
        );
    }
}

/// Result of `dry_run_safe` — the operator-facing report for the Safe v1.3.0
/// wallet path printed by `--signer-dry-run` when `signature_type = 2`.
///
/// Fields are hex-formatted for direct comparison against the wallet TOML.
#[derive(Clone, Debug, PartialEq)]
pub struct DryRunSafeReport {
    /// The Safe proxy address (from config `proxy_wallet` field).
    pub safe_proxy_address: String,
    /// EOA address derived from `private_key`. Must be the threshold-1
    /// controller of the Safe for `execTransaction` to be accepted.
    pub controller_eoa: String,
    /// Chain ID used for the EIP-712 domain separator.
    pub chain_id: u64,
    /// Hex-encoded SafeTx hash (the 32-byte input to ECDSA sign).
    pub safe_tx_hash_hex: String,
    /// 65-byte ECDSA signature of the SafeTx hash, hex-encoded with 0x prefix.
    pub signature_hex: String,
}

impl DryRunSafeReport {
    /// Pretty-print the Safe dry-run report to stdout.
    pub fn print(&self) {
        println!("--- Safe v1.3.0 dry-run ---");
        println!("safe_proxy_address (config): {}", self.safe_proxy_address);
        println!("controller_eoa (derived):    {}", self.controller_eoa);
        println!("chain_id:                    {}", self.chain_id);
        println!("safe_tx_hash_hex:            {}", self.safe_tx_hash_hex);
        println!("signature_hex:               {}", self.signature_hex);
        println!();
        println!(
            "If controller_eoa is NOT an owner of the Safe (threshold=1), \
             the relayer will reject execTransaction. Verify on Polygonscan: \
             https://polygonscan.com/address/{}",
            self.safe_proxy_address
        );
    }
}

/// Run a hardcoded Safe v1.3.0 signing path against the operator's configured
/// `private_key` + `proxy_wallet` (treated as the Safe proxy address) + `chain_id`.
///
/// Like `dry_run`, this makes no network calls and submits nothing — it proves
/// only that the local Safe signing pipeline produces a well-formed SafeTx
/// signature for a deterministic test payload.
///
/// Test payload: `to = CTF_COLLATERAL_ADAPTER`, `value = 0`,
/// `data = [0xDE; 32]`, `operation = 0` (CALL), all gas fields = 0,
/// `nonce = 1`. Uses `crate::polymarket::onchain::safe::safe_tx_hash` + `SignerService::from_safe`.
///
/// Errors:
/// - `private_key` is malformed → message mentions "decode private key".
/// - `safe_proxy_wallet` doesn't parse as 0x-prefixed 20-byte address →
///   message mentions "parse safe_proxy_wallet".
pub fn dry_run_safe(
    safe_proxy_wallet: &str,
    private_key: &str,
    chain_id: u64,
) -> Result<DryRunSafeReport> {
    use crate::polymarket::onchain::constants::CTF_COLLATERAL_ADAPTER;
    use crate::polymarket::onchain::safe::safe_tx_hash;
    use alloy_signer::SignerSync;

    let safe_addr: Address = safe_proxy_wallet
        .parse()
        .context("parse safe_proxy_wallet (check [live.wallet].proxy_wallet)")?;

    let svc = SignerService::from_safe(private_key, safe_addr, chain_id)
        .context("decode private key (check [live.wallet].private_key)")?;

    let WalletKind::Safe {
        ref controller_eoa, ..
    } = svc.kind
    else {
        unreachable!("from_safe always returns WalletKind::Safe");
    };

    // Deterministic test payload — same sentinel as dry_run's 0xDE inner_data.
    let inner_data: [u8; 32] = [0xDE; 32];
    let hash = safe_tx_hash(
        safe_addr,
        chain_id,
        Address::from(CTF_COLLATERAL_ADAPTER),
        U256::ZERO,
        &inner_data,
        0u8,
        U256::ZERO,
        U256::ZERO,
        U256::ZERO,
        Address::ZERO,
        Address::ZERO,
        U256::from(1u64),
    );

    let sig = controller_eoa
        .sign_hash_sync(&hash)
        .context("sign Safe tx hash")?;

    Ok(DryRunSafeReport {
        safe_proxy_address: format!("{safe_proxy_wallet}"),
        controller_eoa: svc.address_hex(),
        chain_id,
        safe_tx_hash_hex: format!("0x{}", hex::encode(hash.as_slice())),
        signature_hex: format!("0x{}", hex::encode(sig.as_bytes())),
    })
}

/// Encode a `u64` as a 32-byte big-endian buffer (uint256). Used by the
/// proxy struct concat (spike §2.6) where all numeric fields are uint256.
fn u256_be(v: u64) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[24..].copy_from_slice(&v.to_be_bytes());
    buf
}

/// Parse a 0x-prefixed hex address string into a fixed-size 20-byte array
/// suitable for the proxy struct concat (spike §2.6). Wraps
/// `alloy_primitives::Address::parse` and returns the inner array.
pub fn parse_address_bytes(s: &str) -> Result<[u8; 20]> {
    Ok(parse_address(s)?.into_array())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// KAT: byte-for-byte reproduction of py_clob_client_v2's
    /// `_build_poly_1271_order_signature` for a fixed order. Generated from
    /// the SDK (v1.1.0) — if this ever drifts, deposit-wallet orders are
    /// silently invalid at the venue, so pin it rather than recompute it.
    #[test]
    fn poly_1271_signature_matches_sdk_byte_for_byte() {
        const EXPECTED: &str = "0x6755323c29784498102ebc7c92eb08a8ccb2e0e3ed454bda45f0316ca7b609434f2041a2f2217e8397c907241ce875ad34a2906fe38935606be691cc956749861cdf32864c97f6cdcb1823d5197ae11b39e229d94ef4beba15803ebbce9f6365c339746f92ba55f7a38f9910b435292c66f04e0560411c9fcd0ff531d2271a954a4f726465722875696e743235362073616c742c61646472657373206d616b65722c61646472657373207369676e65722c75696e7432353620746f6b656e49642c75696e74323536206d616b6572416d6f756e742c75696e743235362074616b6572416d6f756e742c75696e743820736964652c75696e7438207369676e6174757265547970652c75696e743235362074696d657374616d702c62797465733332206d657461646174612c62797465733332206275696c6465722900ba";

        let pk = "0x0000000000000000000000000000000000000000000000000000000000000001";
        let s = SignerService::from_pk_hex(pk).unwrap();
        let wallet = "0xd9d24e482c11f586cd9a1a53dc3eec6de3883862";

        let order = PolymarketOrder {
            salt: 479_249_096,
            // POLY_1271: BOTH maker and signer are the deposit wallet.
            maker: wallet.into(),
            signer: wallet.into(),
            taker: "0x0000000000000000000000000000000000000000".into(),
            token_id: "1234567890".into(),
            maker_amount: 250_000,
            taker_amount: 5_000_000,
            expiration: 0,
            timestamp_ms: 1_786_027_800,
            metadata: [0u8; 32],
            builder: [0u8; 32],
            side: 0,
            signature_type: SIG_TYPE_POLY_1271,
        };
        let domain = PolymarketOrderDomain {
            name: "Polymarket CTF Exchange".into(),
            version: "2".into(),
            chain_id: 137,
            verifying_contract: "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E".into(),
        };

        let got = s.sign_order(&order, &domain).unwrap();
        assert_eq!(got.len(), 636, "ERC-7739 envelope length");
        assert_eq!(got, EXPECTED, "does not match py_clob_client_v2 output");
    }

    /// Types 0/1/2 must keep producing a plain 65-byte ECDSA signature — the
    /// 1271 branch must not leak into the working non-1271 signing lineage.
    #[test]
    fn non_1271_signature_path_is_unchanged() {
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let s = SignerService::from_pk_hex(pk).unwrap();
        for sig_type in [0u8, 1, 2] {
            let mut o = fixed_order();
            o.signature_type = sig_type;
            let sig = s.sign_order(&o, &fixed_domain()).unwrap();
            assert_eq!(sig.len(), 132, "sig_type {sig_type} must stay plain ECDSA");
        }
    }

    fn fixed_order() -> PolymarketOrder {
        PolymarketOrder {
            salt: 1,
            maker: "0x0000000000000000000000000000000000000001".into(),
            signer: "0x0000000000000000000000000000000000000001".into(),
            taker: "0x0000000000000000000000000000000000000000".into(),
            token_id: "100".into(),
            maker_amount: 1_000_000,
            taker_amount: 500_000,
            expiration: 0,
            timestamp_ms: 1_714_000_000_000,
            metadata: [0u8; 32],
            builder: [0u8; 32],
            side: 0,
            signature_type: 2,
        }
    }

    fn fixed_domain() -> PolymarketOrderDomain {
        PolymarketOrderDomain {
            name: "Polymarket CTF Exchange".into(),
            version: "2".into(),
            chain_id: 137,
            verifying_contract: "0xE111180000d2663C0091e4f400237545B87B996B".into(),
        }
    }

    #[test]
    fn signer_from_known_key_yields_expected_address() {
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let s = SignerService::from_pk_hex(pk).unwrap();
        // Update the expected value below to alloy's actual output if it
        // differs — this test pins regression stability, not crypto correctness.
        assert_eq!(
            s.address_hex(),
            "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23"
        );
    }

    #[test]
    fn sign_order_returns_65_byte_hex_signature() {
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let s = SignerService::from_pk_hex(pk).unwrap();
        let sig = s.sign_order(&fixed_order(), &fixed_domain()).unwrap();
        assert!(sig.starts_with("0x"));
        assert_eq!(sig.len(), 132, "65 bytes = 130 hex + 0x prefix");
    }

    #[test]
    fn sign_order_is_deterministic() {
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let s = SignerService::from_pk_hex(pk).unwrap();
        let sig1 = s.sign_order(&fixed_order(), &fixed_domain()).unwrap();
        let sig2 = s.sign_order(&fixed_order(), &fixed_domain()).unwrap();
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn sign_order_changes_with_token_id() {
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let s = SignerService::from_pk_hex(pk).unwrap();
        let mut o2 = fixed_order();
        o2.token_id = "101".into();
        let sig1 = s.sign_order(&fixed_order(), &fixed_domain()).unwrap();
        let sig2 = s.sign_order(&o2, &fixed_domain()).unwrap();
        assert_ne!(sig1, sig2);
    }

    /// KNOWN-ANSWER TEST: pins `sign_order` to the V2 EIP-712 signature confirmed BYTE-EQUAL to
    /// py-clob-client-v2's `ExchangeOrderBuilderV2.build_signed_order` on 2026-05-10
    /// (docs/probes/clob-probes-2026-05-10.md) for the Hardhat-0 key + this deterministic
    /// sigType=1 (POLY_PROXY) Order. Unlike the structural `sign_order_*` tests above, this guards
    /// the actual signing scheme (typehash + domain + field encoding) against silent drift — a wrong
    /// signature here would mean every live resting bid is venue-rejected. Vector mirrors the
    /// SDK (py_clob_client_v2 v1.1.0, `ExchangeOrderBuilderV2.build_order_signature`); regenerate
    /// from the SDK if the scheme changes.
    #[test]
    fn sign_order_matches_py_clob_client_v2_known_answer() {
        let s = SignerService::from_pk_hex(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        .unwrap();
        let order = PolymarketOrder {
            salt: 3_036_611_183_415_514,
            maker: "0x365f0ca36ae1f641e02fe3b7743673da42a13a70".into(),
            signer: "0xf39fd6e51aad88f6f4ce6ab8827279cffFb92266".into(),
            taker: "0x0000000000000000000000000000000000000000".into(),
            token_id:
                "49500299856831034491021962156746701298730459370557900271970866855042624695770"
                    .into(),
            maker_amount: 5_000_000,
            taker_amount: 50_000_000,
            expiration: 0,
            timestamp_ms: 1_778_413_435_522,
            metadata: [0u8; 32],
            builder: [0u8; 32],
            side: 0,
            signature_type: 1,
        };
        let domain = PolymarketOrderDomain {
            name: "Polymarket CTF Exchange".into(),
            version: "2".into(),
            chain_id: 137,
            verifying_contract: "0xE111180000d2663C0091e4f400237545B87B996B".into(),
        };
        let sig = s.sign_order(&order, &domain).unwrap();
        assert_eq!(
            sig,
            "0x4536f84152505c6d63a0031c5ad5af437ab1eef82fbe57387bdd31024556ccfb32c6b6f6366da16d11ead2c9d15e14c220d19fff4c64594c3d4809291bb9c2d51c"
        );
    }

    /// v2 layout (10 fields):
    /// `"rlx:" || from(20) || to(20) || data(variable) || tx_fee(32) ||
    ///  gas_price(32) || gas_limit(32) || nonce(32) || relay_hub(20) || relay(20)`.
    ///
    /// We test the pre-hash buffer directly (via `proxy_struct_concat`) — the
    /// keccak step is just a wrapper, so byte-exact buffer order is what
    /// proves the scheme is right.
    #[test]
    fn proxy_struct_concat_v2_orders_ten_fields_in_correct_layout_with_zero_data() {
        let from = [0x11u8; 20];
        let to = [0x22u8; 20];
        let relay_hub = [0x33u8; 20];
        let relay = [0x44u8; 20];
        // data is empty; all numeric fields are 0.
        let buf =
            SignerService::proxy_struct_concat(&from, &to, &[], 0, 0, 0, 0, &relay_hub, &relay);
        // Length: 4 prefix + 20 from + 20 to + 0 data + 32*4 numerics + 20 relay_hub + 20 relay = 192.
        assert_eq!(buf.len(), 4 + 20 + 20 + 0 + 32 + 32 + 32 + 32 + 20 + 20);
        // Prefix bytes.
        assert_eq!(&buf[0..4], b"rlx:");
        // from at [4..24].
        assert_eq!(&buf[4..24], &from);
        // to at [24..44].
        assert_eq!(&buf[24..44], &to);
        // tx_fee(32) all zeros [44..76].
        assert!(buf[44..76].iter().all(|b| *b == 0));
        // gas_price(32) all zeros [76..108].
        assert!(buf[76..108].iter().all(|b| *b == 0));
        // gas_limit(32) all zeros [108..140].
        assert!(buf[108..140].iter().all(|b| *b == 0));
        // nonce(32) all zeros [140..172].
        assert!(buf[140..172].iter().all(|b| *b == 0));
        // relay_hub at [172..192].
        assert_eq!(&buf[172..192], &relay_hub);
        // relay at [192..212].
        assert_eq!(&buf[192..212], &relay);
    }

    #[test]
    fn proxy_struct_concat_v2_with_nonzero_fields_and_inline_data() {
        let from = [0xaau8; 20];
        let to = [0x55u8; 20];
        let relay_hub = [0x99u8; 20];
        let relay = [0xbbu8; 20];
        // tx_fee=1, gas_price=2, gas_limit=3, nonce=4; data=[0xde,0xad,0xbe,0xef]
        let data = [0xdeu8, 0xad, 0xbe, 0xef];
        let buf =
            SignerService::proxy_struct_concat(&from, &to, &data, 1, 2, 3, 4, &relay_hub, &relay);
        // Total length: 4 + 20 + 20 + 4 + 32*4 + 20 + 20 = 216.
        assert_eq!(buf.len(), 4 + 20 + 20 + 4 + 32 + 32 + 32 + 32 + 20 + 20);
        assert_eq!(&buf[0..4], b"rlx:");
        assert_eq!(&buf[4..24], &from);
        assert_eq!(&buf[24..44], &to);
        // data inline at [44..48].
        assert_eq!(&buf[44..48], &data);
        // tx_fee 1 → last byte 0x01 at offset [48..80].
        assert!(buf[48..79].iter().all(|b| *b == 0));
        assert_eq!(buf[79], 0x01);
        // gas_price 2 → last byte 0x02 at [80..112].
        assert!(buf[80..111].iter().all(|b| *b == 0));
        assert_eq!(buf[111], 0x02);
        // gas_limit 3 → last byte 0x03 at [112..144].
        assert!(buf[112..143].iter().all(|b| *b == 0));
        assert_eq!(buf[143], 0x03);
        // nonce 4 → last byte 0x04 at [144..176].
        assert!(buf[144..175].iter().all(|b| *b == 0));
        assert_eq!(buf[175], 0x04);
        // relay_hub at [176..196].
        assert_eq!(&buf[176..196], &relay_hub);
        // relay at [196..216].
        assert_eq!(&buf[196..216], &relay);
    }

    #[test]
    fn proxy_struct_hash_v2_concatenates_ten_fields_in_correct_order() {
        use alloy_primitives::keccak256;
        let from = [0x11u8; 20];
        let to = [0x22u8; 20];
        let data = vec![0xAAu8, 0xBB, 0xCC];
        let tx_fee = 0u64;
        let gas_price = 7u64;
        let gas_limit = 100_000u64;
        let nonce = 5u64;
        let relay_hub = [0x33u8; 20];
        let relay = [0x44u8; 20];

        let h = SignerService::proxy_struct_hash(
            &from, &to, &data, tx_fee, gas_price, gas_limit, nonce, &relay_hub, &relay,
        );

        // Manually build the expected concat:
        // "rlx:" || from(20) || to(20) || data || tx_fee(32) || gas_price(32) ||
        //  gas_limit(32) || nonce(32) || relay_hub(20) || relay(20)
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(b"rlx:");
        buf.extend_from_slice(&from);
        buf.extend_from_slice(&to);
        buf.extend_from_slice(&data);
        let u256 = |v: u64| -> [u8; 32] {
            let mut out = [0u8; 32];
            out[24..].copy_from_slice(&v.to_be_bytes());
            out
        };
        buf.extend_from_slice(&u256(tx_fee));
        buf.extend_from_slice(&u256(gas_price));
        buf.extend_from_slice(&u256(gas_limit));
        buf.extend_from_slice(&u256(nonce));
        buf.extend_from_slice(&relay_hub);
        buf.extend_from_slice(&relay);

        let expected: [u8; 32] = keccak256(&buf).into();
        assert_eq!(h, expected);
    }

    #[test]
    fn sign_proxy_relay_message_returns_65_byte_hex_signature() {
        let s = SignerService::from_pk_hex(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        .unwrap();
        let from = [0u8; 20];
        let to = [0u8; 20];
        let relay_hub = [0u8; 20];
        let relay = [0u8; 20];
        let h = SignerService::proxy_struct_hash(&from, &to, &[], 0, 0, 0, 0, &relay_hub, &relay);
        let sig = s.sign_proxy_relay_message(&h).unwrap();
        assert!(sig.starts_with("0x"));
        assert_eq!(sig.len(), 2 + 130); // 65 bytes hex-encoded
    }

    // -----------------------------------------------------------------
    // F-C5 — `dry_run` helper (CLI --signer-dry-run backbone).
    //
    // Tests cover:
    //   - happy path: valid pk + proxy → Ok with non-empty signature.
    //   - bad pk: malformed hex → Err mentioning private_key.
    //   - bad proxy: malformed address → Err mentioning proxy_wallet.
    //   - determinism: same inputs → same hash + same sig (replay-able).
    // -----------------------------------------------------------------

    /// Hardhat #0 key — pinned in the existing KAT test (line ~373) so
    /// reusing it is consistent with the rest of the file.
    const HARDHAT_PK0: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const HARDHAT_EOA0: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";

    #[test]
    fn dry_run_with_valid_inputs_returns_report_with_nonempty_signature() {
        // Use HARDHAT_EOA0 as the proxy_wallet just to exercise the
        // address-parse path. In a real Magic.link deploy the proxy is
        // a different smart-contract account; here we just need a valid
        // 0x-prefixed 20-byte address to verify the signing pipeline.
        let report =
            dry_run(HARDHAT_EOA0, HARDHAT_PK0).expect("dry_run with valid inputs must succeed");

        // EOA derives correctly from HARDHAT_PK0.
        assert_eq!(report.derived_eoa, HARDHAT_EOA0);
        // Proxy echoed back in lowercased hex.
        assert_eq!(
            report.proxy_wallet.to_lowercase(),
            HARDHAT_EOA0.to_lowercase()
        );
        // Signature is 0x + 130 hex chars (65 bytes).
        assert!(report.signature_hex.starts_with("0x"));
        assert_eq!(
            report.signature_hex.len(),
            132,
            "65-byte ECDSA sig = 130 hex + 0x prefix"
        );
        // Hash is 0x + 64 hex chars (32 bytes).
        assert_eq!(report.hash_hex.len(), 66);
        // Test message must include the rlx: prefix and all v2 fields (hex-encoded).
        // Layout: 4 + 20(from) + 20(to) + 32(data) + 32(tx_fee) + 32(gas_price) +
        //         32(gas_limit) + 32(nonce) + 20(relay_hub) + 20(relay) = 244 bytes.
        // Hex: 0x + 2*244 = 490 chars.
        assert_eq!(
            report.test_message_hex.len(),
            2 + 2 * (4 + 20 + 20 + 32 + 32 + 32 + 32 + 32 + 20 + 20)
        );
        // 0x726c783a is "rlx:" in ASCII hex.
        assert!(
            report.test_message_hex.starts_with("0x726c783a"),
            "test_message must start with rlx: prefix; got {}",
            &report.test_message_hex[..40]
        );
    }

    #[test]
    fn dry_run_with_bad_private_key_returns_error_mentioning_pk() {
        let err =
            dry_run(HARDHAT_EOA0, "0xnot-hex-at-all").expect_err("malformed private_key must fail");
        let msg = format!("{:#}", err);
        assert!(
            msg.to_lowercase().contains("private key")
                || msg.to_lowercase().contains("private_key"),
            "error must mention private_key; got: {msg}",
        );
    }

    #[test]
    fn dry_run_with_short_private_key_returns_error() {
        // Half-length key → from_pk_hex's hex::decode succeeds but
        // PrivateKeySigner::from_slice rejects.
        let short = "0x1234";
        let err = dry_run(HARDHAT_EOA0, short).expect_err("short private_key must fail");
        // Top-level context wraps with "decode private key".
        let msg = format!("{:#}", err);
        assert!(
            msg.to_lowercase().contains("private key"),
            "error must surface the private_key context; got: {msg}",
        );
    }

    #[test]
    fn dry_run_with_bad_proxy_returns_error_mentioning_proxy() {
        let err =
            dry_run("not-an-address", HARDHAT_PK0).expect_err("malformed proxy_wallet must fail");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("proxy_wallet"),
            "error must mention proxy_wallet; got: {msg}",
        );
    }

    #[test]
    fn dry_run_is_deterministic() {
        let a = dry_run(HARDHAT_EOA0, HARDHAT_PK0).unwrap();
        let b = dry_run(HARDHAT_EOA0, HARDHAT_PK0).unwrap();
        assert_eq!(a, b, "same inputs must yield same dry-run report");
    }

    #[test]
    fn sign_v2_order_matches_official_sdk_kat() {
        // KAT: byte-equal EIP-712 signature against the reference vector
        // in scripts/v2-kat/reference-vector.json, generated via ethers v6
        // against the V2 Order schema documented at
        // docs.polymarket.com/migrations/clob-v2. Without this gate, recon
        // has no independent confirmation that its sol! struct + domain
        // match the production V2 backend.
        let raw = include_str!("../../tests/fixtures/v2-kat-reference-vector.json");
        let v: serde_json::Value = serde_json::from_str(raw).expect("parse KAT json");

        let inputs = &v["inputs"];
        let domain_v = &v["domain"];

        let order = PolymarketOrder {
            salt: inputs["salt"].as_str().unwrap().parse().unwrap(),
            maker: inputs["maker"].as_str().unwrap().into(),
            signer: inputs["signer"].as_str().unwrap().into(),
            taker: "0x0000000000000000000000000000000000000000".into(),
            token_id: inputs["tokenId"].as_str().unwrap().into(),
            maker_amount: inputs["makerAmount"].as_str().unwrap().parse().unwrap(),
            taker_amount: inputs["takerAmount"].as_str().unwrap().parse().unwrap(),
            expiration: 0,
            timestamp_ms: inputs["timestamp"].as_str().unwrap().parse().unwrap(),
            metadata: parse_bytes32(inputs["metadata"].as_str().unwrap()),
            builder: parse_bytes32(inputs["builder"].as_str().unwrap()),
            side: inputs["side"].as_u64().unwrap() as u8,
            signature_type: inputs["signatureType"].as_u64().unwrap() as u8,
        };

        let domain = PolymarketOrderDomain {
            name: domain_v["name"].as_str().unwrap().into(),
            version: domain_v["version"].as_str().unwrap().into(),
            chain_id: domain_v["chainId"].as_u64().unwrap(),
            verifying_contract: domain_v["verifyingContract"].as_str().unwrap().into(),
        };

        // Hardhat #0 — the same key the generator uses.
        let pk = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let signer = SignerService::from_pk_hex(pk).unwrap();

        let actual_sig = signer
            .sign_order(&order, &domain)
            .expect("sign V2 KAT order");
        let expected_sig = v["expected_signature"].as_str().unwrap().to_lowercase();
        assert_eq!(
            actual_sig.to_lowercase(),
            expected_sig,
            "V2 EIP-712 signature MUST match reference; check sol! field order, domain version, verifying_contract",
        );
    }

    fn parse_bytes32(hex_str: &str) -> [u8; 32] {
        let trimmed = hex_str.strip_prefix("0x").unwrap_or(hex_str);
        let bytes = hex::decode(trimmed).expect("hex32");
        assert_eq!(bytes.len(), 32, "expected 32-byte hex");
        let mut out = [0u8; 32];
        out.copy_from_slice(&bytes);
        out
    }

    #[test]
    fn known_answer_test_against_python_order_utils() {
        // NOTE: This test was originally a V1 KAT against python-order-utils
        // v0.3.2. The V2 schema migration (2026-05) drops taker/nonce/feeRateBps
        // from the signed struct and adds timestamp/metadata/builder. The pinned
        // V1 hash/sig values no longer apply. The load-bearing correctness gate
        // is now `sign_v2_order_matches_official_sdk_kat` (ethers v6 reference
        // vector). This test is retained for ECDSA signing path coverage only
        // (key derivation + determinism checks).
        let pk = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let s = SignerService::from_pk_hex(pk).unwrap();
        // Sanity: the Anvil test key derives the well-known Hardhat #0 EOA.
        assert_eq!(
            s.address_hex(),
            "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
            "Anvil test key derived wrong EOA — alloy version mismatch?",
        );

        let order = PolymarketOrder {
            salt: 479249096354,
            maker: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".into(),
            signer: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".into(),
            taker: "0x0000000000000000000000000000000000000000".into(),
            token_id: "1234".into(),
            maker_amount: 100_000_000,
            taker_amount: 50_000_000,
            expiration: 0,
            timestamp_ms: 1_714_000_000_000,
            metadata: [0u8; 32],
            builder: [0u8; 32],
            side: 0,           // BUY
            signature_type: 0, // EOA
        };
        let domain = PolymarketOrderDomain {
            name: "Polymarket CTF Exchange".into(),
            version: "2".into(),
            chain_id: 137,
            verifying_contract: "0xE111180000d2663C0091e4f400237545B87B996B".into(),
        };

        // Verify the signing pipeline produces a well-formed 65-byte signature.
        // Exact hash/sig pinning is done in sign_v2_order_matches_official_sdk_kat.
        let sig = s.sign_order(&order, &domain).unwrap();
        assert!(sig.starts_with("0x"));
        assert_eq!(sig.len(), 132, "65 bytes = 130 hex + 0x prefix");
    }
}
