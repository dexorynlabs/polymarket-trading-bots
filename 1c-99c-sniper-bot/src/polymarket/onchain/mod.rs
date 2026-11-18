//! Polymarket-protocol on-chain primitives shared by all strategies:
//! chain-specific contract addresses, calldata encoders for the
//! proxy-wallet wrapper, the Safe v1.3.0 EIP-712 typehash, and small HTTP
//! clients for the relayer.

pub mod constants;
pub mod polygon_events;
pub mod safe;
