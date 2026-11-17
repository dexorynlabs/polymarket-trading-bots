//! The pure decision core (no I/O, no clock, no async) — POST-TWAP-SPEC §3.

pub mod boundary_close;
pub mod cards;
pub mod engine;
pub mod features;
pub mod flow;
pub mod sizing;
pub mod tick;
