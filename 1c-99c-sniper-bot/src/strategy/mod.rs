//! The pure decision core (no I/O, no clock, no async), shared by the `1c`
//! and `99c` modes. `mode.rs` holds the per-mode presets.

pub mod boundary_close;
pub mod cards;
pub mod engine;
pub mod features;
pub mod flow;
pub mod mode;
pub mod sizing;
pub mod tick;
