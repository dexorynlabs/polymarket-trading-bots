//! Phase 3 paper-mode measurements aggregator.
//!
//! `MeasurementsLayer` (a `tracing_subscriber::Layer<S>`) subscribes to all
//! `recon::measurements.*` tracing events, aggregates them into bounded
//! sketches, and drains to JSON at process shutdown. See spec §5.1.

pub mod aggregator;
pub mod output;
pub mod sketch;

pub use aggregator::MeasurementsLayer;
pub use output::MeasurementsOutputV1;
