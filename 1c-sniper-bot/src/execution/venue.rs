//! The clob-client boundary. `--dry-run` wires [`ShadowVenue`] BY TYPE — it
//! owns no HTTP client, no signer, no credentials, so a real order is
//! unrepresentable in that build rather than merely disabled by a flag
//! (the earlier takers' pattern). `--live` wires `LiveVenue` (Task 14)
//! behind the same trait.
//!
//! This strategy never rests an order, so there is no cancel path at all.

use crate::types::Side;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub struct FakRequest {
    pub token_id: String,
    pub slug: String,
    pub side: Side,
    /// Where the budget walk stopped. Always <= the fired card's cap.
    pub limit_px: f64,
    pub size_shares: f64,
}

#[derive(Clone, Debug)]
pub enum FakOutcome {
    /// Accepted; fills (if any) arrive on the user WS.
    Acked { order_id: String },
    /// Venue killed it with nothing matchable. A CLEAN outcome, not an error:
    /// these misses are free and are the race-haircut measurement.
    ZeroFill { order_id: String },
}

#[derive(Clone, Debug)]
pub struct VenueFill {
    pub order_id: String,
    pub shares: f64,
    pub price: f64,
    pub venue_ts_ms: Option<u64>,
}

#[derive(Clone, Debug)]
pub enum VenueEvent {
    Fill(VenueFill),
    Terminal { order_id: String },
}

pub trait Venue: Send + Sync + 'static {
    fn send_fak(&self, req: FakRequest)
        -> impl Future<Output = anyhow::Result<FakOutcome>> + Send;

    fn events_rx(&self) -> broadcast::Receiver<VenueEvent>;

    /// Called once per slot BEFORE any fire. Live registers the slot's token
    /// ids with the user-WS asset registry — the populate-before-fills
    /// invariant; an unpopulated registry silently DROPS TRADEs. Shadow needs
    /// nothing.
    fn prepare_slot(
        &self,
        slug: &str,
        token_up: &str,
        token_dn: &str,
    ) -> impl Future<Output = ()> + Send {
        let _ = (slug, token_up, token_dn);
        async {}
    }
}

/// Log-only stub: records would-posts, never touches a socket.
pub struct ShadowVenue {
    next_id: AtomicU64,
    posts: Mutex<Vec<FakRequest>>,
    // Held so events_rx() subscribers see a quiet, not closed, channel.
    events_tx: broadcast::Sender<VenueEvent>,
}

impl Default for ShadowVenue {
    fn default() -> Self {
        Self::new()
    }
}

impl ShadowVenue {
    pub fn new() -> Self {
        let (events_tx, _) = broadcast::channel(64);
        Self { next_id: AtomicU64::new(1), posts: Mutex::new(Vec::new()), events_tx }
    }

    /// Everything the strategy WOULD have sent — the audit surface for tests
    /// and the --dry-run readout.
    pub fn posts(&self) -> Vec<FakRequest> {
        self.posts.lock().expect("posts lock").clone()
    }
}

impl Venue for ShadowVenue {
    fn send_fak(
        &self,
        req: FakRequest,
    ) -> impl Future<Output = anyhow::Result<FakOutcome>> + Send {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.posts.lock().expect("posts lock").push(req);
        async move { Ok(FakOutcome::Acked { order_id: format!("shadow-{id}") }) }
    }

    fn events_rx(&self) -> broadcast::Receiver<VenueEvent> {
        self.events_tx.subscribe()
    }
}
