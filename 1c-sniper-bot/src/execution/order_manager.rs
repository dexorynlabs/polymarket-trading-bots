//! Order lifecycle state — minimal scaffolding for Task 3.4.
//!
//! Tracks `Live → Cancelled` transitions confirmed via WS user-channel
//! `CANCELLATION` events (NOT REST DELETE responses). The strategy doc
//! §15 Module 4 + §10 mandate this routing because measured DELETE p99
//! is 7.77 s vs WS p99 of 507 ms — the LIP shock-window watchdog and
//! the Phase 4 Task 4.3 sibling-cancel STRONG branch both need the
//! ~1 s wall-clock confirmation that only the WS path provides.
//!
//! **Scope (Task 3.4 only):**
//! - Record cancel-submit timestamps for latency metrics.
//! - Apply `Live → Cancelled` transitions on `handle_cancellation`.
//! - Fire registered oneshot wakers, including the race-window case
//!   where a CANCELLATION event arrives BEFORE the caller registers a
//!   waker (handled by checking `Cancelled` state at register time).
//! - Provide `wait_for_ws_cancel(...)` for Phase 4's STRONG branch.
//!
//! **Out of scope (Phase 4+ extends):** placement state, fill tracking,
//! retry / persistence, error classification, snapshots beyond `state_of`.
//! Keep this file boring — extensions land in their own tasks.

use crate::types::Side;
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};

/// CANCELLATION event extracted from a user-channel WS payload.
///
/// Only `order_id` and `ts_micros` come from the wire reliably — the
/// CANCELLATION wire shape carries `id` (the order id) and `timestamp`
/// but no `slug` / `side` fields. `slug` and `side` are kept as
/// `Option`s for forensic logging only; resolving them would require an
/// order-id index that doesn't exist today and isn't part of Task 3.4.
#[derive(Clone, Debug)]
pub struct CancellationEvent {
    pub order_id: String,
    pub ts_micros: u64,
    pub slug: Option<String>,
    pub side: Option<Side>,
}

/// Lifecycle state for a single resting order.
///
/// `Live` is the implicit initial state; `Cancelled { ts_micros }`
/// records when the venue confirmed the cancel via WS. We do not yet
/// track `Filled` / `Rejected` here — the spec contract for Task 3.4 is
/// only the `Live → Cancelled` axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderState {
    Live,
    Cancelled { ts_micros: u64 },
}

/// Minimal in-memory order state machine.
///
/// All locks are `std::sync::Mutex` because no method awaits across the
/// lock — matches the existing `position_store: Arc<std::sync::Mutex<_>>`
/// pattern at `user_ws.rs:227`. `tokio::sync::Mutex` would be wrong
/// here: the cost of poisoning is acceptable (a poisoned lock during a
/// state update is a process-wide invariant violation), and we need
/// non-async access from `event_to_fills` callers.
pub struct OrderManager {
    /// `order_id -> state`. Inserted on first state-change observation.
    states: Mutex<HashMap<String, OrderState>>,
    /// `order_id -> oneshot sender`. Drained on `handle_cancellation`.
    /// Exactly-one-shot per registration; a re-register replaces the
    /// prior sender (the prior `Sender` is dropped, which fires
    /// `Err(_)` on its receiver — the caller treats this as "no
    /// confirm" per the helper contract).
    cancel_wakers: Mutex<HashMap<String, oneshot::Sender<()>>>,
    /// `order_id -> submit_ts_micros`. Used solely for the latency
    /// metric in `handle_cancellation`. Drained on confirmation.
    pending_cancels: Mutex<HashMap<String, u64>>,
}

impl Default for OrderManager {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderManager {
    pub fn new() -> Self {
        Self {
            states: Mutex::new(HashMap::new()),
            cancel_wakers: Mutex::new(HashMap::new()),
            pending_cancels: Mutex::new(HashMap::new()),
        }
    }

    /// Test-only alias for `new()`. Kept to honor the Task 3.4 plan's
    /// contract (`OrderManager::new_test()` appears in the spec's test
    /// stubs). With this minimal scope it is redundant — flagged in the
    /// Task 3.4 report so a future task can decide whether a separate
    /// test-only constructor is worth factoring (e.g. once Phase 4
    /// adds production-only fields like a metric handle).
    pub fn new_test() -> Self {
        Self::new()
    }

    /// Record the wall-clock timestamp at which we submitted a cancel
    /// for this order. Used by `handle_cancellation` to log the
    /// submit→confirm latency. Idempotent: re-recording overwrites.
    pub fn record_pending_cancel(&self, order_id: &str, submit_ts_micros: u64) {
        self.pending_cancels
            .lock()
            .unwrap()
            .insert(order_id.to_string(), submit_ts_micros);
    }

    /// Register a oneshot waker to be fired when the venue confirms
    /// cancellation of this order. Handles the LIP shock-window race:
    /// if the order is ALREADY in `Cancelled` state (CANCELLATION
    /// arrived before the caller could register), fire the sender
    /// immediately instead of stashing it.
    ///
    /// If a prior waker was registered for the same `order_id`, the
    /// stale sender is dropped — its receiver wakes with `Err(_)`,
    /// which `wait_for_ws_cancel` treats as "no confirm". Re-registers
    /// from the same caller are not expected; this fail-safe avoids a
    /// stuck waker if a caller bug double-registers.
    pub fn register_cancel_waker(&self, order_id: String, tx: oneshot::Sender<()>) {
        // Race fix: take the states lock FIRST, then the waker map. If
        // we observe `Cancelled` already, fire immediately without
        // touching the waker map at all. Lock-order is consistent
        // across all methods (states → cancel_wakers → pending_cancels)
        // so deadlock is impossible.
        let already_cancelled = matches!(
            self.states.lock().unwrap().get(&order_id),
            Some(OrderState::Cancelled { .. })
        );
        if already_cancelled {
            // Send may fail if the receiver was dropped; that's fine —
            // the caller no longer cares.
            let _ = tx.send(());
            return;
        }
        self.cancel_wakers.lock().unwrap().insert(order_id, tx);
    }

    /// Apply a CANCELLATION event: transition state to `Cancelled`,
    /// fire any registered waker, log the submit→confirm latency if a
    /// pending-cancel timestamp was recorded.
    ///
    /// Idempotent re state — re-applying a CANCELLATION for an already-
    /// `Cancelled` order overwrites the timestamp (last-writer-wins).
    /// This matches Polymarket's behavior where a single cancel can
    /// emit multiple status echoes; the final one is canonical.
    pub fn handle_cancellation(&self, evt: CancellationEvent) {
        // 1. Transition state. Holding the lock just for the insert.
        self.states.lock().unwrap().insert(
            evt.order_id.clone(),
            OrderState::Cancelled {
                ts_micros: evt.ts_micros,
            },
        );

        // 2. Fire any registered waker. Lock-order: states → cancel_wakers.
        if let Some(tx) = self.cancel_wakers.lock().unwrap().remove(&evt.order_id) {
            // Send may fail if the receiver was dropped (caller timed
            // out); that's fine — the cancel still applied to state.
            let _ = tx.send(());
        }

        // 3. Latency metric — single tracing::info! per spec §15
        // Module 4. Drains the pending entry; subsequent re-applies
        // (rare; would mean the venue echoed twice) skip the log.
        if let Some(submitted_at) = self.pending_cancels.lock().unwrap().remove(&evt.order_id) {
            let lat_ms = evt.ts_micros.saturating_sub(submitted_at) / 1_000;
            tracing::info!(
                order_id = %evt.order_id,
                lat_ms,
                "cancel confirmed via WS"
            );
        }
    }

    /// Snapshot current state of a known order. Returns `None` if the
    /// order has never had a state-changing event recorded.
    pub fn state_of(&self, order_id: &str) -> Option<OrderState> {
        self.states.lock().unwrap().get(order_id).copied()
    }
}

/// Wait up to `deadline_ms` for a WS-confirmed cancellation of
/// `order_id`. Returns `Ok(true)` if the CANCELLATION event arrived
/// (or had already arrived — see race handling in
/// `register_cancel_waker`), `Ok(false)` on timeout or sender-drop.
///
/// Used by Phase 4 Task 4.3's sibling-cancel STRONG branch with
/// `deadline_ms = 1000` (= 2 × measured WS p99 of 507 ms).
pub async fn wait_for_ws_cancel(
    order_manager: &OrderManager,
    order_id: &str,
    deadline_ms: u64,
) -> anyhow::Result<bool> {
    let (tx, rx) = oneshot::channel();
    order_manager.register_cancel_waker(order_id.to_string(), tx);
    match timeout(Duration::from_millis(deadline_ms), rx).await {
        Ok(Ok(())) => Ok(true),  // confirmed via WS (live or race-fired)
        Ok(Err(_)) => Ok(false), // sender dropped (no confirm — re-registered or shutting down)
        Err(_) => Ok(false),     // wall-clock deadline elapsed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn cancellation_event_marks_order_as_cancelled() {
        // Plan-stub test 1: a CANCELLATION event transitions an order's
        // state to `Cancelled { ts_micros }` and clears the
        // pending-cancel timestamp (latency log fires once).
        let mgr = OrderManager::new_test();
        mgr.record_pending_cancel("order-123", 1_000);
        let evt = CancellationEvent {
            order_id: "order-123".into(),
            ts_micros: 1_500,
            slug: None,
            side: None,
        };
        mgr.handle_cancellation(evt);
        assert_eq!(
            mgr.state_of("order-123"),
            Some(OrderState::Cancelled { ts_micros: 1_500 })
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_for_ws_cancel_returns_true_on_timely_event() {
        // Plan-stub test 2: the helper observes a CANCELLATION that
        // arrives within the deadline. Spawn the waiter first, then
        // fire the event ~50 ms later — well inside the 500 ms budget.
        let mgr = Arc::new(OrderManager::new_test());
        let h = {
            let mgr2 = mgr.clone();
            tokio::spawn(async move { wait_for_ws_cancel(&mgr2, "order-1", 500).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        mgr.handle_cancellation(CancellationEvent {
            order_id: "order-1".into(),
            ts_micros: 0,
            slug: None,
            side: None,
        });
        assert!(h.await.unwrap().unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_for_ws_cancel_returns_false_on_timeout() {
        // Plan-stub test 3: no CANCELLATION arrives → false on the
        // wall-clock deadline. 100 ms budget keeps the suite fast.
        let mgr = OrderManager::new_test();
        let result = wait_for_ws_cancel(&mgr, "order-X", 100).await.unwrap();
        assert!(!result);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_for_ws_cancel_returns_true_when_event_already_arrived() {
        // Race edge case (Step 4 in the plan, NOT in the original
        // stubs): caller submits cancel → CANCELLATION races back
        // before the caller can call `register_cancel_waker` → caller
        // would otherwise wait on a waker that never fires. The fix in
        // `register_cancel_waker` checks for `Cancelled` state and
        // fires the oneshot immediately. This test pins that the LIP
        // shock-window race does not strand the STRONG branch caller.
        let mgr = OrderManager::new_test();
        // Simulate the race: CANCELLATION applied BEFORE the helper is
        // called. The next helper call must return true ~immediately
        // (well under any conceivable deadline), proving the race fix.
        mgr.handle_cancellation(CancellationEvent {
            order_id: "order-late".into(),
            ts_micros: 12_345,
            slug: None,
            side: None,
        });
        let result = wait_for_ws_cancel(&mgr, "order-late", 500).await.unwrap();
        assert!(
            result,
            "race-arrived CANCELLATION must fire waker immediately"
        );
    }
}
