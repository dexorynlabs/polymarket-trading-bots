//! Order execution layer (strategy-agnostic).
//!
//! Hosts the `ClobClient` trait that strategy-side `OrderSubmitter`
//! dispatches against, the live + paper REST/WS impls of that trait,
//! the spend/position/maker-rebate ledgers, the §7 guards aggregate,
//! the V2 signer + L1 auth headers, the live-readiness verifier, and
//! the paper-mode matching engine.
//!
//! Strategy-specific pieces (LIP sampler family, MERGE daemon, REDEEM
//! sweeper, taker rebalancer, experiment ledger) stay with the
//! strategy crate (`recon::exec`).

pub mod auth;
pub mod balance;
pub mod clob_rate_limit;
pub mod ledger;
pub mod live_clob;
pub mod live_readiness;
pub mod maker_rebate_ledger;
pub mod order_manager;
pub mod position;
pub mod signer;
pub mod user_ws;
pub mod live;
pub mod venue;

use crate::types::{Fill, RestingAsk, RestingBid, RestingOrderId, TakerBuy};
use std::future::Future;
use tokio::sync::broadcast;

pub trait ClobClient: Send + Sync + 'static {
    /// Place a resting BUY limit order. Returns the id to cancel by later.
    fn place_resting_bid(
        &self,
        bid: RestingBid,
    ) -> impl Future<Output = anyhow::Result<RestingOrderId>> + Send;

    /// Place a resting SELL limit order against owned inventory. Returns the
    /// id to cancel by later. Matching semantics (against incoming bids) are
    /// implementation-defined; `PaperClob` matches in `on_book_update`.
    fn place_resting_ask(
        &self,
        ask: RestingAsk,
    ) -> impl Future<Output = anyhow::Result<RestingOrderId>> + Send;

    /// Cancel a previously placed resting order. A cancel of an already-
    /// filled-or-cancelled order returns `Ok(())` (idempotent).
    fn cancel_order(&self, id: &RestingOrderId) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Batch-cancel resting orders (T16 shutdown drain; the reusable actuator for batch
    /// side-pulls / acked wind-down cancels). Same idempotency contract as `cancel_order`.
    /// Default impl = sequential `cancel_order`, best-effort across the whole list (paper and
    /// test venues); `LiveClob` overrides with the venue batch endpoint (`DELETE /orders`,
    /// ID-array, ≤3000 ids/call).
    fn cancel_orders(
        &self,
        ids: &[RestingOrderId],
    ) -> impl Future<Output = anyhow::Result<()>> + Send {
        async move {
            let mut failed = 0usize;
            for id in ids {
                if let Err(e) = self.cancel_order(id).await {
                    failed += 1;
                    tracing::warn!(id = id.as_str(), ?e, "cancel_orders: per-id cancel failed");
                }
            }
            if failed > 0 {
                anyhow::bail!("cancel_orders: {failed}/{} cancels failed", ids.len());
            }
            Ok(())
        }
    }

    /// Cross the book. May return multiple `Fill`s (one per consumed price
    /// level). An empty Vec means the book had no liquidity at or below
    /// `max_ask`. Fills are also broadcast through `fills_rx()` for
    /// consistency with the resting-fill path.
    fn place_taker_buy(
        &self,
        taker: TakerBuy,
    ) -> impl Future<Output = anyhow::Result<Vec<Fill>>> + Send;

    /// Subscribe to resting-order fills. Every successful match produced by
    /// the paper matcher (or live venue) fans out here; the runtime's fills
    /// consumer records them into the ledger + position store.
    fn fills_rx(&self) -> broadcast::Receiver<Fill>;
}

/// Shared-client convenience: any `Arc<C>` where `C: ClobClient` is itself
/// a `ClobClient`. Enables per-market `OrderSubmitter` shards to share a
/// single underlying client (e.g. `Arc<PaperClob>` in paper mode) while
/// dispatching concurrently — each shard holds its own `Arc` clone.
impl<T: ClobClient + ?Sized> ClobClient for std::sync::Arc<T> {
    fn place_resting_bid(
        &self,
        bid: RestingBid,
    ) -> impl std::future::Future<Output = anyhow::Result<RestingOrderId>> + Send {
        (**self).place_resting_bid(bid)
    }

    fn place_resting_ask(
        &self,
        ask: RestingAsk,
    ) -> impl std::future::Future<Output = anyhow::Result<RestingOrderId>> + Send {
        (**self).place_resting_ask(ask)
    }

    fn cancel_order(
        &self,
        id: &RestingOrderId,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send {
        (**self).cancel_order(id)
    }

    // Explicit forward so an inner override (LiveClob's batch DELETE /orders) is reached —
    // without this, `Arc<LiveClob>` would silently fall back to the sequential default.
    fn cancel_orders(
        &self,
        ids: &[RestingOrderId],
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send {
        (**self).cancel_orders(ids)
    }

    fn place_taker_buy(
        &self,
        taker: TakerBuy,
    ) -> impl std::future::Future<Output = anyhow::Result<Vec<Fill>>> + Send {
        (**self).place_taker_buy(taker)
    }

    fn fills_rx(&self) -> broadcast::Receiver<Fill> {
        (**self).fills_rx()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Coin, Side, TakerBuy};

    // Compile-test: any ClobClient must produce Fills from place_taker_buy.
    #[allow(dead_code)]
    fn _compile_check<C: ClobClient>() {
        let _t = TakerBuy {
            slug: "x".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "T".into(),
            max_ask: 0.9,
            max_shares: 10.0,
            ts_micros: 0,
        };
        // We're only checking the signature is callable; don't await.
        let _ = std::marker::PhantomData::<C>;
    }
}
