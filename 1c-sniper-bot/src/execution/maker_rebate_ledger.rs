//! Per-slug accumulator for **maker rebates** (Polymarket rebated taker
//! fees, paid in USDC, accruing as `0.07 × p × (1−p) × 0.20 × size` per
//! maker fill). NOT LIP. LIP is the daily transfer at 01:00 UTC computed
//! from the per-minute Q_min sampler — see `src/exec/lip_accrual.rs`
//! (Batch B Task 5).
//!
//! Naming history: was `RebateLedger`; renamed 2026-04-28 to disambiguate
//! from LIP accrual after a postmortem confused the two streams.
//!
//! Source paths (this is the read; not the place where we drive strategy):
//! - REST: GET `/rebates/current?date=YYYY-MM-DD&maker_address=0x...`,
//!   keyed by `condition_id` on the wire (we translate to slug here).
//! - WS:   Polymarket's user channel does NOT emit MAKER_REBATE / REWARD
//!   events as of 2026-04-28. The forward-compat arms in user_ws.rs
//!   route into this ledger if Polymarket ever starts emitting them.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default, Clone, Debug)]
pub struct RebateEntry {
    pub maker_rebate_usd: f64,
    pub reward_usd: f64,
    pub events: u32,
}

#[derive(Default)]
pub struct MakerRebateLedger {
    by_slug: Mutex<HashMap<String, RebateEntry>>,
}

impl MakerRebateLedger {
    pub fn record_maker_rebate(&self, slug: &str, usd: f64) {
        let mut m = self.by_slug.lock().unwrap();
        let e = m.entry(slug.to_string()).or_default();
        e.maker_rebate_usd += usd;
        e.events += 1;
    }

    pub fn record_reward(&self, slug: &str, usd: f64) {
        let mut m = self.by_slug.lock().unwrap();
        let e = m.entry(slug.to_string()).or_default();
        e.reward_usd += usd;
        e.events += 1;
    }

    pub fn snapshot(&self) -> HashMap<String, RebateEntry> {
        self.by_slug.lock().unwrap().clone()
    }
}

/// Wire shape for the `/rebates/current` REST response per
/// `docs.polymarket.com/api-reference/rebates/get-current-rebated-fees-for-a-maker.md`.
/// `rebated_fees_usdc` is a STRING (server returns the USDC decimal as text);
/// parse it as f64 in the consumer.
#[derive(Debug, Deserialize)]
struct RebatesCurrentEntry {
    pub condition_id: String,
    pub rebated_fees_usdc: String,
}

/// One-shot REST poll on the Polymarket maker-rebates endpoint. Builds the
/// query string `?date=<date>&maker_address=<addr>`, parses the JSON-array
/// response, translates each `condition_id` to a slug via `cid_to_slug`,
/// and routes the parsed `(slug, rebated_fees_usdc)` pair into
/// `MakerRebateLedger::record_maker_rebate`.
///
/// The `cid_to_slug` map is populated by `lifecycle::spawn_market` at
/// per-market admit time. Rows whose `condition_id` is not in the map
/// are dropped with a `tracing::warn!` (e.g. a market that ended before
/// the runtime started, or a stale entry from a different maker_address).
///
/// Endpoint is unauthenticated (per docs) — no `auth.rs` HMAC headers
/// required. Caller passes the full base URL up to and including the
/// `/rebates/current` path; the function appends the query string.
///
/// Errors:
/// - HTTP 4xx/5xx → propagated as `Err` with status + body.
/// - Body unparseable as the documented array shape → `Err` (defensive
///   against an HTML error page sneaking through with status 200).
/// - Per-row `rebated_fees_usdc` unparseable as f64 → row is dropped with
///   a `tracing::warn!`; the rest of the batch still applies.
/// - Per-row `condition_id` not in `cid_to_slug` → row is dropped with a
///   `tracing::warn!`; the rest of the batch still applies.
///
/// The ledger has no idempotency layer; the caller (the runtime spawn
/// task) decides cadence. Repeated polls of the same daily total ADD to
/// the running total — operators should poll once per day or accept the
/// monotonic accumulation.
pub async fn poll_rebates_once(
    endpoint_url: &str,
    maker_address: &str,
    date: &str,
    ledger: &Arc<MakerRebateLedger>,
    cid_to_slug: &Arc<tokio::sync::Mutex<HashMap<String, String>>>,
) -> Result<()> {
    let client = reqwest::Client::new();
    let resp = client
        .get(endpoint_url)
        .query(&[("date", date), ("maker_address", maker_address)])
        .send()
        .await
        .with_context(|| format!("GET {endpoint_url}"))?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("rebates HTTP {status}: {body}");
    }
    let parsed: Vec<RebatesCurrentEntry> = resp
        .json()
        .await
        .context("parse /rebates/current response as JSON array")?;
    let cid_map = cid_to_slug.lock().await;
    for entry in parsed {
        let amount = match entry.rebated_fees_usdc.parse::<f64>() {
            Ok(v) => v,
            Err(_) => {
                tracing::warn!(
                    condition_id = %entry.condition_id,
                    amount = %entry.rebated_fees_usdc,
                    "poll_rebates_once: amount unparseable — dropping row",
                );
                continue;
            }
        };
        match cid_map.get(&entry.condition_id) {
            Some(slug) => ledger.record_maker_rebate(slug, amount),
            None => tracing::warn!(
                condition_id = %entry.condition_id,
                amount,
                "poll_rebates_once: unknown condition_id — dropping row",
            ),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ledger_snapshot_is_empty() {
        let l = MakerRebateLedger::default();
        assert!(l.snapshot().is_empty());
    }

    #[test]
    fn record_maker_rebate_accumulates_per_slug() {
        let l = MakerRebateLedger::default();
        l.record_maker_rebate("slug-a", 0.50);
        l.record_maker_rebate("slug-a", 0.25);
        l.record_maker_rebate("slug-b", 1.00);
        let snap = l.snapshot();
        assert!((snap["slug-a"].maker_rebate_usd - 0.75).abs() < 1e-9);
        assert_eq!(snap["slug-a"].events, 2);
        assert!((snap["slug-b"].maker_rebate_usd - 1.00).abs() < 1e-9);
        assert_eq!(snap["slug-b"].events, 1);
    }

    #[test]
    fn record_reward_accumulates_separately_from_rebate() {
        let l = MakerRebateLedger::default();
        l.record_maker_rebate("slug-a", 0.50);
        l.record_reward("slug-a", 0.30);
        let snap = l.snapshot();
        assert!((snap["slug-a"].maker_rebate_usd - 0.50).abs() < 1e-9);
        assert!((snap["slug-a"].reward_usd - 0.30).abs() < 1e-9);
        assert_eq!(snap["slug-a"].events, 2);
    }
}
