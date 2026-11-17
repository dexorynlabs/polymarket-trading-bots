//! JSONL telemetry sink — the observability surface this strategy's grade is
//! measured from. No money path: this module never influences a decision, it
//! only records one.
//!
//! **Schema 4 is the POST-TWAP re-gating** (spec §6). Three row kinds are gone
//! with the machine that produced them: `arm` and `disarm` (nothing arms — the
//! D-band cards are retired) and `deduped_fire` (one shot per SLOT means a
//! second qualifier in the same second is unreachable). `fire` swapped the
//! card-machine columns (`cap`, `walk_spend_usd`, `budget_usd`,
//! `counterfactual_dose_commit_usd`, `entry_band`, `trigger`) for the gate's
//! own: the standing ask, the flat clip, and the realised burst that fired it.
//! `d`/`v2`/`v3`/`sigma60`/`z_d` stay on `feat` and `fire`, now NULLABLE and
//! now TELEMETRY ONLY (spec §4.1) — do not read them as anything the engine
//! gated on.
//!
//! Pattern: tagged enum, dedicated writer task.
//! Output path: `data/tickburst_telemetry_<process_start_epoch_secs>.jsonl`
//! (stamped from `now_ms()` once at process start, NOT from the first slot's
//! `slot_open`).

use crate::strategy::engine::{Action, TickFire};
use crate::strategy::features::SideFeatures;
use crate::strategy::flow::FlowStats;
use crate::types::{now_micros, Side};
use serde::{Serialize, Serializer};
use std::io::Write as _;
use tokio::sync::mpsc;

/// v4: the POST-TWAP gate (module doc). v3 was the earlier card machine.
pub const SCHEMA_V: u32 = 4;

#[derive(Debug, Clone)]
pub enum TelemetryEvent {
    SlotOpen {
        slot_open: u64,
        slug: String,
        p0: f64,
        token_up: String,
        token_dn: String,
        condition_id: String,
    },
    /// Every second in the gate window, both sides — the conformance surface
    /// an offline recomputation is diffed against.
    ///
    /// `flow` is the SAME value on the `up` and `down` rows of a second (taker
    /// flow is side-independent; the duplication keeps the row shape keyed
    /// like `d`/`v2`/`v3`). `None` — serialized as five `null`s — whenever no
    /// `@aggTrade` print has been seen (tick path off, or none yet this slot).
    Feat {
        slot_open: u64,
        t: usize,
        side: String,
        d: f64,
        v2: f64,
        v3: f64,
        sigma60: f64,
        z_d: f64,
        flow: Option<FlowStats>,
    },
    /// `commit_usd` = `size_shares * limit_px` — what the exposure rail
    /// reserves, and the worst case if every share fills at the 2c limit
    /// instead of the 1c ask we qualified on.
    Fire {
        slot_open: u64,
        t: usize,
        side: String,
        card: String,
        /// The standing ask the entry qualified on (spec §3.4 — 1c).
        best_ask: f64,
        limit_px: f64,
        size_shares: f64,
        clip_usd: f64,
        commit_usd: f64,
        asks: Vec<(f64, f64)>,
        /// Closed-bar features, TELEMETRY ONLY and NULLABLE (module doc).
        feats: Option<SideFeatures>,
        /// The realised burst that fired: Binance's own trade time, the
        /// trailing volume and lean actually observed (never the configured
        /// thresholds), the window, and how old the held book was.
        tick: TickFire,
        /// Trailing taker-flow context at fire time (`flow.rs`).
        flow: Option<FlowStats>,
    },
    OrderAck {
        slot_open: u64,
        t: usize,
        side: String,
        order_id: String,
        limit_px: f64,
        size_shares: f64,
        latency_ms: u64,
    },
    Fill {
        slot_open: u64,
        order_id: String,
        price: f64,
        shares: f64,
        fee_usd: f64,
        venue_ts_ms: Option<u64>,
        attributable: bool,
    },
    Miss {
        slot_open: u64,
        t: usize,
        side: String,
        order_id: String,
        reason: String,
    },
    Settle {
        slot_open: u64,
        slug: String,
        side: String,
        winner: String,
        shares: f64,
        cost_usd: f64,
        fee_usd: f64,
        pnl_usd: f64,
    },
    /// Would-be trade for the ADVERSE-lean mirror. LOG ONLY, forever — its
    /// cumulative PnL must stay negative (the earlier design's §11.5 mirror,
    /// re-pointed at the new gate by POST-TWAP-SPEC §6).
    Placebo {
        slot_open: u64,
        t: usize,
        side: String,
        card: String,
        /// Binance's own trade time for the print the mirror observed — the
        /// join key back to the tape. A `Fire` row carries the same thing as
        /// `tick_venue_ts_ms`.
        venue_ts_ms: u64,
        vol_btc: f64,
        lean: f64,
        best_ask: f64,
        /// Trailing taker-flow context, exactly as on a `Fire` row — the
        /// mirror's whole job is to be comparable with the real lane.
        flow: Option<FlowStats>,
    },
    /// A rail that binds silently is a strategy change nobody approved
    /// (design §5.0) — the suppression rate is derived entirely from these.
    ///
    /// `slot_open` is required (not just `t`) so a rail trip can be attributed
    /// to a slot by a real key rather than by file position between
    /// `SlotOpen` markers — which breaks under crash-truncated files, log
    /// rotation, and restart-concatenated streams.
    RailTrip {
        slot_open: u64,
        rail: String,
        value: f64,
        threshold: f64,
        t: usize,
        side: String,
        card: String,
        commit_usd: f64,
    },
}

// Manual impl, not `#[derive(Serialize)]` with `#[serde(tag = "kind")]`:
// `schema_v` is NOT a field any variant's constructor supplies (see the
// tests below, which build variants without it) — it is a constant every
// row carries, injected here rather than threaded through every call site.
// (Struct-update syntax `..Default::default()` cannot fill it either: Rust
// rejects functional record update on enum struct-variants — E0436.)
// Building via `serde_json::json!` keeps each arm a literal, checkable
// transcription of the field table in the module doc, and never panics:
// serde_json maps non-finite f64 (e.g. zD at degenerate sigma) to JSON
// `null` rather than erroring.
impl Serialize for TelemetryEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let v = match self {
            TelemetryEvent::SlotOpen { slot_open, slug, p0, token_up, token_dn, condition_id } => {
                serde_json::json!({
                    "kind": "slot_open", "schema_v": SCHEMA_V,
                    "slot_open": slot_open, "slug": slug, "p0": p0,
                    "token_up": token_up, "token_dn": token_dn, "condition_id": condition_id,
                })
            }
            TelemetryEvent::Feat { slot_open, t, side, d, v2, v3, sigma60, z_d, flow } => {
                serde_json::json!({
                    "kind": "feat", "schema_v": SCHEMA_V,
                    "slot_open": slot_open, "t": t, "side": side,
                    "d": d, "v2": v2, "v3": v3, "sigma60": sigma60, "z_d": z_d,
                    "flow_tbuy_usd_1s": flow.map(|f| f.tbuy_usd_1s),
                    "flow_notional_usd_1s": flow.map(|f| f.notional_usd_1s),
                    "flow_vol_btc_1s": flow.map(|f| f.vol_btc_1s),
                    "flow_trades_1s": flow.map(|f| f.trades_1s),
                    "flow_max_dose_20s_usd": flow.map(|f| f.max_dose_20s_usd),
                })
            }
            TelemetryEvent::Fire {
                slot_open, t, side, card, best_ask, limit_px, size_shares, clip_usd,
                commit_usd, asks, feats, tick, flow,
            } => {
                // The feature and flow keys are ALWAYS present (JSON `null`
                // when absent) rather than omitted: a reader must never have
                // to distinguish "absent because there was no kline yet" from
                // "absent because a writer forgot".
                serde_json::json!({
                    "kind": "fire", "schema_v": SCHEMA_V,
                    "slot_open": slot_open, "t": t, "side": side, "card": card,
                    "best_ask": best_ask, "limit_px": limit_px, "size_shares": size_shares,
                    "clip_usd": clip_usd, "commit_usd": commit_usd, "asks": asks,
                    "d": feats.map(|f| f.d),
                    "v2": feats.map(|f| f.v2),
                    "v3": feats.map(|f| f.v3),
                    "sigma60": feats.map(|f| f.sigma60),
                    "z_d": feats.map(|f| f.z_d),
                    "tick_venue_ts_ms": tick.venue_ts_ms,
                    "tick_vol_btc": tick.vol_btc,
                    "tick_lean": tick.lean,
                    "tick_window_ms": tick.window_ms,
                    "tick_book_age_ms": tick.book_age_ms,
                    "flow_tbuy_usd_1s": flow.map(|f| f.tbuy_usd_1s),
                    "flow_notional_usd_1s": flow.map(|f| f.notional_usd_1s),
                    "flow_vol_btc_1s": flow.map(|f| f.vol_btc_1s),
                    "flow_trades_1s": flow.map(|f| f.trades_1s),
                    "flow_max_dose_20s_usd": flow.map(|f| f.max_dose_20s_usd),
                })
            }
            TelemetryEvent::OrderAck { slot_open, t, side, order_id, limit_px, size_shares, latency_ms } => {
                serde_json::json!({
                    "kind": "order_ack", "schema_v": SCHEMA_V,
                    "slot_open": slot_open, "t": t, "side": side, "order_id": order_id,
                    "limit_px": limit_px, "size_shares": size_shares, "latency_ms": latency_ms,
                })
            }
            TelemetryEvent::Fill { slot_open, order_id, price, shares, fee_usd, venue_ts_ms, attributable } => {
                serde_json::json!({
                    "kind": "fill", "schema_v": SCHEMA_V,
                    "slot_open": slot_open, "order_id": order_id, "price": price, "shares": shares,
                    "fee_usd": fee_usd, "venue_ts_ms": venue_ts_ms, "attributable": attributable,
                })
            }
            TelemetryEvent::Miss { slot_open, t, side, order_id, reason } => {
                serde_json::json!({
                    "kind": "miss", "schema_v": SCHEMA_V,
                    "slot_open": slot_open, "t": t, "side": side,
                    "order_id": order_id, "reason": reason,
                })
            }
            TelemetryEvent::Settle { slot_open, slug, side, winner, shares, cost_usd, fee_usd, pnl_usd } => {
                serde_json::json!({
                    "kind": "settle", "schema_v": SCHEMA_V,
                    "slot_open": slot_open, "slug": slug, "side": side, "winner": winner,
                    "shares": shares, "cost_usd": cost_usd, "fee_usd": fee_usd, "pnl_usd": pnl_usd,
                })
            }
            TelemetryEvent::Placebo {
                slot_open, t, side, card, venue_ts_ms, vol_btc, lean, best_ask, flow,
            } => {
                serde_json::json!({
                    "kind": "placebo", "schema_v": SCHEMA_V,
                    "slot_open": slot_open, "t": t, "side": side, "card": card,
                    "venue_ts_ms": venue_ts_ms,
                    "vol_btc": vol_btc, "lean": lean, "best_ask": best_ask,
                    "flow_tbuy_usd_1s": flow.map(|f| f.tbuy_usd_1s),
                    "flow_notional_usd_1s": flow.map(|f| f.notional_usd_1s),
                    "flow_vol_btc_1s": flow.map(|f| f.vol_btc_1s),
                    "flow_trades_1s": flow.map(|f| f.trades_1s),
                    "flow_max_dose_20s_usd": flow.map(|f| f.max_dose_20s_usd),
                })
            }
            TelemetryEvent::RailTrip { slot_open, rail, value, threshold, t, side, card, commit_usd } => {
                serde_json::json!({
                    "kind": "rail_trip", "schema_v": SCHEMA_V,
                    "slot_open": slot_open, "rail": rail, "value": value, "threshold": threshold,
                    "t": t, "side": side, "card": card, "commit_usd": commit_usd,
                })
            }
        };
        v.serialize(serializer)
    }
}

pub fn side_str(side: Side) -> String {
    match side {
        Side::Up => "up",
        Side::Down => "down",
    }
    .to_string()
}

/// Maps the 2 `engine::Action` variants to their telemetry rows. The other
/// 7 `TelemetryEvent` variants (`SlotOpen`, `Feat`, `OrderAck`, `Fill`,
/// `Miss`, `Settle`, `RailTrip`) are emitted directly by the shell — they
/// are not derivable from an `Action`.
///
/// `flow` is the driver's flow snapshot at the causing print (`flow.rs`),
/// stamped on BOTH row kinds — the mirror is only useful if it is comparable
/// with the real lane. The engine's own ring is the source, but the engine
/// does not build the descriptive view itself.
pub fn actions_to_events(
    actions: &[Action],
    slot_open: u64,
    flow: Option<FlowStats>,
) -> Vec<TelemetryEvent> {
    actions
        .iter()
        .map(|a| match a {
            Action::Fire {
                t, side, card, best_ask, limit_px, size_shares, clip_usd, feats, asks, tick,
            } => TelemetryEvent::Fire {
                slot_open,
                t: *t,
                side: side_str(*side),
                card: card.as_str().to_string(),
                best_ask: *best_ask,
                limit_px: *limit_px,
                size_shares: *size_shares,
                clip_usd: *clip_usd,
                commit_usd: size_shares * limit_px,
                asks: asks.clone(),
                feats: *feats,
                tick: *tick,
                flow,
            },
            Action::Placebo { t, side, card, venue_ts_ms, vol_btc, lean, best_ask } => {
                TelemetryEvent::Placebo {
                    slot_open,
                    t: *t,
                    side: side_str(*side),
                    card: card.as_str().to_string(),
                    venue_ts_ms: *venue_ts_ms,
                    vol_btc: *vol_btc,
                    lean: *lean,
                    best_ask: *best_ask,
                    flow,
                }
            }
        })
        .collect()
}

/// Dedicated writer task owns the file; `emit` never touches the fs.
///
/// `Clone`: cloning shares the same underlying `mpsc::UnboundedSender`, hence
/// the same single writer task and open file handle — never two independent
/// `Writer::new` calls against one path (that would risk torn/interleaved
/// lines from two file handles). `runtime.rs` is the first caller needing more
/// than one handle to the SAME sink (one moved into `SlotDeps`, one into
/// `SettleLane`), and is also the first caller able to drain it at shutdown —
/// see `new_with_handle`.
#[derive(Clone)]
pub struct Writer {
    tx: mpsc::UnboundedSender<(u64, TelemetryEvent)>,
}

impl Writer {
    /// Opens (creating parent dirs and the file) synchronously so a bad path
    /// surfaces immediately, then spawns the append-only writer task.
    ///
    /// Must be called from inside a Tokio runtime (e.g. under `#[tokio::main]`
    /// or `tokio::spawn`) — `tokio::spawn` panics otherwise.
    pub fn new(path: &std::path::Path) -> anyhow::Result<Self> {
        let (writer, _handle) = Self::new_with_handle(path)?;
        Ok(writer)
    }

    /// Same as [`new`](Self::new), but also returns the writer task's
    /// `JoinHandle`. A caller that owns the process lifecycle drains the sink
    /// at shutdown by dropping every `Writer` clone it holds, then awaiting
    /// this handle: the spawned task's `rx.recv()` loop only returns once
    /// every `Sender` clone (including ones inside a cloned `Writer`) is
    /// gone, so awaiting the handle blocks until every already-queued event
    /// has actually been written — never discarded at exit as `Writer::new`
    /// alone would do.
    pub fn new_with_handle(
        path: &std::path::Path,
    ) -> anyhow::Result<(Self, tokio::task::JoinHandle<()>)> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
        let (tx, mut rx) = mpsc::unbounded_channel::<(u64, TelemetryEvent)>();
        let handle = tokio::spawn(async move {
            while let Some((ts_us, ev)) = rx.recv().await {
                match serialize_stamped(ts_us, &ev) {
                    Ok(line) => {
                        if let Err(e) = writeln!(file, "{line}") {
                            tracing::error!("tickburst telemetry write failed: {e:#}");
                        }
                    }
                    Err(e) => tracing::error!("tickburst telemetry serialize failed: {e:#}"),
                }
            }
        });
        Ok((Self { tx }, handle))
    }

    /// Non-blocking; logs and drops the event if the writer task is gone.
    /// Telemetry must never block or panic the hot loop.
    ///
    /// The `ts_us` wall-clock stamp is taken HERE, on the caller's own task,
    /// not in the writer task below: the channel is unbounded and drained
    /// asynchronously, so a serialize-time stamp would fold the writer's
    /// queueing delay into every interval measured from these rows — which is
    /// precisely the latency this instrumentation exists to measure.
    pub fn emit(&self, ev: TelemetryEvent) {
        if self.tx.send((now_micros(), ev)).is_err() {
            tracing::warn!("tickburst telemetry writer gone; dropping event");
        }
    }
}

/// Serialize one row with the `ts_us` envelope field injected.
///
/// Injected here rather than in `Serialize` for the same reason `schema_v` is
/// injected there: no variant's constructor supplies it. Keeping it out of the
/// per-variant `json!` arms also keeps `each_variant_serializes_to_exactly_its_
/// declared_key_set` (tests/telemetry_mapping.rs) locking the decision-carrying
/// payload of each variant, with the envelope pinned separately below.
fn serialize_stamped(ts_us: u64, ev: &TelemetryEvent) -> serde_json::Result<String> {
    let mut v = serde_json::to_value(ev)?;
    if let Some(obj) = v.as_object_mut() {
        obj.insert("ts_us".into(), serde_json::json!(ts_us));
    }
    serde_json::to_string(&v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_event_carries_schema_v_and_a_tagged_kind() {
        let ev = TelemetryEvent::Miss {
            slot_open: 1_786_098_600, t: 214, side: "up".into(),
            order_id: "abc".into(), reason: "fak_no_match".into(),
        };
        let s = serde_json::to_string(&ev).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["kind"], "miss", "events must be externally tagged as `kind`");
        assert_eq!(v["schema_v"], SCHEMA_V);
        assert_eq!(v["order_id"], "abc");
    }

    fn miss(t: usize) -> TelemetryEvent {
        TelemetryEvent::Miss {
            slot_open: 1_786_098_600, t, side: "up".into(),
            order_id: "abc".into(), reason: "fak_no_match".into(),
        }
    }

    #[test]
    fn written_rows_carry_a_ts_us_envelope() {
        let s = serialize_stamped(1_786_098_900_123_456, &miss(214)).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["ts_us"], 1_786_098_900_123_456u64);
        // the variant payload must survive the injection untouched
        assert_eq!(v["kind"], "miss");
        assert_eq!(v["schema_v"], SCHEMA_V);
        assert_eq!(v["order_id"], "abc");
    }

    /// The load-bearing property: `ts_us` measures when the hot path EMITTED
    /// the event, not when the writer task got around to draining it. A
    /// serialize-time stamp would collapse this gap and silently report the
    /// writer's backlog as zero pipeline latency.
    #[tokio::test]
    async fn ts_us_is_stamped_at_emit_not_at_drain() {
        let dir = std::env::temp_dir().join(format!("tickburst_tele_{}", now_micros()));
        let path = dir.join("t.jsonl");
        let (w, handle) = Writer::new_with_handle(&path).unwrap();

        // Emit two events ~30 ms apart while the writer is deliberately
        // starved, then let it drain both at once.
        let a = now_micros();
        w.emit(miss(1));
        std::thread::sleep(std::time::Duration::from_millis(30));
        w.emit(miss(2));
        drop(w);
        handle.await.unwrap();

        let rows: Vec<serde_json::Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(rows.len(), 2);
        let (t0, t1) = (rows[0]["ts_us"].as_u64().unwrap(), rows[1]["ts_us"].as_u64().unwrap());
        assert!(t0 >= a, "stamp precedes the emit call");
        let gap_us = t1 - t0;
        assert!(
            (25_000..200_000).contains(&gap_us),
            "the two rows must preserve the ~30 ms EMIT gap, got {gap_us} us — \
             a near-zero gap means the stamp moved to serialize time"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn feat_rows_round_trip_the_full_feature_vector() {
        let ev = TelemetryEvent::Feat {
            slot_open: 1, t: 200, side: "up".into(),
            d: 30.0, v2: 7.0, v3: 5.0, sigma60: 4.0, z_d: 1.0,
            flow: None,
        };
        let v: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&ev).unwrap()).unwrap();
        for k in ["d", "v2", "v3", "sigma60", "z_d"] {
            assert!(v.get(k).is_some(), "missing {k}");
        }
    }
}
