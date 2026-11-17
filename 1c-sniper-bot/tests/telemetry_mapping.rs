//! Coverage for `telemetry::actions_to_events` (the `Action` -> wire-row
//! mapping) and a schema-lock on the hand-written `Serialize` impl.
//!
//! This file exists because that impl is hand-written, not derived: nothing
//! else ties the JSON key strings to the schema, and the mapping logic
//! (`commit_usd` vs `clip_usd`, the realised-vs-configured burst fields) is
//! exactly the kind of thing a future edit silently drifts.
//!
//! Schema 4 (POST-TWAP-SPEC §6): the `arm`, `disarm` and `deduped_fire` rows
//! are GONE with the card machine that produced them, `fire` swapped its
//! card-machine columns for the gate's own, and the feature vector is now
//! nullable telemetry. The 12-variant lock below is a 9-variant lock.

use onecentbot::types::Side;
use onecentbot::strategy::cards::CardId;
use onecentbot::strategy::engine::{Action, TickFire};
use onecentbot::strategy::features::SideFeatures;
use onecentbot::strategy::flow::FlowStats;
use onecentbot::bot::telemetry::{actions_to_events, TelemetryEvent, Writer, SCHEMA_V};

fn feats() -> SideFeatures {
    SideFeatures { d: 30.0, v2: 7.0, v3: 4.0, sigma60: 4.0, z_d: 1.0, tau: 100.0 }
}

fn as_json(ev: &TelemetryEvent) -> serde_json::Value {
    serde_json::from_str(&serde_json::to_string(ev).unwrap()).unwrap()
}

fn tick() -> TickFire {
    TickFire {
        venue_ts_ms: 1_786_098_843_366,
        vol_btc: 0.82,
        lean: 0.77,
        window_ms: 1_000,
        book_age_ms: 235,
    }
}

fn fire(feats: Option<SideFeatures>) -> Action {
    Action::Fire {
        t: 243,
        side: Side::Up,
        card: CardId::Burst,
        best_ask: 0.01,
        limit_px: 0.02,
        size_shares: 1_000.0,
        clip_usd: 10.0,
        feats,
        asks: vec![(0.01, 400.0)],
        tick: tick(),
    }
}

// --- actions_to_events: the 2 Action variants, 1:1 --------------------------

#[test]
fn actions_to_events_maps_both_action_variants_one_to_one() {
    let actions = vec![
        fire(Some(feats())),
        Action::Placebo {
            t: 210,
            side: Side::Down,
            card: CardId::Burst,
            venue_ts_ms: 1_786_098_810_042,
            vol_btc: 0.9,
            lean: 0.81,
            best_ask: 0.01,
        },
    ];
    let events = actions_to_events(&actions, 1_786_098_600, None);
    assert_eq!(events.len(), actions.len(), "exactly one event per action");
    assert!(matches!(events[0], TelemetryEvent::Fire { .. }));
    assert!(matches!(events[1], TelemetryEvent::Placebo { .. }));
}

/// `actions_to_events` is the ONLY place a `TickFire` becomes wire fields, and
/// the gate's own grade is re-derived from them. Everything else in this file
/// builds `TelemetryEvent`s by hand and would not catch a mis-wired mapping.
#[test]
fn a_fire_maps_the_realised_burst_and_the_flow_snapshot() {
    let flow = Some(FlowStats {
        tbuy_usd_1s: 187_000.0,
        notional_usd_1s: 188_000.0,
        vol_btc_1s: 1.88,
        trades_1s: 61,
        max_dose_20s_usd: 34.0,
    });
    let v = as_json(&actions_to_events(&[fire(Some(feats()))], 1_786_098_600, flow)[0]);
    assert_eq!(v["card"], "burst");
    assert_eq!(v["tick_venue_ts_ms"], 1_786_098_843_366u64);
    // The OBSERVED burst, never the configured 0.5 BTC / 60% thresholds.
    assert_eq!(v["tick_vol_btc"], 0.82);
    assert_eq!(v["tick_lean"], 0.77);
    assert_eq!(v["tick_window_ms"], 1_000);
    assert_eq!(v["tick_book_age_ms"], 235);
    // The driver's flow snapshot rides the row, each key its own value.
    assert_eq!(v["flow_tbuy_usd_1s"], 187_000.0);
    assert_eq!(v["flow_notional_usd_1s"], 188_000.0);
    assert_eq!(v["flow_vol_btc_1s"], 1.88);
    assert_eq!(v["flow_trades_1s"], 61);
    assert_eq!(v["flow_max_dose_20s_usd"], 34.0);
    // The feature vector is the CLOSED-BAR one and is telemetry only — a fire
    // must not smuggle the burst into `d`.
    assert_eq!(v["d"], feats().d);
}

/// A fire before the slot's first kline has no feature vector at all — and
/// that must not block it (spec §4.1). The five feature keys are then explicit
/// JSON `null`, never omitted: a reader must not have to tell "absent because
/// no kline yet" from "absent because a writer forgot".
#[test]
fn a_fire_without_a_feature_vector_maps_nulls_rather_than_omitting_them() {
    let v = as_json(&actions_to_events(&[fire(None)], 1, None)[0]);
    for k in ["d", "v2", "v3", "sigma60", "z_d"] {
        assert!(v.get(k).is_some(), "{k} must be PRESENT");
        assert!(v[k].is_null(), "{k} must be null without a kline, got {}", v[k]);
    }
    // Same rule for the flow keys when no snapshot exists yet.
    for k in [
        "flow_tbuy_usd_1s",
        "flow_notional_usd_1s",
        "flow_vol_btc_1s",
        "flow_trades_1s",
        "flow_max_dose_20s_usd",
    ] {
        assert!(v.get(k).is_some(), "{k} must be PRESENT without a flow snapshot");
        assert!(v[k].is_null(), "{k} must be null without a flow snapshot, got {}", v[k]);
    }
}

// --- commit_usd vs clip_usd --------------------------------------------------

/// The commit the exposure rail books is the FAK's worst case — every share
/// filling at the 2c limit — never the $10 clip. They differ by exactly 2x at
/// the shipped config, which is precisely the confusion that would go
/// unnoticed if the two were ever aliased.
#[test]
fn fire_commit_usd_is_shares_times_limit_px_not_the_clip() {
    let events = actions_to_events(&[fire(Some(feats()))], 1_786_098_600, None);
    let TelemetryEvent::Fire { commit_usd, clip_usd, .. } = &events[0] else {
        panic!("expected a Fire event");
    };
    assert!((commit_usd - 20.0).abs() < 1e-9, "commit_usd should be 20.0, got {commit_usd}");
    assert!((clip_usd - 10.0).abs() < 1e-9, "clip_usd should stay 10.0, got {clip_usd}");
}

// --- Placebo -----------------------------------------------------------------

/// The mirror row carries what the mirror actually saw — the same volume it
/// would have fired on, leaning the wrong way. Its cumulative PnL staying
/// negative is the regime-change alarm (spec §11.5).
#[test]
fn placebo_row_carries_the_adverse_burst_it_observed() {
    let action = Action::Placebo {
        t: 210,
        side: Side::Down,
        card: CardId::Burst,
        venue_ts_ms: 1_786_098_810_042,
        vol_btc: 0.9,
        lean: 0.81,
        best_ask: 0.01,
    };
    let flow = Some(FlowStats {
        tbuy_usd_1s: 1.0,
        notional_usd_1s: 2.0,
        vol_btc_1s: 3.0,
        trades_1s: 4,
        max_dose_20s_usd: 5.0,
    });
    let v = as_json(&actions_to_events(&[action], 1_786_098_600, flow)[0]);
    assert_eq!(v["kind"], "placebo");
    assert_eq!(v["side"], "down");
    assert_eq!(v["card"], "burst");
    assert_eq!(v["vol_btc"], 0.9);
    assert_eq!(v["lean"], 0.81);
    assert_eq!(v["best_ask"], 0.01);
    // The mirror is only an alarm if it can be joined back to the tape and
    // compared with the real lane — so it carries the same provenance a
    // `Fire` row does.
    assert_eq!(v["venue_ts_ms"], 1_786_098_810_042u64);
    assert_eq!(v["flow_vol_btc_1s"], 3.0);
    assert_eq!(v["flow_trades_1s"], 4);
}

// --- Settle: value-pairing, not just key-set, coverage ----------------------

/// The schema-lock test below pins KEY SETS only — a transposition inside the
/// `Settle` arm of the hand-written `Serialize` impl (e.g. emitting `cost_usd`
/// where `pnl_usd` belongs) compiles and passes it unnoticed. `Settle` is a
/// shell-emitted row with no `actions_to_events` coverage to catch this
/// another way. Eight mutually distinct field values make any such
/// transposition detectable.
#[test]
fn settle_row_pairs_each_key_with_its_own_value() {
    let ev = TelemetryEvent::Settle {
        slot_open: 1_786_098_600,
        slug: "btc-updown-5m-1786098600".into(),
        side: "up".into(),
        winner: "down".into(),
        shares: 11.0,
        cost_usd: 22.0,
        fee_usd: 33.0,
        pnl_usd: 44.0,
    };
    let v = as_json(&ev);
    assert_eq!(v["slot_open"], 1_786_098_600u64);
    assert_eq!(v["slug"], "btc-updown-5m-1786098600");
    assert_eq!(v["side"], "up");
    assert_eq!(v["winner"], "down");
    assert_eq!(v["shares"], 11.0);
    assert_eq!(v["cost_usd"], 22.0);
    assert_eq!(v["fee_usd"], 33.0);
    assert_eq!(v["pnl_usd"], 44.0);
}

/// Same concern for `Fire`, the money row: every numeric field gets its own
/// distinct value so a transposition among them is caught rather than passing
/// the key-set-only schema lock.
#[test]
fn fire_row_pairs_each_key_with_its_own_value() {
    let ev = TelemetryEvent::Fire {
        slot_open: 1_786_098_601,
        t: 201,
        side: "down".into(),
        card: "burst".into(),
        best_ask: 1.0,
        limit_px: 2.0,
        size_shares: 3.0,
        clip_usd: 4.0,
        commit_usd: 5.0,
        asks: vec![(6.0, 7.0)],
        feats: Some(SideFeatures { d: 9.0, v2: 10.0, v3: 11.0, sigma60: 12.0, z_d: 13.0, tau: 14.0 }),
        tick: TickFire {
            venue_ts_ms: 1_786_098_801_137,
            vol_btc: 16.0,
            lean: 17.0,
            window_ms: 18,
            book_age_ms: 19,
        },
        flow: Some(FlowStats {
            tbuy_usd_1s: 20.0,
            notional_usd_1s: 21.0,
            vol_btc_1s: 22.0,
            trades_1s: 23,
            max_dose_20s_usd: 24.0,
        }),
    };
    let v = as_json(&ev);
    assert_eq!(v["slot_open"], 1_786_098_601u64);
    assert_eq!(v["t"], 201);
    assert_eq!(v["side"], "down");
    assert_eq!(v["card"], "burst");
    assert_eq!(v["best_ask"], 1.0);
    assert_eq!(v["limit_px"], 2.0);
    assert_eq!(v["size_shares"], 3.0);
    assert_eq!(v["clip_usd"], 4.0);
    assert_eq!(v["commit_usd"], 5.0);
    assert_eq!(v["asks"], serde_json::json!([[6.0, 7.0]]));
    assert_eq!(v["d"], 9.0);
    assert_eq!(v["v2"], 10.0);
    assert_eq!(v["v3"], 11.0);
    assert_eq!(v["sigma60"], 12.0);
    assert_eq!(v["z_d"], 13.0);
    assert_eq!(v["tick_venue_ts_ms"], 1_786_098_801_137u64);
    assert_eq!(v["tick_vol_btc"], 16.0);
    assert_eq!(v["tick_lean"], 17.0);
    assert_eq!(v["tick_window_ms"], 18);
    assert_eq!(v["tick_book_age_ms"], 19);
    assert_eq!(v["flow_tbuy_usd_1s"], 20.0);
    assert_eq!(v["flow_notional_usd_1s"], 21.0);
    assert_eq!(v["flow_vol_btc_1s"], 22.0);
    assert_eq!(v["flow_trades_1s"], 23);
    assert_eq!(v["flow_max_dose_20s_usd"], 24.0);
}

/// Same concern for `RailTrip` — the other money row. A `value`/`threshold` or
/// `commit_usd`/`value` swap must be caught, not silently pass the key-set
/// lock.
#[test]
fn rail_trip_row_pairs_each_key_with_its_own_value() {
    let ev = TelemetryEvent::RailTrip {
        slot_open: 1_786_098_602,
        rail: "concurrent_exposure".into(),
        value: 100.0,
        threshold: 200.0,
        t: 210,
        side: "up".into(),
        card: "burst".into(),
        commit_usd: 300.0,
    };
    let v = as_json(&ev);
    assert_eq!(v["slot_open"], 1_786_098_602u64);
    assert_eq!(v["rail"], "concurrent_exposure");
    assert_eq!(v["value"], 100.0);
    assert_eq!(v["threshold"], 200.0);
    assert_eq!(v["t"], 210);
    assert_eq!(v["side"], "up");
    assert_eq!(v["card"], "burst");
    assert_eq!(v["commit_usd"], 300.0);
}

// --- Writer actually writes to disk -----------------------------------------

#[tokio::test]
async fn writer_actually_writes_a_valid_json_line_to_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("telemetry.jsonl");
    let w = Writer::new(&path).unwrap();
    w.emit(TelemetryEvent::Miss {
        slot_open: 1_786_098_600, t: 214, side: "up".into(),
        order_id: "abc".into(), reason: "fak_no_match".into(),
    });

    // The writer task runs on a dedicated tokio::spawn; give it real wall-clock
    // time to be scheduled and flush rather than asserting on a fixed sleep,
    // which would be timing-fragile under CI load.
    let mut contents = String::new();
    for _ in 0..200 {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        contents = std::fs::read_to_string(&path).unwrap_or_default();
        if !contents.trim().is_empty() {
            break;
        }
    }

    let lines: Vec<&str> = contents.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 1, "expected exactly one JSON line on disk, got: {contents:?}");
    let v: serde_json::Value = serde_json::from_str(lines[0]).expect("a valid JSON line");
    assert_eq!(v["kind"], "miss");
    assert_eq!(v["order_id"], "abc");
}

// --- Schema lock: exact key set per variant ---------------------------------

/// One fully-populated instance of each `TelemetryEvent` variant, paired with
/// its wire `kind` and its expected (unsorted) key set. A standing invariant:
/// it must fail on a dropped key AND on an added key.
fn schema_lock_cases() -> Vec<(TelemetryEvent, &'static str, Vec<&'static str>)> {
    vec![
        (
            TelemetryEvent::SlotOpen {
                slot_open: 1_786_098_600, slug: "btc-updown-5m-1786098600".into(), p0: 100_000.0,
                token_up: "111".into(), token_dn: "222".into(), condition_id: "0xabc".into(),
            },
            "slot_open",
            vec!["kind", "schema_v", "slot_open", "slug", "p0", "token_up", "token_dn", "condition_id"],
        ),
        (
            TelemetryEvent::Feat {
                slot_open: 1, t: 200, side: "up".into(),
                d: 30.0, v2: 7.0, v3: 5.0, sigma60: 4.0, z_d: 1.0,
                flow: None,
            },
            "feat",
            vec![
                "kind", "schema_v", "slot_open", "t", "side", "d", "v2", "v3", "sigma60", "z_d",
                "flow_tbuy_usd_1s", "flow_notional_usd_1s", "flow_vol_btc_1s", "flow_trades_1s",
                "flow_max_dose_20s_usd",
            ],
        ),
        (
            TelemetryEvent::Fire {
                slot_open: 1, t: 200, side: "up".into(), card: "burst".into(),
                best_ask: 0.01, limit_px: 0.02, size_shares: 1_000.0, clip_usd: 10.0,
                commit_usd: 20.0, asks: vec![(0.01, 400.0)],
                // No kline yet: the five feature keys must still be PRESENT
                // (as JSON null), never omitted. Same rule for the flow keys.
                feats: None, tick: TickFire {
                    venue_ts_ms: 1_786_098_800_137, vol_btc: 0.82, lean: 0.77,
                    window_ms: 1_000, book_age_ms: 235,
                },
                flow: None,
            },
            "fire",
            vec![
                "kind", "schema_v", "slot_open", "t", "side", "card", "best_ask", "limit_px",
                "size_shares", "clip_usd", "commit_usd", "asks", "d", "v2", "v3", "sigma60",
                "z_d", "tick_venue_ts_ms", "tick_vol_btc", "tick_lean", "tick_window_ms",
                "tick_book_age_ms", "flow_tbuy_usd_1s", "flow_notional_usd_1s",
                "flow_vol_btc_1s", "flow_trades_1s", "flow_max_dose_20s_usd",
            ],
        ),
        (
            TelemetryEvent::OrderAck {
                slot_open: 1, t: 200, side: "up".into(), order_id: "abc".into(),
                limit_px: 0.02, size_shares: 1_000.0, latency_ms: 120,
            },
            "order_ack",
            vec!["kind", "schema_v", "slot_open", "t", "side", "order_id", "limit_px", "size_shares", "latency_ms"],
        ),
        (
            TelemetryEvent::Fill {
                slot_open: 1, order_id: "abc".into(), price: 0.01, shares: 400.0,
                fee_usd: 0.05, venue_ts_ms: Some(123), attributable: true,
            },
            "fill",
            vec!["kind", "schema_v", "slot_open", "order_id", "price", "shares", "fee_usd", "venue_ts_ms", "attributable"],
        ),
        (
            TelemetryEvent::Miss {
                slot_open: 1, t: 214, side: "up".into(), order_id: "abc".into(), reason: "fak_no_match".into(),
            },
            "miss",
            vec!["kind", "schema_v", "slot_open", "t", "side", "order_id", "reason"],
        ),
        (
            TelemetryEvent::Settle {
                slot_open: 1, slug: "btc-updown-5m-1786098600".into(), side: "up".into(), winner: "up".into(),
                shares: 400.0, cost_usd: 4.0, fee_usd: 0.05, pnl_usd: 395.95,
            },
            "settle",
            vec!["kind", "schema_v", "slot_open", "slug", "side", "winner", "shares", "cost_usd", "fee_usd", "pnl_usd"],
        ),
        (
            TelemetryEvent::Placebo {
                slot_open: 1, t: 210, side: "down".into(), card: "burst".into(),
                venue_ts_ms: 1_786_098_810_042, vol_btc: 0.9, lean: 0.81, best_ask: 0.01,
                flow: None,
            },
            "placebo",
            vec![
                "kind", "schema_v", "slot_open", "t", "side", "card", "venue_ts_ms", "vol_btc",
                "lean", "best_ask", "flow_tbuy_usd_1s", "flow_notional_usd_1s",
                "flow_vol_btc_1s", "flow_trades_1s", "flow_max_dose_20s_usd",
            ],
        ),
        (
            TelemetryEvent::RailTrip {
                slot_open: 1, rail: "concurrent_exposure".into(), value: 305.0, threshold: 300.0,
                t: 240, side: "up".into(), card: "burst".into(), commit_usd: 20.0,
            },
            "rail_trip",
            vec!["kind", "schema_v", "slot_open", "rail", "value", "threshold", "t", "side", "card", "commit_usd"],
        ),
    ]
}

#[test]
fn each_variant_serializes_to_exactly_its_declared_key_set() {
    let cases = schema_lock_cases();
    assert_eq!(cases.len(), 9, "one case per TelemetryEvent variant");

    for (ev, expected_kind, expected_keys) in cases {
        let v = as_json(&ev);
        assert_eq!(v["kind"], expected_kind);
        assert_eq!(v["schema_v"], SCHEMA_V);

        let mut got: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
        got.sort();
        let mut want: Vec<String> = expected_keys.into_iter().map(String::from).collect();
        want.sort();
        assert_eq!(got, want, "key set drift on `{expected_kind}` — dropped or added a field");
    }
}
