//! The launch gate fails closed (§14.3) and the FAK failure taxonomy (§14.2)
//! never mistakes an unproven outcome for the one response that proves a
//! clean zero-fill. `FillDedup` (ported from the earlier takers) forwards
//! each share exactly once across the two user-WS streams.

use onecentbot::config::{validate_strategy, TickburstToml, StrategySection};
use onecentbot::execution::live::{classify_fak_error, FakErrorClass, FillDedup};

#[test]
fn loss_kill_must_bind_before_the_spend_cap() {
    // loss kill (400) >= spend cap (250): the spend cap always bites first, so
    // the loss kill is inert (the maker-lineage bug). Both pinned explicitly so
    // the RELATION is tested independent of the REV 2 defaults.
    let s = StrategySection {
        realized_loss_kill_usd: 400.0,
        daily_spend_cap_usd: 250.0,
        ..StrategySection::default()
    };
    let err = validate_strategy(&s).unwrap_err().to_string();
    assert!(err.contains("inert"), "the inert-loss-kill relation must be checked: {err}");
}

#[test]
fn a_clip_below_the_dollar_floor_is_rejected() {
    // The floor is the venue's $1 marketable minimum — below it nothing ever
    // fires. A minimal-risk $1.50 canary clip is fine: at the 1c entry price
    // it is 150 shares, well clear of the 5-SHARE LIMIT floor too.
    let below = StrategySection { clip_usd: 0.5, ..StrategySection::default() };
    assert!(validate_strategy(&below).is_err(), "$0.50 cannot clear the $1 floor");
    let ok = StrategySection { clip_usd: 1.5, ..StrategySection::default() };
    assert!(validate_strategy(&ok).is_ok(), "$1.50 clears the $1 floor");
}

#[test]
fn live_section_is_required_for_live_mode() {
    let cfg: TickburstToml = toml::from_str("[strategy]\ncoin = \"btc\"\n").unwrap();
    assert!(cfg.live.is_none());
    let err = onecentbot::execution::live::require_live_section(&cfg).unwrap_err().to_string();
    assert!(err.contains("[live]"), "got: {err}");
}

// NOTE: message literals below use the realistic `"POST <url> -> HTTP
// <status>: <body>"` shape LiveClob actually emits (live_clob.rs:430-434),
// not a bare "HTTP <status>" — extract_http_status is anchored to the
// " -> HTTP " marker (D2) and the CleanZeroFill/AmbiguousNoMatch branch is
// gated on that parsed status being exactly 400 (A1), so a bare "HTTP 400"
// with no arrow is deliberately NOT a status match.

#[test]
fn only_the_no_match_400_is_a_clean_zero_fill() {
    let msg = "POST https://x -> HTTP 400: {\"error\":\"no orders found to match with FAK order\",\
               \"orderID\":\"0xabc\"}";
    assert_eq!(
        classify_fak_error(msg),
        FakErrorClass::CleanZeroFill { order_id: "0xabc".to_string() },
        "the one true positive: HTTP 400 + the no-match phrase + a real orderID"
    );
}

/// C1: table of (input, expected variant) pinning the EXACT class per
/// input, not just "not CleanZeroFill" — before this, swapping
/// Rejected/RateLimited/VenueError/Timeout/LandingUnknown inside
/// `classify_fak_error` was invisible to the whole suite. Every row here
/// also keeps both original safety assertions (never CleanZeroFill, never
/// retryable): none of these inputs is the one true positive — that lives
/// in `only_the_no_match_400_is_a_clean_zero_fill` above.
#[test]
fn every_other_failure_pins_its_exact_variant() {
    let cases: [(&str, FakErrorClass); 10] = [
        ("POST https://x -> HTTP 400: {\"error\":\"invalid price\"}", FakErrorClass::Rejected),
        ("POST https://x -> HTTP 401: unauthorized", FakErrorClass::AuthFailure),
        ("POST https://x -> HTTP 403: forbidden", FakErrorClass::AuthFailure),
        ("POST https://x -> HTTP 429: rate limited", FakErrorClass::RateLimited),
        ("POST https://x -> HTTP 502: bad gateway", FakErrorClass::VenueError),
        ("POST https://x -> HTTP 599: weird upstream", FakErrorClass::VenueError),
        ("error sending request: operation timed out", FakErrorClass::Timeout),
        ("error sending request: connection reset by peer", FakErrorClass::Timeout),
        ("expected value at line 1 column 1", FakErrorClass::LandingUnknown),
        (
            "POST https://x returned success=false: fee_rate_bps mismatch",
            FakErrorClass::LandingUnknown,
        ),
    ];
    for (msg, expected) in cases {
        let c = classify_fak_error(msg);
        assert_eq!(c, expected, "input: {msg}");
        assert!(
            !matches!(c, FakErrorClass::CleanZeroFill { .. }),
            "must not be mistaken for a clean zero-fill: {msg}"
        );
        assert!(
            !c.is_retryable(),
            "NOTHING is retryable — a retry that double-buys is unrecoverable: {msg}"
        );
    }
}

/// A1 regression: the no-match phrase alone must never be sufficient — the
/// status must independently be exactly HTTP 400. Without that gate, a
/// gateway 5xx that echoes an origin body containing the phrase (even with
/// a real orderID attached) — or a 200 `success:false` whose `errorMsg`
/// quotes it — would be misread as a proven zero-fill it never was.
#[test]
fn the_no_match_phrase_outside_a_400_is_never_a_clean_zero_fill() {
    let cases = [
        (
            "POST https://x -> HTTP 502: {\"error\":\"no orders found to match with FAK order\",\
             \"orderID\":\"0xabc\"}",
            FakErrorClass::VenueError,
        ),
        // Real success:false shape (live_clob.rs:443-445) carries no "HTTP "
        // marker at all, so status is None here.
        (
            "POST https://x returned success=false: no orders found to match with FAK order",
            FakErrorClass::LandingUnknown,
        ),
    ];
    for (msg, expected) in cases {
        let c = classify_fak_error(msg);
        assert_eq!(c, expected, "input: {msg}");
        assert!(
            !matches!(c, FakErrorClass::CleanZeroFill { .. }),
            "must not be mistaken for a clean zero-fill: {msg}"
        );
        assert!(!c.is_retryable(), "must not be retryable: {msg}");
    }
}

/// A2 regression: decoy field names adjacent to the no-match phrase must not
/// yield a false CleanZeroFill — `extract_order_id` must find the exact
/// `"orderID":"..."` key, not a substring of a differently-named field.
#[test]
fn decoy_order_id_field_names_do_not_produce_a_false_clean_zero_fill() {
    for msg in [
        "POST https://x -> HTTP 400: {\"error\":\"no orders found to match with FAK order\",\
         \"parentOrderID\":\"0xabc\"}",
        "POST https://x -> HTTP 400: {\"error\":\"no orders found to match with FAK order\",\
         \"makerOrderID\":\"0xabc\"}",
    ] {
        let c = classify_fak_error(msg);
        assert_eq!(c, FakErrorClass::AmbiguousNoMatch, "input: {msg}");
        assert!(!matches!(c, FakErrorClass::CleanZeroFill { .. }));
        assert!(!c.is_retryable());
    }
}

/// Fix 2 (staleness canary) regression: the no-match phrase with no
/// parseable HTTP status at all (no " -> HTTP " marker, so status is `None`)
/// must still land on `LandingUnknown` — the safe, loud fallback — exactly
/// as it did before the canary existed. The canary's `error!` log (naming
/// the diagnosis that the phrase appeared outside a parsed HTTP 400) isn't
/// asserted here — there's no log-capture harness in this crate — but the
/// classification itself pins that the guard is observation-only and never
/// changes the returned `FakErrorClass`.
#[test]
fn no_match_phrase_with_no_parseable_status_still_lands_unknown() {
    let msg = "unexpected upstream shape: no orders found to match with FAK order (legacy relay)";
    let c = classify_fak_error(msg);
    assert_eq!(c, FakErrorClass::LandingUnknown, "input: {msg}");
    assert!(!c.is_retryable());
    assert!(!c.halts_process());
}

#[test]
fn an_auth_failure_demands_a_halt() {
    assert!(classify_fak_error("POST https://x -> HTTP 401: unauthorized").halts_process());
    assert!(!classify_fak_error("POST https://x -> HTTP 502: bad gateway").halts_process());
}

/// Resolution 1 (task-14-report deviation from the earlier taker's live module):
/// the brief is STRICTER than that precedent — a clean zero-fill
/// requires the no-match body AND an orderID. When the phrase appears
/// without an extractable orderID — whether the key is absent entirely, or
/// present as `"orderID":""` (A2: an empty orderID is a *documented*
/// behaviour of this endpoint, not proof of anything) — it must still
/// classify as `AmbiguousNoMatch`: never-retryable (never a silent
/// post_failed lookalike either — a distinct variant), but never mistaken
/// for the one proven-clean outcome.
#[test]
fn no_match_without_a_proven_order_id_is_ambiguous_not_clean() {
    for msg in [
        "POST https://x -> HTTP 400: {\"error\":\"no orders found to match with FAK order\"}",
        "POST https://x -> HTTP 400: {\"error\":\"no orders found to match with FAK order\",\
         \"orderID\":\"\"}",
    ] {
        let c = classify_fak_error(msg);
        assert_eq!(
            c,
            FakErrorClass::AmbiguousNoMatch,
            "no proven orderID means we cannot PROVE the zero-fill — must not take the \
             free/quiet path: {msg}"
        );
        assert!(!c.is_retryable());
        assert!(!c.halts_process());
    }
}

#[test]
fn fill_dedup_forwards_each_share_once() {
    let mut d = FillDedup::new();
    // provisional first (the normal fast path), settled echoes it later
    assert_eq!(d.incremental("o1", 8.0, true), 8.0);
    assert_eq!(d.incremental("o1", 8.0, false), 0.0);
    // settled-first arrival order also works
    assert_eq!(d.incremental("o2", 5.0, false), 5.0);
    assert_eq!(d.incremental("o2", 5.0, true), 0.0);
    // multiple partial fills on one order: forward only increments
    assert_eq!(d.incremental("o3", 3.0, true), 3.0);
    assert_eq!(d.incremental("o3", 4.0, true), 4.0);
    assert_eq!(d.incremental("o3", 7.0, false), 0.0);
}
