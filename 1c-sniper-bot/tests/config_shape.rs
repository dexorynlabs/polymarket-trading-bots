//! The operator TOML's shape contract (POST-TWAP-SPEC §3). `[strategy]` is
//! `deny_unknown_fields`, so the earlier keys this re-gating deleted
//! (`clip_mode`, `flat_clip_usd`, `slip_mult`, `max_entry_ask_usd`,
//! `tick.dose_usd`, `kline_gap_disarm_ms`) now fail LOUDLY at parse — pinned
//! below, because a silent default there would trade the new gate under the
//! dead machine's numbers.

use onecentbot::config::TickburstToml;

const MINIMAL: &str = r#"
[strategy]
coin = "btc"
"#;

#[test]
fn minimal_toml_parses_with_v0_defaults() {
    let cfg: TickburstToml = toml::from_str(MINIMAL).expect("minimal toml must parse");
    assert_eq!(cfg.strategy.coin, "btc");
    // Spec §3.4 is the 1c equality; the SHIPPED band is the operator widening
    // to 0.9c-1.1c (2026-08-09). The FAK limit is 2c, the clip a flat $10.
    assert_eq!(cfg.strategy.ask_price_min, 0.009);
    assert_eq!(cfg.strategy.ask_price_max, 0.011);
    assert_eq!(cfg.strategy.limit_price, 0.02);
    assert_eq!(cfg.strategy.clip_usd, 10.0);
    // Rails: daily re-derived for this gate (~50 entries/day, $10 committed
    // per fire since the 2026-08-10 sizing fix); the rest inherited.
    assert_eq!(cfg.strategy.daily_spend_cap_usd, 2_500.0);
    assert_eq!(cfg.strategy.concurrent_exposure_cap_usd, 150.0);
    assert_eq!(cfg.strategy.per_order_cap_usd, 100.0);
    assert_eq!(cfg.strategy.realized_loss_kill_usd, 400.0);
    assert_eq!(cfg.strategy.self_fire_rate_per_min, 20);
}

/// Each of these configured the machine spec §6 retired. `deny_unknown_fields`
/// must reject them by NAME so an operator carrying an old TOML forward gets a
/// parse error, not a silently different strategy.
#[test]
fn every_retired_key_is_a_hard_parse_error() {
    for key in [
        "clip_mode = \"tiered\"",
        "flat_clip_usd = 5.0",
        "slip_mult = 3.0",
        "max_entry_ask_usd = 0.03",
        "kline_gap_disarm_ms = 2000",
    ] {
        let bad = format!("{MINIMAL}{key}\n");
        let err = toml::from_str::<TickburstToml>(&bad).unwrap_err().to_string();
        let name = key.split(' ').next().unwrap();
        assert!(err.contains(name), "{key}: got {err}");
    }
    let bad = format!("{MINIMAL}\n[strategy.tick]\ndose_usd = 8.0\n");
    let err = toml::from_str::<TickburstToml>(&bad).unwrap_err().to_string();
    assert!(err.contains("dose_usd"), "got: {err}");
}

#[test]
fn unknown_strategy_key_is_a_hard_error() {
    let bad = format!("{MINIMAL}refire_enabled = true\n");
    let err = toml::from_str::<TickburstToml>(&bad).unwrap_err().to_string();
    assert!(err.contains("refire_enabled"), "got: {err}");
}

#[test]
fn eth_is_rejected_in_v0() {
    let cfg: TickburstToml = toml::from_str("[strategy]\ncoin = \"eth\"\n").unwrap();
    let err = onecentbot::config::validate_strategy(&cfg.strategy).unwrap_err().to_string();
    assert!(err.contains("btc"), "got: {err}");
}

#[test]
fn an_entry_price_outside_the_cheap_band_is_rejected() {
    for (key, px) in [("ask_price_min", "0.001"), ("ask_price_max", "0.5")] {
        let bad = format!("{MINIMAL}{key} = {px}\n");
        let cfg: TickburstToml = toml::from_str(&bad).expect("parses; validation rejects");
        let err =
            onecentbot::config::validate_strategy(&cfg.strategy).unwrap_err().to_string();
        assert!(err.contains(key), "{key}={px}: got {err}");
    }
}

/// An inverted band admits nothing, so the strategy would silently never fire.
#[test]
fn an_inverted_entry_band_is_rejected() {
    let bad = format!("{MINIMAL}ask_price_min = 0.012\nask_price_max = 0.011\n");
    let cfg: TickburstToml = toml::from_str(&bad).expect("parses; validation rejects");
    let err = onecentbot::config::validate_strategy(&cfg.strategy).unwrap_err().to_string();
    assert!(err.contains("ask_price_min"), "got: {err}");
}

/// A limit BELOW the price we qualified at is incoherent; one far above stops
/// being race-tail optionality and starts buying the sweep that just killed
/// the 1c level (spec §3.4).
#[test]
fn a_limit_price_below_the_entry_or_far_above_it_is_rejected() {
    // Below the band's DEAR edge (0.011), and far above its cheap one.
    for px in ["0.010", "0.05"] {
        let bad = format!("{MINIMAL}limit_price = {px}\n");
        let cfg: TickburstToml = toml::from_str(&bad).expect("parses; validation rejects");
        let err =
            onecentbot::config::validate_strategy(&cfg.strategy).unwrap_err().to_string();
        assert!(err.contains("limit_price"), "limit_price={px}: got {err}");
    }
    // 4x the CHEAP edge (0.009) is 0.036 — the bound, and inclusive.
    let ok = format!("{MINIMAL}limit_price = 0.036\n");
    let cfg: TickburstToml = toml::from_str(&ok).unwrap();
    assert!(onecentbot::config::validate_strategy(&cfg.strategy).is_ok(), "4x is the bound");
}

/// The venue's LIMIT minimum is 5 SHARES (not $5 of notional), and the $1
/// marketable floor is separate. Both are checked against the clip/price pair
/// rather than either one alone.
#[test]
fn a_clip_that_cannot_clear_the_venue_minimums_is_rejected() {
    let below_dollar = format!("{MINIMAL}clip_usd = 0.5\n");
    let cfg: TickburstToml = toml::from_str(&below_dollar).unwrap();
    let err = onecentbot::config::validate_strategy(&cfg.strategy).unwrap_err().to_string();
    assert!(err.contains("clip_usd"), "got: {err}");

    // $1 at a 9c entry is 11 shares — over the 5-share floor, so this passes
    // on the share check and only the price band could reject it.
    let ok = format!("{MINIMAL}clip_usd = 1.0\n");
    let cfg: TickburstToml = toml::from_str(&ok).unwrap();
    assert!(onecentbot::config::validate_strategy(&cfg.strategy).is_ok());
}

/// A spend rail below ONE fire's commit vetoes EVERY fire — it does not BOUND
/// the strategy, it turns it off, silently, one `RailTrip` row at a time
/// (design §5.0). ALL THREE rails are checked, not just the per-order one:
/// `concurrent_exposure_cap_usd` vetoes at `rails.rs`'s concurrent check and
/// `daily_spend_cap_usd` at its ledger, with exactly the same effect.
#[test]
fn any_spend_rail_below_one_fires_commit_is_rejected() {
    // Since the 2026-08-10 sizing fix the commit IS the clip, flat: the order
    // is `clip_usd / limit_price` shares, so `shares * limit_price = clip_usd`
    // at every ask in the band.
    let commit = 10.0;
    for rail in ["per_order_cap_usd", "concurrent_exposure_cap_usd", "daily_spend_cap_usd"] {
        // `daily` must also stay above the loss kill, so lower that too — this
        // test is about the commit relation, not the kill relation.
        let bad = format!("{MINIMAL}{rail} = {}\nrealized_loss_kill_usd = 5.0\n", commit - 0.01);
        let cfg: TickburstToml = toml::from_str(&bad).expect("parses; validation rejects");
        let err =
            onecentbot::config::validate_strategy(&cfg.strategy).unwrap_err().to_string();
        assert!(err.contains(rail), "{rail} just under the commit: got {err}");

        // Exactly the commit must pass — the bound is inclusive, so this also
        // proves the check isn't just rejecting everything.
        let ok = format!("{MINIMAL}{rail} = {commit}\nrealized_loss_kill_usd = 5.0\n");
        let cfg: TickburstToml = toml::from_str(&ok).unwrap();
        assert!(
            onecentbot::config::validate_strategy(&cfg.strategy).is_ok(),
            "{rail} = {commit} (exactly the commit) must be accepted"
        );
    }
}

/// The commit is INDEPENDENT of the entry band — that is the whole point of
/// sizing off the limit. Before 2026-08-10 the commit was `clip/ask*limit`, so
/// widening the band's cheap edge silently raised every fire's exposure (a
/// $10 clip committed ~2x at a 0.9c ask). Now a rail sized at the clip
/// holds however far down the band reaches.
#[test]
fn widening_the_band_does_not_raise_a_fires_commit() {
    for min in ["0.005", "0.009", "0.011"] {
        let toml_src = format!(
            "{MINIMAL}ask_price_min = {min}\nask_price_max = 0.011\nper_order_cap_usd = 10.0\n\
             concurrent_exposure_cap_usd = 10.0\ndaily_spend_cap_usd = 10.0\n\
             realized_loss_kill_usd = 5.0\n"
        );
        let cfg: TickburstToml = toml::from_str(&toml_src).unwrap();
        assert!(
            onecentbot::config::validate_strategy(&cfg.strategy).is_ok(),
            "ask_price_min = {min}: a $10 rail must still admit a $10 clip"
        );
    }
}

/// The operator-facing live-path config must parse (deny_unknown_fields would
/// reject a stale key) AND pass strategy validation. Nothing else exercises
/// the shipped example end-to-end.
#[test]
fn the_shipped_example_config_parses_and_validates() {
    let raw = include_str!("../config/tickburst.example.toml");
    let cfg: TickburstToml = toml::from_str(raw).expect("example config must parse");
    onecentbot::config::validate_strategy(&cfg.strategy)
        .expect("example config must pass strategy validation");
}

/// The values that make the shipped config the GRADED gate rather than a near
/// neighbour. Each was measured, not chosen, and each is a plausible thing for
/// a later edit to "clean up" — so pin them where a change has to be
/// deliberate.
#[test]
fn the_shipped_example_config_is_the_frozen_gate() {
    let raw = include_str!("../config/tickburst.example.toml");
    let cfg: TickburstToml = toml::from_str(raw).unwrap();
    let s = &cfg.strategy;

    assert_eq!(s.ask_price_min, 0.009, "the operator band's cheap edge");
    assert_eq!(s.ask_price_max, 0.011, "and its dear edge — 2c stays excluded");
    assert_eq!(s.limit_price, 0.02);
    assert_eq!(s.clip_usd, 10.0);
    assert!(s.tick.enabled, "the tick path is the ONLY fire path (spec §6)");
    assert_eq!(s.tick.v_btc, 0.5);
    assert_eq!(s.tick.lean_min, 0.6);
    assert_eq!(s.tick.window_ms, 1_000);
    assert_eq!(s.tick.max_book_age_ms, 15_000);
    // Boundary-close carries live defect D2 and gates nothing under this spec;
    // the key must stay ABSENT, not merely set to something harmless.
    assert_eq!(s.boundary_close_delay_ms, None);
}

// --- the trigger ([strategy.tick]) ---

#[test]
fn the_tick_path_defaults_to_the_frozen_gate_and_is_on() {
    let cfg: TickburstToml = toml::from_str(MINIMAL).expect("minimal toml must parse");
    assert!(cfg.strategy.tick.enabled);
    assert_eq!(cfg.strategy.tick.v_btc, 0.5);
    assert_eq!(cfg.strategy.tick.lean_min, 0.6);
    assert_eq!(cfg.strategy.tick.window_ms, 1_000);
    assert_eq!(cfg.strategy.tick.max_book_age_ms, 15_000);
}

#[test]
fn an_unknown_tick_key_is_a_hard_error() {
    let bad = format!("{MINIMAL}\n[strategy.tick]\nflow_tbb_min = 0.6\n");
    let err = toml::from_str::<TickburstToml>(&bad).unwrap_err().to_string();
    assert!(err.contains("flow_tbb_min"), "got: {err}");
}

/// A non-positive volume floor fires on every print inside the band — the
/// blanket quiet-channel entry the census measured at −$563 to −$2,954.
#[test]
fn a_non_positive_volume_floor_is_rejected() {
    let bad = format!("{MINIMAL}\n[strategy.tick]\nv_btc = 0.0\n");
    let cfg: TickburstToml = toml::from_str(&bad).expect("parses; validation rejects");
    let err = onecentbot::config::validate_strategy(&cfg.strategy).unwrap_err().to_string();
    assert!(err.contains("v_btc"), "got: {err}");
}

/// At or below 0.5 the "lean" is not a direction at all — both sides of one
/// print could pass at once, which would also let the real gate and its own
/// placebo mirror fire on the same tick. 0.5 itself is the boundary and is
/// REJECTED, so the bound and the error message agree.
#[test]
fn a_lean_floor_at_or_below_half_is_rejected() {
    for v in ["0.4", "0.49", "0.5", "1.5"] {
        let bad = format!("{MINIMAL}\n[strategy.tick]\nlean_min = {v}\n");
        let cfg: TickburstToml = toml::from_str(&bad).expect("parses; validation rejects");
        let err =
            onecentbot::config::validate_strategy(&cfg.strategy).unwrap_err().to_string();
        assert!(err.contains("lean_min"), "lean_min={v}: got {err}");
    }
}

#[test]
fn a_degenerate_tick_window_or_book_age_is_rejected() {
    for (key, val) in [("window_ms", "0"), ("window_ms", "60000"), ("max_book_age_ms", "0")] {
        let bad = format!("{MINIMAL}\n[strategy.tick]\n{key} = {val}\n");
        let cfg: TickburstToml = toml::from_str(&bad).expect("parses; validation rejects");
        let err =
            onecentbot::config::validate_strategy(&cfg.strategy).unwrap_err().to_string();
        assert!(err.contains(key), "{key} = {val}: got {err}");
    }
}

/// A DISABLED tick path must not be validated at all — an operator turning it
/// off should not then have to satisfy its parameter constraints.
#[test]
fn a_disabled_tick_path_skips_its_own_validation() {
    let raw = format!("{MINIMAL}\n[strategy.tick]\nenabled = false\nv_btc = 0.0\n");
    let cfg: TickburstToml = toml::from_str(&raw).expect("parses");
    onecentbot::config::validate_strategy(&cfg.strategy)
        .expect("a disabled tick path imposes no constraints");
}

// --- boundary-close reconstruction (telemetry-only under this spec) ---

#[test]
fn boundary_close_is_off_by_default() {
    let cfg: TickburstToml = toml::from_str(MINIMAL).expect("minimal toml must parse");
    assert_eq!(
        cfg.strategy.boundary_close_delay_ms, None,
        "reconstruction must be OPT-IN: absent config keeps the @kline_1s path byte-for-byte, \
         and the live TOML on the box has no such key"
    );
}

#[test]
fn a_configured_boundary_delay_parses_and_validates() {
    let cfg: TickburstToml =
        toml::from_str(&format!("{MINIMAL}boundary_close_delay_ms = 100\n")).expect("parses");
    assert_eq!(cfg.strategy.boundary_close_delay_ms, Some(100));
    onecentbot::config::validate_strategy(&cfg.strategy).expect("100 ms is valid");
}

/// Past the kline's own arrival (p50 152 / p90 176 ms, measured) the
/// reconstruction is strictly slower than simply waiting for Binance, so a
/// delay that large is always an operator mistake.
#[test]
fn a_boundary_delay_slower_than_the_kline_itself_is_rejected() {
    let cfg: TickburstToml =
        toml::from_str(&format!("{MINIMAL}boundary_close_delay_ms = 250\n")).expect("parses");
    let err = onecentbot::config::validate_strategy(&cfg.strategy).unwrap_err().to_string();
    assert!(err.contains("boundary_close_delay_ms"), "got: {err}");
}
