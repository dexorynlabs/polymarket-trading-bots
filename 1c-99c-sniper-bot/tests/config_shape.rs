//! Config shape: EVERYTHING comes from the environment — the strategy and
//! spend rails as `SNIPER_*`, the live account as `POLY_*` — and every typo
//! fails loudly.
//!
//! These tests never touch the process environment — they pass an explicit
//! `EnvVars`, so they are safe to run in parallel.

use sniperbot::config::env::EnvVars;
use sniperbot::config::{validate_live, validate_strategy, BotConfig};
use sniperbot::strategy::mode::SniperMode;
use sniperbot::strategy::tick::TriggerMode;

fn env(pairs: &[(&str, &str)]) -> EnvVars {
    EnvVars::from_pairs(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())))
}

/// Resolves + validates, the way `main` does.
fn load(pairs: &[(&str, &str)]) -> anyhow::Result<BotConfig> {
    let cfg = BotConfig::from_env(&env(pairs))?;
    validate_strategy(&cfg.strategy)?;
    Ok(cfg)
}

fn err(pairs: &[(&str, &str)]) -> String {
    format!("{:#}", load(pairs).expect_err("must be rejected"))
}

const PK: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const ADDR: &str = "0x365f0ca36ae1f641e02fe3b7743673da42a13a70";
const LIVE: &[(&str, &str)] = &[
    ("POLY_PRIVATE_KEY", PK),
    ("POLY_PROXY_WALLET", ADDR),
    ("POLY_API_KEY", "00000000-0000-0000-0000-000000000000"),
    ("POLY_API_SECRET", "c2VjcmV0"),
    ("POLY_API_PASSPHRASE", "pass"),
];

fn with(base: &[(&'static str, &'static str)], extra: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
    base.iter().chain(extra).copied().collect()
}

// ── the mode ────────────────────────────────────────────────────────────────

#[test]
fn sniper_mode_alone_is_a_complete_dry_run_config() {
    for m in ["1c", "99c"] {
        let cfg = load(&[("SNIPER_MODE", m)]).unwrap_or_else(|e| panic!("{m}: {e:#}"));
        assert!(cfg.live.is_none(), "no POLY_* set = no live account");
        assert_eq!(cfg.strategy.daily_spend_cap_usd, 2_500.0, "the rails have defaults");
    }
}

#[test]
fn sniper_mode_is_required() {
    assert!(err(&[]).contains("SNIPER_MODE is not set"));
}

#[test]
fn the_1c_preset_is_the_census_entry() {
    let s = load(&[("SNIPER_MODE", "1c")]).unwrap().strategy;
    assert_eq!(s.mode, SniperMode::OneCent);
    assert_eq!((s.ask_price_min, s.ask_price_max, s.limit_price, s.clip_usd), (0.009, 0.011, 0.02, 10.0));
    assert_eq!(s.realized_loss_kill_usd, 400.0);
    assert_eq!(s.tick.trigger, TriggerMode::Burst);
    assert_eq!((s.tick.v_btc, s.tick.lean_min, s.tick.window_ms), (0.5, 0.6, 1_000));
    assert_eq!(s.tick.max_book_age_ms, 15_000);
    assert!(s.tick.enabled);
}

#[test]
fn the_99c_preset_is_the_leader_entry() {
    let s = load(&[("SNIPER_MODE", "99c")]).unwrap().strategy;
    assert_eq!(s.mode, SniperMode::NinetyNineCent);
    assert_eq!((s.ask_price_min, s.ask_price_max, s.limit_price, s.clip_usd), (0.97, 0.99, 0.99, 20.0));
    assert_eq!(s.realized_loss_kill_usd, 100.0);
    assert_eq!(s.tick.trigger, TriggerMode::Burst);
    assert_eq!(s.tick.max_book_age_ms, 2_000);
}

// ── the rails ───────────────────────────────────────────────────────────────

#[test]
fn the_rails_come_from_the_env() {
    let s = load(&[("SNIPER_MODE", "99c"), ("SNIPER_DAILY_CAP_USD", "900"), ("SNIPER_PER_ORDER_CAP_USD", "30")])
        .unwrap()
        .strategy;
    assert_eq!((s.daily_spend_cap_usd, s.per_order_cap_usd), (900.0, 30.0));
}

#[test]
fn any_spend_rail_below_one_fires_commit_is_rejected() {
    for key in ["SNIPER_PER_ORDER_CAP_USD", "SNIPER_EXPOSURE_CAP_USD", "SNIPER_DAILY_CAP_USD"] {
        let e = err(&[("SNIPER_MODE", "99c"), (key, "15")]);
        assert!(e.contains(key), "{key}: {e}");
        // $15 clears a $10 1c clip; the loss kill must stay under the cap.
        load(&[("SNIPER_MODE", "1c"), (key, "15"), ("SNIPER_LOSS_KILL_USD", "12")])
            .unwrap_or_else(|e| panic!("{key} 15 >= a $10 1c clip: {e:#}"));
    }
}

#[test]
fn boundary_close_is_off_by_default_and_bounded() {
    assert_eq!(load(&[("SNIPER_MODE", "1c")]).unwrap().strategy.boundary_close_delay_ms, None);
    let s = load(&[("SNIPER_MODE", "1c"), ("SNIPER_BOUNDARY_CLOSE_DELAY_MS", "50")]).unwrap().strategy;
    assert_eq!(s.boundary_close_delay_ms, Some(50));
    assert!(err(&[("SNIPER_MODE", "1c"), ("SNIPER_BOUNDARY_CLOSE_DELAY_MS", "500")])
        .contains("SNIPER_BOUNDARY_CLOSE_DELAY_MS"));
}

// ── the live account ───────────────────────────────────────────────────────

#[test]
fn the_live_account_comes_from_poly_variables() {
    let cfg = load(&with(LIVE, &[("SNIPER_MODE", "1c")])).unwrap();
    let live = cfg.live.expect("POLY_* set = live account on");
    assert_eq!((live.wallet.private_key.as_str(), live.wallet.proxy_wallet.as_str()), (PK, ADDR));
    assert_eq!(live.clob.http_base, "https://clob.polymarket.com");
    validate_live(&live).unwrap();
}

#[test]
fn a_partial_live_account_is_an_error_not_a_silent_dry_run() {
    let e = err(&[("SNIPER_MODE", "1c"), ("POLY_PRIVATE_KEY", PK)]);
    assert!(e.contains("POLY_API_KEY") && e.contains("POLY_PROXY_WALLET"), "{e}");
}

#[test]
fn a_malformed_live_key_fails_validation_by_its_env_name() {
    let cfg = load(&with(LIVE, &[("SNIPER_MODE", "1c"), ("POLY_PROXY_WALLET", "0xnope")]));
    // POLY_PROXY_WALLET appears twice; the later pair wins in the map.
    let live = cfg.unwrap().live.unwrap();
    let e = validate_live(&live).unwrap_err().to_string();
    assert!(e.contains("POLY_PROXY_WALLET"), "{e}");
}

#[test]
fn live_safety_overrides_apply() {
    let live = load(&with(LIVE, &[("SNIPER_MODE", "99c"), ("SNIPER_LIVE_BUY_CAP_USD", "60"), ("SNIPER_LIVE_DAILY_LOSS_USD", "25")]))
        .unwrap()
        .live
        .unwrap();
    assert_eq!((live.safety.cap_usd, live.safety.kill_switch_loss_usd), (60.0, 25.0));
}

// ── the env overrides ──────────────────────────────────────────────────────

#[test]
fn overrides_replace_single_preset_values() {
    let s = load(&[("SNIPER_MODE", "99c"), ("SNIPER_CLIP_USD", "8"), ("SNIPER_TRIGGER", "calm"), ("SNIPER_V_BTC", "1.0")])
        .unwrap()
        .strategy;
    assert_eq!(s.clip_usd, 8.0);
    assert_eq!(s.tick.trigger, TriggerMode::Calm);
    assert_eq!(s.tick.v_btc, 1.0);
    assert_eq!(s.limit_price, 0.99, "untouched values keep the preset");
}

#[test]
fn a_misspelled_variable_is_a_hard_error() {
    assert!(err(&[("SNIPER_MODE", "1c"), ("SNIPER_CLIPUSD", "5")]).contains("unknown environment variable"));
    assert!(err(&[("SNIPER_MODE", "1c"), ("POLY_APIKEY", "x")]).contains("unknown environment variable"));
}

#[test]
fn calm_is_99c_only() {
    load(&[("SNIPER_MODE", "99c"), ("SNIPER_TRIGGER", "calm")]).unwrap();
    assert!(err(&[("SNIPER_MODE", "1c"), ("SNIPER_TRIGGER", "calm")]).contains("not allowed in SNIPER_MODE=1c"));
}

// ── per-mode validation ────────────────────────────────────────────────────

#[test]
fn each_mode_rejects_the_other_modes_band() {
    let e = err(&[("SNIPER_MODE", "1c"), ("SNIPER_ASK_MIN", "0.97"), ("SNIPER_ASK_MAX", "0.99"), ("SNIPER_LIMIT", "0.99")]);
    assert!(e.contains("SNIPER_MODE=99c"), "{e}");
    let e = err(&[("SNIPER_MODE", "99c"), ("SNIPER_ASK_MIN", "0.009"), ("SNIPER_ASK_MAX", "0.011"), ("SNIPER_LIMIT", "0.02")]);
    assert!(e.contains("SNIPER_MODE=1c"), "{e}");
}

#[test]
fn an_inverted_band_or_a_limit_under_the_band_is_rejected() {
    assert!(err(&[("SNIPER_MODE", "99c"), ("SNIPER_ASK_MIN", "0.99"), ("SNIPER_ASK_MAX", "0.97")]).contains("inverted"));
    assert!(err(&[("SNIPER_MODE", "99c"), ("SNIPER_LIMIT", "0.98")]).contains("SNIPER_LIMIT"));
    assert!(err(&[("SNIPER_MODE", "1c"), ("SNIPER_LIMIT", "0.010")]).contains("SNIPER_LIMIT"));
}

#[test]
fn the_1c_limit_is_bounded_at_4x_the_band_floor() {
    load(&[("SNIPER_MODE", "1c"), ("SNIPER_LIMIT", "0.036")]).unwrap();
    assert!(err(&[("SNIPER_MODE", "1c"), ("SNIPER_LIMIT", "0.04")]).contains("4x"));
}

#[test]
fn the_99c_limit_is_bounded_by_a_dollar_and_by_the_chase() {
    assert!(err(&[("SNIPER_MODE", "99c"), ("SNIPER_ASK_MAX", "0.999"), ("SNIPER_LIMIT", "1.0")]).contains("<= 0.999"));
    let e = err(&[("SNIPER_MODE", "99c"), ("SNIPER_ASK_MIN", "0.95"), ("SNIPER_LIMIT", "0.99")]);
    assert!(e.contains("within 0.03"), "{e}");
}

#[test]
fn a_clip_that_cannot_clear_the_venue_minimums_is_rejected() {
    // $4 at a 99c limit is 4.04 shares — under the venue's 5-share minimum.
    assert!(err(&[("SNIPER_MODE", "99c"), ("SNIPER_CLIP_USD", "4")]).contains("5-share"));
    // $0.50 cannot clear the venue's $1 minimum in either mode.
    for m in ["1c", "99c"] {
        assert!(err(&[("SNIPER_MODE", m), ("SNIPER_CLIP_USD", "0.5")]).contains(">= 1.0"));
    }
    load(&[("SNIPER_MODE", "99c"), ("SNIPER_CLIP_USD", "5")]).unwrap();
}

#[test]
fn the_loss_kill_must_bind_before_the_spend_cap() {
    let e = err(&[("SNIPER_MODE", "1c"), ("SNIPER_LOSS_KILL_USD", "2500")]);
    assert!(e.contains("inert"), "{e}");
}

#[test]
fn degenerate_trigger_values_are_rejected() {
    for (k, v, needle) in [
        ("SNIPER_V_BTC", "0", "SNIPER_V_BTC"),
        ("SNIPER_LEAN_MIN", "0.5", "SNIPER_LEAN_MIN"),
        ("SNIPER_LEAN_MIN", "1.1", "SNIPER_LEAN_MIN"),
        ("SNIPER_WINDOW_MS", "0", "SNIPER_WINDOW_MS"),
        ("SNIPER_WINDOW_MS", "60000", "SNIPER_WINDOW_MS"),
        ("SNIPER_MAX_BOOK_AGE_MS", "0", "SNIPER_MAX_BOOK_AGE_MS"),
        ("SNIPER_ADVERSE_MAX_BTC", "0", "SNIPER_ADVERSE_MAX_BTC"),
    ] {
        let e = err(&[("SNIPER_MODE", "99c"), (k, v)]);
        assert!(e.contains(needle), "{k}={v}: {e}");
    }
}

#[test]
fn a_disabled_tick_path_skips_its_own_validation() {
    let s = load(&[("SNIPER_MODE", "1c"), ("SNIPER_TICK_ENABLED", "false"), ("SNIPER_V_BTC", "0")])
        .unwrap()
        .strategy;
    assert!(!s.tick.enabled);
}

// ── .env.example ───────────────────────────────────────────────────────────

#[test]
fn every_env_key_is_documented_in_the_env_example() {
    let example = include_str!("../.env.example");
    for (k, _) in sniperbot::config::env::all_keys() {
        assert!(example.contains(&format!("{k}=")), ".env.example is missing {k}");
    }
}

#[test]
fn the_env_example_itself_is_a_valid_dry_run() {
    let kv = sniperbot::config::env::parse_dotenv(include_str!("../.env.example")).unwrap();
    let pairs: Vec<(&str, &str)> = kv.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let cfg = load(&pairs).expect("the shipped .env.example must load as-is");
    assert!(cfg.live.is_none(), "the example ships with every POLY_* empty");
}
