//! All configuration comes from the environment (or a `.env` file). There is
//! no config file.
//!
//! - `SNIPER_MODE` is required and picks the strategy preset
//!   (`strategy::mode`). Every other strategy `SNIPER_*` variable is optional
//!   and overrides one preset value.
//! - The spend rails (`SNIPER_DAILY_CAP_USD`, ...) are optional, with defaults.
//! - The live account (`POLY_*`: wallet key, CLOB API key, endpoints) is only
//!   needed by `--live` and `--preflight`. Setting ANY `POLY_*` variable turns
//!   it on, and then the five secrets are required.
//!
//! An empty value counts as unset, so `.env.example` can list every key.
//! A `.env` file in the working directory is loaded at startup
//! ([`load_dotenv`]); variables already set in the process environment win.
//!
//! Strict: an unknown `SNIPER_*` / `POLY_*` name or an unparseable value is a
//! hard error, never a silent default.

use std::collections::BTreeMap;

use crate::config::live::{ApiCfg, ChainCfg, ClobCfg, LiveConfig, SafetyCfg, WalletCfg};
use crate::config::{RailsSection, StrategySection};
use crate::strategy::mode::SniperMode;
use crate::strategy::tick::TriggerMode;

/// The variable-name prefixes this bot owns.
pub const PREFIXES: &[&str] = &["SNIPER_", "POLY_"];

/// The strategy: `SNIPER_MODE` + optional preset overrides.
pub const STRATEGY_KEYS: &[(&str, &str)] = &[
    ("SNIPER_MODE", "REQUIRED. 1c = buy the trailer at ~1c, 99c = buy the leader at ~99c"),
    ("SNIPER_TRIGGER", "burst (default) or calm (99c only)"),
    ("SNIPER_CLIP_USD", "dollars committed per fire (preset: 1c $10, 99c $20)"),
    ("SNIPER_ASK_MIN", "lowest standing ask to enter at (preset: 1c 0.009, 99c 0.97)"),
    ("SNIPER_ASK_MAX", "highest standing ask to enter at (preset: 1c 0.011, 99c 0.99)"),
    ("SNIPER_LIMIT", "FAK limit price (preset: 1c 0.02, 99c 0.99)"),
    ("SNIPER_LOSS_KILL_USD", "halt after this much CLOB-settled loss (preset: 1c $400, 99c $100)"),
    ("SNIPER_TICK_ENABLED", "false = record books and telemetry but never fire (default true)"),
    ("SNIPER_V_BTC", "burst volume floor, BTC in the trailing window (preset 0.5)"),
    ("SNIPER_LEAN_MIN", "share of burst volume that must point one way, (0.5, 1] (preset 0.6)"),
    ("SNIPER_ADVERSE_MAX_BTC", "calm only: veto while adverse volume >= this, BTC (preset 0.3)"),
    ("SNIPER_WINDOW_MS", "trailing window width, ms (preset 1000)"),
    ("SNIPER_MAX_BOOK_AGE_MS", "ignore a held book older than this, ms (preset: 1c 15000, 99c 2000)"),
];

/// The spend rails. All optional.
pub const RAILS_KEYS: &[(&str, &str)] = &[
    ("SNIPER_DAILY_CAP_USD", "rolling-24h committed spend cap (default 2500)"),
    ("SNIPER_EXPOSURE_CAP_USD", "max open (unsettled) exposure (default 150)"),
    ("SNIPER_PER_ORDER_CAP_USD", "max commit of a single fire (default 100)"),
    ("SNIPER_FIRE_RATE_PER_MIN", "fault latch on fires per minute (default 20)"),
    ("SNIPER_BOOK_STALE_MS", "disarm when the book WS is silent this long, ms (default 5000)"),
    ("SNIPER_BOUNDARY_CLOSE_DELAY_MS", "telemetry only; leave empty (off)"),
];

/// The live account. Only `--live` / `--preflight` use it.
pub const LIVE_KEYS: &[(&str, &str)] = &[
    ("POLY_PRIVATE_KEY", "live: 0x + 64 hex, the EOA key that controls the proxy wallet"),
    ("POLY_PROXY_WALLET", "live: 0x address of the proxy wallet that holds pUSD"),
    ("POLY_SIGNATURE_TYPE", "live: 1 = Magic.link proxy (default), 2 = Safe, 3 = 2026-08+ accounts"),
    ("POLY_API_KEY", "live: CLOB API key (UUID)"),
    ("POLY_API_SECRET", "live: CLOB API secret (base64)"),
    ("POLY_API_PASSPHRASE", "live: CLOB API passphrase"),
    ("POLY_RPC_URL", "live: Polygon HTTP RPC (default https://polygon-rpc.com)"),
    ("POLY_RPC_WS_URL", "live: optional Polygon WS RPC, checked by --preflight"),
    ("POLY_CHAIN_ID", "live: default 137"),
    ("POLY_CLOB_HTTP", "live: default https://clob.polymarket.com"),
    ("POLY_CLOB_WS", "live: default wss://ws-subscriptions-clob.polymarket.com"),
    ("SNIPER_LIVE_BUY_CAP_USD", "live: rolling-24h BUY cap checked by the launch gate (default 150)"),
    ("SNIPER_LIVE_DAILY_LOSS_USD", "live: halt when 24h ledger P&L drops below -this (default 100)"),
    ("SNIPER_LIVE_LOSS_PER_HOUR_USD", "live: velocity halt, loss per hour (default 40)"),
    ("SNIPER_LIVE_LOSS_HOURS", "live: ...sustained for this many hours (default 2)"),
];

/// The five live secrets: no default exists for any of them.
const LIVE_REQUIRED: &[&str] =
    &["POLY_PRIVATE_KEY", "POLY_PROXY_WALLET", "POLY_API_KEY", "POLY_API_SECRET", "POLY_API_PASSPHRASE"];

/// Every recognised variable, in `.env.example` order.
pub fn all_keys() -> impl Iterator<Item = &'static (&'static str, &'static str)> {
    STRATEGY_KEYS.iter().chain(RAILS_KEYS).chain(LIVE_KEYS)
}

/// The `SNIPER_*` / `POLY_*` variables in effect, with empty values dropped.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EnvVars(BTreeMap<String, String>);

impl EnvVars {
    /// Reads the process environment.
    pub fn from_process() -> Self {
        Self::from_pairs(std::env::vars())
    }

    /// Keeps only this bot's names with a non-empty value.
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut m = BTreeMap::new();
        for (k, v) in pairs {
            let (k, v) = (k.into(), v.into());
            if PREFIXES.iter().any(|p| k.starts_with(p)) && !v.trim().is_empty() {
                m.insert(k, v.trim().to_string());
            }
        }
        Self(m)
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Fails on any name that is not in [`all_keys`] — a typo must never
    /// silently fall back to a default.
    pub fn reject_unknown(&self) -> anyhow::Result<()> {
        let known: Vec<&str> = all_keys().map(|(k, _)| *k).collect();
        for k in self.0.keys() {
            anyhow::ensure!(
                known.contains(&k.as_str()),
                "unknown environment variable {k} — see .env.example for the recognised \
                 SNIPER_* and POLY_* names"
            );
        }
        Ok(())
    }
}

fn parse_num<T: std::str::FromStr>(env: &EnvVars, key: &str) -> anyhow::Result<Option<T>> {
    match env.get(key) {
        None => Ok(None),
        Some(raw) => raw
            .parse::<T>()
            .map(Some)
            .map_err(|_| anyhow::anyhow!("{key}={raw:?} is not a valid number")),
    }
}

fn parse_bool(env: &EnvVars, key: &str) -> anyhow::Result<Option<bool>> {
    match env.get(key) {
        None => Ok(None),
        Some(raw) => match raw.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(Some(true)),
            "0" | "false" | "no" | "off" => Ok(Some(false)),
            _ => anyhow::bail!("{key}={raw:?} must be true or false"),
        },
    }
}

/// Overwrites `slot` when `key` is set.
fn set<T: std::str::FromStr>(env: &EnvVars, key: &str, slot: &mut T) -> anyhow::Result<()> {
    if let Some(v) = parse_num(env, key)? {
        *slot = v;
    }
    Ok(())
}

/// The spend rails: defaults, then any `SNIPER_*` rail overrides.
pub fn resolve_rails(env: &EnvVars) -> anyhow::Result<RailsSection> {
    let mut r = RailsSection::default();
    set(env, "SNIPER_DAILY_CAP_USD", &mut r.daily_spend_cap_usd)?;
    set(env, "SNIPER_EXPOSURE_CAP_USD", &mut r.concurrent_exposure_cap_usd)?;
    set(env, "SNIPER_PER_ORDER_CAP_USD", &mut r.per_order_cap_usd)?;
    set(env, "SNIPER_FIRE_RATE_PER_MIN", &mut r.self_fire_rate_per_min)?;
    set(env, "SNIPER_BOOK_STALE_MS", &mut r.book_stale_disarm_ms)?;
    if let Some(v) = parse_num(env, "SNIPER_BOUNDARY_CLOSE_DELAY_MS")? {
        r.boundary_close_delay_ms = Some(v);
    }
    Ok(r)
}

/// Builds the strategy: `SNIPER_MODE`'s preset, then every `SNIPER_*`
/// override, then the rails. Validation of the RESULT is
/// `config::validate_strategy`'s job; this only parses.
pub fn resolve_strategy(env: &EnvVars, rails: &RailsSection) -> anyhow::Result<StrategySection> {
    env.reject_unknown()?;

    let raw_mode = env.get("SNIPER_MODE").ok_or_else(|| {
        anyhow::anyhow!(
            "SNIPER_MODE is not set. Set SNIPER_MODE=1c (buy the trailer at ~1c) or \
             SNIPER_MODE=99c (buy the leader at ~99c), in the environment or in .env"
        )
    })?;
    let mode = SniperMode::parse(raw_mode)
        .ok_or_else(|| anyhow::anyhow!("SNIPER_MODE={raw_mode:?} must be 1c or 99c"))?;

    let mut s = StrategySection::from_preset(mode, rails);

    if let Some(raw) = env.get("SNIPER_TRIGGER") {
        s.tick.trigger = TriggerMode::parse(raw)
            .ok_or_else(|| anyhow::anyhow!("SNIPER_TRIGGER={raw:?} must be burst or calm"))?;
    }
    set(env, "SNIPER_CLIP_USD", &mut s.clip_usd)?;
    set(env, "SNIPER_ASK_MIN", &mut s.ask_price_min)?;
    set(env, "SNIPER_ASK_MAX", &mut s.ask_price_max)?;
    set(env, "SNIPER_LIMIT", &mut s.limit_price)?;
    set(env, "SNIPER_LOSS_KILL_USD", &mut s.realized_loss_kill_usd)?;
    if let Some(v) = parse_bool(env, "SNIPER_TICK_ENABLED")? {
        s.tick.enabled = v;
    }
    set(env, "SNIPER_V_BTC", &mut s.tick.v_btc)?;
    set(env, "SNIPER_LEAN_MIN", &mut s.tick.lean_min)?;
    set(env, "SNIPER_ADVERSE_MAX_BTC", &mut s.tick.adverse_max_btc)?;
    set(env, "SNIPER_WINDOW_MS", &mut s.tick.window_ms)?;
    set(env, "SNIPER_MAX_BOOK_AGE_MS", &mut s.tick.max_book_age_ms)?;
    Ok(s)
}

/// The live account, or `None` when no `POLY_*` variable is set (dry run).
/// Once any `POLY_*` is set, all five secrets are required — a half-filled
/// account is an error naming what is missing, never a silent dry run.
/// Shape checks (address/key format, ...) are `validate_live`'s job.
pub fn resolve_live(env: &EnvVars) -> anyhow::Result<Option<LiveConfig>> {
    if !env.0.keys().any(|k| k.starts_with("POLY_")) {
        return Ok(None);
    }
    let missing: Vec<&str> = LIVE_REQUIRED.iter().copied().filter(|k| env.get(k).is_none()).collect();
    anyhow::ensure!(
        missing.is_empty(),
        "a POLY_* variable is set, so the live account is on, but these are empty: {} \
         (fill them in, or clear every POLY_* variable for a dry run)",
        missing.join(", ")
    );
    let req = |k: &str| env.get(k).unwrap_or_default().to_string();
    let or = |k: &str, d: &str| env.get(k).unwrap_or(d).to_string();

    let signature_type: u8 = parse_num(env, "POLY_SIGNATURE_TYPE")?.unwrap_or(1);
    let chain_id: u64 = parse_num(env, "POLY_CHAIN_ID")?.unwrap_or(137);
    let mut safety = SafetyCfg {
        cap_usd: 150.0,
        kill_switch_loss_usd: 100.0,
        velocity_loss_per_hour: 40.0,
        velocity_consecutive_hrs: 2,
        skip_alternate_cids: false,
        cutover_at_unix: None,
        max_market_age_at_spawn_secs: Some(60),
        max_per_slot_total_usd: None,
        exposure_cap_per_side_shares: None,
    };
    set(env, "SNIPER_LIVE_BUY_CAP_USD", &mut safety.cap_usd)?;
    set(env, "SNIPER_LIVE_DAILY_LOSS_USD", &mut safety.kill_switch_loss_usd)?;
    set(env, "SNIPER_LIVE_LOSS_PER_HOUR_USD", &mut safety.velocity_loss_per_hour)?;
    set(env, "SNIPER_LIVE_LOSS_HOURS", &mut safety.velocity_consecutive_hrs)?;

    Ok(Some(LiveConfig {
        wallet: WalletCfg {
            proxy_wallet: req("POLY_PROXY_WALLET"),
            private_key: req("POLY_PRIVATE_KEY"),
            signature_type,
            safe_chain_id: chain_id,
        },
        chain: ChainCfg {
            rpc_url: or("POLY_RPC_URL", "https://polygon-rpc.com"),
            chain_id,
            rpc_ws_url: env.get("POLY_RPC_WS_URL").map(str::to_string),
        },
        clob: ClobCfg {
            http_base: or("POLY_CLOB_HTTP", "https://clob.polymarket.com"),
            ws_url: or("POLY_CLOB_WS", "wss://ws-subscriptions-clob.polymarket.com"),
            signature_type,
        },
        api: ApiCfg {
            key: req("POLY_API_KEY"),
            secret: req("POLY_API_SECRET"),
            passphrase: req("POLY_API_PASSPHRASE"),
        },
        safety,
    }))
}

/// Parses `.env` text: `KEY=VALUE` lines, `#` comments, blank lines, an
/// optional `export ` prefix, and optional matching single or double quotes
/// around the value. Malformed lines are an error naming the line number.
pub fn parse_dotenv(text: &str) -> anyhow::Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let (k, v) = line
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!(".env line {}: expected KEY=VALUE", i + 1))?;
        let k = k.trim();
        anyhow::ensure!(
            !k.is_empty() && k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            ".env line {}: {k:?} is not a valid variable name",
            i + 1
        );
        let mut v = v.trim();
        let quoted = v.len() >= 2
            && ((v.starts_with('"') && v.ends_with('"')) || (v.starts_with('\'') && v.ends_with('\'')));
        if quoted {
            v = &v[1..v.len() - 1];
        } else if v.starts_with('#') {
            // `KEY=   # comment` is an empty value, not the comment.
            v = "";
        } else if let Some(idx) = v.find(" #") {
            // An unquoted trailing comment.
            v = v[..idx].trim_end();
        }
        out.push((k.to_string(), v.to_string()));
    }
    Ok(out)
}

/// Loads `path` (if it exists) into the process environment, skipping any
/// variable that is already set. Returns how many variables it set.
///
/// MUST be called before any other thread exists (`main` does it before the
/// tokio runtime starts): mutating the environment is not thread-safe.
pub fn load_dotenv(path: &std::path::Path) -> anyhow::Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let text = std::fs::read_to_string(path)?;
    let mut n = 0;
    for (k, v) in parse_dotenv(&text)? {
        if std::env::var_os(&k).is_none() {
            std::env::set_var(&k, &v);
            n += 1;
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> EnvVars {
        EnvVars::from_pairs(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())))
    }

    #[test]
    fn mode_is_required() {
        let err = resolve_strategy(&env(&[]), &RailsSection::default()).unwrap_err().to_string();
        assert!(err.contains("SNIPER_MODE is not set"), "{err}");
    }

    #[test]
    fn each_mode_resolves_to_its_preset() {
        let one = resolve_strategy(&env(&[("SNIPER_MODE", "1c")]), &RailsSection::default()).unwrap();
        assert_eq!(one.mode, SniperMode::OneCent);
        assert_eq!((one.ask_price_min, one.ask_price_max, one.limit_price), (0.009, 0.011, 0.02));
        let nn = resolve_strategy(&env(&[("SNIPER_MODE", "99c")]), &RailsSection::default()).unwrap();
        assert_eq!(nn.mode, SniperMode::NinetyNineCent);
        assert_eq!((nn.ask_price_min, nn.ask_price_max, nn.limit_price), (0.97, 0.99, 0.99));
        assert_eq!(nn.tick.max_book_age_ms, 2_000);
    }

    #[test]
    fn overrides_apply_and_empty_values_are_unset() {
        let s = resolve_strategy(
            &env(&[
                ("SNIPER_MODE", "99c"),
                ("SNIPER_TRIGGER", "calm"),
                ("SNIPER_CLIP_USD", "7.5"),
                ("SNIPER_LIMIT", ""),
                ("SNIPER_TICK_ENABLED", "false"),
            ]),
            &RailsSection::default(),
        )
        .unwrap();
        assert_eq!(s.tick.trigger, TriggerMode::Calm);
        assert_eq!(s.clip_usd, 7.5);
        assert_eq!(s.limit_price, 0.99, "an empty value keeps the preset");
        assert!(!s.tick.enabled);
    }

    #[test]
    fn unknown_names_and_bad_values_are_errors() {
        let r = RailsSection::default();
        let e = resolve_strategy(&env(&[("SNIPER_MODE", "1c"), ("SNIPER_CLIP", "5")]), &r).unwrap_err();
        assert!(e.to_string().contains("unknown environment variable SNIPER_CLIP"), "{e}");
        let e = resolve_strategy(&env(&[("SNIPER_MODE", "1c"), ("SNIPER_CLIP_USD", "ten")]), &r).unwrap_err();
        assert!(e.to_string().contains("not a valid number"), "{e}");
        let e = resolve_strategy(&env(&[("SNIPER_MODE", "50c")]), &r).unwrap_err();
        assert!(e.to_string().contains("must be 1c or 99c"), "{e}");
        let e = resolve_strategy(&env(&[("SNIPER_MODE", "99c"), ("SNIPER_TRIGGER", "fast")]), &r).unwrap_err();
        assert!(e.to_string().contains("burst or calm"), "{e}");
    }

    #[test]
    fn unknown_poly_names_are_errors_too() {
        let e = resolve_strategy(&env(&[("SNIPER_MODE", "1c"), ("POLY_PRIVKEY", "x")]), &RailsSection::default())
            .unwrap_err();
        assert!(e.to_string().contains("unknown environment variable POLY_PRIVKEY"), "{e}");
    }

    #[test]
    fn other_variables_are_ignored() {
        let s = resolve_strategy(&env(&[("SNIPER_MODE", "1c"), ("PATH", "/bin"), ("RUST_LOG", "x")]), &RailsSection::default());
        assert!(s.is_ok());
    }

    #[test]
    fn the_rails_have_defaults_and_env_overrides() {
        let e = env(&[("SNIPER_MODE", "1c"), ("SNIPER_DAILY_CAP_USD", "999")]);
        let rails = resolve_rails(&e).unwrap();
        assert_eq!(rails.daily_spend_cap_usd, 999.0);
        assert_eq!(rails.per_order_cap_usd, RailsSection::default().per_order_cap_usd);
        let s = resolve_strategy(&e, &rails).unwrap();
        assert_eq!(s.daily_spend_cap_usd, 999.0);
    }

    const PK: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    const ADDR: &str = "0x365f0ca36ae1f641e02fe3b7743673da42a13a70";

    fn live_env(extra: &[(&str, &str)]) -> EnvVars {
        let mut v = vec![
            ("POLY_PRIVATE_KEY", PK),
            ("POLY_PROXY_WALLET", ADDR),
            ("POLY_API_KEY", "k"),
            ("POLY_API_SECRET", "s"),
            ("POLY_API_PASSPHRASE", "p"),
        ];
        v.extend_from_slice(extra);
        env(&v)
    }

    #[test]
    fn no_poly_variables_means_no_live_account() {
        assert_eq!(resolve_live(&env(&[("SNIPER_MODE", "1c")])).unwrap(), None);
    }

    #[test]
    fn the_live_account_resolves_with_defaults() {
        let live = resolve_live(&live_env(&[])).unwrap().unwrap();
        assert_eq!(live.wallet.private_key, PK);
        assert_eq!(live.chain.rpc_url, "https://polygon-rpc.com");
        assert_eq!((live.chain.chain_id, live.clob.signature_type), (137, 1));
        assert_eq!(live.safety.cap_usd, 150.0);
        crate::config::validate_live(&live).unwrap();

        let live = resolve_live(&live_env(&[("POLY_SIGNATURE_TYPE", "3"), ("SNIPER_LIVE_BUY_CAP_USD", "40")]))
            .unwrap()
            .unwrap();
        assert_eq!((live.clob.signature_type, live.safety.cap_usd), (3, 40.0));
    }

    #[test]
    fn a_half_filled_live_account_names_what_is_missing() {
        let e = resolve_live(&env(&[("POLY_RPC_URL", "https://x")])).unwrap_err().to_string();
        for k in LIVE_REQUIRED {
            assert!(e.contains(k), "{k} must be named: {e}");
        }
        let e = resolve_live(&env(&[("POLY_PRIVATE_KEY", PK)])).unwrap_err().to_string();
        assert!(!e.contains("POLY_PRIVATE_KEY"), "a set key is not listed as missing: {e}");
    }

    #[test]
    fn dotenv_parsing() {
        let kv = parse_dotenv(
            "# comment\n\nSNIPER_MODE=99c\nexport SNIPER_TRIGGER = \"calm\"\nSNIPER_CLIP_USD=5 # five\nA='x # y'\n",
        )
        .unwrap();
        assert_eq!(
            kv,
            vec![
                ("SNIPER_MODE".into(), "99c".into()),
                ("SNIPER_TRIGGER".into(), "calm".into()),
                ("SNIPER_CLIP_USD".into(), "5".into()),
                ("A".into(), "x # y".into()),
            ]
        );
        assert_eq!(
            parse_dotenv("POLY_PRIVATE_KEY=     # 0x + 64 hex\n").unwrap(),
            vec![("POLY_PRIVATE_KEY".into(), "".into())],
            "a comment after an empty value must not become the value"
        );
        assert!(parse_dotenv("NOT A LINE").is_err());
        assert!(parse_dotenv("BAD-NAME=1").is_err());
    }

    #[test]
    fn every_key_is_documented_in_the_example() {
        let example = include_str!("../../.env.example");
        for (k, _) in all_keys() {
            assert!(example.contains(&format!("{k}=")), ".env.example is missing {k}");
        }
    }
}
