//! Polymarket Gamma API client for upcoming market discovery.
//!
//! Handles the spec §1.1 updown families: 15-min (`{coin}-updown-15m-
//! {unix_start_ts}`) and 1-hour ET (`{coin}-up-or-down-{date}-{hour}{am|pm}
//! -et`). 5-min family is deferred. Slugs are deterministic in their time
//! index so pull-based discovery is sufficient — no push subscription
//! needed.

use crate::types::{Coin, MarketMeta};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike, Weekday};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// Trait abstracting "given a slug, give me the market metadata if gamma
/// has minted it". `Ok(None)` means gamma returned `[]` (market not yet
/// minted or unknown slug); `Ok(Some(meta))` is a parsed `MarketMeta`;
/// `Err` is a transient fetch / parse failure (after the in-`GammaClient`
/// retry budget is exhausted) worth surfacing to the caller.
///
/// `Send + Sync + 'static` mirrors [`crate::polymarket::clob_http::MarketFetch`]
/// so the trait can be moved into a `tokio::spawn` context. Unlike
/// `MarketFetch`, this trait uses `#[async_trait]` (a proc-macro that
/// rewrites `async fn` → `fn -> Pin<Box<dyn Future + Send>>`) so the
/// trait is **dyn-compatible**: callers can erase the concrete client
/// behind `Arc<dyn Gamma>`. That dyn-compatibility is a hard requirement
/// for the planned Phase 4b `LifecycleSupervisor` refactor (this
/// allocates one boxed future per call — a non-hot-path cost that buys
/// dyn-compatibility, justified because gamma polling runs on a 30 s
/// interval, not the order hot loop).
///
/// Production callers should keep using the concrete [`GammaClient`]
/// directly; the trait exists to let tests inject scripted gamma
/// responses (see `polymarket-client/tests/gamma_trait_mock.rs`).
/// `LifecycleSupervisor` currently stores a concrete `GammaClient` and
/// is unaffected by this trait — Phase 4b will switch it to
/// `Arc<dyn Gamma>` so a supervisor-level test can pin the
/// `poll_gamma_and_spawn` behavior end-to-end.
#[async_trait]
pub trait Gamma: Send + Sync + 'static {
    /// Fetch a single market by slug. Contract mirrors
    /// [`GammaClient::fetch_by_slug`].
    async fn fetch_by_slug(&self, slug: &str) -> Result<Option<MarketMeta>>;
}

#[async_trait]
impl Gamma for GammaClient {
    async fn fetch_by_slug(&self, slug: &str) -> Result<Option<MarketMeta>> {
        // Delegate to the inherent method so the existing retry / parse
        // path is exercised unchanged. Inherent-method-with-same-name
        // is allowed (Rust resolves `client.fetch_by_slug(...)` to the
        // inherent impl for concrete callers; only `<G as Gamma>` paths
        // route through the trait method).
        GammaClient::fetch_by_slug(self, slug).await
    }
}

const SLOT_5M_SEC: u64 = 300;
const SLOT_SEC: u64 = 900;
const HOUR_SEC: u64 = 3600;

const MONTH_LONG_NAMES: [&str; 12] = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];

/// Returns the Nth (1-indexed) occurrence of `weekday` in the given month.
fn nth_weekday_of_month(year: i32, month: u32, weekday: Weekday, n: u32) -> NaiveDate {
    let first = NaiveDate::from_ymd_opt(year, month, 1).expect("valid first-of-month");
    let first_wd_delta =
        (weekday.num_days_from_sunday() + 7 - first.weekday().num_days_from_sunday()) % 7;
    let day = 1 + first_wd_delta + 7 * (n - 1);
    NaiveDate::from_ymd_opt(year, month, day).expect("valid nth weekday")
}

fn month_from_long_name(s: &str) -> Option<u32> {
    match s {
        "january" => Some(1),
        "february" => Some(2),
        "march" => Some(3),
        "april" => Some(4),
        "may" => Some(5),
        "june" => Some(6),
        "july" => Some(7),
        "august" => Some(8),
        "september" => Some(9),
        "october" => Some(10),
        "november" => Some(11),
        "december" => Some(12),
        _ => None,
    }
}

/// Parse a Polymarket hour suffix like `"3am"`, `"12pm"` into a 0-23 hour.
/// Rejects values outside 1-12 before the am/pm suffix.
fn parse_hour_ampm(s: &str) -> Option<u32> {
    let (digits, add_12) = if let Some(d) = s.strip_suffix("am") {
        (d, 0u32)
    } else if let Some(d) = s.strip_suffix("pm") {
        (d, 12u32)
    } else {
        return None;
    };
    let h: u32 = digits.parse().ok()?;
    if !(1..=12).contains(&h) {
        return None;
    }
    Some((h % 12) + add_12)
}

/// Parse a 1 h ET-format slug per spec §1.1 into `(coin, market_start_ts_utc)`.
/// Returns `None` for non-ET-format slugs or malformed parts; callers should
/// fall through to the numeric (5 m/15 m) path on `None`.
fn parse_hour_et_slug(slug: &str) -> Option<(Coin, u64)> {
    let parts: Vec<&str> = slug.split('-').collect();
    if parts.len() != 9 {
        return None;
    }
    if parts[1] != "up" || parts[2] != "or" || parts[3] != "down" || parts[8] != "et" {
        return None;
    }
    let coin = Coin::from_long_name(parts[0])?;
    let month = month_from_long_name(parts[4])?;
    let day: u32 = parts[5].parse().ok()?;
    let year: i32 = parts[6].parse().ok()?;
    let hour24 = parse_hour_ampm(parts[7])?;
    let local = NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour24, 0, 0)?;
    let offset_hrs = us_eastern_offset_hours(local);
    let utc_naive = local - chrono::Duration::hours(offset_hrs as i64);
    let ts = utc_naive.and_utc().timestamp();
    if ts < 0 {
        return None;
    }
    Some((coin, ts as u64))
}

/// UTC offset (in hours) for US Eastern at `local_dt`. Returns -4 during EDT
/// (second Sunday of March 02:00 → first Sunday of November 02:00) and -5
/// otherwise (EST). Required by spec §1.1: 1 h slugs are specified in ET, so
/// the bot must know the running offset to map wall-clock hours to UTC.
fn us_eastern_offset_hours(local_dt: NaiveDateTime) -> i32 {
    let year = local_dt.date().year();
    let dst_start = nth_weekday_of_month(year, 3, Weekday::Sun, 2)
        .and_hms_opt(2, 0, 0)
        .expect("valid 02:00 DST-start");
    let dst_end = nth_weekday_of_month(year, 11, Weekday::Sun, 1)
        .and_hms_opt(2, 0, 0)
        .expect("valid 02:00 DST-end");
    if local_dt >= dst_start && local_dt < dst_end {
        -4
    } else {
        -5
    }
}

#[derive(Clone)]
pub struct GammaClient {
    http: reqwest::Client,
    base_url: String,
}

impl GammaClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let http = reqwest::ClientBuilder::new()
            .pool_idle_timeout(Duration::from_secs(60))
            .timeout(Duration::from_secs(10))
            .user_agent(concat!("recon/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            http,
            base_url: base_url.into(),
        })
    }

    /// Fetch a single market by slug. `Ok(None)` means gamma returned `[]`
    /// (market not yet minted or unknown slug). `Err` is reserved for
    /// fatal parse/network failures after retries.
    pub async fn fetch_by_slug(&self, slug: &str) -> Result<Option<MarketMeta>> {
        let url = format!("{}/markets?slug={}", self.base_url, slug);
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.http.get(&url).send().await {
                Ok(r) if r.status().is_success() => {
                    let arr: Vec<GammaMarket> = r
                        .json()
                        .await
                        .with_context(|| format!("parse gamma response for {slug}"))?;
                    return match arr.into_iter().next() {
                        Some(m) => Ok(Some(gamma_to_meta(m)?)),
                        None => Ok(None),
                    };
                }
                Ok(r) if r.status() == reqwest::StatusCode::TOO_MANY_REQUESTS && attempt < 4 => {
                    tokio::time::sleep(Duration::from_millis(500 * attempt)).await;
                }
                Ok(r) if r.status().is_server_error() && attempt < 4 => {
                    tokio::time::sleep(Duration::from_millis(500 * attempt)).await;
                }
                Ok(r) => {
                    let status = r.status();
                    let body = r.text().await.unwrap_or_default();
                    bail!("gamma HTTP {status} for {slug}: {body}");
                }
                Err(e) if attempt < 4 && (e.is_timeout() || e.is_connect()) => {
                    tokio::time::sleep(Duration::from_millis(500 * attempt)).await;
                }
                Err(e) => return Err(e).with_context(|| format!("gamma GET {slug}")),
            }
        }
    }

    /// Fetch many slugs in parallel with bounded concurrency. Individual
    /// slug failures are reported per-entry; the batch does not abort on one
    /// error. Order of output is unspecified.
    pub async fn fetch_many(
        &self,
        slugs: Vec<String>,
        concurrency: usize,
    ) -> Vec<(String, Result<Option<MarketMeta>>)> {
        let sem = Arc::new(Semaphore::new(concurrency.max(1)));
        let mut set = tokio::task::JoinSet::new();
        for slug in slugs {
            let permit = sem.clone().acquire_owned().await.expect("semaphore closed");
            let client = self.clone();
            set.spawn(async move {
                let _p = permit;
                let r = client.fetch_by_slug(&slug).await;
                (slug, r)
            });
        }
        let mut out = Vec::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok(v) => out.push(v),
                Err(e) => tracing::error!(?e, "gamma fetch task panicked"),
            }
        }
        out
    }

    /// Legacy: all families × all coins. Retained for tests but prefer the
    /// filtered variant.
    pub fn upcoming_slugs(now_unix: u64, lookahead_secs: u64) -> Vec<String> {
        Self::upcoming_slugs_filtered(
            now_unix,
            lookahead_secs,
            &[
                crate::config::MarketFamily::Fifteenm,
                crate::config::MarketFamily::OnehourEt,
            ],
            &Coin::ALL,
        )
    }

    pub fn upcoming_slugs_filtered(
        now_unix: u64,
        lookahead_secs: u64,
        families: &[crate::config::MarketFamily],
        coins: &[Coin],
    ) -> Vec<String> {
        use crate::config::MarketFamily;
        let mut slugs = Vec::new();

        if families.contains(&MarketFamily::Fivem) {
            let first = (now_unix / SLOT_5M_SEC) * SLOT_5M_SEC;
            let last = first + lookahead_secs;
            let mut s = first;
            while s <= last {
                for c in coins {
                    slugs.push(format!("{}-updown-5m-{}", c.as_str(), s));
                }
                s += SLOT_5M_SEC;
            }
        }

        if families.contains(&MarketFamily::Fifteenm) {
            let first = (now_unix / SLOT_SEC) * SLOT_SEC;
            let last = first + lookahead_secs;
            let mut s = first;
            while s <= last {
                for c in coins {
                    slugs.push(format!("{}-updown-15m-{}", c.as_str(), s));
                }
                s += SLOT_SEC;
            }
        }

        if families.contains(&MarketFamily::OnehourEt) {
            let first = (now_unix / HOUR_SEC) * HOUR_SEC;
            let last = first + lookahead_secs;
            let mut h = first;
            while h <= last {
                for c in coins {
                    slugs.push(format_hour_et_slug(*c, h));
                }
                h += HOUR_SEC;
            }
        }

        slugs
    }
}

/// Format a UTC hour-aligned timestamp into a Polymarket 1 h ET slug.
/// Inverse of [`parse_hour_et_slug`]. Only defined for timestamps aligned to
/// an hour boundary; the caller is responsible for alignment.
fn format_hour_et_slug(coin: Coin, utc_ts: u64) -> String {
    let utc = chrono::DateTime::from_timestamp(utc_ts as i64, 0)
        .expect("valid unix timestamp")
        .naive_utc();
    // Find the local offset with one fixed-point pass so the result is
    // correct even near (but not at) a DST transition.
    let guess = us_eastern_offset_hours(utc);
    let mid = utc + chrono::Duration::hours(guess as i64);
    let offset = us_eastern_offset_hours(mid);
    let local = utc + chrono::Duration::hours(offset as i64);

    let month_name = MONTH_LONG_NAMES[(local.month() - 1) as usize];
    let (h12, ampm) = hour24_to_12h_ampm(local.hour());
    format!(
        "{}-up-or-down-{}-{}-{}-{}{}-et",
        coin.long_name(),
        month_name,
        local.day(),
        local.year(),
        h12,
        ampm
    )
}

fn hour24_to_12h_ampm(h24: u32) -> (u32, &'static str) {
    match h24 {
        0 => (12, "am"),
        1..=11 => (h24, "am"),
        12 => (12, "pm"),
        _ => (h24 - 12, "pm"),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    condition_id: String,
    slug: String,
    /// Outcomes come as a JSON-encoded string (`"[\"Up\", \"Down\"]"`).
    outcomes: String,
    /// CLOB token ids, same nested-JSON quirk.
    clob_token_ids: String,
    order_price_min_tick_size: f64,
    order_min_size: f64,
    neg_risk: bool,
    #[serde(default)]
    maker_base_fee: u32,
    #[serde(default)]
    taker_base_fee: u32,
    #[serde(default)]
    rewards: Option<GammaRewards>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct GammaRewards {
    #[serde(default)]
    min_size: f64,
    #[serde(default)]
    max_spread: f64,
    /// Daily LIP budget in USD. Gamma key: `rewardsDailyRate`. Defaults to 0
    /// when the field is absent (older gamma responses) or the market carries
    /// no budget.
    #[serde(default)]
    rewards_daily_rate: f64,
}

fn gamma_to_meta(g: GammaMarket) -> Result<MarketMeta> {
    let (coin, market_start_ts, duration_secs) = parse_slug(&g.slug)?;

    let outcomes: Vec<String> =
        serde_json::from_str(&g.outcomes).context("parse outcomes nested JSON")?;
    let token_ids: Vec<String> =
        serde_json::from_str(&g.clob_token_ids).context("parse clobTokenIds nested JSON")?;

    if outcomes.len() != 2 || token_ids.len() != 2 {
        bail!(
            "expected 2 outcomes/tokens, got {}/{}",
            outcomes.len(),
            token_ids.len()
        );
    }

    let up_idx = outcomes
        .iter()
        .position(|s| s == "Up")
        .ok_or_else(|| anyhow!("no 'Up' outcome in {:?}", outcomes))?;
    let down_idx = outcomes
        .iter()
        .position(|s| s == "Down")
        .ok_or_else(|| anyhow!("no 'Down' outcome in {:?}", outcomes))?;

    Ok(MarketMeta {
        slug: g.slug,
        coin,
        market_start_ts,
        market_end_ts: market_start_ts + duration_secs,
        condition_id: g.condition_id,
        token_id_up: token_ids[up_idx].clone(),
        token_id_down: token_ids[down_idx].clone(),
        tick_size: g.order_price_min_tick_size,
        min_order_size: g.order_min_size,
        neg_risk: g.neg_risk,
        maker_fee_bps: g.maker_base_fee,
        taker_fee_bps: g.taker_base_fee,
        rewards: g
            .rewards
            .map(|r| crate::types::RewardsCfg {
                min_size: r.min_size,
                max_spread: r.max_spread,
                daily_rate: r.rewards_daily_rate,
            })
            .unwrap_or_default(),
        phase_clock: crate::polymarket::phase::PhaseClock::new(0, crate::polymarket::phase::PhaseClockCfg::default_5m()),
    })
}

/// Parse a Polymarket slug into `(coin, market_start_ts_utc, duration_secs)`.
/// Recognised shapes (spec §1.1):
///   - 1 h ET format:  `{coin_long}-up-or-down-{month}-{d}-{yyyy}-{h}{am|pm}-et`
///   - short numeric:  `{coin}-updown-{5m|15m|1h}-{unix_start}`
///
/// Returns an informative error on unrecognised shapes.
fn parse_slug(slug: &str) -> Result<(Coin, u64, u64)> {
    if let Some((coin, start)) = parse_hour_et_slug(slug) {
        return Ok((coin, start, 3600));
    }
    if let Some(triple) = parse_short_numeric_slug(slug) {
        return Ok(triple);
    }
    bail!("unrecognised slug format: {}", slug);
}

/// Parse `{coin}-updown-{5m|15m|1h}-{unix}` → `(coin, start_ts, duration_secs)`.
fn parse_short_numeric_slug(slug: &str) -> Option<(Coin, u64, u64)> {
    let parts: Vec<&str> = slug.split('-').collect();
    if parts.len() != 4 || parts[1] != "updown" {
        return None;
    }
    let coin = Coin::from_slug_prefix(parts[0])?;
    let duration = match parts[2] {
        "5m" => 300,
        "15m" => 900,
        "1h" => 3600,
        _ => return None,
    };
    let start_ts: u64 = parts[3].parse().ok()?;
    Some((coin, start_ts, duration))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn dst_january_is_est() {
        let dt = NaiveDate::from_ymd_opt(2026, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        assert_eq!(us_eastern_offset_hours(dt), -5);
    }

    #[test]
    fn dst_april_is_edt() {
        // Phase-1 paper mode runs in April → EDT = UTC-4.
        let dt = NaiveDate::from_ymd_opt(2026, 4, 22)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        assert_eq!(us_eastern_offset_hours(dt), -4);
    }

    #[test]
    fn dst_december_is_est() {
        let dt = NaiveDate::from_ymd_opt(2026, 12, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        assert_eq!(us_eastern_offset_hours(dt), -5);
    }

    #[test]
    fn dst_transition_spring_forward_at_2am() {
        // 2026: second Sunday of March is March 8. DST starts at 02:00 local.
        let before = NaiveDate::from_ymd_opt(2026, 3, 8)
            .unwrap()
            .and_hms_opt(1, 59, 0)
            .unwrap();
        let after = NaiveDate::from_ymd_opt(2026, 3, 8)
            .unwrap()
            .and_hms_opt(3, 0, 0)
            .unwrap();
        assert_eq!(us_eastern_offset_hours(before), -5);
        assert_eq!(us_eastern_offset_hours(after), -4);
    }

    #[test]
    fn parses_et_hour_slug_during_edt() {
        // Real observed slug from data/wallet_B_pnl_v3c_20260422-0854.json (line 27):
        //   "bitcoin-up-or-down-april-21-2026-3am-et"
        // April is EDT (UTC-4), so 3am EDT = 07:00 UTC on 2026-04-21.
        // 2026-04-21 00:00 UTC = 1776816000 - 86400 = 1776729600.
        // + 7h = 1776754800.
        let (coin, start) =
            parse_hour_et_slug("bitcoin-up-or-down-april-21-2026-3am-et").expect("parses");
        assert_eq!(coin, Coin::Btc);
        assert_eq!(start, 1_776_754_800);
    }

    #[test]
    fn parses_et_hour_slug_during_est() {
        // 2026-01-15 3pm EST (UTC-5) = 2026-01-15 20:00 UTC.
        // 2026-01-01 00:00 UTC = 1767225600; +14 days = 1768435200; +20h = 1768507200.
        let (coin, start) =
            parse_hour_et_slug("solana-up-or-down-january-15-2026-3pm-et").expect("parses");
        assert_eq!(coin, Coin::Sol);
        assert_eq!(start, 1_768_507_200);
    }

    #[test]
    fn parses_et_hour_slug_handles_12am_and_12pm() {
        // 12am → hour 0 local. 12pm → hour 12 local.
        // 2026-04-22 00:00 UTC = 1776816000.
        // 2026-04-22 12am EDT = 2026-04-22 04:00 UTC = 1776830400.
        let (_, midnight) =
            parse_hour_et_slug("bitcoin-up-or-down-april-22-2026-12am-et").expect("12am parses");
        assert_eq!(midnight, 1_776_830_400);
        // 2026-04-22 12pm EDT = 2026-04-22 16:00 UTC = 1776873600.
        let (_, noon) =
            parse_hour_et_slug("bitcoin-up-or-down-april-22-2026-12pm-et").expect("12pm parses");
        assert_eq!(noon, 1_776_873_600);
    }

    #[test]
    fn parses_et_hour_slug_all_four_coin_prefixes() {
        // Coverage for the four long-name prefixes actually emitted by gamma.
        for (slug, coin) in [
            ("bitcoin-up-or-down-april-21-2026-3am-et", Coin::Btc),
            ("ethereum-up-or-down-april-21-2026-3am-et", Coin::Eth),
            ("solana-up-or-down-april-21-2026-3am-et", Coin::Sol),
            ("xrp-up-or-down-april-21-2026-3am-et", Coin::Xrp),
        ] {
            let (got, _) = parse_hour_et_slug(slug).expect(slug);
            assert_eq!(got, coin);
        }
    }

    #[test]
    fn rejects_malformed_or_non_et_slug() {
        assert!(parse_hour_et_slug("doge-up-or-down-april-21-2026-8pm-et").is_none());
        assert!(parse_hour_et_slug("bitcoin-up-or-down-fake-21-2026-8pm-et").is_none());
        assert!(parse_hour_et_slug("bitcoin-up-or-down-april-21-2026-25pm-et").is_none());
        assert!(parse_hour_et_slug("bitcoin-up-or-down-april-21-2026-8pm").is_none());
        // The existing numeric 15 m format must NOT match.
        assert!(parse_hour_et_slug("eth-updown-15m-1776808800").is_none());
    }

    #[test]
    fn dst_transition_fall_back_at_2am() {
        // 2026: first Sunday of November is November 1. DST ends at 02:00 local.
        let before = NaiveDate::from_ymd_opt(2026, 11, 1)
            .unwrap()
            .and_hms_opt(1, 59, 0)
            .unwrap();
        let after = NaiveDate::from_ymd_opt(2026, 11, 1)
            .unwrap()
            .and_hms_opt(2, 0, 0)
            .unwrap();
        assert_eq!(us_eastern_offset_hours(before), -4);
        assert_eq!(us_eastern_offset_hours(after), -5);
    }

    const FIXTURE_ONE: &str = r#"[{
        "id": "2037748",
        "question": "Ethereum Up or Down - April 21, 6:00PM-6:15PM ET",
        "conditionId": "0xe57d9266a45240dd1897ac6d3f1057591fca11a2d5b9f8b3a30275cce175d99a",
        "slug": "eth-updown-15m-1776808800",
        "endDate": "2026-04-21T22:15:00Z",
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"63690937192273579900704704922008264059993793755595822665646415356230559848653\", \"69624396728419584962315717192546111996160351576735356713891255292099332673100\"]",
        "orderPriceMinTickSize": 0.01,
        "orderMinSize": 5,
        "negRisk": false,
        "makerBaseFee": 1000,
        "takerBaseFee": 1000,
        "acceptingOrders": true,
        "createdAt": "2026-04-20T22:07:26.749953Z"
    }]"#;

    const FIXTURE_HOUR_ET: &str = r#"[{
        "conditionId": "0xaabb",
        "slug": "bitcoin-up-or-down-april-21-2026-3am-et",
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"111\", \"222\"]",
        "orderPriceMinTickSize": 0.01,
        "orderMinSize": 5,
        "negRisk": false,
        "makerBaseFee": 1000,
        "takerBaseFee": 1000
    }]"#;

    const FIXTURE_DOWN_FIRST: &str = r#"[{
        "conditionId": "0xabc",
        "slug": "btc-updown-15m-1776810600",
        "outcomes": "[\"Down\", \"Up\"]",
        "clobTokenIds": "[\"111\", \"222\"]",
        "orderPriceMinTickSize": 0.01,
        "orderMinSize": 5,
        "negRisk": false,
        "makerBaseFee": 1000,
        "takerBaseFee": 1000
    }]"#;

    #[test]
    fn parses_canonical_gamma_response() {
        let arr: Vec<GammaMarket> = serde_json::from_str(FIXTURE_ONE).unwrap();
        let m = gamma_to_meta(arr.into_iter().next().unwrap()).unwrap();
        assert_eq!(m.slug, "eth-updown-15m-1776808800");
        assert_eq!(m.coin, Coin::Eth);
        assert_eq!(m.market_start_ts, 1776808800);
        assert_eq!(m.market_end_ts, 1776808800 + 900);
        assert_eq!(
            m.condition_id,
            "0xe57d9266a45240dd1897ac6d3f1057591fca11a2d5b9f8b3a30275cce175d99a"
        );
        assert_eq!(
            m.token_id_up,
            "63690937192273579900704704922008264059993793755595822665646415356230559848653"
        );
        assert_eq!(
            m.token_id_down,
            "69624396728419584962315717192546111996160351576735356713891255292099332673100"
        );
        assert_eq!(m.tick_size, 0.01);
        assert_eq!(m.min_order_size, 5.0);
        assert!(!m.neg_risk);
        assert_eq!(m.maker_fee_bps, 1000);
        assert_eq!(m.taker_fee_bps, 1000);
    }

    #[test]
    fn gamma_to_meta_parses_old_1h_numeric_slug_with_3600s_duration() {
        // Spec §1.1 P12: the old 1 h slug format `{coin}-updown-1h-{unix}`
        // must yield duration 3600, not 900. Without this, the quote-loop
        // would exit early and fills would be single-side-heavy.
        let fixture = r#"[{
            "conditionId": "0x",
            "slug": "btc-updown-1h-1776808800",
            "outcomes": "[\"Up\", \"Down\"]",
            "clobTokenIds": "[\"1\", \"2\"]",
            "orderPriceMinTickSize": 0.01,
            "orderMinSize": 5,
            "negRisk": false,
            "makerBaseFee": 1000,
            "takerBaseFee": 1000
        }]"#;
        let arr: Vec<GammaMarket> = serde_json::from_str(fixture).unwrap();
        let m = gamma_to_meta(arr.into_iter().next().unwrap()).unwrap();
        assert_eq!(m.coin, Coin::Btc);
        assert_eq!(m.market_start_ts, 1776808800);
        assert_eq!(m.market_end_ts, 1776808800 + 3600);
    }

    #[test]
    fn gamma_to_meta_parses_hour_et_slug_with_3600s_duration() {
        // 1 h ET slug must yield `market_end_ts = market_start_ts + 3600`, not
        // +900 as the 15 m path does.
        let arr: Vec<GammaMarket> = serde_json::from_str(FIXTURE_HOUR_ET).unwrap();
        let m = gamma_to_meta(arr.into_iter().next().unwrap()).unwrap();
        assert_eq!(m.slug, "bitcoin-up-or-down-april-21-2026-3am-et");
        assert_eq!(m.coin, Coin::Btc);
        assert_eq!(m.market_start_ts, 1_776_754_800); // 2026-04-21 07:00 UTC
        assert_eq!(m.market_end_ts, 1_776_754_800 + 3600);
        assert_eq!(m.token_id_up, "111");
        assert_eq!(m.token_id_down, "222");
    }

    #[test]
    fn parses_reversed_outcomes_correctly() {
        // Verifies we map Up/Down by name, not position.
        let arr: Vec<GammaMarket> = serde_json::from_str(FIXTURE_DOWN_FIRST).unwrap();
        let m = gamma_to_meta(arr.into_iter().next().unwrap()).unwrap();
        // Outcomes are ["Down", "Up"] so token_ids[0]=Down, token_ids[1]=Up.
        assert_eq!(m.token_id_up, "222");
        assert_eq!(m.token_id_down, "111");
    }

    #[test]
    fn rejects_unknown_coin_prefix() {
        let bad = r#"{
            "conditionId": "0x",
            "slug": "doge-updown-15m-1776808800",
            "outcomes": "[\"Up\", \"Down\"]",
            "clobTokenIds": "[\"1\", \"2\"]",
            "orderPriceMinTickSize": 0.01,
            "orderMinSize": 5,
            "negRisk": false,
            "makerBaseFee": 1000,
            "takerBaseFee": 1000
        }"#;
        let g: GammaMarket = serde_json::from_str(bad).unwrap();
        assert!(gamma_to_meta(g).is_err());
    }

    #[test]
    fn rejects_malformed_slug_timestamp() {
        let bad = r#"{
            "conditionId": "0x",
            "slug": "eth-updown-15m-notanumber",
            "outcomes": "[\"Up\", \"Down\"]",
            "clobTokenIds": "[\"1\", \"2\"]",
            "orderPriceMinTickSize": 0.01,
            "orderMinSize": 5,
            "negRisk": false,
            "makerBaseFee": 1000,
            "takerBaseFee": 1000
        }"#;
        let g: GammaMarket = serde_json::from_str(bad).unwrap();
        assert!(gamma_to_meta(g).is_err());
    }

    #[test]
    fn rejects_outcomes_missing_up_or_down() {
        let bad = r#"{
            "conditionId": "0x",
            "slug": "eth-updown-15m-1776808800",
            "outcomes": "[\"Yes\", \"No\"]",
            "clobTokenIds": "[\"1\", \"2\"]",
            "orderPriceMinTickSize": 0.01,
            "orderMinSize": 5,
            "negRisk": false,
            "makerBaseFee": 1000,
            "takerBaseFee": 1000
        }"#;
        let g: GammaMarket = serde_json::from_str(bad).unwrap();
        assert!(gamma_to_meta(g).is_err());
    }

    #[test]
    fn upcoming_slugs_covers_expected_slots() {
        // now = 1776808800 = 2026-04-21 22:00 UTC = 18:00 EDT (6pm). Hour-aligned.
        // 30 min lookahead → 3 × 15 m slots × 4 coins = 12 slugs
        //                 + 1 × 1 h slot  × 4 coins =  4 slugs (6pm EDT only; 7pm is beyond).
        let now = 1776808800;
        let slugs = GammaClient::upcoming_slugs(now, 1800);
        assert_eq!(slugs.len(), 16);
        let slugs_15m: Vec<&String> = slugs
            .iter()
            .filter(|s| s.contains("-updown-15m-"))
            .collect();
        assert_eq!(slugs_15m.len(), 12);
        assert!(slugs.contains(&"btc-updown-15m-1776808800".to_string()));
        assert!(slugs.contains(&"eth-updown-15m-1776808800".to_string()));
        assert!(slugs.contains(&"sol-updown-15m-1776810600".to_string()));
        assert!(slugs.contains(&"xrp-updown-15m-1776810600".to_string()));
        let slugs_1h: Vec<&String> = slugs.iter().filter(|s| s.ends_with("-et")).collect();
        assert_eq!(slugs_1h.len(), 4);
        assert!(slugs.contains(&"bitcoin-up-or-down-april-21-2026-6pm-et".to_string()));
    }

    #[test]
    fn upcoming_slugs_emits_1h_et_slugs_for_next_hours() {
        // 1776808800 = 2026-04-21 22:00 UTC = 2026-04-21 18:00 EDT = 6pm EDT.
        // A 1h lookahead should emit 1 h slots at 6pm and 7pm EDT for all 4 coins.
        let slugs = GammaClient::upcoming_slugs(1_776_808_800, 3600);
        assert!(slugs.contains(&"bitcoin-up-or-down-april-21-2026-6pm-et".to_string()));
        assert!(slugs.contains(&"ethereum-up-or-down-april-21-2026-6pm-et".to_string()));
        assert!(slugs.contains(&"solana-up-or-down-april-21-2026-6pm-et".to_string()));
        assert!(slugs.contains(&"xrp-up-or-down-april-21-2026-6pm-et".to_string()));
        assert!(slugs.contains(&"bitcoin-up-or-down-april-21-2026-7pm-et".to_string()));
    }

    #[test]
    fn upcoming_slugs_snaps_to_prior_slot_boundary() {
        // Mid-slot input snaps down to 900 s boundary for 15 m and 3600 s for 1 h.
        // now = 1776808800 + 123 falls in the slot starting at 1776808800 for both.
        let now = 1776808800 + 123;
        let slugs = GammaClient::upcoming_slugs(now, 0);
        // 1 × 15 m × 4 coins = 4 ; 1 × 1 h × 4 coins = 4. Total 8.
        assert_eq!(slugs.len(), 8);
        let slugs_15m: Vec<&String> = slugs
            .iter()
            .filter(|s| s.contains("-updown-15m-"))
            .collect();
        assert_eq!(slugs_15m.len(), 4);
        for s in &slugs_15m {
            assert!(
                s.ends_with("-1776808800"),
                "slug {s} should end with 1776808800"
            );
        }
        let slugs_1h: Vec<&String> = slugs.iter().filter(|s| s.ends_with("-et")).collect();
        assert_eq!(slugs_1h.len(), 4);
        for s in &slugs_1h {
            assert!(
                s.ends_with("-april-21-2026-6pm-et"),
                "1 h slug {s} should end with -april-21-2026-6pm-et"
            );
        }
    }

    #[test]
    fn client_constructs_with_valid_base_url() {
        let c = GammaClient::new("https://gamma-api.polymarket.com");
        assert!(c.is_ok());
    }

    const FIXTURE_WITH_REWARDS: &str = r#"[{
        "conditionId": "0xREW",
        "slug": "btc-updown-5m-1776810600",
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"111\", \"222\"]",
        "orderPriceMinTickSize": 0.01,
        "orderMinSize": 5,
        "negRisk": false,
        "makerBaseFee": 0,
        "takerBaseFee": 1000,
        "rewards": {
            "minSize": 15,
            "maxSpread": 0.03
        }
    }]"#;

    #[test]
    fn gamma_to_meta_parses_rewards_config() {
        let arr: Vec<GammaMarket> = serde_json::from_str(FIXTURE_WITH_REWARDS).unwrap();
        let m = gamma_to_meta(arr.into_iter().next().unwrap()).unwrap();
        assert_eq!(m.rewards.min_size, 15.0);
        assert_eq!(m.rewards.max_spread, 0.03);
        assert!(m.rewards.is_incentivised());
    }

    #[test]
    fn gamma_to_meta_tolerates_missing_rewards_object() {
        // Existing FIXTURE_ONE has no `rewards` key; must still parse with
        // defaulted `rewards: RewardsCfg::default()` and `is_incentivised() == false`.
        let arr: Vec<GammaMarket> = serde_json::from_str(FIXTURE_ONE).unwrap();
        let m = gamma_to_meta(arr.into_iter().next().unwrap()).unwrap();
        assert!(!m.rewards.is_incentivised());
    }

    #[test]
    fn upcoming_slugs_respects_family_and_coin_filters() {
        use crate::config::MarketFamily;
        // Hour-aligned test input so the 1h branch produces a single slot.
        let now = 1_776_808_800;
        // Only 5m, only BTC, 900 s lookahead → 4 slots (now, +300, +600, +900).
        let slugs =
            GammaClient::upcoming_slugs_filtered(now, 900, &[MarketFamily::Fivem], &[Coin::Btc]);
        assert_eq!(slugs.len(), 4);
        assert!(slugs.iter().all(|s| s.starts_with("btc-updown-5m-")));
    }

    #[test]
    fn upcoming_slugs_fivem_snaps_to_300s_boundary() {
        use crate::config::MarketFamily;
        // Mid-slot input (now % 300 != 0) must snap back to the slot boundary.
        let slugs = GammaClient::upcoming_slugs_filtered(
            1_776_808_800 + 137,
            0,
            &[MarketFamily::Fivem],
            &[Coin::Btc],
        );
        assert_eq!(slugs.len(), 1);
        assert_eq!(slugs[0], "btc-updown-5m-1776808800");
    }

    #[test]
    fn upcoming_slugs_empty_families_returns_empty() {
        // Paranoia guard — empty filter must not accidentally default to all.
        let slugs = GammaClient::upcoming_slugs_filtered(1_776_808_800, 3600, &[], &[Coin::Btc]);
        assert!(slugs.is_empty());
    }

    const FIXTURE_WITH_DAILY_RATE: &str = r#"[{
        "conditionId": "0xBUDGETED",
        "slug": "btc-updown-5m-1776810600",
        "outcomes": "[\"Up\", \"Down\"]",
        "clobTokenIds": "[\"111\", \"222\"]",
        "orderPriceMinTickSize": 0.01,
        "orderMinSize": 5,
        "negRisk": false,
        "makerBaseFee": 0,
        "takerBaseFee": 1000,
        "rewards": {
            "minSize": 50,
            "maxSpread": 0.03,
            "rewardsDailyRate": 891.0
        }
    }]"#;

    #[test]
    fn gamma_to_meta_parses_daily_rate() {
        let arr: Vec<GammaMarket> = serde_json::from_str(FIXTURE_WITH_DAILY_RATE).unwrap();
        let m = gamma_to_meta(arr.into_iter().next().unwrap()).unwrap();
        assert_eq!(m.rewards.min_size, 50.0);
        assert_eq!(m.rewards.max_spread, 0.03);
        assert_eq!(m.rewards.daily_rate, 891.0);
        assert!(m.rewards.meets_min_daily_rate(50.0));
    }

    #[test]
    fn gamma_to_meta_defaults_daily_rate_when_missing() {
        // Existing FIXTURE_WITH_REWARDS has no `rewardsDailyRate`; must default to 0.0
        // so back-compat with older gamma responses is preserved.
        let arr: Vec<GammaMarket> = serde_json::from_str(FIXTURE_WITH_REWARDS).unwrap();
        let m = gamma_to_meta(arr.into_iter().next().unwrap()).unwrap();
        assert_eq!(m.rewards.daily_rate, 0.0);
    }

    #[tokio::test]
    #[ignore = "hits live gamma-api; run with: cargo test -- --ignored"]
    async fn live_fetches_one_upcoming_slot() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let target = (now / SLOT_SEC) * SLOT_SEC + 1800; // +30 min ahead
        let slug = format!("eth-updown-15m-{target}");
        let client = GammaClient::new("https://gamma-api.polymarket.com").unwrap();
        let meta = client
            .fetch_by_slug(&slug)
            .await
            .expect("fetch ok")
            .unwrap_or_else(|| panic!("expected market {slug} to exist ~30 min out"));
        assert_eq!(meta.slug, slug);
        assert_eq!(meta.coin, Coin::Eth);
        assert_eq!(meta.market_start_ts, target);
        assert_eq!(meta.market_end_ts, target + 900);
        assert!(meta.condition_id.starts_with("0x"));
        assert!(!meta.token_id_up.is_empty());
        assert!(!meta.token_id_down.is_empty());
        assert_ne!(meta.token_id_up, meta.token_id_down);
        assert!(meta.tick_size > 0.0);
        assert!(meta.min_order_size > 0.0);
    }
}
