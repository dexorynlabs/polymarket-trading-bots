//! Polymarket CLOB HTTP client. Queries `clob.polymarket.com/markets/<conditionId>`
//! for resolved-outcome data used by the post-market-end resolution sweeper
//! (spec §6.1 / `src/exec/redeem.rs`).

use crate::types::Side;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::future::Future;
use std::time::Duration;

/// Trait abstracting "given a condition id, tell me who won (if anyone)".
/// `Ok(None)` means the market is not yet closed; `Ok(Some(side))` is the
/// resolved winner; `Err` is a transient fetch/parse error worth retrying.
pub trait MarketFetch: Send + Sync + 'static {
    fn fetch_resolved(
        &self,
        condition_id: &str,
    ) -> impl Future<Output = Result<Option<Side>>> + Send;
}

#[derive(Clone)]
pub struct ClobHttpClient {
    http: reqwest::Client,
    base_url: String,
}

impl ClobHttpClient {
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
}

impl MarketFetch for ClobHttpClient {
    fn fetch_resolved(
        &self,
        condition_id: &str,
    ) -> impl Future<Output = Result<Option<Side>>> + Send {
        let url = format!("{}/markets/{}", self.base_url, condition_id);
        let http = self.http.clone();
        async move {
            let resp = http
                .get(&url)
                .send()
                .await
                .with_context(|| format!("clob GET {url}"))?;
            if !resp.status().is_success() {
                anyhow::bail!("clob HTTP {} for {url}", resp.status());
            }
            let parsed: ClobMarketResponse = resp
                .json()
                .await
                .with_context(|| format!("parse clob response for {url}"))?;
            Ok(parsed.winner_side())
        }
    }
}

/// Subset of the CLOB `/markets/<conditionId>` response we care about.
#[derive(Clone, Debug, Deserialize)]
pub struct ClobMarketResponse {
    pub closed: bool,
    pub tokens: Vec<Token>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Token {
    pub outcome: String,
    #[serde(default)]
    pub winner: bool,
}

impl ClobMarketResponse {
    /// Extract the winning `Side`, if the market has closed with a winner.
    /// Returns `None` if `!closed`, if no token has `winner: true`, or if the
    /// winning token's `outcome` isn't `"Up"` or `"Down"`.
    pub fn winner_side(&self) -> Option<Side> {
        if !self.closed {
            return None;
        }
        let w = self.tokens.iter().find(|t| t.winner)?;
        match w.outcome.as_str() {
            "Up" => Some(Side::Up),
            "Down" => Some(Side::Down),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE_UP_WIN: &str = r#"{
        "closed": true,
        "tokens": [
            {"outcome": "Up", "winner": true},
            {"outcome": "Down", "winner": false}
        ]
    }"#;

    const FIXTURE_DOWN_WIN: &str = r#"{
        "closed": true,
        "tokens": [
            {"outcome": "Up", "winner": false},
            {"outcome": "Down", "winner": true}
        ]
    }"#;

    const FIXTURE_NOT_CLOSED: &str = r#"{
        "closed": false,
        "tokens": [
            {"outcome": "Up", "winner": false},
            {"outcome": "Down", "winner": false}
        ]
    }"#;

    const FIXTURE_CLOSED_NO_WINNER: &str = r#"{
        "closed": true,
        "tokens": [
            {"outcome": "Up", "winner": false},
            {"outcome": "Down", "winner": false}
        ]
    }"#;

    #[test]
    fn parses_up_winner() {
        let m: ClobMarketResponse = serde_json::from_str(FIXTURE_UP_WIN).unwrap();
        assert_eq!(m.winner_side(), Some(Side::Up));
    }

    #[test]
    fn parses_down_winner() {
        let m: ClobMarketResponse = serde_json::from_str(FIXTURE_DOWN_WIN).unwrap();
        assert_eq!(m.winner_side(), Some(Side::Down));
    }

    #[test]
    fn not_closed_yields_none() {
        let m: ClobMarketResponse = serde_json::from_str(FIXTURE_NOT_CLOSED).unwrap();
        assert_eq!(m.winner_side(), None);
    }

    #[test]
    fn closed_without_winner_yields_none() {
        let m: ClobMarketResponse = serde_json::from_str(FIXTURE_CLOSED_NO_WINNER).unwrap();
        assert_eq!(m.winner_side(), None);
    }

    #[test]
    fn ignores_extra_fields() {
        let json = r#"{
            "closed": true,
            "market_slug": "eth-updown-15m-1",
            "maker_base_fee": 1000,
            "taker_base_fee": 1000,
            "description": "some chainlink stream url",
            "tokens": [
                {"outcome": "Up", "winner": true, "token_id": "123", "price": "1.0"},
                {"outcome": "Down", "winner": false, "token_id": "456", "price": "0.0"}
            ]
        }"#;
        let m: ClobMarketResponse = serde_json::from_str(json).unwrap();
        assert_eq!(m.winner_side(), Some(Side::Up));
    }
}
