//! Live-mode Polymarket CLOB client. Implements `ClobClient`.
//!
//! Audit deltas baked in:
//! - #1 POST /order body shape: signature lives INSIDE order; side is
//!   "BUY"/"SELL" string; top-level keys are owner, orderType.
//! - #2 cancel = DELETE /order (Task B3).
//! - #4 L2 HMAC headers on every authenticated call.
//!
//! V1 scope: only non-neg-risk markets are supported (audit-confirmed).
//! Address-format note: `signer.address_hex()` produces lowercase
//! Debug-format addresses, which match Polymarket's wire convention —
//! do not switch to checksummed addresses.

use crate::config::ContractAddresses;
use crate::config::LiveConfig;
use crate::types::{Fill, ImmediateMatch, RestingAsk, RestingBid, RestingOrderId, Side, TakerBuy};
use crate::execution::auth::{auth_headers, ApiCreds};
use crate::execution::signer::{PolymarketOrder, PolymarketOrderDomain, SignerService};
use crate::execution::ClobClient;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

#[derive(Clone, Copy, Debug)]
pub enum CollateralDecimals {
    Six,
}

pub struct LiveClob {
    /// Default client — always populated; used by every path with no coin
    /// context (cancel, balance poll, `place_buy_with_order_type`).
    http: reqwest::Client,
    /// Tier-5 (Phase 9, ported 2026-06-13 from worktree-phase9-tier1-
    /// reaction-budget @ 4f0a373): per-coin clients keyed by
    /// `Coin::as_str()`. Empty when
    /// `LiveClobOptions::isolated_clients_per_coin = false` (the default,
    /// historical shared-pool behavior).
    http_per_coin: HashMap<&'static str, Arc<reqwest::Client>>,
    base_url: String,
    signer: Arc<SignerService>,
    proxy_wallet: String,
    contracts: ContractAddresses,
    creds: ApiCreds,
    signature_type: u8,
    decimals: CollateralDecimals,
    fills_tx: broadcast::Sender<Fill>,
    /// POST-response immediate matches (status == "matched"); see
    /// `immediate_match_tx()`. Always present; send errors (no subscriber)
    /// are ignored.
    immediate_match_tx: broadcast::Sender<ImmediateMatch>,
    /// Polygon JSON-RPC URL for USDC.e balance polls. Captured from
    /// `LiveConfig.chain.rpc_url` at construction; reused by
    /// `fetch_usdc_balance_micro` (Phase 10A — runtime balance-poll task).
    rpc_url: String,
}

/// Snap order amounts to V2 API decimal accuracy.
///
/// V2 requires: USD (makerAmount) ≤ 2 decimals; shares (takerAmount) ≤ 4 decimals.
/// The fix recomputes shares from the snapped USD so that `usd / shares`
/// remains internally consistent within tick rounding — important because
/// the server re-decodes the order and validates the price ratio.
///
/// Slot `1777320000` rejected `21.63 × 0.41 = 8.8683` makerAmount with
/// `"max accuracy of 2 decimals"`. After snapping: USD=8.87, shares=21.6341.
/// `#[cfg(test)]` re-export so the integration test crate
/// `tests/paper_replay_btc_updown_5m_2026_04_28.rs` can validate that wire
/// values pass the slot-1777320000 "max accuracy of 2 decimals" gate
/// without touching the production submitter.
#[doc(hidden)]
pub fn snap_amounts_for_test(price: f64, shares: f64) -> (f64, f64) {
    snap_amounts(price, shares)
}

pub(crate) fn snap_amounts(price: f64, shares: f64) -> (f64, f64) {
    let s4 = (shares * 1e4).round() / 1e4;
    let u2 = ((s4 * price) * 1e2).round() / 1e2;
    let s_final = ((u2 / price) * 1e4).round() / 1e4;
    (u2, s_final)
}

/// Build a reqwest client tuned for Polymarket CLOB hot-path latency.
/// Tier-2 settings (Phase 9, measured 2026-05-12; ported 2026-06-13):
///   - `connect_timeout = 2s` — fail fast on a dead pool entry instead of
///     burning the full 10s request timeout.
///   - `pool_idle_timeout = 300s` + `pool_max_idle_per_host = 32` — the
///     prior 60s idle reap was SHORTER than a ~1/min post
///     cadence, so most posts paid TCP+TLS setup again (the 1.2-1.5s ack
///     tails in the 2026-06-12 canary). 300s outlives Cloudflare's ~100s
///     edge reap; the keepalive heartbeat (below) keeps the socket warm.
///   - `tcp_keepalive = 30s`, `tcp_nodelay = true` (explicit: a future
///     reqwest default-flip must not re-introduce ~40ms Nagle stalls).
///   - `http2_keep_alive_*` + `http2_adaptive_window` — no-ops on h1.1.
///
/// Tier-3: `force_http1_only` adds `.http1_only()` to skip the ALPN h2
/// tail — pass `true` only when [`probe_alpn_http2`] returned `false`.
pub(crate) fn build_tuned_http_client(force_http1_only: bool) -> Result<reqwest::Client> {
    let mut b = reqwest::ClientBuilder::new()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(2))
        .user_agent(concat!("recon-live/", env!("CARGO_PKG_VERSION")))
        .pool_idle_timeout(Some(Duration::from_secs(300)))
        .pool_max_idle_per_host(32)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Some(Duration::from_secs(30)))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .http2_keep_alive_while_idle(true)
        .http2_adaptive_window(true);
    if force_http1_only {
        b = b.http1_only();
    }
    b.build().context("build reqwest client")
}

/// One-time ALPN probe: `true` iff `<base_url>/time` answers over HTTP/2
/// when the client speaks h2 prior-knowledge; any error → `false` (caller
/// keeps ALPN-negotiated default). Re-probing per run catches an eventual
/// server-side h2 flip automatically (Tier 3, Phase 9).
pub async fn probe_alpn_http2(base_url: &str) -> bool {
    let url = format!("{}/time", base_url.trim_end_matches('/'));
    let probe_client = match reqwest::ClientBuilder::new()
        .http2_prior_knowledge()
        .timeout(Duration::from_secs(5))
        .user_agent(concat!("recon-http2-probe/", env!("CARGO_PKG_VERSION")))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    match probe_client.get(&url).send().await {
        Ok(resp) => resp.version() == reqwest::Version::HTTP_2,
        Err(_) => false,
    }
}

/// Pick the order's `signer` field for a wallet kind.
///
/// POLY_1271 deposit wallets are validated through the wallet CONTRACT's
/// `isValidSignature`, so the contract — not the EOA — is the signer.
/// Types 0/1/2 sign as the EOA that owns the proxy/Safe.
fn pick_order_signer(signature_type: u8, proxy_wallet: &str, eoa: &str) -> String {
    if signature_type == crate::execution::signer::SIG_TYPE_POLY_1271 {
        proxy_wallet.to_string()
    } else {
        eoa.to_string()
    }
}

/// Knobs for [`LiveClob::new_with_options`]. `Default` is the production
/// configuration: ALPN-negotiated protocol, one shared client.
#[derive(Debug, Clone, Default)]
pub struct LiveClobOptions {
    /// Tier 3: skip ALPN h2 negotiation. Set only after [`probe_alpn_http2`]
    /// returned `false`.
    pub force_http1_only: bool,
    /// Tier 5: one independent client (connection pool) per coin —
    /// eliminates h1.1 head-of-line blocking across concurrent coin
    /// workers. Off by default (single-coin strategies gain nothing).
    pub isolated_clients_per_coin: bool,
}

/// Cloudflare / origin diagnostics from a CLOB response (Tier 4). All
/// fields optional — missing headers never fail a request. Captured
/// BEFORE the body is consumed so error paths keep them too (branch fix
/// 273ebb5).
#[derive(Debug, Clone, Default)]
pub struct HttpDiagnostics {
    /// `cf-ray`: last 3 chars after `-` = Cloudflare POP code.
    pub cf_ray: Option<String>,
    pub cf_cache_status: Option<String>,
    /// May carry origin RTT components (`edge;dur=…, origin;dur=…`).
    pub server_timing: Option<String>,
    pub cf_request_id: Option<String>,
    /// Negotiated version (`HTTP/2.0`, `HTTP/1.1`).
    pub http_version: Option<String>,
}

fn extract_http_diagnostics(resp: &reqwest::Response) -> HttpDiagnostics {
    let h = |name: &str| {
        resp.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    };
    HttpDiagnostics {
        cf_ray: h("cf-ray"),
        cf_cache_status: h("cf-cache-status"),
        server_timing: h("server-timing"),
        cf_request_id: h("cf-request-id"),
        http_version: Some(format!("{:?}", resp.version())),
    }
}

impl LiveClob {
    pub fn new(live: &LiveConfig, contracts: ContractAddresses) -> Result<Self> {
        Self::new_with_options(live, contracts, LiveClobOptions::default())
    }

    pub fn new_with_options(
        live: &LiveConfig,
        contracts: ContractAddresses,
        options: LiveClobOptions,
    ) -> Result<Self> {
        let signer = SignerService::from_pk_hex(&live.wallet.private_key)?;
        let eoa = signer.address_hex();
        let http = build_tuned_http_client(options.force_http1_only)?;
        let mut http_per_coin = HashMap::new();
        if options.isolated_clients_per_coin {
            for coin in ["btc", "eth", "sol", "xrp"] {
                http_per_coin
                    .insert(coin, Arc::new(build_tuned_http_client(options.force_http1_only)?));
            }
        }
        let (fills_tx, _) = broadcast::channel(256);
        let (immediate_match_tx, _) = broadcast::channel(64);
        let creds = ApiCreds {
            key: live.api.key.clone(),
            secret: live.api.secret.clone(),
            passphrase: live.api.passphrase.clone(),
            // B3: POLY_ADDRESS must be the signer EOA, not the proxy wallet.
            address: eoa,
        };
        Ok(Self {
            http,
            http_per_coin,
            base_url: live.clob.http_base.clone(),
            signer: Arc::new(signer),
            proxy_wallet: live.wallet.proxy_wallet.clone(),
            contracts,
            creds,
            signature_type: live.clob.signature_type,
            decimals: CollateralDecimals::Six,
            fills_tx,
            immediate_match_tx,
            rpc_url: live.chain.rpc_url.clone(),
        })
    }

    /// Read the proxy-wallet's USDC.e balance from Polygon RPC, returning
    /// micro-USDC (1e6 scale, fits u64 for any plausible balance). Used by
    /// the runtime balance-poll task (Phase 10A) to feed
    /// `Guards::on_balance_sample` for §7.3 (balance floor) + §7.4
    /// (velocity kill).
    ///
    /// Mirrors the JSON-RPC pattern in `preflight::balance_usdc_e` but
    /// returns the integer balance in base units rather than f64 USD.
    pub async fn fetch_usdc_balance_micro(&self) -> Result<u64> {
        let payload = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{{"to":"{}","data":"0x70a08231{}"}},"latest"]}}"#,
            self.contracts.collateral_erc20,
            pad_addr(&self.proxy_wallet),
        );
        let resp = self
            .http
            .post(&self.rpc_url)
            .header("content-type", "application/json")
            .body(payload)
            .send()
            .await
            .with_context(|| format!("POST {} (eth_call balanceOf)", self.rpc_url))?;
        let v: serde_json::Value = resp.json().await.context("parse JSON-RPC response body")?;
        let raw = v
            .get("result")
            .and_then(|r| r.as_str())
            .ok_or_else(|| anyhow::anyhow!("rpc response missing `result`: {v}"))?;
        parse_balance_hex_to_micro(raw)
    }

    /// ERC-1155 CTF `balanceOf(proxy_wallet, token_id)` in base-1e6 units
    /// — the slot-inventory read (BUY-only + never-sell
    /// + within-slot ⇒ the token balances ARE the cumulative fills).
    /// `token_id_dec` is the CLOB asset id / gamma `clobTokenIds` entry.
    pub async fn fetch_ctf_balance_micro(&self, token_id_dec: &str) -> Result<u64> {
        crate::execution::balance::fetch_erc1155_balance_micro(
            &self.http,
            &self.rpc_url,
            &self.contracts.ctf,
            &self.proxy_wallet,
            token_id_dec,
        )
        .await
    }

    /// Select the client for an operation: `Some(coin)` routes through that
    /// coin's dedicated pool when Tier-5 isolation is on; `None` (or
    /// isolation off) always falls through to the default client.
    fn http_for(&self, coin: Option<&str>) -> &reqwest::Client {
        if let Some(c) = coin {
            if let Some(arc) = self.http_per_coin.get(c) {
                return arc.as_ref();
            }
        }
        &self.http
    }

    /// Keep the CLOB connection pool + TLS session warm for low-cadence
    /// posters (2026-06-12 canary lesson: a ~1/min poster; the
    /// previous 60s pool idle reap made nearly every FAK pay TCP+TLS setup
    /// — 1.2-1.5s ack tails after idle gaps). GETs `<base>/time` on the
    /// SAME client every `every_secs`; failures log and never kill the
    /// task (a blip must not flap anything — this is plumbing, not health).
    pub fn spawn_keepalive_heartbeat(
        self: &Arc<Self>,
        every_secs: u64,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        let clob = self.clone();
        tokio::spawn(async move {
            let url = format!("{}/time", clob.base_url.trim_end_matches('/'));
            let mut interval =
                tokio::time::interval(Duration::from_secs(every_secs.max(5)));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = interval.tick() => {}
                }
                let t0 = std::time::Instant::now();
                match clob.http.get(&url).send().await {
                    Ok(resp) => tracing::debug!(
                        elapsed_ms = t0.elapsed().as_millis() as u64,
                        version = ?resp.version(),
                        status = %resp.status(),
                        "clob keepalive heartbeat"
                    ),
                    Err(e) => tracing::warn!(?e, "clob keepalive heartbeat failed"),
                }
            }
        });
    }

    pub fn fills_tx(&self) -> broadcast::Sender<Fill> {
        self.fills_tx.clone()
    }

    /// Synchronous POST-response matches (`status == "matched"`). Subscribers
    /// get the matched portion hundreds of ms before the user-WS MATCHED —
    /// decision-exposure signal only, never settled inventory.
    pub fn immediate_match_tx(&self) -> broadcast::Sender<ImmediateMatch> {
        self.immediate_match_tx.clone()
    }

    pub fn creds(&self) -> ApiCreds {
        self.creds.clone()
    }

    /// Address that goes in the order's `signer` field.
    ///
    /// For POLY_1271 (deposit wallets) this is the WALLET CONTRACT, not the
    /// EOA — the exchange validates the order through the contract's
    /// `isValidSignature`. Sending the EOA here is rejected with
    /// `the order signer address has to be the address of the API KEY`.
    /// Types 0/1/2 keep the EOA, which owns the proxy/Safe.
    ///
    /// Note this is the ORDER field only. The `POLY_ADDRESS` auth header
    /// stays the EOA for every type (B3) — the L2 API key binds to the EOA
    /// regardless of wallet kind.
    fn order_signer(&self) -> String {
        pick_order_signer(
            self.signature_type,
            &self.proxy_wallet,
            &self.signer.address_hex(),
        )
    }

    fn to_base_units(&self, usd: f64) -> u128 {
        let scale = match self.decimals {
            CollateralDecimals::Six => 1_000_000.0,
        };
        (usd * scale).round() as u128
    }

    fn build_buy_order(&self, token_id: &str, price: f64, size: f64) -> PolymarketOrder {
        use std::time::{SystemTime, UNIX_EPOCH};
        let (usd, shares) = snap_amounts(price, size);
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        PolymarketOrder {
            salt: rand_salt(),
            maker: self.proxy_wallet.clone(),
            signer: self.order_signer(),
            taker: "0x0000000000000000000000000000000000000000".into(),
            token_id: token_id.into(),
            // BUY: makerAmount = USD spent, takerAmount = shares received.
            maker_amount: self.to_base_units(usd),
            taker_amount: self.to_base_units(shares),
            expiration: 0,
            // V2: timestamp_ms replaces nonce for per-address order uniqueness.
            timestamp_ms,
            metadata: [0u8; 32],
            builder: [0u8; 32],
            side: 0,
            signature_type: self.signature_type,
        }
    }

    /// Marketable-limit buy with an explicit venue order type.
    /// `post_only=false` for anything intended to cross — including
    /// the GTC-with-cancel fallback, which is a *marketable* limit and would be
    /// venue-rejected under postOnly. Enforces the venue LIMIT-order floor
    /// (≥ 5 shares, operator-confirmed 2026-06-02) before signing.
    pub async fn place_buy_with_order_type(
        &self,
        token_id: &str,
        price: f64,
        shares: f64,
        order_type: OrderType,
        post_only: bool,
    ) -> Result<RestingOrderId> {
        if shares < 5.0 {
            anyhow::bail!(
                "venue LIMIT-order floor is 5 shares; refusing {shares} sh on token {token_id}"
            );
        }
        let order = self.build_buy_order(token_id, price, shares);
        self.post_order(order, order_type.as_str(), post_only, None).await
    }

    pub(crate) async fn post_order(
        &self,
        order: PolymarketOrder,
        order_type: &str,
        post_only: bool,
        coin: Option<&str>,
    ) -> Result<RestingOrderId> {
        let domain = PolymarketOrderDomain {
            name: "Polymarket CTF Exchange".into(),
            version: "2".into(),
            chain_id: 137,
            verifying_contract: self.contracts.exchange.clone(),
        };
        let signature = self.signer.sign_order(&order, &domain)?;
        let body = OrderPostBody {
            order: order_to_json(&order, &signature),
            // B1: owner must be the API key UUID, not the proxy wallet hex.
            owner: self.creds.key.clone(),
            order_type: order_type.into(),
            // I5: postOnly=true on GTC resting bids; false for FOK takers.
            post_only,
        };
        let body_str = serde_json::to_string(&body)?;
        let path = "/order";
        let url = format!("{}{path}", self.base_url);
        let headers = auth_headers(&self.creds, "POST", path, &body_str)?;
        let mut req = self
            .http_for(coin)
            .post(&url)
            .header("content-type", "application/json")
            .body(body_str);
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        let t0 = std::time::Instant::now();
        let resp = req.send().await.with_context(|| format!("POST {url}"))?;
        // Tier-4: capture CF/origin diagnostics BEFORE consuming the body so
        // error paths keep them (branch fix 273ebb5) — slow or rejected posts
        // are exactly where the POP / origin-timing attribution matters.
        let diag = extract_http_diagnostics(&resp);
        let elapsed_ms = t0.elapsed().as_millis() as u64;
        tracing::debug!(
            elapsed_ms,
            version = diag.http_version.as_deref().unwrap_or(""),
            cf_ray = diag.cf_ray.as_deref().unwrap_or(""),
            cf_cache = diag.cf_cache_status.as_deref().unwrap_or(""),
            server_timing = diag.server_timing.as_deref().unwrap_or(""),
            "clob order POST round-trip"
        );
        let status = resp.status();
        if !status.is_success() {
            let txt = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "POST {url} -> HTTP {status}: {txt} [elapsed_ms={elapsed_ms} cf_ray={} http={}]",
                diag.cf_ray.as_deref().unwrap_or("-"),
                diag.http_version.as_deref().unwrap_or("-"),
            );
        }
        let parsed: OrderPostResponse = resp
            .json()
            .await
            .with_context(|| format!("parse POST {url} body"))?;
        // I7: 200 OK + success=false (e.g., fee_rate_bps mismatch) was
        // previously surfaced downstream as "empty orderID" rather than the
        // real reason. Bail with the server-provided message.
        if !parsed.success {
            let msg = parsed.error_msg.unwrap_or_else(|| "(no error_msg)".into());
            anyhow::bail!("POST {url} returned success=false: {msg}");
        }
        if let Some(im) =
            immediate_match_from_response(&parsed, &order, crate::types::now_micros())
        {
            tracing::info!(
                order_id = %im.order_id.as_str(),
                token = %im.token_id,
                shares = im.shares,
                usd = im.usd,
                elapsed_ms,
                "POST /order matched synchronously (pre-WS own-fill signal)"
            );
            let _ = self.immediate_match_tx.send(im);
        }
        Ok(RestingOrderId::from_string(parsed.order_id))
    }
}

/// One open order row from `GET /data/orders` (CONFIRMED live
/// 2026-08-01). Numeric fields arrive as
/// decimal strings ("0.16", "5", "0" — `size_matched:"0"` captured live);
/// `size_matched` is the partial-fill counter §4.7 restart reconciliation
/// consumes; `expiration` "0" = GTC. `created_at` is a number per the venue
/// docs, but the docs have lied twice on this endpoint family — kept as raw
/// JSON so a type flip can never fail the whole reconciliation read.
#[derive(Clone, Debug, Deserialize)]
pub struct OpenOrder {
    pub id: String,
    #[serde(default)]
    pub market: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub price: String,
    #[serde(default)]
    pub original_size: String,
    #[serde(default)]
    pub size_matched: String,
    #[serde(default)]
    pub outcome: String,
    #[serde(default, alias = "type")]
    pub order_type: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub created_at: serde_json::Value,
    #[serde(default)]
    pub expiration: String,
}

/// ⚠ The `/data/orders` response is a cursor ENVELOPE, not the flat array
/// the venue docs claim (doc lie caught live 2026-08-01): terminal cursor
/// `"LTE="`, 500/page — same scheme as builder-trades. A flat-array parser
/// would silently drop every page past 500.
#[derive(Debug, Deserialize)]
struct OpenOrdersEnvelope {
    data: Vec<OpenOrder>,
    next_cursor: String,
}

const OPEN_ORDERS_TERMINAL_CURSOR: &str = "LTE=";

/// Fold one `/data/orders` page into `out`. Returns the cursor to request
/// next, or `None` when this page was terminal. A non-JSON or non-envelope
/// body is an ERROR, never "no orders"; a cursor that fails to advance is
/// an ERROR, never a spin.
fn fold_open_orders_page(
    out: &mut Vec<OpenOrder>,
    body: &str,
    cursor_sent: Option<&str>,
) -> Result<Option<String>> {
    let env: OpenOrdersEnvelope = serde_json::from_str(body).map_err(|e| {
        let head: String = body.chars().take(200).collect();
        anyhow::anyhow!(
            "GET /data/orders body is not the cursor envelope {{data, next_cursor, ..}} \
             ({e}); body starts: {head}"
        )
    })?;
    out.extend(env.data);
    if env.next_cursor == OPEN_ORDERS_TERMINAL_CURSOR || env.next_cursor.is_empty() {
        return Ok(None);
    }
    if Some(env.next_cursor.as_str()) == cursor_sent {
        anyhow::bail!(
            "GET /data/orders next_cursor did not advance (stuck at {:?}) — refusing to spin",
            env.next_cursor
        );
    }
    Ok(Some(env.next_cursor))
}

impl LiveClob {
    /// Open orders for this API key via `GET /data/orders` — the §4.7
    /// restart-reconciliation read (LiveClob had `cancel_orders` but no
    /// getter until Phase 2). L2 HMAC signature message is
    /// `timestamp + "GET" + "/data/orders"`: query params are NOT signed;
    /// the optional `market` (condition id) filter rides in the query
    /// string only. Follows `next_cursor` to the terminal `"LTE="`.
    pub async fn open_orders(&self, market: Option<&str>) -> Result<Vec<OpenOrder>> {
        let path = "/data/orders";
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut url = format!("{}{path}", self.base_url);
            let mut sep = '?';
            if let Some(m) = market {
                url.push(sep);
                url.push_str("market=");
                url.push_str(m);
                sep = '&';
            }
            if let Some(c) = &cursor {
                url.push(sep);
                url.push_str("next_cursor=");
                url.push_str(c);
            }
            let headers = auth_headers(&self.creds, "GET", path, "")?;
            let mut req = self.http.get(&url);
            for (k, v) in &headers {
                req = req.header(k, v);
            }
            let resp = req.send().await.with_context(|| format!("GET {url}"))?;
            let status = resp.status();
            let txt = resp
                .text()
                .await
                .with_context(|| format!("read GET {url} body"))?;
            if !status.is_success() {
                anyhow::bail!("GET {url} -> HTTP {status}: {txt}");
            }
            match fold_open_orders_page(&mut out, &txt, cursor.as_deref())? {
                Some(next) => cursor = Some(next),
                None => return Ok(out),
            }
        }
    }

    /// Cancel every open order on ONE market — `DELETE /cancel-market-orders`
    /// with body `{"market": <condition id>}` (venue fact pinned 2026-08-01).
    /// Returns how many the venue reports cancelled.
    ///
    /// Off the hot path by construction: this is the close sweep, the cold
    /// restart and the kill-switch halt, none of which are latency-sensitive.
    /// An HTTP failure is an ERROR — reporting a clean sweep that did not
    /// happen would leave live bids resting.
    pub async fn cancel_market_orders(&self, market: &str) -> Result<usize> {
        let body_str = serde_json::json!({ "market": market }).to_string();
        let path = "/cancel-market-orders";
        let url = format!("{}{path}", self.base_url);
        let headers = auth_headers(&self.creds, "DELETE", path, &body_str)?;
        let mut req = self
            .http
            .delete(&url)
            .header("content-type", "application/json")
            .body(body_str);
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        let resp = req.send().await.with_context(|| format!("DELETE {url}"))?;
        let status = resp.status();
        let txt = resp
            .text()
            .await
            .with_context(|| format!("read DELETE {url} body"))?;
        if !status.is_success() {
            anyhow::bail!("DELETE {url} -> HTTP {status}: {txt}");
        }
        let parsed: CancelResponse = serde_json::from_str(&txt)
            .with_context(|| format!("parse DELETE {url} body: {txt}"))?;
        if !parsed.not_canceled.is_empty() {
            tracing::info!(
                market,
                not_canceled = ?parsed.not_canceled,
                "cancel-market-orders left ids uncancelled (already filled or cancelled)"
            );
        }
        Ok(parsed.canceled.len())
    }
}

#[derive(Debug, Default, Deserialize)]
struct CancelResponse {
    #[serde(default)]
    canceled: Vec<String>,
    #[serde(default)]
    not_canceled: serde_json::Map<String, serde_json::Value>,
}

/// Venue time-in-force for limit orders (py-clob-client parity: GTC, GTD,
/// FOK, FAK). FAK = marketable limit that fills what is displayed and kills
/// the remainder (partials allowed); FOK is all-or-nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderType {
    Gtc,
    Gtd,
    Fok,
    Fak,
}

impl OrderType {
    pub fn as_str(self) -> &'static str {
        match self {
            OrderType::Gtc => "GTC",
            OrderType::Gtd => "GTD",
            OrderType::Fok => "FOK",
            OrderType::Fak => "FAK",
        }
    }
}

#[derive(Serialize)]
struct OrderPostBody {
    order: OrderJson,
    #[serde(rename = "orderType")]
    order_type: String,
    owner: String,
    /// I5: `true` for GTC resting bids guarantees the order never
    /// accidentally crosses and pays taker fee. `false` for FOK takers.
    #[serde(rename = "postOnly")]
    post_only: bool,
}

#[derive(Serialize)]
struct OrderJson {
    /// V2 wire format: salt is a JSON number (u64), not a string.
    /// Confirmed against polymarket_client_sdk_v2 OrderV2WithSignature
    /// (Polymarket/rs-clob-client-v2, src/clob/types/mod.rs ser_salt →
    /// serializer.serialize_u64). Sending a string yields HTTP 400
    /// "Invalid order payload".
    salt: u64,
    maker: String,
    signer: String,
    #[serde(rename = "tokenId")]
    token_id: String,
    #[serde(rename = "makerAmount")]
    maker_amount: String,
    #[serde(rename = "takerAmount")]
    taker_amount: String,
    side: String,
    /// V2: still in wire body for GTD support; not in the signed struct.
    /// Recon doesn't use GTD; always "0".
    expiration: String,
    #[serde(rename = "signatureType")]
    signature_type: u8,
    /// V2: order creation time in ms (decimal string).
    timestamp: String,
    /// V2: bytes32 hex with 0x prefix; recon defaults to all-zero.
    metadata: String,
    /// V2: bytes32 hex with 0x prefix; recon defaults to all-zero (no builder code).
    builder: String,
    signature: String,
}

fn order_to_json(o: &PolymarketOrder, signature: &str) -> OrderJson {
    OrderJson {
        salt: o.salt,
        maker: o.maker.clone(),
        signer: o.signer.clone(),
        token_id: o.token_id.clone(),
        maker_amount: o.maker_amount.to_string(),
        taker_amount: o.taker_amount.to_string(),
        side: if o.side == 0 {
            "BUY".into()
        } else {
            "SELL".into()
        },
        expiration: o.expiration.to_string(),
        signature_type: o.signature_type,
        timestamp: o.timestamp_ms.to_string(),
        metadata: format!("0x{}", hex::encode(o.metadata)),
        builder: format!("0x{}", hex::encode(o.builder)),
        signature: signature.into(),
    }
}

#[derive(Deserialize, Default)]
struct OrderPostResponse {
    #[serde(default)]
    success: bool,
    #[serde(rename = "orderID", default)]
    order_id: String,
    /// I7: surface server-side rejection reasons (e.g., "fee rate mismatch")
    /// instead of presenting them as "empty orderID" downstream. Polymarket's
    /// Go backend serializes camelCase; alias `error_msg` for python-style.
    #[serde(rename = "errorMsg", alias = "error_msg", default)]
    error_msg: Option<String>,
    /// Immediate-match info (avg-price latency investigation 2026-06-13).
    /// status ∈ {live, matched, delayed}; "matched" = the venue matched (part
    /// of) the order synchronously — the only sub-WS own-fill signal that
    /// exists. Amounts are DECIMAL strings; the official docs called them
    /// base-1e6 fixed-math, and they are not (see `immediate_match_from_response`).
    #[serde(default)]
    status: Option<String>,
    #[serde(rename = "makingAmount", default)]
    making_amount: Option<String>,
    #[serde(rename = "takingAmount", default)]
    taking_amount: Option<String>,
}

/// Decode a synchronous match from a POST /order response. `None` unless
/// `status == "matched"` with a positive matched amount. BUY (side 0): maker
/// provides USDC, taking side is shares; SELL inverts.
///
/// Amounts are DECIMAL strings ("3.75", "187.5") — not base-1e6 integers, and
/// not symmetric with the outgoing order, which really is signed in base units.
/// Verified 2026-07-29 against 9/9 live taker-tickburst matches cross-checked
/// with the user-WS TRADE size; the raw strings carry fractional parts
/// ("187.41"), which a base-1e6 integer never could.
fn immediate_match_from_response(
    parsed: &OrderPostResponse,
    order: &PolymarketOrder,
    now_us: u64,
) -> Option<ImmediateMatch> {
    if parsed.status.as_deref() != Some("matched") {
        return None;
    }
    // `"NaN"` / `"inf"` parse happily as f64, and neither is caught downstream: NaN fails
    // every comparison (including `shares <= 0.0`) and +inf passes it.
    let amt = |s: &Option<String>| {
        s.as_deref().and_then(|v| v.parse::<f64>().ok()).filter(|v| v.is_finite())
    };
    let making = amt(&parsed.making_amount)?;
    let taking = amt(&parsed.taking_amount)?;
    // Regression guard on the amount FORMAT, not a fill check. The maker leg is what
    // we give up, and the venue can never take more than we signed: a BUY pays at most
    // `maker_amount` USDC, a SELL delivers at most `maker_amount` shares. Bounding the
    // TAKER leg would be wrong — price improvement routinely returns more shares than
    // ordered (250 sh @3c ordered -> 750 sh @1c filled, live 2026-07-29).
    // (`amt` has already guaranteed both legs are finite, so a plain comparison is safe.)
    let maker_signed = order.maker_amount as f64 / 1e6;
    if making > maker_signed * (1.0 + 1e-6) {
        tracing::error!(
            order_id = %parsed.order_id,
            making,
            taking,
            maker_signed,
            "POST /order match amounts exceed the signed maker amount — refusing to decode \
             (venue amount format changed?); falling back to the user-WS fill"
        );
        return None;
    }
    let (shares, usd) = if order.side == 0 { (taking, making) } else { (making, taking) };
    if shares <= 0.0 {
        return None;
    }
    Some(ImmediateMatch {
        order_id: RestingOrderId::from_string(parsed.order_id.clone()),
        token_id: order.token_id.clone(),
        shares,
        usd,
        ts_micros: now_us,
    })
}

/// Parse a hex-encoded uint256 (with or without `0x`) into micro-USDC.
/// Saturates to `u64::MAX` for values above u64 (e.g. max-uint256 from a
/// "Enable Trading" allowance default that could leak into a balance read
/// in pathological cases). The balance-poll task feeds this into
/// `Guards::on_balance_sample` which expects a `u64` micro count.
fn parse_balance_hex_to_micro(hex: &str) -> Result<u64> {
    let trimmed = hex.strip_prefix("0x").unwrap_or(hex);
    if trimmed.is_empty() {
        anyhow::bail!("empty hex balance string");
    }
    if let Ok(n) = u64::from_str_radix(trimmed, 16) {
        return Ok(n);
    }
    // u128 covers ≤ ~3.4e38 — sufficient to detect overflow above u64.
    if u128::from_str_radix(trimmed, 16).is_ok() {
        return Ok(u64::MAX);
    }
    // Genuinely unparseable hex (non-hex chars, etc.) → saturate. We never
    // want a parse error to halt the balance-poll task; the caller already
    // logs warnings on failure.
    Ok(u64::MAX)
}

/// Pad a 0x-prefixed 20-byte address to a 32-byte hex string for ABI calls.
fn pad_addr(a: &str) -> String {
    let trimmed = a.strip_prefix("0x").unwrap_or(a);
    format!("000000000000000000000000{}", trimmed.to_lowercase())
}

/// Cryptographically-random salt masked to 2^53 - 1.
///
/// B7: macOS empirical 97% collision rate on `SystemTime::as_micros()`.
/// B8: Polymarket Go backend decodes salt via float64; values > 2^53
/// truncate silently → orderHash mismatch → InvalidSignature. The 53-bit
/// mask matches rs-clob-client/src/clob/order_builder.rs:482-485 (their
/// canonical Rust client pins this as a unit-test invariant).
fn rand_salt() -> u64 {
    use rand::RngCore;
    let mut buf = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut buf);
    u64::from_le_bytes(buf) & ((1u64 << 53) - 1)
}

impl ClobClient for LiveClob {
    fn place_resting_bid(
        &self,
        bid: RestingBid,
    ) -> impl std::future::Future<Output = Result<RestingOrderId>> + Send {
        async move {
            let order = self.build_buy_order(&bid.token_id, bid.price, bid.size_shares);
            // I5: GTC resting bid → postOnly=true (LIP-eligible).
            self.post_order(order, "GTC", true, Some(bid.coin.as_str())).await
        }
    }

    fn place_resting_ask(
        &self,
        _ask: RestingAsk,
    ) -> impl std::future::Future<Output = Result<RestingOrderId>> + Send {
        async {
            anyhow::bail!(
                "LiveClob::place_resting_ask: pre-merge asks excised (LIP-farmer is BUY-only)"
            )
        }
    }

    fn cancel_order(
        &self,
        id: &RestingOrderId,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let id_str = id.as_str().to_string();
        let creds = self.creds.clone();
        let http = self.http.clone();
        let base_url = self.base_url.clone();
        async move {
            let body_str = serde_json::json!({"orderID": id_str}).to_string();
            let path = "/order";
            let url = format!("{base_url}{path}");
            let headers = auth_headers(&creds, "DELETE", path, &body_str)?;
            let mut req = http
                .delete(&url)
                .header("content-type", "application/json")
                .body(body_str);
            for (k, v) in &headers {
                req = req.header(k, v);
            }
            let resp = req.send().await.with_context(|| format!("DELETE {url}"))?;
            let status = resp.status();
            if !status.is_success() {
                let txt = resp.text().await.unwrap_or_default();
                // 404 on already-filled / already-cancelled is idempotent OK.
                if status.as_u16() == 404 {
                    return Ok(());
                }
                anyhow::bail!("DELETE {url} -> HTTP {status}: {txt}");
            }
            Ok(())
        }
    }

    fn cancel_orders(
        &self,
        ids: &[RestingOrderId],
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let ids: Vec<String> = ids.iter().map(|id| id.as_str().to_string()).collect();
        let creds = self.creds.clone();
        let http = self.http.clone();
        let base_url = self.base_url.clone();
        async move {
            // Venue batch endpoint: DELETE /orders, body = ID-array, ≤3000 ids per call
            // (operator-verified 2026-05-12). The 2xx response carries {canceled,
            // not_canceled}; not_canceled covers already-filled / already-cancelled ids —
            // idempotent-OK per the single-order 404 contract, logged for audit only.
            for chunk in ids.chunks(3000) {
                let body_str = serde_json::to_string(chunk)?;
                let path = "/orders";
                let url = format!("{base_url}{path}");
                let headers = auth_headers(&creds, "DELETE", path, &body_str)?;
                let mut req = http
                    .delete(&url)
                    .header("content-type", "application/json")
                    .body(body_str);
                for (k, v) in &headers {
                    req = req.header(k, v);
                }
                let resp = req.send().await.with_context(|| format!("DELETE {url}"))?;
                let status = resp.status();
                let txt = resp.text().await.unwrap_or_default();
                if !status.is_success() {
                    anyhow::bail!("DELETE {url} -> HTTP {status}: {txt}");
                }
                tracing::debug!(batch = chunk.len(), response = %txt, "batch cancel executed");
            }
            Ok(())
        }
    }

    fn place_taker_buy(
        &self,
        taker: TakerBuy,
    ) -> impl std::future::Future<Output = Result<Vec<Fill>>> + Send {
        async move {
            let order = self.build_buy_order(&taker.token_id, taker.max_ask, taker.max_shares);
            // FOK = fill or kill. Polymarket reports trade events back via
            // the user-channel WS (Task B4); the synchronous response gives
            // us the order ID but no fill detail. Return empty Vec — the
            // trait's "0 means no fill arrived synchronously" convention.
            // FOK takers cannot postOnly by definition.
            let id = self.post_order(order, "FOK", false, Some(taker.coin.as_str())).await?;
            let _ = id;
            Ok(Vec::new())
        }
    }

    fn fills_rx(&self) -> broadcast::Receiver<Fill> {
        self.fills_tx.subscribe()
    }
}

// Suppress unused-import warnings while Side is referenced in trait body
const _: fn() = || {
    let _: Side = Side::Up;
};

#[cfg(test)]
mod tests {
    use super::{parse_balance_hex_to_micro, pick_order_signer, rand_salt};
    use std::collections::HashSet;

    const PROXY: &str = "0xd9d24e482c11f586cd9a1a53dc3eec6de3883862";
    const EOA: &str = "0x70997970c51812dc3a010c7d01b50e0d17dc79c8";

    #[test]
    fn poly_1271_orders_are_signed_by_the_deposit_wallet_not_the_eoa() {
        // Sending the EOA here is what the venue rejects with
        // "the order signer address has to be the address of the API KEY".
        assert_eq!(pick_order_signer(3, PROXY, EOA), PROXY);
    }

    #[test]
    fn non_1271_orders_keep_signing_as_the_eoa() {
        // Regression guard for the working non-1271 signing lineage.
        for sig_type in [0u8, 1, 2] {
            assert_eq!(pick_order_signer(sig_type, PROXY, EOA), EOA);
        }
    }

    #[test]
    fn parse_balance_hex_zero_yields_zero() {
        assert_eq!(parse_balance_hex_to_micro("0x0").unwrap(), 0);
    }

    #[test]
    fn parse_balance_hex_150_usdc_yields_150_million_micro() {
        // 150 USDC.e at 6 decimals = 150_000_000 micro = 0x8f0d180
        assert_eq!(
            parse_balance_hex_to_micro("0x8f0d180").unwrap(),
            150_000_000
        );
    }

    #[test]
    fn parse_balance_hex_saturates_above_u64_max() {
        // max-uint256 (Polymarket "Enable Trading" allowance default in
        // some flows) must NOT panic. For balance reads we saturate to
        // u64::MAX rather than INFINITY since the consumer expects u64.
        let max_u256 = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        assert_eq!(parse_balance_hex_to_micro(max_u256).unwrap(), u64::MAX);
    }

    #[test]
    fn rand_salt_is_under_2_pow_53() {
        // B8: Polymarket Go backend decodes salt via float64, which loses
        // precision above 2^53. rs-clob-client masks salt to u53.
        let max = 1u64 << 53;
        for _ in 0..1000 {
            let s = rand_salt();
            assert!(s < max, "salt {s} >= 2^53");
        }
    }

    #[test]
    fn rand_salt_has_no_collisions_in_10k_samples() {
        // B7: empirical macOS time-based salt had 97.4% collision rate.
        // CSPRNG over 53 bits is uniform; 10k samples should hit zero
        // collisions (birthday-bound: 50% at sqrt(2^53) ≈ 95M samples).
        let mut seen = HashSet::new();
        for _ in 0..10_000 {
            assert!(seen.insert(rand_salt()), "duplicate salt observed");
        }
    }

    #[test]
    fn snap_amounts_handles_slot_1777320000_rejection_inputs() {
        let (usd, shares) = super::snap_amounts(0.41, 21.63);
        // FP-tolerant 2dp check: |usd*100 - round(usd*100)| < eps.
        assert!(
            ((usd * 100.0).round() - usd * 100.0).abs() < 1e-6,
            "USD not 2dp: {usd}"
        );
        assert!((usd - 8.87).abs() < 1e-9, "USD ≠ 8.87: {usd}");
        assert!(
            ((shares * 10_000.0).round() - shares * 10_000.0).abs() < 1e-6,
            "shares not 4dp: {shares}"
        );
        assert!(
            (shares * 0.41 - usd).abs() < 0.005,
            "internal inconsistency: shares*price {} vs usd {}",
            shares * 0.41,
            usd
        );
    }

    #[test]
    fn snap_amounts_already_clean_is_idempotent() {
        let (usd, shares) = super::snap_amounts(0.50, 10.0);
        assert!((usd - 5.00).abs() < 1e-9);
        assert!((shares - 10.0).abs() < 1e-9);
    }

    #[test]
    fn snap_amounts_round_to_2dp_on_usd() {
        let (usd, _) = super::snap_amounts(0.333, 9.0);
        assert!((usd - 3.00).abs() < 1e-9, "got {usd}");
    }

    // --- POST /order immediate-match parse (avg-price latency investigation 2026-06-13) ---
    // status ∈ {live, matched, delayed}. makingAmount/takingAmount are DECIMAL strings
    // ("3.75", "187.5") — NOT base-1e6 integers, whatever the docs said in 2026-06-13.
    // Every fixture below is a real response shape back-solved from the taker-tickburst
    // live logs of 2026-07-29 (9/9 matches cross-checked against the user-WS TRADE size).

    fn test_buy_order(token: &str) -> crate::execution::signer::PolymarketOrder {
        crate::execution::signer::PolymarketOrder {
            salt: 1,
            maker: "0xmaker".into(),
            signer: "0xsigner".into(),
            taker: "0x0000000000000000000000000000000000000000".into(),
            token_id: token.into(),
            maker_amount: 4_000_000, // $4.00 USDC
            taker_amount: 8_000_000, // 8 shares
            expiration: 0,
            timestamp_ms: 0,
            metadata: [0u8; 32],
            builder: [0u8; 32],
            side: 0, // BUY
            signature_type: 1,
        }
    }

    /// A signed BUY with an explicit USD budget / share count, in base units (the
    /// outgoing wire format, which IS base-1e6 — only the response is decimal).
    fn test_buy_order_sized(
        token: &str,
        usd: f64,
        shares: f64,
    ) -> crate::execution::signer::PolymarketOrder {
        let mut o = test_buy_order(token);
        o.maker_amount = (usd * 1e6).round() as u128;
        o.taker_amount = (shares * 1e6).round() as u128;
        o
    }

    #[test]
    fn order_post_response_parses_match_fields() {
        let json = r#"{
            "success": true,
            "orderID": "0xabc",
            "status": "matched",
            "makingAmount": "3.75",
            "takingAmount": "187.5",
            "transactionsHashes": ["0xdead"],
            "tradeIDs": ["t1"],
            "errorMsg": ""
        }"#;
        let parsed: super::OrderPostResponse = serde_json::from_str(json).unwrap();
        assert!(parsed.success);
        assert_eq!(parsed.status.as_deref(), Some("matched"));
        assert_eq!(parsed.making_amount.as_deref(), Some("3.75"));
        assert_eq!(parsed.taking_amount.as_deref(), Some("187.5"));
    }

    #[test]
    fn order_post_response_without_match_fields_still_parses() {
        // The pre-2026-06-13 minimal shape (and FAK 400 bodies) must keep parsing.
        let json = r#"{"success": true, "orderID": "0xabc"}"#;
        let parsed: super::OrderPostResponse = serde_json::from_str(json).unwrap();
        assert!(parsed.status.is_none());
    }

    #[test]
    fn immediate_match_matched_buy_maps_taking_to_shares() {
        // test_buy_order signs $4.00 for 8 shares (50c). A full match returns those
        // amounts as decimals.
        let parsed: super::OrderPostResponse = serde_json::from_str(
            r#"{"success":true,"orderID":"0xabc","status":"matched","makingAmount":"4","takingAmount":"8"}"#,
        )
        .unwrap();
        let im = super::immediate_match_from_response(&parsed, &test_buy_order("tok"), 123)
            .expect("matched response must yield an ImmediateMatch");
        assert_eq!(im.order_id.as_str(), "0xabc");
        assert_eq!(im.token_id, "tok");
        assert!((im.shares - 8.0).abs() < 1e-9, "shares {}", im.shares);
        assert!((im.usd - 4.0).abs() < 1e-9, "usd {}", im.usd);
        assert_eq!(im.ts_micros, 123);
    }

    #[test]
    fn immediate_match_decodes_live_decimal_amounts() {
        // Captured live 2026-07-29, taker-tickburst order 0x3445a6d4…: signed 125 sh @ a 3c
        // limit ($3.75 budget), filled 187.5 sh @ 2c. Cross-checked against the user-WS
        // TRADE (size="187.50" price="0.02"). Decoding these as base-1e6 yields 1.875e-4
        // shares — a millionth of the truth, and far below the venue's 5-share floor.
        let parsed: super::OrderPostResponse = serde_json::from_str(
            r#"{"success":true,"orderID":"0x3445","status":"matched","makingAmount":"3.75","takingAmount":"187.5"}"#,
        )
        .unwrap();
        let im =
            super::immediate_match_from_response(&parsed, &test_buy_order_sized("tok", 3.75, 125.0), 0)
                .expect("a real live matched response must decode");
        assert!((im.shares - 187.5).abs() < 1e-9, "shares {}", im.shares);
        assert!((im.usd - 3.75).abs() < 1e-9, "usd {}", im.usd);
    }

    #[test]
    fn immediate_match_accepts_more_shares_than_ordered_on_price_improvement() {
        // Live order 0x611be6aa…: signed 250 sh @ a 3c limit, FILLED 750 sh @ 1c. The venue
        // spends the signed USD budget, so a better price buys MORE shares than ordered —
        // 8 of the 9 live matches did this. Any bound on the ordered SHARE count is wrong;
        // only the maker leg (what we give up) is capped by what we signed.
        let parsed: super::OrderPostResponse = serde_json::from_str(
            r#"{"success":true,"orderID":"0x611b","status":"matched","makingAmount":"7.5","takingAmount":"750"}"#,
        )
        .unwrap();
        let im =
            super::immediate_match_from_response(&parsed, &test_buy_order_sized("tok", 7.50, 250.0), 0)
                .expect("price-improved fills must still decode");
        assert!((im.shares - 750.0).abs() < 1e-9, "shares {}", im.shares);
        assert!((im.usd - 7.5).abs() < 1e-9, "usd {}", im.usd);
    }

    #[test]
    fn immediate_match_partial_buy_uses_response_amounts_not_order_size() {
        // Live order 0x82de3a3e…: signed 2500 sh @ a 0.3c limit ($7.50 budget); only
        // $3.6271 matched (3627.1 sh @ 0.1c). The response carries the MATCHED portion,
        // which here both under-spends the budget and over-shoots the ordered share count.
        let parsed: super::OrderPostResponse = serde_json::from_str(
            r#"{"success":true,"orderID":"0x82de","status":"matched","makingAmount":"3.6271","takingAmount":"3627.1"}"#,
        )
        .unwrap();
        let im =
            super::immediate_match_from_response(&parsed, &test_buy_order_sized("tok", 7.50, 2500.0), 0)
                .unwrap();
        assert!((im.shares - 3627.1).abs() < 1e-9, "shares {}", im.shares);
        assert!((im.usd - 3.6271).abs() < 1e-9, "usd {}", im.usd);
    }

    #[test]
    fn immediate_match_rejects_amounts_above_the_signed_maker_amount() {
        // REGRESSION GUARD: if the venue ever reverts to base-1e6 integer strings, reading
        // them as decimals would over-book by 1e6x — the dangerous direction for a
        // provisional-exposure consumer. Fail closed (None) so the caller falls back to the
        // user-WS fill instead of silently mis-booking.
        let parsed: super::OrderPostResponse = serde_json::from_str(
            r#"{"success":true,"orderID":"0xabc","status":"matched","makingAmount":"3750000","takingAmount":"187500000"}"#,
        )
        .unwrap();
        assert!(super::immediate_match_from_response(
            &parsed,
            &test_buy_order_sized("tok", 3.75, 125.0),
            0
        )
        .is_none());
    }

    #[test]
    fn immediate_match_rejects_non_finite_amounts() {
        // "NaN" and "inf" both parse as f64; neither must reach inventory math. The maker
        // leg is caught by the bound (written so NaN fails it rather than slipping through
        // a `>` comparison).
        for amounts in [
            r#""makingAmount":"NaN","takingAmount":"NaN""#,
            r#""makingAmount":"inf","takingAmount":"1""#,
        ] {
            let json =
                format!(r#"{{"success":true,"orderID":"0xabc","status":"matched",{amounts}}}"#);
            let parsed: super::OrderPostResponse = serde_json::from_str(&json).unwrap();
            assert!(
                super::immediate_match_from_response(
                    &parsed,
                    &test_buy_order_sized("tok", 3.75, 125.0),
                    0
                )
                .is_none(),
                "must reject {amounts}"
            );
        }
    }

    #[test]
    fn immediate_match_rejects_non_finite_taker_leg() {
        // The maker-side bound cannot catch a bad TAKER leg, and `shares <= 0.0` does not
        // either: NaN fails every comparison, and +inf passes. On a BUY that leg IS shares,
        // so an unguarded non-finite value would poison the consumer's provisional pool.
        for taking in ["NaN", "inf"] {
            let json = format!(
                r#"{{"success":true,"orderID":"0xabc","status":"matched","makingAmount":"3.75","takingAmount":"{taking}"}}"#
            );
            let parsed: super::OrderPostResponse = serde_json::from_str(&json).unwrap();
            assert!(
                super::immediate_match_from_response(
                    &parsed,
                    &test_buy_order_sized("tok", 3.75, 125.0),
                    0
                )
                .is_none(),
                "must reject takingAmount={taking}"
            );
        }
    }

    #[test]
    fn immediate_match_live_status_is_none() {
        let parsed: super::OrderPostResponse = serde_json::from_str(
            r#"{"success":true,"orderID":"0xabc","status":"live","makingAmount":"0","takingAmount":"0"}"#,
        )
        .unwrap();
        assert!(super::immediate_match_from_response(&parsed, &test_buy_order("tok"), 0).is_none());
    }

    #[test]
    fn immediate_match_matched_zero_amount_is_none() {
        let parsed: super::OrderPostResponse = serde_json::from_str(
            r#"{"success":true,"orderID":"0xabc","status":"matched","makingAmount":"0","takingAmount":"0"}"#,
        )
        .unwrap();
        assert!(super::immediate_match_from_response(&parsed, &test_buy_order("tok"), 0).is_none());
    }

    // --- GET /data/orders cursor envelope ---
    // The venue docs claim a flat array; the live response is an envelope
    // {data, next_cursor, limit, count} with terminal cursor "LTE="
    // (caught live 2026-08-01).

    /// The exact count-0 body captured live 2026-08-01 09:46Z.
    #[test]
    fn open_orders_count_zero_envelope_parses_empty_and_terminal() {
        let body = r#"{"data": [], "next_cursor": "LTE=", "limit": 500, "count": 0}"#;
        let mut out = Vec::new();
        let next = super::fold_open_orders_page(&mut out, body, None).unwrap();
        assert!(out.is_empty());
        assert!(next.is_none(), "LTE= is terminal");
    }

    #[test]
    fn open_orders_single_row_envelope_parses_fields() {
        // Field inventory per L21.28 (docs pass) with the live-confirmed
        // string forms (size_matched:"0" captured on the canary A order).
        let body = r#"{"data": [{
            "id": "0x6359ab1e",
            "market": "0x0a1aaa0f",
            "asset_id": "71321045679252212594626385532706912750332728571942532289631379312455583992563",
            "owner": "00000000-0000-0000-0000-000000000001",
            "maker_address": "0x365f0ca36ae1f641e02fe3b7743673da42a13a70",
            "side": "BUY",
            "price": "0.16",
            "original_size": "5",
            "size_matched": "0",
            "outcome": "Down",
            "order_type": "GTC",
            "status": "LIVE",
            "associate_trades": [],
            "created_at": 1754041000,
            "expiration": "0"
        }], "next_cursor": "LTE=", "limit": 500, "count": 1}"#;
        let mut out = Vec::new();
        let next = super::fold_open_orders_page(&mut out, body, None).unwrap();
        assert!(next.is_none());
        assert_eq!(out.len(), 1);
        let o = &out[0];
        assert_eq!(o.id, "0x6359ab1e");
        assert_eq!(o.market, "0x0a1aaa0f");
        assert_eq!(o.side, "BUY");
        assert_eq!(o.price, "0.16");
        assert_eq!(o.original_size, "5");
        assert_eq!(o.size_matched, "0");
        assert_eq!(o.outcome, "Down");
        assert_eq!(o.order_type, "GTC");
        assert_eq!(o.status, "LIVE");
        assert_eq!(o.expiration, "0", "expiration \"0\" = GTC");
    }

    #[test]
    fn open_orders_multi_page_synthetic_accumulates_and_follows_cursor() {
        let page1 = r#"{"data": [{"id": "0xa"}, {"id": "0xb"}], "next_cursor": "NTAw", "limit": 500, "count": 2}"#;
        let page2 = r#"{"data": [{"id": "0xc"}], "next_cursor": "LTE=", "limit": 500, "count": 1}"#;
        let mut out = Vec::new();
        let next = super::fold_open_orders_page(&mut out, page1, None).unwrap();
        assert_eq!(next.as_deref(), Some("NTAw"), "non-terminal cursor must be followed");
        let next2 = super::fold_open_orders_page(&mut out, page2, next.as_deref()).unwrap();
        assert!(next2.is_none());
        let ids: Vec<&str> = out.iter().map(|o| o.id.as_str()).collect();
        assert_eq!(ids, ["0xa", "0xb", "0xc"]);
    }

    /// The documented-but-wrong flat array must be an ERROR, never "no
    /// orders" — a flat-array reading would silently drop pages past 500.
    #[test]
    fn open_orders_flat_array_body_is_an_error_not_empty() {
        let mut out = Vec::new();
        let err = super::fold_open_orders_page(&mut out, "[]", None).unwrap_err();
        assert!(
            err.to_string().contains("not the cursor envelope"),
            "{err:#}"
        );
    }

    #[test]
    fn open_orders_non_json_body_is_an_error() {
        let mut out = Vec::new();
        assert!(super::fold_open_orders_page(&mut out, "<html>502</html>", None).is_err());
    }

    #[test]
    fn open_orders_stuck_cursor_is_an_error_not_a_spin() {
        let body = r#"{"data": [], "next_cursor": "NTAw", "limit": 500, "count": 0}"#;
        let mut out = Vec::new();
        let err = super::fold_open_orders_page(&mut out, body, Some("NTAw")).unwrap_err();
        assert!(err.to_string().contains("did not advance"), "{err:#}");
    }

    #[test]
    fn immediate_match_sell_maps_making_to_shares() {
        // SELL: maker provides shares, taker provides USDC — so on a SELL the maker leg is
        // the SHARE count, and it is that leg the signed maker_amount bounds. No crate
        // builds a SELL today; this pins the generic plumbing so a future resting-ask
        // strategy doesn't invert inventory.
        let mut order = test_buy_order_sized("tok", 4.0, 8.0);
        order.side = 1;
        let parsed: super::OrderPostResponse = serde_json::from_str(
            r#"{"success":true,"orderID":"0xabc","status":"matched","makingAmount":"4","takingAmount":"2"}"#,
        )
        .unwrap();
        let im = super::immediate_match_from_response(&parsed, &order, 0).unwrap();
        assert!((im.shares - 4.0).abs() < 1e-9, "shares {}", im.shares);
        assert!((im.usd - 2.0).abs() < 1e-9, "usd {}", im.usd);
    }
}
