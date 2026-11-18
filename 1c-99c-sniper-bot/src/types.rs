//! Shared value types used across the replicant.

use serde::{Deserialize, Serialize};

/// Which crypto asset a market tracks.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Coin {
    Btc,
    Eth,
    Sol,
    Xrp,
}

impl Coin {
    pub const ALL: [Coin; 4] = [Coin::Btc, Coin::Eth, Coin::Sol, Coin::Xrp];

    pub fn as_str(&self) -> &'static str {
        match self {
            Coin::Btc => "btc",
            Coin::Eth => "eth",
            Coin::Sol => "sol",
            Coin::Xrp => "xrp",
        }
    }

    pub fn binance_symbol(&self) -> &'static str {
        match self {
            Coin::Btc => "btcusdt",
            Coin::Eth => "ethusdt",
            Coin::Sol => "solusdt",
            Coin::Xrp => "xrpusdt",
        }
    }

    pub fn from_slug_prefix(s: &str) -> Option<Self> {
        match s {
            "btc" => Some(Coin::Btc),
            "eth" => Some(Coin::Eth),
            "sol" => Some(Coin::Sol),
            "xrp" => Some(Coin::Xrp),
            _ => None,
        }
    }

    /// Full lowercase name used in the 1 h ET slug format
    /// (`bitcoin-up-or-down-…`). XRP is the one coin whose "long" name
    /// happens to be the same token as its short name — Polymarket does not
    /// use "ripple" in these slugs (verified from on-chain activity data).
    pub fn long_name(&self) -> &'static str {
        match self {
            Coin::Btc => "bitcoin",
            Coin::Eth => "ethereum",
            Coin::Sol => "solana",
            Coin::Xrp => "xrp",
        }
    }

    pub fn from_long_name(s: &str) -> Option<Self> {
        match s {
            "bitcoin" => Some(Coin::Btc),
            "ethereum" => Some(Coin::Eth),
            "solana" => Some(Coin::Sol),
            "xrp" => Some(Coin::Xrp),
            _ => None,
        }
    }
}

/// Which side of an Up/Down market.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Up,
    Down,
}

impl Side {
    pub fn opposite(self) -> Self {
        match self {
            Side::Up => Side::Down,
            Side::Down => Side::Up,
        }
    }
}

/// A single observation of the underlying spot price.
#[derive(Clone, Copy, Debug)]
pub struct SpotTick {
    pub coin: Coin,
    pub price: f64,
    pub ts_micros: u64,
}

/// A single Binance `@aggTrade` print (compressed trade).
/// `exch_ts_ms` is Binance trade time `T`; `recv_ts_us` is stamped locally at
/// parse so consumers can reconstruct exchange→local clock skew.
#[derive(Clone, Copy, Debug)]
pub struct SpotTrade {
    pub coin: Coin,
    pub price: f64,
    pub qty: f64,
    /// Binance `m`: the BUYER was the resting (maker) side, i.e. the
    /// aggressor SOLD. Taker-buy flow is `!buyer_is_maker`.
    pub buyer_is_maker: bool,
    pub exch_ts_ms: u64,
    pub recv_ts_us: u64,
}

/// One CLOSED 1-second spot kline. `close_ts_ms` is the kline close time
/// (Binance `k.T`) — venue event time, join on this not local receive.
#[derive(Debug, Clone, PartialEq)]
pub struct SpotClose1s {
    pub coin: Coin,
    pub close_px: f64,
    pub close_ts_ms: u64,
}

/// Per-second log-return stdev from the σEstimator.
#[derive(Clone, Copy, Debug)]
pub struct Sigma {
    pub coin: Coin,
    pub value: f64,
    pub ts_micros: u64,
    /// True if `value` was clamped to `[SIGMA_MIN, SIGMA_MAX]` from the raw
    /// estimate. Consumers should treat clamped σ as degenerate and skip
    /// the fire decision for this tick.
    pub was_clamped: bool,
}

/// Opaque identifier for a resting limit order. Issued by the `ClobClient`
/// implementation (UUID in paper mode, CLOB order id in live mode). Owned by
/// the quoter so it can target the order for cancellation or replacement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RestingOrderId(String);

impl RestingOrderId {
    pub fn from_string(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Quoter-generated client order id. Threaded through OrderCommand →
/// submitter → IdAssignment so the quoter can identify which placement
/// an assignment corresponds to without depending on FIFO ordering of
/// the broadcast channel.
///
/// Process-wide monotonically increasing u64 from `AtomicU64`. Starts
/// at 1 (zero is reserved as "unset" / sentinel). Wraps at u64::MAX
/// after ~5×10¹¹ years at 1B placements/sec — never in practice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ClientOrderId(pub u64);

static COID_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

impl ClientOrderId {
    pub fn next() -> Self {
        Self(COID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}

/// Request to place a resting BUY limit order on Polymarket CLOB.
/// `price` is clamped in-band by the quoter; submitter does not re-clamp.
#[derive(Clone, Debug)]
pub struct RestingBid {
    pub slug: String,
    pub coin: Coin,
    pub side: Side,
    pub token_id: String,
    pub price: f64,
    pub size_shares: f64,
    pub tick_size: f64,
    pub ts_micros: u64,
}

impl RestingBid {
    /// Gross notional (before fee) = shares × price.
    #[inline]
    pub fn size_usd(&self) -> f64 {
        self.size_shares * self.price
    }
}

/// Request to place a resting SELL limit order on Polymarket CLOB against
/// owned inventory. Mirrors `RestingBid` but the share flow goes the other
/// way: matched bids on the book debit `shares` from the bot's position and
/// credit `price × shares` USDC. Pre-merge ask production was excised under
/// the BUY-only invariant (§H1); this type remains as scaffolding for the
/// broader ASK execution decision.
#[derive(Clone, Debug)]
pub struct RestingAsk {
    pub slug: String,
    pub coin: Coin,
    pub side: Side,
    pub token_id: String,
    pub price: f64,
    pub size_shares: f64,
    pub tick_size: f64,
    pub ts_micros: u64,
}

impl RestingAsk {
    /// Gross notional (before fee) = shares × price.
    #[inline]
    pub fn size_usd(&self) -> f64 {
        self.size_shares * self.price
    }
}

/// Request to cross the book and acquire up to `max_shares` at price ≤ `max_ask`.
/// The matcher walks asks ascending from best, accumulating fills until it hits
/// `max_shares` or the first level with price > `max_ask`. Partial fills are OK.
#[derive(Clone, Debug)]
pub struct TakerBuy {
    pub slug: String,
    pub coin: Coin,
    pub side: Side,
    pub token_id: String,
    pub max_ask: f64,
    pub max_shares: f64,
    pub ts_micros: u64,
}

/// Command sent from the quoter to the submitter.
///
/// `CancelBySlugSide` is the fallback path used when the quoter knows a bid
/// exists on `(slug, side)` but hasn't yet received the `IdAssignment` for
/// it (the fill-before-id-assignment race in `on_fill`). The submitter
/// resolves it via a secondary `(slug, side) → RestingOrderId` index kept
/// in lockstep with `cancel_index`.
///
/// `CancelBySlugSideWithAck` is a variant with an embedded oneshot used by
/// `cancel_asks_and_merge` (LIP-mimic §7.5) to gate the post-fill MERGE on
/// the cancel actually completing — the submitter signals `()` after the
/// CLOB cancel returns. The submitter still broadcasts the matching
/// `CancelAck` so non-ack-aware listeners are unaffected.
///
/// `Clone` is intentionally not derived — `oneshot::Sender` is move-only,
/// and nothing in the codebase clones an `OrderCommand` (verified via grep:
/// `Clone` is used on the inner payloads only). `Debug` is preserved via a
/// hand-written impl that elides the ack channel.
pub enum OrderCommand {
    PlaceResting {
        bid: RestingBid,
        coid: ClientOrderId,
    },
    PlaceRestingAsk {
        ask: RestingAsk,
        coid: ClientOrderId,
    },
    CancelResting(RestingOrderId),
    CancelBySlugSide {
        slug: String,
        side: Side,
    },
    /// Variant of `CancelBySlugSide` whose submitter signals completion on
    /// `ack` after the CLOB cancel returns. See `cancel_asks_and_merge` in
    /// `src/exec/merge.rs`.
    CancelBySlugSideWithAck {
        slug: String,
        side: Side,
        ack: tokio::sync::oneshot::Sender<()>,
    },
    /// Batch cancel via `ClobClient::cancel_orders` in ONE venue call (DELETE /orders on the
    /// live CLOB). Built for the shutdown drain; reusable for batch side-pulls / acked
    /// wind-down cancels. `ack` fires after the venue call returns — success or failure — so
    /// the sender can bound its drain wait. The ids come from the caller's own order ledger,
    /// which is authoritative over the submitter's `cancel_index` (a partially-filled order
    /// leaves the index on its first fill but still rests at the venue), so the handler must
    /// NOT filter the ids against the index.
    CancelBatchWithAck {
        ids: Vec<RestingOrderId>,
        ack: tokio::sync::oneshot::Sender<()>,
    },
    PlaceTaker(TakerBuy),
}

/// Triggered by the Phase 4 sibling-tree LockMerge branch (and Phase 5
/// Sweeper passive-sweep + post-resolution sweep). Consumer is the
/// Phase 5 capital Sweeper task. Lives in `domain` to avoid a circular
/// import between `quoter` and the future `sweeper` module.
#[derive(Debug, Clone)]
pub struct MergeIntent {
    pub slug: String,
    pub condition_id: String,
    pub pairs: f64,
    pub reason: MergeReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeReason {
    LockMerge,
    PassiveSweep,
    PostResolution,
}

impl std::fmt::Debug for OrderCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PlaceResting { bid, coid } => f
                .debug_struct("PlaceResting")
                .field("bid", bid)
                .field("coid", coid)
                .finish(),
            Self::PlaceRestingAsk { ask, coid } => f
                .debug_struct("PlaceRestingAsk")
                .field("ask", ask)
                .field("coid", coid)
                .finish(),
            Self::CancelResting(id) => f.debug_tuple("CancelResting").field(id).finish(),
            Self::CancelBySlugSide { slug, side } => f
                .debug_struct("CancelBySlugSide")
                .field("slug", slug)
                .field("side", side)
                .finish(),
            Self::CancelBySlugSideWithAck { slug, side, .. } => f
                .debug_struct("CancelBySlugSideWithAck")
                .field("slug", slug)
                .field("side", side)
                .field("ack", &"<oneshot::Sender>")
                .finish(),
            Self::CancelBatchWithAck { ids, .. } => f
                .debug_struct("CancelBatchWithAck")
                .field("ids", ids)
                .field("ack", &"<oneshot::Sender>")
                .finish(),
            Self::PlaceTaker(t) => f.debug_tuple("PlaceTaker").field(t).finish(),
        }
    }
}

/// Distinguishes a resting bid placement from a resting ask placement.
/// Used by `IdAssignment` so the quoter can route the assignment into
/// `live_bids[side]` vs `live_asks[side]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderKind {
    Bid,
    Ask,
}

/// Broadcast by `OrderSubmitter` after a successful `place_resting_bid` or
/// `place_resting_ask`, so the quoter that emitted the placement command
/// can learn the CLOB-assigned id for later cancellation. `kind` routes
/// the assignment into `live_bids[side]` vs `live_asks[side]`.
#[derive(Clone, Debug)]
pub struct IdAssignment {
    pub slug: String,
    pub side: Side,
    pub id: RestingOrderId,
    pub kind: OrderKind,
    /// Client order id assigned by the quoter at PlaceResting/PlaceRestingAsk
    /// dispatch time, echoed by the submitter on this IdAssignment so the
    /// quoter can identify the originating LiveLevel without FIFO assumptions.
    /// See spec §3 Item J.
    pub coid: ClientOrderId,
}

/// Broadcast by `OrderSubmitter` after a `cancel_order` call returns Ok (or after
/// the ClobClient reports the order was already gone — both count as "the bid is
/// definitely not live"). The quoter uses this to clear `cancel_pending[side]`.
#[derive(Clone, Debug)]
pub struct CancelAck {
    pub slug: String,
    pub side: Side,
    pub order_id: Option<RestingOrderId>,
}

/// Broadcast by `OrderSubmitter` when a `PlaceResting`/`PlaceRestingAsk` could not be placed —
/// dropped by `Guards` pre-wire or rejected by the venue. Mirrors `IdAssignment`, keyed by
/// `coid`, so the strategy loop can remove/tombstone the Pending rung occupying that price key
/// (REG-1: a stranded id-less rung darkens its key for the rest of the slot — 06-11 shakedown,
/// 31+25 rejects → 30+24 permanently-dark keys). The submitter sends exactly one of
/// `IdAssignment` / `PlaceReject` per place command, so a reject ack can never fight a late
/// success ack for the same coid.
#[derive(Clone, Debug)]
pub struct PlaceReject {
    pub slug: String,
    pub side: Side,
    pub coid: ClientOrderId,
    pub kind: OrderKind,
    /// Venue/transport error string, or the `GuardErr` description for pre-wire drops.
    pub reason: String,
}

/// Polymarket Liquidity Incentive Program (LIP) config for a market.
///
/// Polymarket Liquidity Incentive Program (LIP) config for a market.
///
/// `min_size` is the minimum order size (shares) required for that side of
/// the book to earn rewards. `max_spread` is how far from the mid-price a
/// quote can be and still qualify. `daily_rate` is the market's daily LIP
/// budget in USD (from `rewardsDailyRate` on the gamma response). A market
/// without a `rewards` sub-object in gamma comes back as `default()` —
/// `is_incentivised()` returns false, band-clipping is a no-op, and the
/// daily-rate gate defaults to "treat as $0".
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RewardsCfg {
    pub min_size: f64,
    pub max_spread: f64,
    pub daily_rate: f64,
}

impl RewardsCfg {
    pub fn is_incentivised(&self) -> bool {
        self.min_size > 0.0 || self.max_spread > 0.0 || self.daily_rate > 0.0
    }

    /// True iff this market's LIP budget meets the operator-configured
    /// minimum. A `threshold` of `0.0` disables the gate (any market passes).
    pub fn meets_min_daily_rate(&self, threshold_usd: f64) -> bool {
        if threshold_usd <= 0.0 {
            return true;
        }
        self.daily_rate >= threshold_usd
    }
}

/// Polymarket market metadata (fetched once from gamma-api per market).
#[derive(Clone, Debug)]
pub struct MarketMeta {
    pub slug: String,
    pub coin: Coin,
    /// Unix seconds when the market's window opens (s_0 sampled here).
    pub market_start_ts: u64,
    /// Unix seconds when the market resolves. Duration varies by family:
    /// 900 s for 15 m markets, 3600 s for 1 h, 300 s for 5 m.
    pub market_end_ts: u64,
    pub condition_id: String,
    pub token_id_up: String,
    pub token_id_down: String,
    pub tick_size: f64,
    pub min_order_size: f64,
    pub neg_risk: bool,
    pub maker_fee_bps: u32,
    pub taker_fee_bps: u32,
    pub rewards: RewardsCfg,
    /// Per-slot phase clock (Strategy §9 / §15 Module 3). Constructed once
    /// at spawn from `market_start_ts` + the slot timeframe; read by the
    /// quoter on every tick to determine the current `SlotPhase` for
    /// phase-specific dead-band, replace cadence, and kill triggers.
    pub phase_clock: crate::polymarket::phase::PhaseClock,
}

impl MarketMeta {
    pub fn token_id_for(&self, side: Side) -> &str {
        match side {
            Side::Up => &self.token_id_up,
            Side::Down => &self.token_id_down,
        }
    }

    /// Slot timeframe in seconds (e.g. 300 for 5m, 900 for 15m, 3600 for 1h).
    /// Used by `lifecycle::spawn_market` to select the matching
    /// `PhaseClockCfg` preset.
    ///
    /// Returns `0` if the slot is inverted (`end < start`) — gamma should
    /// never deliver such a market, but the `as i64` cast on a wrapped u64
    /// would otherwise produce a huge negative value and silently degrade
    /// downstream phase classification to `Resolved` for every market.
    pub fn timeframe_secs(&self) -> i64 {
        debug_assert!(
            self.market_end_ts >= self.market_start_ts,
            "MarketMeta::timeframe_secs invariant: end ({}) < start ({}) for slug {}",
            self.market_end_ts,
            self.market_start_ts,
            self.slug,
        );
        self.market_end_ts.saturating_sub(self.market_start_ts) as i64
    }
}

/// Top-of-book snapshot for one token, received from Polymarket WS.
#[derive(Clone, Debug)]
pub struct BookUpdate {
    pub slug: String,
    pub side: Side,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub ts_micros: u64,
}

/// Whether a fill came in as the maker (resting order matched) or taker
/// (aggressive order crossed). Distinguishes inventory unwinds (taker)
/// from passive fills (maker) in guards / accounting.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LiquidityRole {
    Maker,
    Taker,
    /// User-channel didn't carry `trader_side` (rare; older WS shapes).
    /// Treated as Maker by guards (the conservative choice — counts toward
    /// the adverse-selection streak).
    Unknown,
}

/// A fill (paper or live), emitted by the `ClobClient` implementation.
///
/// `id` identifies the resting order that filled — used by the submitter's
/// fills-observer task to clean up its `cancel_index` / `by_slug_side`
/// entries on fill (otherwise those indexes grow unbounded, since fills
/// never flow through the cancel path).
#[derive(Clone, Debug)]
pub struct Fill {
    pub slug: String,
    pub coin: Coin,
    pub side: Side,
    pub token_id: String,
    pub id: RestingOrderId,
    pub shares: f64,
    pub price: f64,
    pub usd: f64,
    pub fee_usd: f64,
    pub ts_micros: u64,
    pub liquidity_role: LiquidityRole,
}

/// Synchronous match info decoded from a POST /order response with
/// `status == "matched"`: the venue matched (part of) the order at placement,
/// hundreds of ms before the user-WS MATCHED event arrives.
///
/// Amounts are decoded from the response's DECIMAL strings ("3.75", "187.5").
/// They are NOT base-1e6 fixed-math integers: the docs said so
/// (docs.polymarket.com/api-reference/trade/post-a-new-order, read 2026-06-13)
/// and the parser divided by 1e6 on that basis, which made every field here a
/// millionth of the truth until 2026-07-29. Note the asymmetry — the OUTGOING
/// signed order really is in base units; only the response is decimal.
///
/// Carries no slug/side — the consumer resolves `token_id` against its own
/// market state. Decision-exposure signal only; settled inventory still comes
/// from the user-WS MINED/CONFIRMED fill.
#[derive(Clone, Debug)]
pub struct ImmediateMatch {
    pub order_id: RestingOrderId,
    pub token_id: String,
    /// Matched shares (the response's matched portion, not the order size).
    pub shares: f64,
    /// Matched USDC notional (gross, ex-fee).
    pub usd: f64,
    /// Local receive time of the POST response.
    pub ts_micros: u64,
}

/// The resolved winner of a Polymarket 15-min market, or Unresolved if the
/// market hasn't posted an outcome yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvedOutcome {
    Resolved(Side),
    Unresolved,
}

impl ResolvedOutcome {
    /// Parse the `outcome` string from a Polymarket CLOB market response.
    /// Returns `None` if the string is neither `"Up"` nor `"Down"`.
    pub fn from_outcome_str(s: &str) -> Option<Self> {
        match s {
            "Up" => Some(Self::Resolved(Side::Up)),
            "Down" => Some(Self::Resolved(Side::Down)),
            _ => None,
        }
    }
}

/// Credit emitted by the resolution sweeper when a market closes with shares
/// held on the winning side. `usd == shares × $1` (Polymarket's $1-per-winning-
/// share payout rule).
#[derive(Clone, Debug)]
pub struct RedeemCredit {
    pub slug: String,
    pub coin: Coin,
    pub winner: Side,
    pub shares: f64,
    pub usd: f64,
    pub ts_micros: u64,
}

/// Whether to fire MERGEs only when they are net-profitable (CONSERVATIVE) or
/// regardless of margin (AGGRESSIVE). Reference: [docs/wallet-951bd-factcheck-20260423-v2.md §7.2.5 MERGE].
///
/// The live wallet uses AGGRESSIVE (37 % of merges occur at negative margin
/// as capital recycling). The replicant defaults to CONSERVATIVE: only fire
/// when `avg_up + avg_dn + merge_fee < 1.0`, so a MERGE is never a guaranteed
/// loss. Expected give-up vs. AGGRESSIVE: ~$800 over 48 h (per spec table).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MergePolicy {
    #[default]
    Conservative,
    Aggressive,
}

/// Synthesised MERGE event emitted by `exec::merge::try_merge`. In paper mode
/// the MERGE has no live counterpart; this struct is the audit / log record.
///
/// `pairs` is the integer pair count combined into USDC.
/// `gross_credit_usd == pairs as f64` (each pair ≡ $1).
/// `fee_usd == pairs × merge_fee` (deducted by the protocol; 0 today).
/// `net_credit_usd == gross_credit_usd − fee_usd`, the amount credited to the
/// `SpendLedger`.
#[derive(Clone, Debug)]
pub struct MergeFill {
    pub slug: String,
    pub coin: Coin,
    pub pairs: u64,
    pub gross_credit_usd: f64,
    pub fee_usd: f64,
    pub net_credit_usd: f64,
    pub ts_micros: u64,
}

/// Wall-clock timestamp in microseconds since epoch. Preferred to `SystemTime::now()`
/// at call sites so we can stub it later for tests.
#[inline]
pub fn now_micros() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Convert a Unix-seconds timestamp into a monotonic `Instant` suitable for
/// `tokio::time::sleep_until`. Computed as `(target_sys - now_sys) + now_inst`
/// so the returned `Instant` is anchored to the monotonic clock but tracks
/// the wall-clock target.
///
/// If the target is in the past (or `unix_secs` is negative), returns
/// `Instant::now()` — i.e. fires immediately. Callers using this for
/// scheduled wake-ups (e.g. the post-resolution sweeper at
/// `market_end_ts + offset`) treat a past target as "due now".
pub fn unix_to_instant(unix_secs: i64) -> std::time::Instant {
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    let target_sys = UNIX_EPOCH + Duration::from_secs(unix_secs.max(0) as u64);
    let now_sys = SystemTime::now();
    let now_inst = Instant::now();
    match target_sys.duration_since(now_sys) {
        Ok(delta) => now_inst + delta,
        Err(_) => now_inst, // target in past → fire immediately
    }
}

/// Container that holds one `T` per supported coin.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PerCoin<T> {
    pub btc: T,
    pub eth: T,
    pub sol: T,
    pub xrp: T,
}

impl<T> PerCoin<T> {
    pub fn get(&self, c: Coin) -> &T {
        match c {
            Coin::Btc => &self.btc,
            Coin::Eth => &self.eth,
            Coin::Sol => &self.sol,
            Coin::Xrp => &self.xrp,
        }
    }

    pub fn get_mut(&mut self, c: Coin) -> &mut T {
        match c {
            Coin::Btc => &mut self.btc,
            Coin::Eth => &mut self.eth,
            Coin::Sol => &mut self.sol,
            Coin::Xrp => &mut self.xrp,
        }
    }
}

impl<T: Copy> PerCoin<T> {
    pub fn get_copy(&self, c: Coin) -> T {
        *self.get(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coin_roundtrip_slug_prefix() {
        for c in Coin::ALL {
            assert_eq!(Coin::from_slug_prefix(c.as_str()), Some(c));
        }
        assert_eq!(Coin::from_slug_prefix("doge"), None);
    }

    #[test]
    fn coin_long_name_roundtrip() {
        // Spec §1.1: the 1h ET slug format uses full coin names
        // (`bitcoin-up-or-down-...`, not `btc-up-or-down-...`).
        assert_eq!(Coin::Btc.long_name(), "bitcoin");
        assert_eq!(Coin::Eth.long_name(), "ethereum");
        assert_eq!(Coin::Sol.long_name(), "solana");
        assert_eq!(Coin::Xrp.long_name(), "xrp");
        for c in Coin::ALL {
            assert_eq!(Coin::from_long_name(c.long_name()), Some(c));
        }
        assert_eq!(Coin::from_long_name("dogecoin"), None);
    }

    #[test]
    fn side_opposite_is_involution() {
        assert_eq!(Side::Up.opposite(), Side::Down);
        assert_eq!(Side::Down.opposite(), Side::Up);
        assert_eq!(Side::Up.opposite().opposite(), Side::Up);
    }

    #[test]
    fn market_meta_token_id_selects_side() {
        let m = MarketMeta {
            slug: "eth-updown-15m-1".into(),
            coin: Coin::Eth,
            market_start_ts: 1,
            market_end_ts: 901,
            condition_id: "0x00".into(),
            token_id_up: "U".into(),
            token_id_down: "D".into(),
            tick_size: 0.01,
            min_order_size: 5.0,
            neg_risk: false,
            maker_fee_bps: 0,
            taker_fee_bps: 0,
            rewards: RewardsCfg::default(),
            phase_clock: crate::polymarket::phase::PhaseClock::new(
                0,
                crate::polymarket::phase::PhaseClockCfg::default_5m(),
            ),
        };
        assert_eq!(m.token_id_for(Side::Up), "U");
        assert_eq!(m.token_id_for(Side::Down), "D");
    }

    #[test]
    fn resolved_outcome_from_outcome_str() {
        assert_eq!(
            ResolvedOutcome::from_outcome_str("Up"),
            Some(ResolvedOutcome::Resolved(Side::Up))
        );
        assert_eq!(
            ResolvedOutcome::from_outcome_str("Down"),
            Some(ResolvedOutcome::Resolved(Side::Down))
        );
        assert_eq!(ResolvedOutcome::from_outcome_str("Maybe"), None);
        assert_eq!(ResolvedOutcome::from_outcome_str(""), None);
    }

    #[test]
    fn redeem_credit_carries_shares_and_usd() {
        let c = RedeemCredit {
            slug: "eth-updown-15m-1".into(),
            coin: Coin::Eth,
            winner: Side::Up,
            shares: 12.5,
            usd: 12.5,
            ts_micros: 1_000,
        };
        assert_eq!(c.shares, 12.5);
        assert_eq!(c.usd, 12.5);
        assert_eq!(c.winner, Side::Up);
    }

    #[test]
    fn merge_policy_default_is_conservative() {
        // CONSERVATIVE is the safe default; AGGRESSIVE matches the live wallet
        // but accepts ~37 % negative-margin MERGEs (per factcheck-v2 §7.2.5).
        assert_eq!(MergePolicy::default(), MergePolicy::Conservative);
    }

    #[test]
    fn merge_policy_roundtrips_through_serde() {
        let json = serde_json::to_string(&MergePolicy::Conservative).unwrap();
        assert_eq!(json, "\"conservative\"");
        let back: MergePolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back, MergePolicy::Conservative);
        let json = serde_json::to_string(&MergePolicy::Aggressive).unwrap();
        assert_eq!(json, "\"aggressive\"");
        let back: MergePolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back, MergePolicy::Aggressive);
    }

    #[test]
    fn merge_fill_carries_pairs_and_credits() {
        // Zero-fee case (today's CLOB behaviour). Even with fee = 0, assert the
        // three-field accounting identity gross - net - fee == 0 so a future
        // regression that populates fee but forgets to subtract it is caught.
        let mf = MergeFill {
            slug: "eth-updown-15m-1".into(),
            coin: Coin::Eth,
            pairs: 60,
            gross_credit_usd: 60.0,
            fee_usd: 0.0,
            net_credit_usd: 60.0,
            ts_micros: 1_234,
        };
        assert_eq!(mf.pairs, 60);
        assert_eq!(mf.coin, Coin::Eth);
        assert_eq!(mf.fee_usd, 0.0);
        assert!((mf.gross_credit_usd - mf.net_credit_usd - mf.fee_usd).abs() < 1e-9);

        // Non-zero-fee case: exercises the identity meaningfully.
        let mf2 = MergeFill {
            slug: "eth-updown-15m-1".into(),
            coin: Coin::Eth,
            pairs: 10,
            gross_credit_usd: 10.0,
            fee_usd: 0.30,
            net_credit_usd: 9.70,
            ts_micros: 1_234,
        };
        assert!((mf2.gross_credit_usd - mf2.net_credit_usd - mf2.fee_usd).abs() < 1e-9);
    }

    #[test]
    fn resting_order_id_round_trip_string() {
        let id = RestingOrderId::from_string("abc-123");
        assert_eq!(id.as_str(), "abc-123");
        let cloned = id.clone();
        assert_eq!(cloned, id);
    }

    #[test]
    fn client_order_id_is_monotonic_and_distinct() {
        let a = ClientOrderId::next();
        let b = ClientOrderId::next();
        let c = ClientOrderId::next();
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert!(b.0 > a.0, "monotonic: b={} > a={}", b.0, a.0);
        assert!(c.0 > b.0, "monotonic: c={} > b={}", c.0, b.0);
    }

    #[test]
    fn resting_bid_carries_routing_and_price_size() {
        let r = RestingBid {
            slug: "eth-updown-15m-1".into(),
            coin: Coin::Eth,
            side: Side::Up,
            token_id: "TOKEN".into(),
            price: 0.47,
            size_shares: 60.0, // transitional cast (Task 5 fixes); see plan 2026-04-23-951bd-quoter-parity.md
            tick_size: 0.01,
            ts_micros: 1_234,
        };
        assert_eq!(r.side, Side::Up);
        assert!((r.price - 0.47).abs() < 1e-9);
        // size_usd() is now a derived getter: shares × price = 60 × 0.47
        assert!((r.size_usd() - 28.20).abs() < 1e-9);
    }

    #[test]
    fn resting_bid_carries_shares_and_derives_usd() {
        let r = RestingBid {
            slug: "btc-updown-5m-1".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "T".into(),
            price: 0.50,
            size_shares: 60.0,
            tick_size: 0.01,
            ts_micros: 0,
        };
        assert_eq!(r.size_shares, 60.0);
        // Derived helper: shares × price.
        assert!((r.size_usd() - 30.0).abs() < 1e-9);
    }

    #[test]
    fn order_command_variants_construct() {
        let bid = RestingBid {
            slug: "eth-updown-15m-1".into(),
            coin: Coin::Eth,
            side: Side::Up,
            token_id: "T".into(),
            price: 0.5,
            size_shares: 60.0,
            tick_size: 0.01,
            ts_micros: 1,
        };
        let place = OrderCommand::PlaceResting {
            bid,
            coid: ClientOrderId::next(),
        };
        let cancel = OrderCommand::CancelResting(RestingOrderId::from_string("abc"));
        match place {
            OrderCommand::PlaceResting { .. } => {}
            _ => panic!("wrong variant"),
        }
        match cancel {
            OrderCommand::CancelResting(_) => {}
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn taker_buy_carries_price_ceiling_and_max_shares() {
        let t = TakerBuy {
            slug: "btc-updown-5m-1".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "U".into(),
            max_ask: 0.88,
            max_shares: 55.0,
            ts_micros: 1_234,
        };
        assert_eq!(t.side, Side::Up);
        assert!((t.max_ask - 0.88).abs() < 1e-9);
    }

    #[test]
    fn order_command_place_taker_variant() {
        let t = TakerBuy {
            slug: "btc-updown-5m-1".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "U".into(),
            max_ask: 0.88,
            max_shares: 55.0,
            ts_micros: 0,
        };
        let cmd = OrderCommand::PlaceTaker(t);
        match cmd {
            OrderCommand::PlaceTaker(_) => {}
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn resting_ask_carries_routing_and_price_size() {
        let a = RestingAsk {
            slug: "btc-updown-5m-1".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "U".into(),
            price: 0.52,
            size_shares: 50.0,
            tick_size: 0.01,
            ts_micros: 1_234,
        };
        assert_eq!(a.side, Side::Up);
        assert!((a.price - 0.52).abs() < 1e-9);
        assert!((a.size_usd() - 26.0).abs() < 1e-9);
    }

    #[test]
    fn order_command_place_resting_ask_variant() {
        let a = RestingAsk {
            slug: "btc-updown-5m-1".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "U".into(),
            price: 0.52,
            size_shares: 50.0,
            tick_size: 0.01,
            ts_micros: 0,
        };
        let cmd = OrderCommand::PlaceRestingAsk {
            ask: a,
            coid: ClientOrderId::next(),
        };
        match cmd {
            OrderCommand::PlaceRestingAsk { .. } => {}
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn id_assignment_carries_slug_side_id() {
        let a = IdAssignment {
            slug: "eth-updown-15m-1".into(),
            side: Side::Up,
            id: RestingOrderId::from_string("paper-7"),
            kind: OrderKind::Bid,
            coid: ClientOrderId::next(),
        };
        assert_eq!(a.side, Side::Up);
        assert_eq!(a.id.as_str(), "paper-7");
        assert_eq!(a.slug, "eth-updown-15m-1");
        assert_eq!(a.kind, OrderKind::Bid);
    }

    #[test]
    fn cancel_ack_carries_slug_side() {
        let a = CancelAck {
            slug: "eth-updown-15m-1".into(),
            side: Side::Down,
            order_id: None,
        };
        assert_eq!(a.side, Side::Down);
        assert_eq!(a.slug, "eth-updown-15m-1");
    }

    #[test]
    fn rewards_cfg_defaults_to_non_rewarding() {
        // Markets without a rewards object should parse as "no incentive" so the
        // rest of the code treats them as plain markets.
        let r = RewardsCfg::default();
        assert_eq!(r.min_size, 0.0);
        assert_eq!(r.max_spread, 0.0);
        assert!(!r.is_incentivised());
    }

    #[test]
    fn rewards_cfg_is_incentivised_when_either_positive() {
        // A market is considered LIP-incentivised when min_size > 0 or max_spread > 0.
        // Either field alone is enough to trigger the in-band quoter path.
        assert!(RewardsCfg {
            min_size: 15.0,
            max_spread: 0.0,
            daily_rate: 0.0
        }
        .is_incentivised());
        assert!(RewardsCfg {
            min_size: 0.0,
            max_spread: 0.05,
            daily_rate: 0.0
        }
        .is_incentivised());
        assert!(!RewardsCfg {
            min_size: 0.0,
            max_spread: 0.0,
            daily_rate: 0.0
        }
        .is_incentivised());
    }

    #[test]
    fn rewards_cfg_carries_daily_rate_with_default_zero() {
        let r = RewardsCfg::default();
        assert_eq!(r.daily_rate, 0.0);
        assert!(
            !r.is_incentivised(),
            "zero on all fields is not incentivised"
        );
    }

    #[test]
    fn rewards_cfg_is_incentivised_when_daily_rate_positive() {
        let r = RewardsCfg {
            min_size: 0.0,
            max_spread: 0.0,
            daily_rate: 50.0,
        };
        assert!(r.is_incentivised());
    }

    #[test]
    fn rewards_cfg_meets_min_daily_rate_threshold() {
        let r = RewardsCfg {
            min_size: 50.0,
            max_spread: 0.05,
            daily_rate: 200.0,
        };
        assert!(r.meets_min_daily_rate(50.0));
        assert!(r.meets_min_daily_rate(200.0));
        assert!(!r.meets_min_daily_rate(500.0));
    }

    #[test]
    fn rewards_cfg_meets_min_daily_rate_zero_threshold_always_true() {
        // `0.0` threshold = "no gate". Even unincentivised markets pass.
        let r = RewardsCfg::default();
        assert!(r.meets_min_daily_rate(0.0));
    }

    #[test]
    fn unix_to_instant_future_returns_later() {
        use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
        let now_sys = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let target = now_sys + 60;
        let before = Instant::now();
        let inst = unix_to_instant(target);
        // The Instant must point at least ~59 s into the future (allow some
        // wall-clock slack for scheduling between SystemTime::now() captures).
        let delta = inst.saturating_duration_since(before);
        assert!(
            delta >= Duration::from_secs(58),
            "unix_to_instant 60s in the future should be ≥58s away; got {:?}",
            delta,
        );
        assert!(
            delta <= Duration::from_secs(62),
            "unix_to_instant 60s in the future should be ≤62s away; got {:?}",
            delta,
        );
    }

    #[test]
    fn unix_to_instant_past_returns_now() {
        use std::time::{Duration, Instant};
        let before = Instant::now();
        // 60 s in the past (relative to epoch + 0 is well in the past).
        let inst = unix_to_instant(0);
        let elapsed = before.saturating_duration_since(inst);
        // Either inst == before (fired-immediately path) or inst is moments
        // behind before (function captured `now_inst` after we did). Either
        // way the result must NOT be more than a tiny epsilon in the future.
        let future_delta = inst.saturating_duration_since(before);
        assert!(
            future_delta <= Duration::from_millis(50),
            "past target must clamp to ~now; future_delta={:?}",
            future_delta,
        );
        // And it must be no more than the test's runtime in the past.
        assert!(
            elapsed <= Duration::from_secs(5),
            "past target should be very close to now; elapsed={:?}",
            elapsed,
        );
    }

    #[test]
    fn fill_carries_liquidity_role() {
        let f = Fill {
            slug: "btc-updown-5m-1".into(),
            coin: Coin::Btc,
            side: Side::Up,
            token_id: "100".into(),
            id: RestingOrderId::from_string("0xabc"),
            shares: 50.0,
            price: 0.50,
            usd: 25.0,
            fee_usd: 0.0,
            ts_micros: 0,
            liquidity_role: LiquidityRole::Maker,
        };
        assert_eq!(f.liquidity_role, LiquidityRole::Maker);
    }
}
