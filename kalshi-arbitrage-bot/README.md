# Polymarket–Kalshi Arbitrage Bot

> Part of the [Polymarket Trading Bots](../README.md) pack · **Status: 🚧 in development**

An open-source **Polymarket Kalshi arbitrage bot**: a cross-venue arbitrage scanner and executor for the two largest prediction markets, **Polymarket** and **Kalshi**. The bot matches the same real-world event listed on both venues, pulls both order books, and reports price gaps after fees - then (optionally) executes both legs.

Both venues are regulated US markets with KYC. This bot does not bypass any of that; you need your own accounts.

## What it will do

1. **Event matching** - map a Polymarket market to its Kalshi counterpart (title, resolution date, outcome wording). This is the hard part and the main value of the repo.
2. **Spread scanner** - fetch both books, compute net edge after Polymarket and Kalshi fees, print a ranked table.
3. **Paper mode** - simulate both legs and log PnL, including resolution-rule mismatches (the real risk in this trade).
4. **Live mode** - gated behind an explicit flag with per-leg size caps.

## Release plan

| Layer | Needs keys? | Status |
|-------|-------------|--------|
| Scanner (public data) | No | 🚧 |
| Paper trading | No | planned |
| Live execution | Yes (both venues) | planned |

## Open vs. our live version

| In this repo | Kept private |
|--------------|--------------|
| Event matching, fee model, spread table | Latency stack and venue-specific execution tricks |
| Paper simulation and logs | Capital allocation across events |
| Gated live mode with caps | Resolution-risk filters tuned on our history |

Want the production build or custom work? [Telegram @dexoryn](https://t.me/dexoryn).

## Quick start

Coming with the first release. The folder will be self-contained: `requirements.txt`, `config.yaml.example`, and a `python -m` entry point.

## FAQ

**How does Polymarket–Kalshi arbitrage work?**
The same event is often listed on both prediction markets at different prices. If YES on one venue plus NO on the other costs less than $1 after fees, buying both locks in the difference when the event resolves.

**Is prediction market arbitrage risk-free?**
No. The two venues can word or resolve the same event differently, one leg can fill while the other does not, and fees can eat a thin spread. The paper mode exists to measure those risks before any capital is used.

**Do I need accounts on both Polymarket and Kalshi?**
For the scanner, no: it reads public order books. For live execution you need your own funded, verified account on each venue.

**When will the Kalshi arbitrage bot be released?**
The scanner ships first, then paper trading, then gated live execution. Watch the repo or follow [@Dexoryn](https://x.com/Dexoryn).

---

← Back to the [bot pack](../README.md) · Follow [@Dexoryn](https://x.com/Dexoryn) for the launch post.
