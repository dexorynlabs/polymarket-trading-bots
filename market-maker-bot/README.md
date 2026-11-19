# Polymarket Market Maker Bot

> Part of the [Polymarket Trading Bots](../README.md) pack · **Status: 🚧 demo version planned**

An open-source **Polymarket market maker bot**: a reference market-making bot for the Polymarket CLOB that quotes both sides of a binary market around mid, manages inventory, and cancels/replaces resting orders on the CLOB. It is a working, paper-tradable skeleton of the market maker we run live - minus the parts that make ours profitable.

We publish it so you can see how a Polymarket MM is structured end to end, not as a plug-and-play money printer.

## What the demo will include

- Symmetric bid/ask quoting around mid with configurable spread and size
- Inventory caps and quote skew when you get long or short
- Cancel/replace loop with rest timeout on GTC orders
- Paper fills from the live book, PnL and rebate logging
- Dry-run by default; live mode gated by config

## What stays private

| In this repo | Our live desk |
|--------------|---------------|
| Mid-based symmetric quotes | Fair-value model and skew logic |
| Static spread / size | Adverse-selection and toxicity filters |
| Single-market loop | Market selection and rebate optimisation across markets |
| Standard REST / WebSocket | Latency stack |

That table is the honest answer to "will this make money?" - the demo teaches the mechanics; the edge is in what is not listed on the left.

## Quick start

Coming with the first release. The folder will be self-contained: `requirements.txt`, `config.yaml.example`, and a `python -m` entry point with `mode: paper` as default.

## FAQ

**How does a Polymarket market maker bot work?**
It rests a buy order below the mid price and a sell order above it on the Polymarket order book (CLOB). When both fill, it earns the spread; in between, it manages the inventory it is left holding.

**Can you make money market making on Polymarket?**
Yes, but not with symmetric quotes alone. The profit comes from pricing fair value better than the book, avoiding informed flow, and picking the right markets. This demo teaches the mechanics; it is not the edge.

**Does Polymarket pay market makers?**
Polymarket runs liquidity reward and rebate programs on selected markets. The demo logs estimated rebates alongside PnL so you can see what they contribute.

**Is it safe to try?**
It defaults to paper mode: fills are simulated from the live order book and no orders are sent. Live mode is gated by config.

---

← Back to the [bot pack](../README.md) · Live MM threads and stats: [@Dexoryn](https://x.com/Dexoryn) · Production build: [Telegram @dexoryn](https://t.me/dexoryn).
