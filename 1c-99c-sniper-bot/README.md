# Polymarket 1¢ & 99¢ Sniper Bot

> Part of the [Polymarket Trading Bots](../README.md) pack

A late-slot taker for Polymarket `btc-updown-5m`. One engine runs two opposite strategies, and you pick one with a single environment variable:

```bash
SNIPER_MODE=1c    # buy the TRAILING token at ~1¢
SNIPER_MODE=99c   # buy the LEADING token at ~99¢
```

Both modes are **live-tested and profitable**.

## Same strategy as these Polymarket traders

| Mode | Trader | Strategy |
|---|---|---|
| `1c` | [@vega7](https://polymarket.com/@vega7) | buys the underdog at ~1¢ in BTC 5-minute markets just before it turns |
| `99c` | [@7kfrombigyahu](https://polymarket.com/@7kfrombigyahu) | buys the near-certain winner at 97–99¢ in the final minutes before resolution |

Open their profiles to see the pattern on real fills: the same markets, the same price levels, the same late-slot timing. This bot runs that strategy automatically, every 5-minute slot, with fixed sizing and hard risk limits.

## Why one bot can do both

In a binary market the two token prices add up to about $1. When one token's ask is ~1¢, the other is worth ~99¢. Both strategies look for the same moment: the last ~2.5 minutes of a 5-minute slot, with one token priced near certainty. They differ only in **which side of that moment they buy**.

The engine has no `if mode == …` branch. The mode picks a **price band**, and the band alone decides which token qualifies, because no market can have both tokens in the same band at once.

| | `SNIPER_MODE=1c` | `SNIPER_MODE=99c` |
|---|---|---|
| **Buys** | the trailer, ask 0.9–1.1¢ | the leader, ask 97–99¢ |
| **FAK limit** | 2¢ | 99¢ |
| **Clip** | $10 (~1,000 shares) | $20 (~20 shares) |
| **Per share** | win ~+99¢ / lose ~1¢ | win ~+1¢ / lose ~99¢ |
| **Break-even win rate** | ~1.07% | ~99.07% |
| **Live result** | ✅ profitable — same strategy as [@vega7](https://polymarket.com/@vega7) | ✅ profitable — same strategy as [@7kfrombigyahu](https://polymarket.com/@7kfrombigyahu) |
| **Triggers** | `burst` | `burst` or `calm` |
| **Max book age** | 15 s | 2 s |
| **Loss kill** | $400 | $100 |

## How it decides

From second 150 to 291 of each slot, on every Binance BTC spot trade, the engine checks two separate things and **never blends them**:

1. **Permission.** A token's standing best ask sits in the mode's band.
2. **Trigger** (`SNIPER_TRIGGER`):
   - `burst` (default): at least 0.5 BTC traded in the last second, with at least 60% of it moving BTC *toward* that token. In `1c` mode this is the trailer's recovery burst. In `99c` mode it is a burst confirming the leader.
   - `calm` (`99c` only): fire as long as less than 0.3 BTC in the last second moved *against* the leader.

When both pass, it sends **one** fill-and-kill buy for the clip and holds to resolution. One shot per slot, and a miss is never retried.

A burst *against* the token is logged as the **placebo**: it never trades, and its running PnL should stay negative. In `99c` mode the placebo is exactly the 1¢ bot's entry seen from the other side. If the trailer is about to recover, that is the worst moment to hold the leader.

## ⚠️ Know the risk of `99c`

- **One loss erases ~99 wins.** At a 99¢ fill you need ~99.07% wins after the C=0.07 taker fee.
- **The limit cannot save you from a reversal.** A buy limit fills at *any lower* ask, so if the leader collapses as your order arrives, you buy the collapse. Only the trigger and the 2-second book-age bound protect you.
- **`calm` fires early.** It is checked on every trade, so the first trades of an adverse burst that is still building can still read as calm.

Run `--dry-run` first and start live with a small `SNIPER_CLIP_USD`. Both modes need discipline: a 1¢ bot loses small and often, a 99¢ bot wins small and often.

## Quick start

```bash
cd 1c-99c-sniper-bot
cargo build --profile release-test

cp .env.example .env               # set SNIPER_MODE=1c or 99c
./target/release-test/sniperbot --dry-run
```

A dry run needs no keys and sends nothing.

## Configuration

Everything is set in **one place**: environment variables, or a `.env` file next to the binary ([`.env.example`](.env.example) lists every key). There is no config file. Variables set in your shell win over `.env`. An empty value means "use the default".

**1. Strategy.** `SNIPER_MODE` is required. Every other variable overrides one value of the mode's preset.

| Variable | Default (`1c` / `99c`) | |
|---|---|---|
| `SNIPER_MODE` | **required** | `1c` or `99c` |
| `SNIPER_TRIGGER` | `burst` | `burst`, or `calm` (`99c` only) |
| `SNIPER_CLIP_USD` | 10 / 20 | dollars committed per fire |
| `SNIPER_ASK_MIN`, `SNIPER_ASK_MAX` | 0.009–0.011 / 0.97–0.99 | entry band |
| `SNIPER_LIMIT` | 0.02 / 0.99 | FAK limit price |
| `SNIPER_LOSS_KILL_USD` | 400 / 100 | halt after this much settled loss |
| `SNIPER_TICK_ENABLED` | `true` | `false` = record only, never fire |
| `SNIPER_V_BTC`, `SNIPER_LEAN_MIN`, `SNIPER_WINDOW_MS` | 0.5, 0.6, 1000 | the burst |
| `SNIPER_ADVERSE_MAX_BTC` | 0.3 | `calm` veto threshold |
| `SNIPER_MAX_BOOK_AGE_MS` | 15000 / 2000 | ignore older order books |

**2. Spend rails.** All optional.

| Variable | Default | |
|---|---|---|
| `SNIPER_DAILY_CAP_USD` | 2500 | rolling-24h committed spend |
| `SNIPER_EXPOSURE_CAP_USD` | 150 | open (unsettled) exposure |
| `SNIPER_PER_ORDER_CAP_USD` | 100 | one fire's commit |
| `SNIPER_FIRE_RATE_PER_MIN` | 20 | fault latch (the bot fires at most once per slot) |
| `SNIPER_BOOK_STALE_MS` | 5000 | disarm when the book feed goes quiet |

**3. Live account.** Only `--live` and `--preflight` use these. Leave every `POLY_*` empty for a dry run; once any is set, the five secrets are required.

| Variable | Default | |
|---|---|---|
| `POLY_PRIVATE_KEY` | **required for live** | EOA key that controls the proxy wallet |
| `POLY_PROXY_WALLET` | **required for live** | proxy wallet that holds pUSD |
| `POLY_API_KEY`, `POLY_API_SECRET`, `POLY_API_PASSPHRASE` | **required for live** | CLOB API credentials |
| `POLY_SIGNATURE_TYPE` | 1 | 1 Magic.link, 2 Safe, 3 accounts created 2026-08+ |
| `POLY_RPC_URL`, `POLY_RPC_WS_URL` | polygon-rpc.com, none | Polygon RPC |
| `POLY_CHAIN_ID`, `POLY_CLOB_HTTP`, `POLY_CLOB_WS` | 137, Polymarket's public endpoints | |
| `SNIPER_LIVE_BUY_CAP_USD` | 150 | rolling-24h BUY cap |
| `SNIPER_LIVE_DAILY_LOSS_USD` | 100 | halt on 24h ledger loss |
| `SNIPER_LIVE_LOSS_PER_HOUR_USD`, `SNIPER_LIVE_LOSS_HOURS` | 40, 2 | halt on sustained hourly loss |

Everything is validated **per mode** before anything connects:
- `1c` bands must sit in [0.5¢, 10¢], and the limit can be at most 4× the band floor.
- `99c` bands must sit in [90¢, 99.9¢], and the limit can be at most 99.9¢ and at most 3¢ above the band floor.
- A clip must buy at least 5 shares (the venue minimum).
- Every spend rail must cover one fire.

A misspelled `SNIPER_*` or `POLY_*` name, a half-filled live account, or a leftover `config/tickburst.toml` from the old layout is an error, never a silent default.

**Never commit `.env`.** It is gitignored, and it holds your private key. Use a wallet dedicated to this bot.

## Run

```bash
./target/release-test/sniperbot --dry-run          # decide, send nothing
./target/release-test/sniperbot --preflight        # read-only checks of the POLY_* account
./target/release-test/sniperbot --live-readiness   # runs the test suite, writes the readiness artifact
RECON_GIT_SHA=$(git rev-parse HEAD) ./target/release-test/sniperbot --live
```

Running one `1c` process and one `99c` process side by side is fine, because they buy opposite tokens. Give each its own wallet and working directory. Never run two processes **in the same mode**, because they race for the same asks.

## Layout

```
.env.example              every setting (copy to .env; gitignored)
data/tickburst/           the 1¢ census fixture (tracked test input)
src/
  main.rs                 CLI: loads .env, then --dry-run / --preflight / --live-readiness / --live
  config.rs, config/      env.rs: SNIPER_* → strategy + rails, POLY_* → live account
  strategy/               PURE decision core — no I/O, no clock, no async
    mode.rs               SNIPER_MODE presets (the only place 1c and 99c differ)
    tick.rs               burst / calm predicates
    cards.rs              the [150, 291) frame, the band check, the placebo
    engine.rs             the per-slot automaton (Ready → Spent)
  polymarket/             CLOB WS/HTTP, market discovery, order books, on-chain
  binance/                Binance spot aggTrade / kline feed
  execution/              live FAK venue, signer, user WS, ledger, positions
  risk/                   risk rails + kill switches
  bot/                    runtime, slot driver, settlement, telemetry, readiness
tests/
  posttwap_parity.rs      replays the 1¢ census through the engine (gating)
  leader_trigger.rs       99¢ behaviour: mirror rule, burst/calm, clocks (gating)
  one_cent_trigger.rs     1¢ behaviour: frame, clocks, book age
  config_shape.rs         env resolution and per-mode validation
```

## Tests

```bash
cargo test --profile release-test
```

Tests marked `#[ignore]` need a live network.

---

← Back to the [bot pack](../README.md) · Launch post: [@Dexoryn](https://x.com/Dexoryn) · Questions: [Telegram @dexoryn](https://t.me/dexoryn).
