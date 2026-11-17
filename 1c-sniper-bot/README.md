# 1centbot

A late-slot cheap-side taker for Polymarket `btc-updown-5m`. In the last ~2.5 minutes
of a 5-minute slot, it watches for the trailing side's ask sitting at ~1¢. When Binance
spot prints a short burst of volume leaning toward that side's recovery, it sends ONE
fill-and-kill buy for a small fixed dollar clip and holds to resolution. One shot per
slot; a missed race costs nothing.

This is the original `Jr-1centbot/taker-tickburst` bot (same code, same strategy,
same tests). It has been re-laid out as a single crate in the standard 5m bot folder
structure, built as `1centbot`. Modules the bot never uses (leftovers of the shared
market-maker client: paper engine, submitter, guards, sigma, rebate, merge/redeem encoders)
were removed. There is no control-server integration.

## Layout

```
Cargo.toml                package `onecentbot` (lib) → binary `1centbot`
config/                   tickburst.example.toml → tickburst.toml (gitignored, holds secrets)
data/                     parity fixture (tracked) + runtime telemetry / readiness output
src/
  main.rs                 CLI: --dry-run / --preflight / --live-readiness / --live
  lib.rs
  config.rs, config/      operator TOML + shared live wallet/chain/clob/api/safety config
  types.rs                domain types (Coin, Side, orders, fills)
  strategy/               PURE decision core: engine, flow, tick, cards, sizing, features
  polymarket/             CLOB WS/HTTP, gamma discovery, book registry, onchain/ (Safe, polygon events)
  binance/                Binance spot aggTrade / kline feed (feed.rs)
  execution/              live FAK venue, signer, user WS, ledger, position, balance
  risk/                   risk rails + kill switches
  bot/                    runtime, slot driver, settlement, telemetry, readiness
  monitoring/             health, latency, measurements, preflight
tests/                    integration tests (incl. posttwap_parity — the correctness proof) + fixtures/
```

## Build and test

```bash
cargo build --profile release-test
cargo test  --profile release-test
```

`release-test` is a fast-linking release profile. Tests marked `#[ignore]` need live network.

## Configure

The first run creates `config/tickburst.toml` from `config/tickburst.example.toml`.
The `[strategy]` and `[strategy.tick]` sections are the trigger and risk rails. Read the
comments before changing anything. `[live.*]` (wallet, chain, clob, api, safety) is
commented out in the example. Uncomment and fill all five sections before `--live`.

Never commit `config/tickburst.toml`: it holds your private key and API secret.

## Run

```bash
# 1. Watch it decide without sending anything (no [live.*] needed).
./target/release-test/1centbot --dry-run

# 2. Read-only checks of your filled-in [live.*] (wallet, RPC, API key, balance).
./target/release-test/1centbot --preflight

# 3. Produce the readiness artifact the live gate requires (runs the test suite).
./target/release-test/1centbot --live-readiness

# 4. Real money. Needs the readiness artifact from step 3 for the SAME commit.
RECON_GIT_SHA=$(git rev-parse HEAD) ./target/release-test/1centbot --live
```

`--live` refuses to start unless `RECON_GIT_SHA` matches a real commit, so put this tree
in git first: `git init && git add -A && git commit -m init`.

Telemetry is written as JSONL under `data/`. Stop with Ctrl-C.

Do not run two cheap-ask takers on the same market at once. They race each other for
the same asks.

`--help` on the binary is the authoritative flag list.
