# Solana Meme Tracker — 2.2 Smart Restore (Version 2)

Discord 2.2 is the foundation. Its token, milestone, migration, and wallet alert embeds are unchanged. Discord 3 was used only as a source of selected trade-flow ideas; its UI, duplicate alert feed, command set, and API-heavy scanners were not copied.

## Version 2 summary

- **Default wallet removed.** The built-in dev wallet, the `Default Sniper` line in `/status`, the guild `-1` index entry, the `remove_inactive_wallets` exemption and the all-guild wallet fan-out are gone. The default existed only to serve a fallback recipient; wallet alerts now reach **only** guilds that added the wallet with `/addwallet` (or `/importwallets`). There is no fallback recipient.
- **Read-only JSON API** on the existing web port (see [API](#api)).
- **DexScreener discovery polling** and a batched market-data refresh (see [DexScreener polling](#dexscreener-polling)).
- **Real buy-side volume**, stored and shown by `/history` filters and the API.
- **Dynamic milestones** instead of a fixed multiplier list.
- **Per-guild alert filters** stored in the database, configured through `/setmode`.
- **Token lifecycle cleanup** every hour with a single summary line.
- **Integrations reconnected:** Raydium and Meteora discovery, pump.fun on-chain backstop, and a manual verification script.
- **Footers** on alert embeds are exactly `Solana Meme Tracker | Alert|Fresh launch|Migrated|Milestone|Wallet`.
- **Config cut from 84 environment variables to 16.**

## Earlier changes (2.2)

- Added bounded, keyless PumpPortal trade subscriptions with reconnect/resubscribe.
- Added early trade intelligence: dev-sell detection, buy/sell ratio, net SOL flow, independent buyers, duplicate-event suppression, and early market-cap drop checks.
- Integrated those checks into 2.2's existing pre-alert hold. Sparse trade data never rejects a token by itself.
- Added bounded token-processing workers so enrichment cannot block the launch WebSocket.
- Failed verification now releases the alert lock and uses persisted exponential retry/backoff instead of suppressing that band forever.
- Kept one global Discord command sync per boot; guild cleanup remains opt-in.
- Kept the queued Discord sender with 429/5xx backoff, disabled mentions, per-channel token cooldowns, and a bounded queue.
- Added market-data caching, per-cycle tracking caps, snapshot retention, and a conservative RPC request budget.
- Added keyless public RPC failover for network failures. Helius metadata/holder calls now share the same RPC budget and pause state.
- Limited paid/deep Helius analysis to plausible alert-band candidates instead of every launch.
- Added `Database.get_all_guilds()`.
- Wallet changes now refresh the scanner index immediately. JSON/TXT imports validate base58, de-duplicate, cap input size/count, and use deferred Discord responses.
- Added optional Helius webhook authentication with `HELIUS_WEBHOOK_SECRET`.
- Added `/checktoken` and `/history` (v2 also folded `/recent` into `/history`). `/status` now includes WebSocket, queue, early-flow, and RPC-budget diagnostics.

## Deliberately not imported from Discord 3

- Replacement embeds.
- `/setscore`, `/resync`, duplicate dev-wallet commands, and other overlapping controls.
- The funding-cluster tracer: it can spend up to ten RPC calls per candidate and treats a fee payer as a funder, which is not reliable evidence.
- The per-candidate third-party bonding-curve request.
- Automatic dev blacklisting based on timeouts or weak activity. A timeout is not proof of a rug.
- Per-guild command wiping/syncing on every ready event.

## Fresh launch callouts (5-second smart scan)

Every new pump.fun launch with a real name, symbol and mint is checked like this (only when `/togglelaunchalerts` is ON and `/setfreshchannel` is set):

1. **T+0** — Initial MC is captured from the free PumpPortal launch event.
2. **T+5** (`PRE_ALERT_VERIFY_SECONDS = 5`) — MC is re-checked from the PumpPortal trade feed (DexScreener is the fallback when no trade has happened yet).
3. MC down **more than 25%** (`PRE_ALERT_MAX_MC_DROP_PCT`), a dev sell, or weak early flow → dropped silently and recorded as rug evidence for dev reputation.
4. MC held or grew → one callout in the fresh channel: `Initial MC → Current MC (+x%)`, plus launch SOL, liquidity/volume (only if available), dev wallet (Solscan link), dev reputation and PVP siblings (DexScreener links). The CA is plain text in backticks.
5. If the T+5 data is missing or a fetch fails, nothing is sent. There is no pre-scoring embed.

Callouts are de-duplicated per mint. Normal Low/Medium/High alerts still go to the `/setchannel` channel with the unchanged scoring bands.

## Commands

- Configuration: `/setchannel`, `/setfreshchannel`, `/togglelaunchalerts`, `/setmode`, `/devtrack`
- Watched wallets: `/addwallet`, `/removewallet`, `/resetwallets`, `/listwallets`, `/importwallets`
- Inspection: `/checktoken` (deep dive), `/history`, `/top`, `/milestones`
- Diagnostics: `/status`

There are exactly 15 global slash commands (was 16; `/recent` was removed and folded into `/history`; the filters live in `/setmode`, so no command was added). `tests/test_v2.py::test_command_manifest_matches_readme` fails if this list and `commands.py` ever drift. Tokens with a missing, null or placeholder name/symbol are never stored as callouts and never shown by any command.

`/history [limit] [ephemeral] [filter]` — `ephemeral` defaults to true (set false to post publicly, which is what `/recent` offered). With no `filter` it behaves exactly as before. The filter is a space/comma separated list of conditions, AND-ed together:

| Condition | Meaning |
|---|---|
| `volume>5k` | 1h USD volume at or above 5,000 (`<` for a maximum) |
| `buyvol>2k` | real buy-side 1h USD volume |
| `vol24>50k` | 24h USD volume |
| `liq>3k` | liquidity |
| `mc>5k`, `mc<50k`, `mc:5k-20k` | market-cap bound or range |
| `band:high` or bare `high medium` | band name(s), OR-ed |
| `age<30m`, `age>2h`, `age:10m-1h` | token age window (s, m, h, d) |

Numbers take `k`/`m`/`b` suffixes. Bad input returns a short syntax hint instead of an error.

`/checktoken` shows Initial MC / Current MC, ATH, Volume, LP, the dev wallet (Solscan link), dev history (launched vs rugged) and up to 10 PVP siblings with MC, Volume, LP and age. It makes one free batched DexScreener call; fields with no data are omitted. Dev history and PVP siblings come from tokens this bot has recorded (plus archived dev totals), so they grow as the bot runs.

## Per-guild alert filters

Stored in `guild_settings` (not environment variables) and set with `/setmode`; every filter defaults to **off**, so guilds that never configure them behave as before. `/setmode` still sets the scan mode (now optional). Passing `0` turns a single filter off; `reset_filters:true` clears all. The reply lists the active filters.

| `/setmode` option | Column | Suppresses the alert when… |
|---|---|---|
| `min_buy_volume` | `min_buy_volume_usd` | real buy-side 1h volume (USD) is below it |
| `min_volume_5m` | `min_volume_5m_usd` | 5-minute volume (USD) is below it |
| `max_snipers` | `max_snipers` | non-dev buyers in the first 3 seconds exceed it |
| `min_age_minutes` | `min_age_seconds` | the token is younger than it |
| `min_liquidity` | `min_liquidity_usd` | liquidity (USD) is below it |
| `max_dev_rugs` | `max_dev_rugs` | the dev's recorded rugs exceed it |

Filters apply to band alerts, migration alerts and milestone alerts, per guild. A failing token stays in the database and keeps being tracked; only that guild's alert is skipped. A *minimum* filter treats unknown data as failing; a *maximum* filter treats unknown data as passing. The T+5 fresh-launch callouts are not filtered (there is no volume data yet at T+5). Sniper counts exist only for tokens whose launch was seen on PumpPortal.

## Milestones (dynamic)

On every refresh of a tracked token the bot computes `current market cap / initial market cap` (initial = first market cap ever seen). If that multiple is **higher than the highest multiple the coin has ever reported**, it sends a milestone embed (new multiple, market cap, price). There is no list of thresholds anywhere in the code — every new high is valid. Nothing fires at or below 1x; a drop sends nothing and never lowers the high-water mark (`coins.max_multiple`, raised atomically in SQL). Multiples are compared at the 2-decimal precision they are shown at. Milestones cover every category: fresh launches, migrated tokens, ordinary tracked tokens and PVP siblings. After the first band alert, a token can re-alert only through milestones. Existing tokens are backfilled at upgrade so they do not re-announce multiples they already reached.

Because every new high is valid, a steadily rising token produces many milestone embeds. That is the specified behaviour; the Discord send queue spaces them out.

## DexScreener polling

Every **3 seconds** each of four free discovery feeds is polled, each in its own loop with its own rate limiter, its own backoff and its own consecutive-failure counter:

| Feed | Path |
|---|---|
| paid orders | `/orders/v1/solana` |
| latest boosts | `/token-boosts/latest/v1` |
| top boosts | `/token-boosts/top/v1` |
| latest profiles | `/token-profiles/latest/v1` |

Every Solana mint seen in any feed is upserted into `coins` immediately with whatever the feed provides. A separate slower loop (every `SCAN_INTERVAL_SECONDS // 3` seconds, 10 by default) refreshes every actively tracked token with `/latest/dex/tokens/{mints}`, **30 mints per request**. Each refresh sets initial market cap on first sight, overwrites current market cap, raises ATH, and updates the initial-vs-current multiple. A 429 backs off that feed only; a gap is accepted and the normal cadence resumes — requests are never sped up to catch up.

Run `python verify_integrations.py dexscreener` to confirm the four feed paths against the live service (the `/orders/v1/solana` path in particular is worth checking on your account's first run).

## Real buy-side volume

DexScreener gives total volume and buy/sell counts, not buy-side USD. The approximation is `volume * buys / (buys + sells)` (1h, and 5m where the filter needs it). For tokens whose trades were observed on PumpPortal, the bot uses its own rolling trade buffer instead (bounded per mint; it falls back to the approximation if the buffer overflowed). Both numbers are stored (`buy_volume_1h` and `buy_volume_1h_approx`, plus the 5m pair and `buy_volume_source`), so `/history` filters, per-guild filters and the API all show the same value. Observed trades exist only while a mint is in the PumpPortal flow tracker (its first 5 minutes); after that the approximation is used.

## Token lifecycle

Every token discovered stays in the database whether or not it alerts. The hourly cleanup (one summary line, never one per token):

1. marks a token **dead** after `DEAD_TOKEN_MINUTES` without trades (`is_dead`, stops tracking);
2. deletes dead tokens `DEAD_DELETE_HOURS` after they were marked dead;
3. deletes **rugged** tokens `DEAD_DELETE_HOURS // 4` hours after the rug was recorded;
4. deactivates non-moving $100–$3k market-cap tokens after `DEAD_TOKEN_MINUTES // 3` without trades, then deletes them `DEAD_DELETE_HOURS // 3` hours later;
5. removes idle wallets and old snapshots.

A new token whose name and symbol match a token seen in the last 10 minutes is deleted silently at the existing duplicate check (older duplicates keep the previous flag-and-deactivate behaviour). Deleted tokens' launched/rugged counts are rolled into `dev_stats` first, so dev reputation survives cleanup. Milestone rows are deleted with their token.

## API

Read-only JSON on the same port as `/health`, served from the database and a 3-second in-process cache (nothing proxies DexScreener). UTF-8 JSON; fields with no data are omitted (never `null`/`0` placeholders); rejection reasons and retry timings are never exposed; 60 requests/minute per IP (`429` + `Retry-After`).

| Endpoint | Returns |
|---|---|
| `GET /api/tokens/{mint}` | one tracked token; `404 {"error":"not_tracked"}` if unknown, `400` if not a valid mint |
| `GET /api/tokens?sort=score\|market_cap&limit=20&offset=0` | `{total, count, tokens:[…]}` of active tokens (`limit` ≤ 100) |
| `GET /api/milestones?limit=10` | `{milestones:[{mint,name,symbol,multiplier,market_cap,…}]}` — same data as `/milestones` |
| `GET /api/history?limit=10&filter=band:high volume>5k` | `{tokens:[…]}` — same filters as `/history` |

Per-token shape: `mint` is top-level and `pairs[0]` follows DexScreener's `/latest/dex/tokens/{mint}` pair layout (`baseToken`, `priceUsd`, `marketCap`, `liquidity.usd`, `volume.{h24,h1,m5}`, `txns`, `pairCreatedAt`); everything DexScreener lacks is in `tracker` in snake_case (documented at the top of `api.py`).

```text
GET /api/tokens/So11111111111111111111111111111111111111112
200 {
  "schemaVersion": "1.0.0",
  "mint": "So111…112",
  "pairs": [{
    "chainId": "solana", "dexId": "pumpfun",
    "baseToken": {"address": "So111…112", "name": "Pepe", "symbol": "PEPE"},
    "priceUsd": "0.0003", "marketCap": 24000, "liquidity": {"usd": 5000},
    "volume": {"h24": 90000, "h1": 4000}, "txns": {"h1": {"buys": 30, "sells": 10}}
  }],
  "tracker": {
    "program_type": "pumpfun", "initial_market_cap": 8000, "ath_market_cap": 30000,
    "current_multiple": 3.0, "launch_sol": 1.5, "dev_wallet": "7BNa…",
    "dev_reputation": {"launched": 4, "rugged": 1},
    "pvp_siblings": [{"mint": "6EF8…", "name": "Pepe", "symbol": "PEPE", "market_cap": 9000}],
    "band": "medium", "score": 60, "red_flags": ["fat_bundle"],
    "real_buy_volume_1h": 3000, "buy_volume_source": "approx",
    "milestones_hit": [{"multiplier": 3.0, "market_cap": 24000, "hit_at": "2026-11-02T00:00:00+00:00"}],
    "created_at": "2026-11-01T00:00:00+00:00", "last_tracked_at": "2026-11-02T00:00:00+00:00"
  }
}

GET /api/tokens/6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
404 {"error": "not_tracked", "message": "This mint is not tracked.", "mint": "6EF8…"}
```

Collection responses carry `mint` on every item (a collection has no single top-level mint). The client IP for rate limiting is the right-most `X-Forwarded-For` hop (the left side is client-controlled).

## Integrations

| Integration | How it is used |
|---|---|
| DexScreener | four discovery feeds + batched market refresh (above) |
| PumpPortal | launch / migration / trade WebSocket (unchanged; a clean close now waits before reconnecting) |
| Raydium (`raydium.py`) | free api-v3 pool list polled every 60s; new non-quote mints are upserted, nothing deeper |
| Meteora (`meteora.py`) | free Meteora data API, same pattern as Raydium |
| pump.fun (`pumpfun.py`) | on-chain RPC detector used as a backstop only while PumpPortal has been silent for 2 minutes (small limits, inside the RPC budget) |

Verify them yourself (live network; prints success/failure with the real response summary):

```bash
python verify_integrations.py              # all five
python verify_integrations.py meteora      # one
pytest -m integration -v                   # the same checks as pytest smoke tests
```

The Meteora endpoint and field names and the DexScreener `/orders/v1/solana` path were written from documentation and could not be exercised in the build environment, so run the script once before trusting them.

## Run on Render

- Service type: **Web Service**
- Plan: **Free**
- Runtime: **Python 3.11.9**
- Build: `pip install -r requirements.txt`
- Start: `python -u bot.py`
- Health check: `/health`

### Environment variables (16)

Everything else is a constant in `config.py` or computed from these. Filters live in the database; the API uses `PORT`.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `DISCORD_TOKEN` | yes | — | bot token |
| `DATABASE_URL` | yes | — | PostgreSQL connection string |
| `HELIUS_API_KEY` | no | — | enables metadata, holder concentration, early-buyer analysis, webhooks |
| `HELIUS_WEBHOOK_SECRET` | no | — | rejects unsigned Helius webhook posts |
| `WEBHOOK_BASE_URL` | no | — | public URL for wallet webhooks (needs `HELIUS_API_KEY`) |
| `SOLANA_RPC_URL` | no | Helius or public RPC | custom RPC endpoint |
| `SOLANA_RPC_FALLBACK_URL` | no | public RPC when keyless | RPC failover |
| `MADEONSOL_API_KEY` | no | — | enables MadeOnSol enrichment (its URL is now built in) |
| `PORT` | no | `10000` | HTTP port for health, webhook and API |
| `DEFAULT_SCAN_MODE` | no | `medium` | `all`, `medium` or `high`; validated at startup |
| `SCAN_INTERVAL_SECONDS` | no | `30` | base cadence; the refresh sweep runs every third of it |
| `TRACKING_WINDOW_HOURS` | no | `24` | how long a token is actively tracked |
| `RPC_DAILY_REQUEST_BUDGET` | no | `2000` | raise when using a paid RPC |
| `RPC_MIN_GAP_SECONDS` | no | `1.0` | minimum gap between RPC calls |
| `DEAD_TOKEN_MINUTES` | no | `60` | no-trade window before a token is dead |
| `DEAD_DELETE_HOURS` | no | `24` | how long dead tokens linger before deletion |

Removed: `DEFAULT_WALLET`, `WIPE_GUILD_COMMANDS` (it iterated an empty guild list in `setup_hook`, so it never cleared anything; a one-off cleanup is better done by hand), `HELIUS_WEBHOOK_ID` (never read), `MADEONSOL_API_URL`, `ENABLE_RAYDIUM_DETECTION`, `RAYDIUM_*`/`PUMP*` program ids and ~60 tuning values whose defaults nobody overrides (they are plain constants now). Database tables and additive columns are created/migrated at startup.

### Free/keyless mode

No Helius or MadeOnSol key is required. PumpPortal supplies launch/trade events and DexScreener supplies market data. Public RPC is throttled, budgeted, and has a network-failure fallback. Public services still have no uptime guarantee; the scanner degrades gracefully instead of fabricating data or hammering retries.

Helius enables optional metadata, holder concentration, early-buyer analysis, and wallet webhooks. Wallet alerts need `WEBHOOK_BASE_URL` in addition to `HELIUS_API_KEY`. Set a long random `HELIUS_WEBHOOK_SECRET` to reject unsigned webhook posts.

## Tests

```bash
pip install -r requirements-dev.txt
pytest -q
```

Live-network smoke tests for the five integrations are excluded from that default run; run them with `pytest -m integration -v` (or `python verify_integrations.py`).

The suite covers the exact command set (15), `/history` filters, dynamic milestone re-firing, per-guild filters applied and skipped, API endpoints (valid JSON, omitted fields, 404/400, rate limit), cleanup windows, per-feed backoff, volume maths, footers, command registration and every command callback, original alert embed usage, successful verified sends, dev-sell rejection, retryable market-cap rejection, Discord mention safety, trade deduplication/unit normalization, PumpPortal routing, JSON/TXT wallet parsing, daily quota cutoffs, and RPC endpoint failover.

`tests/conftest.py` adds the small `Embed.f(name)` field helper the alert tests use. A live Discord send/login test requires your own bot token, guild, and PostgreSQL database and is intentionally not part of the automated suite.

## Sensible next improvements

1. Add a small staging guild smoke test that sends one embed, edits it, and deletes it before production rollout.
2. Persist RPC/quota counters in `system_state` if exact daily budgets must survive frequent restarts.
3. Add structured rejection metrics to `/status` so filter thresholds can be tuned from evidence rather than guesswork.
4. If public RPC remains unreliable at your hosting region, configure a dedicated `SOLANA_RPC_URL` and a separate fallback; do not simply increase request rates.
5. Add per-guild alert allowlists only if servers ask for different strategies; avoid duplicating the global scanner per guild.
