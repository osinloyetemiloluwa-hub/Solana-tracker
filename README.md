# Solana Meme Tracker — 2.2 Smart Restore

Discord 2.2 is the foundation. Its token, milestone, migration, and wallet alert embeds are unchanged. Discord 3 was used only as a source of selected trade-flow ideas; its UI, duplicate alert feed, command set, and API-heavy scanners were not copied.

## What changed

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
- Fixed the missing `Database.get_all_guilds()` method used by default-wallet broadcasts.
- Wallet changes now refresh the scanner index immediately. JSON/TXT imports validate base58, de-duplicate, cap input size/count, and use deferred Discord responses.
- Added optional Helius webhook authentication with `HELIUS_WEBHOOK_SECRET`.
- Added `/checktoken` and `/history`. `/status` now includes WebSocket, queue, early-flow, and RPC-budget diagnostics.

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
- Inspection: `/checktoken` (deep dive), `/history`, `/recent` (public/private choice), `/top`, `/milestones`
- Diagnostics: `/status`

There are exactly 16 global slash commands. Tokens with a missing, null or placeholder name/symbol are never stored as callouts and never shown by any command.

`/checktoken` shows Initial MC / Current MC, ATH, Volume, LP, the dev wallet (Solscan link), dev history (launched vs rugged) and up to 10 PVP siblings with MC, Volume, LP and age. It makes one free batched DexScreener call; fields with no data are omitted. Dev history and PVP siblings come from tokens this bot has recorded, so they grow as the bot runs.

## Run on Render

- Service type: **Web Service**
- Plan: **Free**
- Runtime: **Python 3.11.9**
- Build: `pip install -r requirements.txt`
- Start: `python -u bot.py`
- Health check: `/health`

Required environment variables:

```text
DISCORD_TOKEN=your_bot_token
DATABASE_URL=your_postgres_connection_string
```

Copy `.env.example` for optional settings. Database tables and additive alert-retry columns are created/migrated at startup.

### Free/keyless mode

No Helius or MadeOnSol key is required. PumpPortal supplies launch/trade events and DexScreener supplies market data. Public RPC is throttled, budgeted, and has a network-failure fallback. Public services still have no uptime guarantee; the scanner degrades gracefully instead of fabricating data or hammering retries.

Helius enables optional metadata, holder concentration, early-buyer analysis, and wallet webhooks. Wallet alerts need `WEBHOOK_BASE_URL` in addition to `HELIUS_API_KEY`. Set a long random `HELIUS_WEBHOOK_SECRET` to reject unsigned webhook posts.

## Tests

```bash
pip install -r requirements-dev.txt
pytest -q
```

The suite covers command registration and every command callback, original alert embed usage, successful verified sends, dev-sell rejection, retryable market-cap rejection, Discord mention safety, trade deduplication/unit normalization, PumpPortal routing, JSON/TXT wallet parsing, daily quota cutoffs, and RPC endpoint failover.

A live Discord send/login test requires your own bot token, guild, and PostgreSQL database and is intentionally not part of the automated suite.

## Sensible next improvements

1. Add a small staging guild smoke test that sends one embed, edits it, and deletes it before production rollout.
2. Persist RPC/quota counters in `system_state` if exact daily budgets must survive frequent restarts.
3. Add structured rejection metrics to `/status` so filter thresholds can be tuned from evidence rather than guesswork.
4. If public RPC remains unreliable at your hosting region, configure a dedicated `SOLANA_RPC_URL` and a separate fallback; do not simply increase request rates.
5. Add per-guild alert allowlists only if servers ask for different strategies; avoid duplicating the global scanner per guild.
