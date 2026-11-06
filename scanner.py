import asyncio
import time
from collections import OrderedDict
from datetime import datetime, timezone, timedelta

from alert_filters import evaluate_filters
from cache import TTLCache
from config import Config
from dexscreener import DexScreenerFeeds, parse_pair, pick_best_pair
from meteora import MeteoraDetector
from pumpfun import PumpFunDetector
from raydium import RaydiumDetector, QUOTE_MINTS
from pumpportal import PumpPortalDetector
from volume import compute_volume_metrics
from scoring import ScoringEngine
from milestones import MilestoneTracker
from red_flags import RedFlagDetector
from tiers import TierFilter
from enricher import TokenEnricher, is_valid_solana_address, has_valid_identity
from embeds import (
    create_token_alert_embed,
    create_milestone_embed,
    create_migration_embed,
    create_wallet_alert_embed,
    create_link_view,
    create_fresh_callout_embed,
)
from wallets import WalletTracker
from trade_flow import TradeFlowTracker


MIGRATION_MC_CEILING = 100_000
MIGRATION_MC_FLOOR = 5_000
MIGRATION_FLAG = "migrated"
SOL_MINT = "So11111111111111111111111111111111111111112"
FRESH_MAX_PENDING = 500
FRESH_SEEN_MAX = 5000
FEED_SEEN_MAX = 20000


class Scanner:
    def __init__(self, bot, db):
        self.bot = bot
        self.db = db
        self.rpc = None
        self.raydium = None
        self.meteora = None
        self.pumpfun = None
        self.pumpportal = None
        self.scoring = ScoringEngine()
        self.milestones = MilestoneTracker(db)
        self.wallet_tracker = WalletTracker()
        self.enricher = TokenEnricher(self.wallet_tracker)
        self.running = False

        self._dexscreener_consecutive_failures = 0
        self._helius_available = bool(Config.HELIUS_API_KEY)
        self._wallet_index = {}
        self._wallet_index_refresh_task = None
        self._buyer_analysis_cache = TTLCache(
            Config.BUYER_ANALYSIS_CACHE_MAX_ENTRIES, Config.BUYER_ANALYSIS_CACHE_TTL_SECONDS
        )
        self._snapshot_gate = TTLCache(5000, 60)
        self.feeds = DexScreenerFeeds(tokens_limiter=self.enricher.dexscreener_limiter)
        self._feed_seen = TTLCache(FEED_SEEN_MAX, 3600)
        self._loop_tasks = []
        self._milestone_tasks = set()
        self._started_wall = time.time()

        self._in_flight_mints = set()
        self._queued_mints = set()
        self._token_queue = asyncio.Queue(maxsize=Config.SCANNER_QUEUE_MAXSIZE)
        self._worker_tasks = []
        self._verification_tasks = set()
        self.flow = TradeFlowTracker()
        self._market_cache = {}
        self._started = False
        self._dropped_tokens = 0

        # Fresh-launch (T+5) smart scan state
        self._fresh_tasks = set()
        self._fresh_seen = OrderedDict()
        self._fresh_targets_cache = (0.0, [])
        self._sol_usd_cache = (0.0, None)
        self._pending_rugged = OrderedDict()

    async def initialize(self):
        from solana_rpc import SolanaRPC
        self.rpc = SolanaRPC()
        self.enricher._rpc = self.rpc
        self.enricher._owns_rpc = False
        self.raydium = RaydiumDetector(lambda: self.wallet_tracker.session)
        self.meteora = MeteoraDetector(lambda: self.wallet_tracker.session)
        self.pumpfun = PumpFunDetector(self.rpc)
        self.pumpportal = PumpPortalDetector(
            self._on_new_token_from_ws, self.flow.on_trade
        )

        await self.wallet_tracker.init_session()
        await self._refresh_wallet_index()

        if self._helius_available:
            print("Helius API key detected - smart money + bundle detection enabled")
        else:
            print("No HELIUS_API_KEY set - smart money + bundle detection DISABLED")

        if Config.MADEONSOL_API_KEY:
            print("MadeOnSol API key detected - full filter set active")
        else:
            print("No MadeOnSol config - using DexScreener + Helius only")

    async def close(self):
        if self._wallet_index_refresh_task:
            self._wallet_index_refresh_task.cancel()
            try:
                await self._wallet_index_refresh_task
            except (asyncio.CancelledError, Exception):
                pass
        tasks = [
            *self._loop_tasks, *self._worker_tasks, *self._verification_tasks,
            *self._fresh_tasks, *self._milestone_tasks,
        ]
        for task in tasks:
            if task and not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._loop_tasks.clear()
        self._worker_tasks.clear()
        self._verification_tasks.clear()
        self._fresh_tasks.clear()
        self._milestone_tasks.clear()
        if self.pumpportal:
            await self.pumpportal.stop()
        if self.rpc:
            await self.rpc.close()
        if self.enricher:
            await self.enricher.close()
        await self.wallet_tracker.close()

    async def start(self):
        if self._started:
            return
        self._started = True
        self.running = True
        self._started_wall = time.time()
        print(
            f"Scanner started. Feeds every {Config.FEED_POLL_SECONDS:g}s, "
            f"market refresh every {Config.REFRESH_INTERVAL_SECONDS}s"
        )

        await self.pumpportal.start()
        self._worker_tasks = [
            asyncio.create_task(self._token_worker(i), name=f"token-worker-{i}")
            for i in range(Config.SCANNER_WORKERS)
        ]
        self._wallet_index_refresh_task = asyncio.create_task(
            self._wallet_index_loop(), name="wallet-index-refresh"
        )
        # Every feed runs in its own loop with its own limiter/backoff.
        self._loop_tasks = [
            asyncio.create_task(self._feed_loop(name), name=f"feed-{name}")
            for name in self.feeds.feeds
        ]
        self._loop_tasks += [
            asyncio.create_task(self._refresh_loop(), name="market-refresh"),
            asyncio.create_task(self._pool_discovery_loop(), name="pool-discovery"),
            asyncio.create_task(self._pumpfun_fallback_loop(), name="pumpfun-fallback"),
            asyncio.create_task(self._cleanup_loop(), name="cleanup"),
        ]
        await asyncio.gather(*self._loop_tasks, return_exceptions=True)

    async def stop(self):
        self.running = False
        self._started = False
        if self.pumpportal:
            await self.pumpportal.stop()

    async def _token_worker(self, worker_id):
        while True:
            token_data = await self._token_queue.get()
            try:
                raw = token_data.get("raw_event") or {}
                if raw.get("txType") == "migrate":
                    await self._handle_migration(token_data)
                else:
                    await self.process_token(token_data)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                mint = token_data.get("mint_address", "?")
                print(f"Token worker {worker_id} failed for {mint[:8]}: {type(exc).__name__}: {exc}")
            finally:
                self._queued_mints.discard(token_data.get("mint_address"))
                self._token_queue.task_done()

    async def _wallet_index_loop(self):
        while self.running:
            try:
                await asyncio.sleep(300)
                await self._refresh_wallet_index()
            except asyncio.CancelledError:
                return
            except Exception as e:
                print(f"Wallet index refresh error: {type(e).__name__}: {e}")
                await asyncio.sleep(30)

    async def _refresh_wallet_index(self):
        wallets = await self.db.get_all_wallets()
        idx = {}
        for w in wallets:
            addr = (w["address"] or "").strip()
            if not addr:
                continue
            idx[addr.lower()] = {
                "guild_id": w["guild_id"],
                "label": w["label"],
                "channel_id": w["channel_id"],
            }

        self._wallet_index = idx
        print(f"Wallet index loaded: {len(idx)} entry(ies)")

    def _match_wallet(self, wallet_address):
        if not wallet_address:
            return None
        return self._wallet_index.get(wallet_address.lower())

    async def _on_new_token_from_ws(self, token_data):
        raw = token_data.get("raw_event") or {}
        mint = token_data.get("mint_address")
        try:
            if raw.get("txType") != "migrate":
                self.flow.register(raw)
            if raw.get("txType") == "create":
                await self._maybe_start_fresh_scan(token_data)
            if mint:
                # Always tracked: record the mint now so the refresh loop and
                # milestones cover it even if the later checks reject or drop it.
                name, symbol = token_data.get("name"), token_data.get("symbol")
                good = has_valid_identity(name, symbol, mint)
                await self._upsert_discovered([{
                    "mint_address": mint, "program_type": token_data.get("program_type") or "pumpfun",
                    "name": name if good else None, "symbol": symbol if good else None,
                    "image_url": token_data.get("image_url"), "source": "pumpportal",
                }])
            if not mint or mint in self._queued_mints or mint in self._in_flight_mints:
                return
            if self._token_queue.full():
                self._dropped_tokens += 1
                if self._dropped_tokens % 25 == 1:
                    print(f"Scanner queue full; dropped {self._dropped_tokens} launch event(s)")
                return
            self._queued_mints.add(mint)
            self._token_queue.put_nowait(token_data)
        except Exception as exc:
            self._queued_mints.discard(mint)
            print(f"WS token processing error: {type(exc).__name__}: {exc}")

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------

    async def _feed_loop(self, name):
        """Poll one DexScreener discovery feed on its own cadence.

        A failed or rate-limited poll is simply a gap: the next attempt is at
        the normal cadence (the feed's own backoff may delay it), never faster.
        """
        while self.running:
            try:
                items = await self.feeds.poll_feed(self.wallet_tracker.session, name)
                await self._upsert_discovered(items, default_program="dexscreener")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"Feed {name} error: {type(exc).__name__}: {exc}")
            await asyncio.sleep(Config.FEED_POLL_SECONDS)

    async def _upsert_discovered(self, items, default_program="dexscreener"):
        """Upsert newly seen mints into `coins` with whatever the source gave."""
        new = 0
        for item in items or []:
            mint = item.get("mint_address")
            if not mint or self._feed_seen.get(mint):
                continue
            if mint.lower() in QUOTE_MINTS or not is_valid_solana_address(mint):
                continue
            self._feed_seen.set(mint, True)
            try:
                _coin_id, inserted = await self.db.upsert_discovered(
                    mint, item.get("program_type") or default_program,
                    name=item.get("name"), symbol=item.get("symbol"),
                    image_url=item.get("image_url"), source=item.get("source") or item.get("feed"),
                    revive=bool(item.get("feed")),  # DexScreener feed sightings re-track
                )
                new += 1 if inserted else 0
            except Exception as exc:
                self._feed_seen.set(mint, False)
                print(f"Discovery upsert failed for {mint[:8]}: {type(exc).__name__}: {exc}")
        return new

    async def _refresh_loop(self):
        while self.running:
            try:
                await self.refresh_all()
                await self.enricher.drain_deferred()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"Refresh error: {type(exc).__name__}: {exc}")
            await asyncio.sleep(Config.REFRESH_INTERVAL_SECONDS)

    async def _pool_discovery_loop(self):
        """Raydium and Meteora: discover new pools, upsert mints, nothing deeper."""
        while self.running:
            for detector in (self.raydium, self.meteora):
                try:
                    await self._upsert_discovered(await detector.detect_new_pairs())
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    print(f"Pool discovery error: {type(exc).__name__}: {exc}")
            await asyncio.sleep(Config.POOL_POLL_INTERVAL_SECONDS)

    async def _pumpfun_fallback_loop(self):
        """On-chain pump.fun detector; runs only while PumpPortal is silent."""
        while self.running:
            await asyncio.sleep(Config.PUMPFUN_FALLBACK_POLL_SECONDS)
            try:
                last = self.pumpportal.last_event_at or self._started_wall
                if time.time() - last < Config.PUMPFUN_FALLBACK_SILENCE_SECONDS:
                    continue
                if not self.rpc or self.rpc.is_paused():
                    continue
                tokens = await self.pumpfun.detect_new_tokens()
                await self._upsert_discovered(tokens, default_program="pumpfun")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"PumpFun fallback error: {type(exc).__name__}: {exc}")

    async def _cleanup_loop(self):
        await asyncio.sleep(60)
        while self.running:
            await self.run_cleanup()
            await asyncio.sleep(3600)

    async def run_cleanup(self):
        """Hourly lifecycle cleanup. One summary line; never one line per token."""
        parts = {}
        try:
            parts.update(await self.db.run_lifecycle_cleanup())
        except Exception as exc:
            print(f"Lifecycle cleanup error: {type(exc).__name__}: {exc}")
        try:
            parts["wallets_removed"] = await self.db.remove_inactive_wallets(Config.IDLE_WALLET_MAX_DAYS)
            parts["snapshots_removed"] = await self.db.cleanup_old_snapshots(
                Config.SNAPSHOT_RETENTION_HOURS
            )
        except Exception as exc:
            print(f"Cleanup error: {type(exc).__name__}: {exc}")
        print("Cleanup: " + ", ".join(f"{k}={v}" for k, v in parts.items()))
        return parts

    async def process_token(self, token_data):
        mint = token_data.get("mint_address")
        if not mint or not is_valid_solana_address(mint):
            return

        if mint in self._in_flight_mints:
            return
        self._in_flight_mints.add(mint)
        try:
            await self._process_token_locked(mint, token_data)
        finally:
            self._in_flight_mints.discard(mint)

    async def _process_token_locked(self, mint, token_data):
        existing_coin = await self.db.get_coin_by_mint(mint)
        if existing_coin and existing_coin.get("alerted_high"):
            return

        priority = "high" if token_data.get("from_wallet_buy") else "normal"
        enrichment = await self.enricher.enrich(mint, priority=priority)

        for k in (
            "price", "market_cap", "image_url", "liquidity", "volume_24h",
            "volume_1h", "buys_1h", "sells_1h", "price_change_h1",
            "holder_count", "top10_holder_pct",
            "deployer_tier", "deployer_bonded", "deployer_total",
            "kol_buying", "swarm_3plus_pct", "volume_5m", "buys_5m", "sells_5m",
        ):
            token_data[k] = enrichment.get(k)

        if not token_data.get("name"):
            token_data["name"] = enrichment.get("name")
        if not token_data.get("symbol"):
            token_data["symbol"] = enrichment.get("symbol")
        if not token_data.get("uri"):
            token_data["uri"] = enrichment.get("uri")

        # A token without a real name/symbol is never stored or alerted.
        if not has_valid_identity(token_data.get("name"), token_data.get("symbol"), mint):
            print(f"Ignoring {mint[:8]}: missing or placeholder name/symbol")
            return

        # Chase guard: if it already ran, you missed it.
        ch1 = token_data.get("price_change_h1")
        if ch1 is not None and ch1 > Config.CHASE_MAX_H1_PCT:
            print(
                f"Chase skip for {mint[:8]}: "
                f"+{ch1:.0f}% in 1h > {Config.CHASE_MAX_H1_PCT:.0f}% (missed)"
            )
            token_data["red_flags"] = ["chased"]
            try:
                coin_id = await self.db.insert_coin(token_data)
                await self._apply_pending_rug(mint)
                await self.db.set_red_flags(coin_id, "chased")
                await self.db.deactivate_coin(coin_id)
            except Exception as e:
                print(f"Chase insert failed: {type(e).__name__}: {e}")
            return

        try:
            dup = await self.db.find_recent_duplicate(
                token_data.get("name"), token_data.get("symbol"), mint
            )
        except Exception as e:
            print(f"Duplicate check failed: {type(e).__name__}: {e}")
            dup = None

        if dup and self._within_minutes(dup.get("created_at"), Config.DUPLICATE_DELETE_WINDOW_MINUTES):
            # Very recent copy of an existing name+symbol: removed silently.
            try:
                await self.db.delete_coin_by_mint(mint)
            except Exception as e:
                print(f"Duplicate delete failed: {type(e).__name__}: {e}")
            return

        if dup:
            print(
                f"{mint[:8]}: duplicate of "
                f"{dup['mint_address'][:8]} ({dup['name']} / {dup['symbol']})"
            )
            token_data["red_flags"] = ["duplicate"]
            try:
                coin_id = await self.db.insert_coin(token_data)
                await self._apply_pending_rug(mint)
                await self.db.set_red_flags(coin_id, "duplicate")
                await self.db.deactivate_coin(coin_id)
            except Exception as e:
                print(f"Duplicate insert failed: {type(e).__name__}: {e}")
            return

        created_at = token_data.get("created_at")
        token_data["age_seconds"] = (time.time() - created_at) if created_at else None

        try:
            deep_scan_mc = float(token_data.get("market_cap") or 0)
        except (TypeError, ValueError):
            deep_scan_mc = 0.0
        deep_scan_candidate = (
            Config.HELIUS_DEEP_SCAN_MIN_MC <= deep_scan_mc <= Config.HELIUS_DEEP_SCAN_MAX_MC
        )
        if deep_scan_candidate and token_data.get("liquidity"):
            analysis = await self._analyze_early_buyers(mint, token_data)
            token_data.update(analysis)
        else:
            token_data.update({
                "bundle_detected": False,
                "bot_farm": False,
                "smart_money_count": 0,
                "watched_wallet_match": False,
                "unique_buyer_ratio": 1.0,
                "red_flags": [],
            })

        if RedFlagDetector.fat_bundle(
            token_data.get("age_seconds"),
            token_data.get("market_cap"),
            Config.FAT_BUNDLE_MC_THRESHOLD,
            Config.FAT_BUNDLE_AGE_SECONDS,
        ):
            token_data["fat_bundle"] = True
            token_data.setdefault("red_flags", []).append("fat_bundle")
            print(f"Fat bundle detected: {mint[:8]} - skipping")

        tier = TierFilter.classify(token_data)
        token_data["tier"] = tier

        score_data = self.scoring.calculate_score(token_data)
        token_data["score"] = score_data["score"]
        token_data["score_label"] = score_data["label"]

        coin_id = await self.db.insert_coin(token_data)
        await self._apply_pending_rug(mint)
        if token_data.get("red_flags"):
            await self.db.set_red_flags(coin_id, ",".join(token_data["red_flags"]))

        has_market = token_data.get("market_cap") and token_data.get("liquidity")
        if has_market:
            await self._alert_token(coin_id, token_data, score_data, tier)

        sm_count = token_data.get("smart_money_count", 0) or 0
        if sm_count >= 1:
            await self._send_matching_wallet_alerts(token_data)

    async def _alert_token(self, coin_id, token_data, score_data, tier):
        band = score_data.get("band")
        if not band:
            return False
        if not has_valid_identity(
            token_data.get("name"), token_data.get("symbol"), token_data.get("mint_address")
        ):
            return False

        coin_row = await self.db.get_coin_by_mint(token_data["mint_address"])
        # One band alert per token. Re-alerts happen only through milestones.
        if coin_row and (coin_row.get("alerted_low") or coin_row.get("alerted_medium")
                         or coin_row.get("alerted_high")):
            return False
        retry_at = coin_row.get("next_alert_retry_at") if coin_row else None
        if retry_at is not None:
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            if retry_at > datetime.now(timezone.utc):
                return False

        await self.db.mark_band_alerted(coin_id, band)

        task = asyncio.create_task(
            self._verified_send(coin_id, dict(token_data), score_data),
            name=f"verify-{token_data['mint_address'][:8]}",
        )
        self._verification_tasks.add(task)
        task.add_done_callback(self._verification_done)
        return True

    def _verification_done(self, task):
        self._verification_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            print(f"Verification task failed: {type(exc).__name__}: {exc}")

    async def _reject_alert(self, coin_id, band, mint, reason, hard=False):
        print(f"Alert rejected for {mint[:8]}: {reason}")
        attempts = 0
        try:
            row = await self.db.get_coin_by_mint(mint)
            attempts = int(row.get("alert_attempts") or 0) if row else 0
            delay = min(
                Config.ALERT_RETRY_BASE_SECONDS * (2 ** min(attempts, 4)),
                Config.ALERT_RETRY_MAX_SECONDS,
            )
            await self.db.reject_alert(coin_id, band, reason, delay)
            if hard:
                await self.db.deactivate_coin(coin_id)
                if "Dev sold" in (reason or ""):
                    await self._mark_rugged(mint, reason)
        except Exception as exc:
            print(f"Alert rejection persistence failed for {mint[:8]}: {type(exc).__name__}: {exc}")

    async def _verified_send(self, coin_id, token_data, score_data):
        mint = token_data["mint_address"]
        band = score_data.get("band") or score_data.get("label")
        initial_mc = token_data.get("market_cap") or 0
        delay = Config.PRE_ALERT_VERIFY_SECONDS

        try:
            if delay > 0:
                await asyncio.sleep(delay)

            flow_reason = self.flow.rejection_reason(mint)
            if flow_reason:
                hard = "Dev sold" in flow_reason
                await self._reject_alert(coin_id, band, mint, flow_reason, hard=hard)
                return

            try:
                market = await self.get_market_data_with_fallback(mint, force=True)
            except Exception as e:
                await self._reject_alert(
                    coin_id, band, mint, f"Verify fetch failed: {type(e).__name__}"
                )
                return

            if not market or market.get("market_cap") is None:
                await self._reject_alert(coin_id, band, mint, "No market data on re-check")
                return

            new_mc = float(market["market_cap"])
            if initial_mc > 0:
                drop_pct = (initial_mc - new_mc) / initial_mc * 100.0
                if drop_pct >= Config.PRE_ALERT_MAX_MC_DROP_PCT:
                    await self._reject_alert(
                        coin_id, band, mint,
                        f"MC fell {drop_pct:.1f}% during {delay}s hold",
                    )
                    return

            ch1 = market.get("price_change_h1")
            if ch1 is not None and ch1 > Config.CHASE_MAX_H1_PCT:
                await self._reject_alert(
                    coin_id, band, mint,
                    f"+{ch1:.0f}% in 1h after hold (chase guard)",
                )
                return

            liveness = self._passes_liveness(market)
            if not liveness["ok"]:
                await self._reject_alert(coin_id, band, mint, liveness["reason"])
                return

            token_data["market_cap"] = new_mc
            token_data["liquidity"] = market.get("liquidity") or token_data.get("liquidity")
            token_data["volume_24h"] = market.get("volume_24h") or token_data.get("volume_24h")
            token_data["volume_1h"] = market.get("volume_1h")
            if market.get("image_url"):
                token_data["image_url"] = market["image_url"]

            await self._attach_context(token_data)
            token_data.update(await self._volume_metrics(mint, market))
            token_data["sniper_count"] = self._sniper_count(mint)
            metrics = self._alert_metrics(token_data)

            eligible = 0
            sent = 0
            for guild_id in await self.get_all_guilds():
                settings = await self.db.get_guild_settings(guild_id)
                if not settings or not settings["alert_channel_id"]:
                    continue
                mode = settings["scan_mode"] or Config.DEFAULT_SCAN_MODE
                if not self.scoring.should_alert(score_data["label"], mode):
                    continue
                if not evaluate_filters(settings, metrics)[0]:
                    continue  # this guild's filters suppress the alert only
                eligible += 1
                try:
                    embed = create_token_alert_embed(token_data, score_data, None)
                    view = create_link_view(mint)
                    message = await self.bot.send_discord_alert(
                        settings["alert_channel_id"], embed=embed, view=view,
                        is_token_alert=True,
                    )
                    await self.db.update_coin_alert(
                        coin_id, message.id, settings["alert_channel_id"]
                    )
                    sent += 1
                except Exception as e:
                    print(f"Verified send error: {type(e).__name__}: {e}")

            if eligible and not sent:
                await self._reject_alert(coin_id, band, mint, "All Discord sends failed")
            else:
                await self.db.confirm_alert(coin_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._reject_alert(
                coin_id, band, mint, f"Verification error: {type(exc).__name__}"
            )

    def _passes_liveness(self, market):
        mc = market.get("market_cap")
        lp = market.get("liquidity") or 0
        vol1h = market.get("volume_1h")
        vol24h = market.get("volume_24h") or 0
        buys = market.get("buys_1h") or 0
        sells = market.get("sells_1h") or 0

        if not mc or mc <= 0:
            return {"ok": False, "reason": "missing market cap"}

        if lp < 500:
            return {"ok": False, "reason": f"LP ${lp:,.0f} < $500 (rug risk)"}

        v = vol1h if vol1h is not None else vol24h
        if v <= 0:
            return {"ok": False, "reason": "zero volume"}

        ratio = v / mc
        if ratio < Config.FILTER_MIN_VOL_MC_RATIO:
            return {
                "ok": False,
                "reason": f"vol/mc {ratio:.2%} < {Config.FILTER_MIN_VOL_MC_RATIO:.0%}",
            }

        if sells > 0:
            bs_ratio = buys / sells
            if bs_ratio < Config.FILTER_MIN_BUY_SELL_RATIO:
                return {
                    "ok": False,
                    "reason": f"buy/sell {bs_ratio:.2f} < {Config.FILTER_MIN_BUY_SELL_RATIO}",
                }

        return {"ok": True, "reason": "alive"}

    async def _analyze_early_buyers(self, mint, token_data):
        result = {
            "bundle_detected": False,
            "bot_farm": False,
            "smart_money_count": 0,
            "watched_wallet_match": False,
            "unique_buyer_ratio": 1.0,
            "red_flags": [],
        }
        if not self._helius_available:
            return result
        if self.rpc and self.rpc.is_paused():
            return result

        cached = self._buyer_analysis_cache.get(mint)
        if cached:
            return cached

        try:
            sigs = await self.rpc.get_recent_signatures(
                mint, limit=Config.EARLY_BUYER_LOOKBACK + 5
            )
            if not sigs:
                self._buyer_analysis_cache.set(mint, result)
                return result

            sigs_sorted = sorted(sigs, key=lambda s: s.get("slot", 0))
            dev_wallet = (token_data.get("dev_wallet") or "").lower()
            slot_map = {}
            buyer_wallets = set()
            buy_amounts = []
            timestamps = []
            total_txs = 0

            for sig_info in sigs_sorted[:min(Config.EARLY_BUYER_LOOKBACK, 5)]:
                sig = sig_info.get("signature")
                slot = sig_info.get("slot")
                block_time = sig_info.get("blockTime", 0)
                if not sig:
                    continue
                tx = await self.rpc.get_transaction(sig)
                if not tx:
                    continue
                buyer = self._extract_fee_payer(tx)
                if not buyer:
                    continue
                buyer_lower = buyer.lower()
                if dev_wallet and buyer_lower == dev_wallet:
                    continue
                total_txs += 1
                buyer_wallets.add(buyer_lower)
                if slot:
                    slot_map.setdefault(slot, set()).add(buyer_lower)
                if block_time:
                    timestamps.append(block_time)
                amount = self._extract_buy_amount(tx)
                if amount:
                    buy_amounts.append(amount)

            if RedFlagDetector.similar_buy_amounts(buy_amounts, Config.SIMILAR_AMOUNT_TOLERANCE):
                result["red_flags"].append("similar_amounts")
            if RedFlagDetector.same_slot_bundle(slot_map):
                result["bundle_detected"] = True
                result["red_flags"].append("same_slot_bundle")
            if RedFlagDetector.sequential_buys(timestamps, Config.SEQUENTIAL_GAP_STD_MAX):
                result["red_flags"].append("sequential_buys")
            if total_txs >= 6:
                ratio = len(buyer_wallets) / total_txs
                result["unique_buyer_ratio"] = ratio
                if ratio < 0.4 and len(buyer_wallets) < 4:
                    result["bot_farm"] = True
                    result["red_flags"].append("low_unique_buyer_ratio")

            sm_hits = sum(1 for b in buyer_wallets if b in self._wallet_index)
            result["smart_money_count"] = sm_hits
            result["watched_wallet_match"] = sm_hits >= 1

            self._buyer_analysis_cache.set(mint, result)
            return result
        except Exception as e:
            msg = str(e)
            if "paused" in msg.lower() or "exhausted" in msg.lower():
                return result
            print(f"Early buyer analysis error: {type(e).__name__}: {e}")
            return result

    def _extract_fee_payer(self, tx):
        try:
            tx_obj = tx.get("transaction") or {}
            msg = tx_obj.get("message") or {}
            keys = msg.get("accountKeys") or []
            if not keys:
                return None
            first = keys[0]
            return first.get("pubkey") if isinstance(first, dict) else first
        except Exception:
            return None

    def _extract_buy_amount(self, tx):
        try:
            meta = tx.get("meta") or {}
            pre = meta.get("preBalances") or []
            post = meta.get("postBalances") or []
            if pre and post:
                return abs(pre[0] - post[0]) / 1e9
            return 0.0
        except Exception:
            return 0.0

    async def _handle_migration(self, token_data):
        mint = token_data.get("mint_address")
        if not mint:
            return

        coin = await self.db.get_coin_by_mint(mint)

        flags = (coin.get("red_flags") or "") if coin else ""
        flag_set = {f for f in flags.split(",") if f}
        if MIGRATION_FLAG in flag_set:
            return

        if coin and (coin.get("alerted_low")
                     or coin.get("alerted_medium")
                     or coin.get("alerted_high")):
            print(f"Migration suppressed for {mint[:8]}: token already alerted")
            flag_set.add(MIGRATION_FLAG)
            try:
                await self.db.set_red_flags(coin["id"], ",".join(sorted(flag_set)))
            except Exception as e:
                print(f"Migration flag update failed: {type(e).__name__}: {e}")
            return

        market = None
        try:
            market = await self.get_market_data_with_fallback(mint)
        except Exception as e:
            print(f"Migration market fetch failed for {mint[:8]}: {type(e).__name__}: {e}")

        fresh_mc = market.get("market_cap") if market else None

        if fresh_mc is None:
            print(f"Migration suppressed for {mint[:8]}: no fresh market data")
            if coin:
                flag_set.add(MIGRATION_FLAG)
                await self.db.set_red_flags(coin["id"], ",".join(sorted(flag_set)))
            return

        if fresh_mc > MIGRATION_MC_CEILING:
            print(
                f"Migration suppressed for {mint[:8]}: "
                f"MC ${fresh_mc:,.0f} > ceiling ${MIGRATION_MC_CEILING:,}"
            )
            if coin:
                flag_set.add(MIGRATION_FLAG)
                await self.db.set_red_flags(coin["id"], ",".join(sorted(flag_set)))
            return

        if fresh_mc < MIGRATION_MC_FLOOR:
            print(
                f"Migration suppressed for {mint[:8]}: "
                f"MC ${fresh_mc:,.0f} < floor ${MIGRATION_MC_FLOOR:,} (dead/rug)"
            )
            if coin:
                flag_set.add(MIGRATION_FLAG)
                await self.db.set_red_flags(coin["id"], ",".join(sorted(flag_set)))
            return

        if not coin:
            token_data["is_migrated"] = True
            token_data["program_type"] = "pumpswap"
            if market:
                token_data["price"] = market.get("price")
                token_data["market_cap"] = market.get("market_cap")
                token_data["liquidity"] = market.get("liquidity")
                token_data["volume_24h"] = market.get("volume_24h")
                token_data["image_url"] = market.get("image_url")
            await self.process_token(token_data)
            coin = await self.db.get_coin_by_mint(mint)
            if not coin:
                return
            flags = (coin.get("red_flags") or "")
            flag_set = {f for f in flags.split(",") if f}

        if market and market.get("price") is not None:
            try:
                await self.db.update_coin_snapshot(
                    coin["id"], market["price"], market.get("market_cap") or 0
                )
                await self.db.insert_snapshot(
                    coin["id"], market["price"], market.get("market_cap") or 0
                )
            except Exception as e:
                print(f"Migration DB update failed for {mint[:8]}: {type(e).__name__}: {e}")

        flag_set.add(MIGRATION_FLAG)
        try:
            await self.db.set_red_flags(coin["id"], ",".join(sorted(flag_set)))
        except Exception as e:
            print(f"Migration flag update failed: {type(e).__name__}: {e}")

        fresh = dict(coin)
        if market:
            fresh["current_market_cap"] = market.get("market_cap")
            fresh["image_url"] = market.get("image_url") or coin.get("image_url")

        first_mc = coin.get("first_seen_market_cap")
        cur_mc = fresh.get("current_market_cap")
        if first_mc is not None and cur_mc:
            try:
                if float(first_mc) > float(cur_mc) * 10:
                    print(
                        f"Discarding bogus first_seen_market_cap for {mint[:8]}: "
                        f"${float(first_mc):,.0f} vs current ${float(cur_mc):,.0f}"
                    )
                    first_mc = None
            except (TypeError, ValueError):
                pass
        fresh["first_seen_market_cap"] = first_mc
        fresh["first_seen_price"] = coin.get("first_seen_price")

        metrics = self._alert_metrics(fresh)
        for guild_id in await self.get_all_guilds():
            settings = await self.db.get_guild_settings(guild_id)
            if not settings or not settings["alert_channel_id"]:
                continue
            if not evaluate_filters(settings, metrics)[0]:
                continue
            try:
                embed = create_migration_embed(fresh, token_data)
                view = create_link_view(mint)
                await self.bot.send_discord_alert(
                    settings["alert_channel_id"], embed=embed, view=view,
                    is_token_alert=False,
                )
            except Exception as e:
                print(f"Migration alert send failed for {guild_id}: {type(e).__name__}: {e}")

    async def get_market_data_with_fallback(self, mint, force=False):
        now = time.monotonic()
        cached = self._market_cache.get(mint)
        if not force and cached and now - cached[0] <= Config.MARKET_CACHE_TTL_SECONDS:
            return dict(cached[1])

        data = await self._dexscreener_market_data(mint)
        if data:
            self._dexscreener_consecutive_failures = 0
        else:
            self._dexscreener_consecutive_failures += 1
            if (self._dexscreener_consecutive_failures >= Config.DEXSCREENER_FAILURE_THRESHOLD
                    and self._helius_available):
                data = await self._helius_market_data(mint)

        if data:
            self._market_cache[mint] = (now, dict(data))
            if len(self._market_cache) > 1000:
                oldest = min(self._market_cache, key=lambda key: self._market_cache[key][0])
                self._market_cache.pop(oldest, None)
        return data

    async def _dexscreener_market_data(self, mint):
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
            async with self.enricher.dexscreener_limiter:
                async with self.wallet_tracker.session.get(url, timeout=10) as response:
                    if response.status == 429:
                        self.enricher.dexscreener_limiter.record_429()
                        return None
                    if response.status != 200:
                        return None
                    self.enricher.dexscreener_limiter.record_success()
                    data = await response.json()

            best = pick_best_pair(data.get("pairs") or [], mint)
            if not best:
                return None
            return parse_pair(best, mint)
        except Exception as exc:
            print(f"DexScreener error: {type(exc).__name__}: {exc}")
            return None

    async def _helius_market_data(self, mint):
        if not self.rpc or self.rpc.is_paused() or not Config.HELIUS_API_KEY:
            return None
        try:
            result = await self.rpc.get_asset(mint) or {}
            content = result.get("content") or {}
            links = content.get("links") or {}
            return {
                "price": None, "market_cap": None,
                "liquidity": None, "volume_24h": None,
                "volume_1h": None, "buys_1h": 0, "sells_1h": 0,
                "price_change_h1": None,
                "image_url": links.get("image"),
                "source": "helius",
            }
        except Exception:
            return None

    async def _send_matching_wallet_alerts(self, token_data):
        dev_wallet = token_data.get("dev_wallet")
        match = self._match_wallet(dev_wallet) if dev_wallet else None
        if not match:
            return
        channel_id = match.get("channel_id")
        if not channel_id:
            settings = await self.db.get_guild_settings(match["guild_id"])
            if settings:
                channel_id = settings["alert_channel_id"]
        if not channel_id:
            return
        try:
            embed = create_wallet_alert_embed(dev_wallet, match.get("label"), token_data)
            view = create_link_view(token_data["mint_address"])
            await self.bot.send_discord_alert(
                channel_id, embed=embed, view=view, is_token_alert=False,
            )
        except Exception as e:
            print(f"Wallet alert error: {type(e).__name__}: {e}")

    async def refresh_all(self):
        """Refresh every actively tracked token with batched DexScreener calls."""
        coins = [
            c for c in await self.db.get_active_coins()
            if is_valid_solana_address(c["mint_address"])
        ]
        if not coins or not self.wallet_tracker.session:
            return
        sol_usd = await self._sol_usd()
        for offset in range(0, len(coins), Config.REFRESH_BATCH_SIZE):
            batch = coins[offset:offset + Config.REFRESH_BATCH_SIZE]
            markets = await self.feeds.fetch_tokens(
                self.wallet_tracker.session, [c["mint_address"] for c in batch]
            )
            if markets is None:
                break  # rate-limited or failing: accept the gap, resume next sweep
            for coin in batch:
                try:
                    market = markets.get(coin["mint_address"])
                    if market:
                        await self._apply_refresh(coin, market, sol_usd)
                    elif self.is_expired(coin):
                        await self.db.deactivate_coin(coin["id"])
                except Exception as e:
                    print(f"Tracking error for {coin['mint_address']}: {type(e).__name__}: {e}")

    async def _apply_refresh(self, coin, market, sol_usd):
        mint = coin["mint_address"]
        if market.get("price") is None and market.get("market_cap") is None:
            return
        obs_1h = self.flow.window_stats(mint, 3600, sol_usd)
        obs_5m = self.flow.window_stats(mint, 300, sol_usd)
        metrics = compute_volume_metrics(market, obs_1h, obs_5m)
        payload = {**market, **metrics}
        payload["sniper_count"] = self._sniper_count(mint)
        payload["traded"] = obs_5m is not None or (
            (market.get("buys_5m") or 0) + (market.get("sells_5m") or 0) > 0
        )
        row = await self.db.update_market(coin["id"], payload)
        if not row:
            return
        row = dict(row)

        if self._snapshot_gate.get(mint) is None:
            self._snapshot_gate.set(mint, True)
            await self.db.insert_snapshot(
                row["id"], float(row["current_price"] or 0), float(row["current_market_cap"] or 0)
            )

        current_mc = row.get("current_market_cap")
        if market.get("market_cap") and market.get("liquidity"):
            enriched_token = {
                "mint_address": mint,
                "program_type": row.get("program_type") or "pumpfun",
                "name": row.get("name"),
                "symbol": row.get("symbol"),
                "uri": row.get("uri"),
                "image_url": row.get("image_url"),
                "dev_wallet": row.get("dev_wallet"),
                "launch_sol": row.get("launch_sol"),
                "market_cap": market.get("market_cap"),
                "liquidity": market.get("liquidity"),
                "volume_24h": market.get("volume_24h"),
                "volume_1h": payload.get("volume_1h"),
                "buys_1h": payload.get("buys_1h"),
                "sells_1h": payload.get("sells_1h"),
                "price_change_h1": market.get("price_change_h1"),
                "holder_count": None,
                "top10_holder_pct": None,
                "age_seconds": self._age_seconds(row),
                "deployer_tier": None,
                "deployer_bonded": None,
                "deployer_total": None,
                "kol_buying": None,
                "swarm_3plus_pct": None,
                "score": row.get("score") or 0,
                "score_label": row.get("score_label") or "low",
            }
            score_data = self.scoring.calculate_score(enriched_token)
            tier = TierFilter.classify(enriched_token)
            await self._alert_token(row["id"], enriched_token, score_data, tier)

        milestone = await self.milestones.check(
            row["id"], row.get("first_seen_market_cap"), current_mc, row.get("current_price"),
        )
        if milestone:
            task = asyncio.create_task(
                self.send_milestone_alert(row, milestone), name=f"milestone-{mint[:8]}"
            )
            self._milestone_tasks.add(task)
            task.add_done_callback(self._milestone_tasks.discard)
        elif self.is_expired(row):
            await self.db.deactivate_coin(row["id"])

    async def send_milestone_alert(self, coin_data, milestone):
        """Milestones cover every category (fresh, migrated, tracked, PVP siblings)."""
        if not has_valid_identity(
            coin_data.get("name"), coin_data.get("symbol"), coin_data.get("mint_address")
        ):
            return
        try:
            embed = create_milestone_embed(
                coin_data, milestone["multiplier"],
                float(milestone["price"] or 0), float(milestone["market_cap"] or 0),
            )
            view = create_link_view(coin_data["mint_address"])
            metrics = self._alert_metrics(coin_data)
            if metrics.get("dev_rugged") is None and coin_data.get("dev_wallet"):
                try:
                    rep = await self.db.get_dev_reputation(coin_data["dev_wallet"])
                    metrics["dev_rugged"] = rep["rugged"] if rep else None
                except Exception:
                    pass
            for guild_id in await self.get_all_guilds():
                settings = await self.db.get_guild_settings(guild_id)
                if not settings or not settings["alert_channel_id"]:
                    continue
                if not evaluate_filters(settings, metrics)[0]:
                    continue
                await self.bot.send_discord_alert(
                    settings["alert_channel_id"], embed=embed, view=view,
                    is_token_alert=False,
                )
        except Exception as e:
            print(f"Milestone alert error: {type(e).__name__}: {e}")

    # ------------------------------------------------------------------
    # Metrics helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _within_minutes(created, minutes):
        if created is None:
            return False
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - created).total_seconds() <= minutes * 60

    @staticmethod
    def _age_seconds(row):
        """Token age from the pair's creation time when known, else first sighting."""
        stamp = row.get("pair_created_at") or row.get("created_at")
        if stamp is None:
            return None
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - stamp).total_seconds())

    def _sniper_count(self, mint):
        snap = self.flow.snapshot(mint)
        return snap["snipers"] if snap else None

    async def _volume_metrics(self, mint, market):
        sol_usd = await self._sol_usd()
        return compute_volume_metrics(
            market,
            self.flow.window_stats(mint, 3600, sol_usd),
            self.flow.window_stats(mint, 300, sol_usd),
        )

    def _alert_metrics(self, data):
        """Values the per-guild filters read, from a token dict or a coins row."""
        age = data.get("age_seconds")
        if age is None and (data.get("pair_created_at") or data.get("created_at")):
            created = data.get("pair_created_at") or data.get("created_at")
            if isinstance(created, (int, float)):
                age = max(0.0, time.time() - created)
            else:
                age = self._age_seconds({"pair_created_at": data.get("pair_created_at"),
                                         "created_at": data.get("created_at")})
        rep = data.get("dev_rep") or {}
        return {
            "buy_volume_1h": data.get("buy_volume_1h"),
            "volume_5m": data.get("volume_5m"),
            "sniper_count": data.get("sniper_count"),
            "age_seconds": age,
            "liquidity": data.get("liquidity"),
            "dev_rugged": rep.get("rugged") if rep else None,
        }

    def is_expired(self, coin):
        created = coin.get("reactivated_at") or coin.get("created_at")
        if created is None:
            return False
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        expiry = datetime.now(timezone.utc) - timedelta(hours=Config.TRACKING_WINDOW_HOURS)
        return created < expiry

    # ------------------------------------------------------------------
    # Fresh-launch smart scan (T+0 -> T+5 verification)
    # ------------------------------------------------------------------

    @staticmethod
    def _to_float(value, default=0.0):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    async def _get_fresh_targets(self):
        """(guild_id, channel_id) pairs with launch alerts ON; cached for 10s."""
        ts, cached = self._fresh_targets_cache
        now = time.monotonic()
        if now - ts < 10:
            return cached
        try:
            targets = await self.db.get_fresh_targets()
        except Exception as exc:
            print(f"Fresh target lookup failed: {type(exc).__name__}: {exc}")
            targets = cached
        self._fresh_targets_cache = (now, targets)
        return targets

    async def _maybe_start_fresh_scan(self, token_data):
        """Capture Initial MC at T+0 and start the T+5 verification task."""
        try:
            mint = token_data.get("mint_address")
            raw = token_data.get("raw_event") or {}
            if not has_valid_identity(token_data.get("name"), token_data.get("symbol"), mint):
                return
            if mint in self._fresh_seen:
                return
            initial_sol = self._to_float(raw.get("marketCapSol"))
            if initial_sol <= 0:
                return
            if len(self._fresh_tasks) >= FRESH_MAX_PENDING:
                return
            if not await self._get_fresh_targets():
                return

            self._fresh_seen[mint] = time.monotonic()
            while len(self._fresh_seen) > FRESH_SEEN_MAX:
                self._fresh_seen.popitem(last=False)

            task = asyncio.create_task(
                self._fresh_scan(dict(token_data), initial_sol),
                name=f"fresh-{mint[:8]}",
            )
            self._fresh_tasks.add(task)
            task.add_done_callback(self._fresh_done)
        except Exception as exc:
            print(f"Fresh scan start failed: {type(exc).__name__}: {exc}")

    def _fresh_done(self, task):
        self._fresh_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            print(f"Fresh scan failed: {type(exc).__name__}: {exc}")

    async def _fresh_scan(self, token_data, initial_sol):
        """Wait for T+5, re-check market cap, and send only verified callouts.

        Anything unverifiable or risky is dropped without sending a message.
        """
        mint = token_data["mint_address"]
        delay = Config.PRE_ALERT_VERIFY_SECONDS
        if delay > 0:
            await asyncio.sleep(delay)

        targets = await self._get_fresh_targets()
        if not targets:
            return

        # Evidence-based rug checks from the free PumpPortal trade feed.
        flow = self.flow.snapshot(mint)
        reason = self.flow.rejection_reason(mint)
        if reason:
            if "Dev sold" in reason:
                await self._mark_rugged(mint, "Dev sold within the 5s scan")
            elif "market cap fell" in reason:
                await self._mark_rugged(mint, reason)
            return

        sol_usd = await self._sol_usd()
        market = None
        if flow and flow["trades"] >= 1 and flow["last_mcap_sol"] > 0:
            current_sol = flow["last_mcap_sol"]
            pct = (current_sol - initial_sol) / initial_sol * 100.0
            if sol_usd:
                initial_mc, current_mc, unit = initial_sol * sol_usd, current_sol * sol_usd, "usd"
            else:
                initial_mc, current_mc, unit = initial_sol, current_sol, "sol"
        else:
            # No trade seen yet; only DexScreener can verify. Otherwise suppress.
            market = await self._safe_dex_market(mint)
            if not market or not market.get("market_cap") or not sol_usd:
                return
            initial_mc = initial_sol * sol_usd
            current_mc = float(market["market_cap"])
            unit = "usd"
            pct = (current_mc - initial_mc) / initial_mc * 100.0

        drop_pct = -pct
        if drop_pct > Config.PRE_ALERT_MAX_MC_DROP_PCT:
            await self._mark_rugged(mint, f"MC fell {drop_pct:.0f}% by T+5")
            return

        if market is None:
            market = await self._safe_dex_market(mint)
        market = market or {}

        created_at = token_data.get("created_at")
        age_seconds = (time.time() - created_at) if created_at else None
        band = None
        if unit == "usd":
            band = self.scoring.calculate_score(
                {"market_cap": current_mc, "age_seconds": age_seconds}
            ).get("band")

        ctx = await self._context_for(
            mint, token_data.get("name"), token_data.get("symbol"),
            token_data.get("dev_wallet"), None,
        )
        callout = {
            "mint_address": mint,
            "name": str(token_data["name"]).strip(),
            "symbol": str(token_data["symbol"]).strip(),
            "image_url": market.get("image_url"),
            "initial_mc": initial_mc,
            "current_mc": current_mc,
            "mc_unit": unit,
            "change_pct": pct,
            "band": band,
            "launch_sol": token_data.get("launch_sol"),
            "liquidity": market.get("liquidity"),
            "volume": market.get("volume_24h") or market.get("volume_1h"),
            "dev_wallet": token_data.get("dev_wallet"),
            "dev_rep": ctx["dev_rep"],
            "pvp_siblings": ctx["pvp_siblings"],
            "age_seconds": age_seconds,
        }
        embed = create_fresh_callout_embed(callout)
        for _guild_id, channel_id in targets:
            try:
                await self.bot.send_discord_alert(
                    channel_id, embed=embed, view=create_link_view(mint),
                    is_token_alert=False,
                )
            except Exception as exc:
                print(f"Fresh callout send failed: {type(exc).__name__}: {exc}")

    async def _safe_dex_market(self, mint):
        """Direct DexScreener lookup with a short timeout; never raises."""
        try:
            return await asyncio.wait_for(self._dexscreener_market_data(mint), timeout=4)
        except Exception:
            return None

    async def _sol_usd(self):
        """SOL/USD from DexScreener (free), cached for 60s; last good value kept 10 min."""
        ts, cached = self._sol_usd_cache
        now = time.monotonic()
        if cached and now - ts < 60:
            return cached
        price = None
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{SOL_MINT}"
            async with self.enricher.dexscreener_limiter:
                async with self.wallet_tracker.session.get(url, timeout=5) as response:
                    if response.status == 200:
                        data = await response.json()
                        best, best_liq = None, -1.0
                        for pair in data.get("pairs") or []:
                            base = (pair.get("baseToken") or {}).get("address")
                            quote = ((pair.get("quoteToken") or {}).get("symbol") or "").upper()
                            if base != SOL_MINT or quote not in ("USDC", "USDT"):
                                continue
                            liq = self._to_float((pair.get("liquidity") or {}).get("usd"))
                            px = self._to_float(pair.get("priceUsd"))
                            if px > 0 and liq > best_liq:
                                best, best_liq = px, liq
                        price = best
        except Exception as exc:
            print(f"SOL price fetch failed: {type(exc).__name__}: {exc}")
        if price:
            self._sol_usd_cache = (now, price)
            return price
        if cached and now - ts < 600:
            return cached
        return None

    async def fetch_markets(self, mints):
        """One batched DexScreener call (free). {mint: market dict}; missing = no data."""
        mints = [m for m in dict.fromkeys(mints or []) if is_valid_solana_address(m)][:30]
        if not mints or not self.wallet_tracker or not self.wallet_tracker.session:
            return {}
        data = await self.feeds.fetch_tokens(self.wallet_tracker.session, mints)
        return {
            mint: {
                "market_cap": m.get("market_cap"),
                "liquidity": m.get("liquidity"),
                "volume": m.get("volume_24h"),
                "image_url": m.get("image_url"),
            }
            for mint, m in (data or {}).items()
        }

    async def _context_for(self, mint, name, symbol, dev_wallet, around):
        """Dev reputation and local PVP siblings; each part is omitted if unavailable."""
        ctx = {"dev_rep": None, "pvp_siblings": []}
        try:
            ctx["dev_rep"] = await self.db.get_dev_reputation(dev_wallet)
        except Exception:
            pass
        try:
            now = datetime.now(timezone.utc)
            rows = await self.db.get_pvp_siblings(mint, name, symbol, around or now, limit=3)
            siblings = []
            for row in rows or []:
                created = row["created_at"]
                if created is not None and created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                mc = row["current_market_cap"] or row["first_seen_market_cap"]
                siblings.append({
                    "mint_address": row["mint_address"],
                    "market_cap": float(mc) if mc else None,
                    "age_seconds": (now - created).total_seconds() if created else None,
                })
            ctx["pvp_siblings"] = siblings
        except Exception:
            pass
        return ctx

    async def _attach_context(self, token_data):
        try:
            coin = await self.db.get_coin_by_mint(token_data["mint_address"])
            around = coin.get("created_at") if coin else None
        except Exception:
            around = None
        ctx = await self._context_for(
            token_data.get("mint_address"), token_data.get("name"),
            token_data.get("symbol"), token_data.get("dev_wallet"), around,
        )
        if ctx["dev_rep"]:
            token_data["dev_rep"] = ctx["dev_rep"]
        if ctx["pvp_siblings"]:
            token_data["pvp_siblings"] = ctx["pvp_siblings"]

    async def _mark_rugged(self, mint, reason):
        """Persist rug evidence for dev reputation; retried once the coin row exists."""
        try:
            if not await self.db.set_rugged(mint, reason):
                self._pending_rugged[mint] = reason
                while len(self._pending_rugged) > 500:
                    self._pending_rugged.popitem(last=False)
        except Exception as exc:
            print(f"Rug record failed for {mint[:8]}: {type(exc).__name__}: {exc}")

    async def _apply_pending_rug(self, mint):
        reason = self._pending_rugged.pop(mint, None)
        if reason:
            try:
                await self.db.set_rugged(mint, reason)
            except Exception as exc:
                print(f"Pending rug record failed for {mint[:8]}: {type(exc).__name__}: {exc}")

    async def get_all_guilds(self):
        return await self.db.get_all_guilds()

    def diagnostics(self):
        flow = self.flow.stats()
        return {
            "queue_size": self._token_queue.qsize(),
            "pending_verifications": len(self._verification_tasks),
            "pending_fresh_scans": len(self._fresh_tasks),
            "dropped_tokens": self._dropped_tokens,
            "websocket_connected": bool(self.pumpportal and self.pumpportal.connected),
            "flow": flow,
            "rpc": self.rpc.stats() if self.rpc else None,
            "feeds": self.feeds.stats(),
        }