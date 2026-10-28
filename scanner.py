import asyncio
import time
from collections import OrderedDict
from datetime import datetime, timezone, timedelta

from config import Config
from raydium import RaydiumDetector
from pumpportal import PumpPortalDetector
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


class Scanner:
    def __init__(self, bot, db):
        self.bot = bot
        self.db = db
        self.rpc = None
        self.raydium = None
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
        self._buyer_analysis_cache = {}
        self._last_cleanup = None

        self._raydium_seen = set()
        self._raydium_last_poll = 0

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
        self.raydium = RaydiumDetector(self.rpc)
        self.pumpportal = PumpPortalDetector(
            self._on_new_token_from_ws, self.flow.on_trade
        )

        await self.wallet_tracker.init_session()
        await self._refresh_wallet_index()

        if self._helius_available:
            print("Helius API key detected - smart money + bundle detection enabled")
        else:
            print("No HELIUS_API_KEY set - smart money + bundle detection DISABLED")

        if Config.MADEONSOL_API_KEY and Config.MADEONSOL_API_URL:
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
        tasks = [*self._worker_tasks, *self._verification_tasks, *self._fresh_tasks]
        for task in tasks:
            if task and not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._worker_tasks.clear()
        self._verification_tasks.clear()
        self._fresh_tasks.clear()
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
        print(f"Scanner started. Interval: {Config.SCAN_INTERVAL_SECONDS}s")

        await self.pumpportal.start()
        self._worker_tasks = [
            asyncio.create_task(self._token_worker(i), name=f"token-worker-{i}")
            for i in range(Config.SCANNER_WORKERS)
        ]
        self._wallet_index_refresh_task = asyncio.create_task(
            self._wallet_index_loop(), name="wallet-index-refresh"
        )

        while self.running:
            try:
                await self.scan_cycle()
                await asyncio.sleep(Config.SCAN_INTERVAL_SECONDS)
            except Exception as e:
                print(f"Scanner cycle error: {type(e).__name__}: {e}")
                await asyncio.sleep(5)

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

        default = (Config.DEFAULT_WALLET or "").strip().lower()
        if default and default not in idx:
            idx[default] = {
                "guild_id": -1,
                "label": "Default Sniper",
                "channel_id": None,
            }

        self._wallet_index = idx
        print(f"Wallet index loaded: {len(idx)} entry(ies); default injected: {bool(default)}")

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

    async def scan_cycle(self):
        now = datetime.now(timezone.utc)
        if self._last_cleanup is None or (now - self._last_cleanup).total_seconds() >= 3600:
            try:
                removed = await self.db.remove_inactive_wallets(Config.IDLE_WALLET_MAX_DAYS)
                self._last_cleanup = now
                if removed:
                    print(
                        f"Inactive wallet cleanup: removed {removed} wallet(s) "
                        f"with no buys in {Config.IDLE_WALLET_MAX_DAYS} days"
                    )
                snapshots_removed = await self.db.cleanup_old_snapshots(
                    Config.SNAPSHOT_RETENTION_HOURS
                )
                if snapshots_removed:
                    print(f"Snapshot retention: removed {snapshots_removed} old row(s)")
            except Exception as e:
                print(f"Inactive wallet cleanup error: {type(e).__name__}: {e}")

        if Config.ENABLE_RAYDIUM_DETECTION:
            now_mono = time.monotonic()
            if now_mono - self._raydium_last_poll >= Config.RAYDIUM_POLL_INTERVAL_SECONDS:
                self._raydium_last_poll = now_mono
                try:
                    await self._poll_raydium_new_pools()
                except Exception as e:
                    print(f"Raydium poll error: {type(e).__name__}: {e}")

        await self.update_tracking()

        try:
            await self.enricher.drain_deferred()
        except Exception as e:
            print(f"Deferred drain error: {type(e).__name__}: {e}")

    async def _poll_raydium_new_pools(self):
        try:
            url = "https://api-v3.raydium.io/pools/info/list"
            params = {
                "poolType": "all",
                "poolSortField": "createdTime",
                "sortType": "desc",
                "pageSize": 20,
                "page": 1,
            }
            async with self.enricher.raydium_limiter:
                async with self.wallet_tracker.session.get(url, params=params, timeout=10) as r:
                    if r.status == 429:
                        self.enricher.raydium_limiter.record_429()
                        return
                    if r.status != 200:
                        return
                    self.enricher.raydium_limiter.record_success()
                    data = await r.json()

            pools = (data.get("data") or {}).get("data") or []
            for pool in pools:
                mint_a = (pool.get("mintA") or {}).get("address")
                mint_b = (pool.get("mintB") or {}).get("address")
                candidates = [m for m in (mint_a, mint_b) if m]
                if not candidates:
                    continue
                sol_mint = "so11111111111111111111111111111111111111112"
                usdc_mint = "epjfwdd5aufqssqem2qn1xzybapc8g4weggkzwytdt1v"
                mint = None
                for c in candidates:
                    if c.lower() not in (sol_mint, usdc_mint):
                        mint = c
                        break
                if not mint:
                    mint = candidates[0]

                if mint in self._raydium_seen:
                    continue
                self._raydium_seen.add(mint)
                if len(self._raydium_seen) > 5000:
                    self._raydium_seen = set(list(self._raydium_seen)[-2500:])

                token_data = {
                    "mint_address": mint,
                    "program_type": "raydium",
                    "name": None,
                    "symbol": None,
                    "uri": None,
                    "dev_wallet": None,
                    "created_at": time.time(),
                }
                await self.process_token(token_data)
        except Exception as e:
            print(f"Raydium pool parse error: {type(e).__name__}: {e}")

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
            "kol_buying", "swarm_3plus_pct",
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
        if coin_row and coin_row.get(f"alerted_{band}"):
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

            eligible = 0
            sent = 0
            for guild_id in await self.get_all_guilds():
                settings = await self.db.get_guild_settings(guild_id)
                if not settings or not settings["alert_channel_id"]:
                    continue
                mode = settings["scan_mode"] or Config.DEFAULT_SCAN_MODE
                if not self.scoring.should_alert(score_data["label"], mode):
                    continue
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
                self._buyer_analysis_cache[mint] = result
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

            self._buyer_analysis_cache[mint] = result
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

        for guild_id in await self.get_all_guilds():
            settings = await self.db.get_guild_settings(guild_id)
            if not settings or not settings["alert_channel_id"]:
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

            pairs = data.get("pairs") or []
            if not pairs:
                return None

            mint_lower = mint.lower()

            def _liq(p):
                try:
                    return float((p.get("liquidity") or {}).get("usd", 0) or 0)
                except (TypeError, ValueError):
                    return 0.0

            matching = []
            for pair in pairs:
                base_addr = ((pair.get("baseToken") or {}).get("address") or "").lower()
                quote_addr = ((pair.get("quoteToken") or {}).get("address") or "").lower()
                if base_addr == mint_lower or quote_addr == mint_lower:
                    matching.append(pair)

            if not matching:
                return None

            best = max(matching, key=_liq)
            info = best.get("info") or {}

            mc = best.get("marketCap")
            if mc is None:
                mc = best.get("fdv")
            try:
                mc_val = float(mc) if mc is not None else None
            except (TypeError, ValueError):
                mc_val = None

            volume_obj = best.get("volume") or {}
            txns_obj = best.get("txns") or {}
            h1_txns = txns_obj.get("h1") or {}
            price_change_obj = best.get("priceChange") or {}

            try:
                price_change_h1 = float(price_change_obj.get("h1")) if price_change_obj.get("h1") is not None else None
            except (TypeError, ValueError):
                price_change_h1 = None

            return {
                "price": float(best.get("priceUsd", 0) or 0) or None,
                "market_cap": mc_val,
                "liquidity": _liq(best) or None,
                "volume_24h": float(volume_obj.get("h24", 0) or 0) or None,
                "volume_1h": float(volume_obj.get("h1", 0) or 0) or None,
                "buys_1h": int(h1_txns.get("buys", 0) or 0),
                "sells_1h": int(h1_txns.get("sells", 0) or 0),
                "price_change_h1": price_change_h1,
                "image_url": info.get("imageUrl"),
                "source": "dexscreener",
            }
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

    async def send_alert(self, guild_id, channel_id, token_data, score_data, safety_data):
        try:
            embed = create_token_alert_embed(token_data, score_data, safety_data)
            view = create_link_view(token_data["mint_address"])
            message = await self.bot.send_discord_alert(
                channel_id, embed=embed, view=view, is_token_alert=True,
            )
            coin = await self.db.get_coin_by_mint(token_data["mint_address"])
            if coin:
                await self.db.update_coin_alert(coin["id"], message.id, channel_id)
        except Exception as e:
            print(f"Send alert error: {type(e).__name__}: {e}")

    async def _send_matching_wallet_alerts(self, token_data):
        dev_wallet = token_data.get("dev_wallet")
        match = self._match_wallet(dev_wallet) if dev_wallet else None
        if not match:
            return
        if match.get("guild_id") == -1:
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

    async def update_tracking(self):
        active_coins = await self.db.get_active_coins()
        active_coins = active_coins[:Config.TRACKING_MAX_COINS_PER_CYCLE]
        for coin in active_coins:
            try:
                market = await self.get_market_data_with_fallback(coin["mint_address"])
                if not market or market.get("price") is None:
                    continue

                current_price = market["price"]
                current_mc = market.get("market_cap") or 0

                await self.db.set_first_seen(
                    coin["id"], current_price, current_mc, market.get("image_url")
                )

                if not coin.get("image_url") and market.get("image_url"):
                    await self.db.set_coin_image(coin["id"], market["image_url"])
                    coin = dict(coin)
                    coin["image_url"] = market["image_url"]

                await self.db.update_coin_snapshot(coin["id"], current_price, current_mc)
                await self.db.insert_snapshot(coin["id"], current_price, current_mc)

                if market.get("market_cap") and market.get("liquidity"):
                    created = coin.get("created_at")
                    if created is not None and created.tzinfo is None:
                        created = created.replace(tzinfo=timezone.utc)
                    age_sec = (
                        (datetime.now(timezone.utc) - created).total_seconds()
                        if created else None
                    )
                    enriched_token = {
                        "mint_address": coin["mint_address"],
                        "program_type": coin.get("program_type") or "pumpfun",
                        "name": coin.get("name"),
                        "symbol": coin.get("symbol"),
                        "uri": coin.get("uri"),
                        "image_url": coin.get("image_url") or market.get("image_url"),
                        "dev_wallet": coin.get("dev_wallet"),
                        "launch_sol": coin.get("launch_sol"),
                        "market_cap": market.get("market_cap"),
                        "liquidity": market.get("liquidity"),
                        "volume_24h": market.get("volume_24h"),
                        "volume_1h": market.get("volume_1h"),
                        "buys_1h": market.get("buys_1h"),
                        "sells_1h": market.get("sells_1h"),
                        "price_change_h1": market.get("price_change_h1"),
                        "holder_count": None,
                        "top10_holder_pct": None,
                        "age_seconds": age_sec,
                        "deployer_tier": None,
                        "deployer_bonded": None,
                        "deployer_total": None,
                        "kol_buying": None,
                        "swarm_3plus_pct": None,
                        "score": coin.get("score") or 0,
                        "score_label": coin.get("score_label") or "low",
                    }
                    score_data = self.scoring.calculate_score(enriched_token)
                    tier = TierFilter.classify(enriched_token)
                    await self._alert_token(coin["id"], enriched_token, score_data, tier)

                refreshed = await self.db.get_coin_by_mint(coin["mint_address"])
                if not refreshed:
                    continue

                alerted = (
                    refreshed.get("alerted_low")
                    or refreshed.get("alerted_medium")
                    or refreshed.get("alerted_high")
                )
                milestones = []
                if alerted and current_mc >= 6000:
                    milestones = await self.milestones.check_milestones(
                        refreshed["id"],
                        float(refreshed["first_seen_price"]) if refreshed["first_seen_price"] else None,
                        float(refreshed["first_seen_market_cap"]) if refreshed["first_seen_market_cap"] else None,
                        current_price, current_mc,
                    )
                    for milestone in milestones:
                        await self.send_milestone_alert(refreshed, milestone)

                if not milestones and self.is_expired(refreshed):
                    await self.db.deactivate_coin(refreshed["id"])
            except Exception as e:
                print(f"Tracking error for {coin['mint_address']}: {type(e).__name__}: {e}")

    async def send_milestone_alert(self, coin_data, milestone):
        if not has_valid_identity(
            coin_data.get("name"), coin_data.get("symbol"), coin_data.get("mint_address")
        ):
            return
        try:
            embed = create_milestone_embed(
                coin_data, milestone["multiplier"],
                milestone["price"], milestone["market_cap"],
            )
            view = create_link_view(coin_data["mint_address"])
            for guild_id in await self.get_all_guilds():
                settings = await self.db.get_guild_settings(guild_id)
                if not settings or not settings["alert_channel_id"]:
                    continue
                await self.bot.send_discord_alert(
                    settings["alert_channel_id"], embed=embed, view=view,
                    is_token_alert=False,
                )
        except Exception as e:
            print(f"Milestone alert error: {type(e).__name__}: {e}")

    def is_expired(self, coin):
        created = coin.get("created_at")
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
        """One batched DexScreener call (free). Returns {mint: market dict}; missing = no data."""
        mints = [m for m in dict.fromkeys(mints or []) if is_valid_solana_address(m)][:30]
        if not mints or not self.wallet_tracker or not self.wallet_tracker.session:
            return {}
        try:
            url = "https://api.dexscreener.com/latest/dex/tokens/" + ",".join(mints)
            async with self.enricher.dexscreener_limiter:
                async with self.wallet_tracker.session.get(url, timeout=10) as response:
                    if response.status == 429:
                        self.enricher.dexscreener_limiter.record_429()
                        return {}
                    if response.status != 200:
                        return {}
                    self.enricher.dexscreener_limiter.record_success()
                    data = await response.json()
        except Exception as exc:
            print(f"Batch market fetch failed: {type(exc).__name__}: {exc}")
            return {}

        by_lower = {m.lower(): m for m in mints}
        best = {}
        for pair in data.get("pairs") or []:
            base = ((pair.get("baseToken") or {}).get("address") or "").lower()
            mint = by_lower.get(base)
            if not mint:
                continue
            liq = self._to_float((pair.get("liquidity") or {}).get("usd"))
            if mint not in best or liq > best[mint][0]:
                best[mint] = (liq, pair)

        result = {}
        for mint, (liq, pair) in best.items():
            mc = pair.get("marketCap")
            if mc is None:
                mc = pair.get("fdv")
            result[mint] = {
                "market_cap": self._to_float(mc) or None,
                "liquidity": liq or None,
                "volume": self._to_float((pair.get("volume") or {}).get("h24")) or None,
                "image_url": (pair.get("info") or {}).get("imageUrl"),
            }
        return result

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
        }