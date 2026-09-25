import asyncio
import time
from datetime import datetime, timezone, timedelta

from config import Config
from raydium import RaydiumDetector
from pumpportal import PumpPortalDetector
from scoring import ScoringEngine
from milestones import MilestoneTracker
from red_flags import RedFlagDetector
from tiers import TierFilter
from enricher import TokenEnricher, is_valid_solana_address
from embeds import (
    create_token_alert_embed,
    create_milestone_embed,
    create_migration_embed,
    create_wallet_alert_embed,
    create_link_view,
)
from wallets import WalletTracker


MIGRATION_MC_CEILING = 100_000
MIGRATION_FLAG = "migrated"


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

    async def initialize(self):
        from solana_rpc import SolanaRPC
        self.rpc = SolanaRPC()
        self.enricher._rpc = self.rpc
        self.enricher._owns_rpc = False
        self.raydium = RaydiumDetector(self.rpc)
        self.pumpportal = PumpPortalDetector(self._on_new_token_from_ws)

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
        if self.pumpportal:
            await self.pumpportal.stop()
        if self.rpc:
            await self.rpc.close()
        if self.enricher:
            await self.enricher.close()
        await self.wallet_tracker.close()

    async def start(self):
        self.running = True
        print(f"Scanner started. Interval: {Config.SCAN_INTERVAL_SECONDS}s")

        await self.pumpportal.start()
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
        try:
            if raw.get("txType") == "migrate":
                await self._handle_migration(token_data)
            else:
                await self.process_token(token_data)
        except Exception as exc:
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

        existing_coin = await self.db.get_coin_by_mint(mint)
        if existing_coin and existing_coin.get("alert_message_id"):
            return

        priority = "high" if token_data.get("from_wallet_buy") else "normal"
        enrichment = await self.enricher.enrich(mint, priority=priority)

        for k in (
            "price", "market_cap", "image_url", "liquidity", "volume_24h",
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

        if not token_data.get("name"):
            token_data["name"] = f"Token {mint[:6]}"
        if not token_data.get("symbol"):
            token_data["symbol"] = mint[:4].upper()

        created_at = token_data.get("created_at")
        token_data["age_seconds"] = (time.time() - created_at) if created_at else None

        # Early-buyer analysis is RPC-heavy (one signature query plus multiple
        # transaction lookups). Only spend that budget after the token has
        # usable market data; otherwise a burst of new tokens can exhaust a
        # Helius account before alerts are produced.
        if token_data.get("market_cap") and token_data.get("liquidity"):
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
        if token_data.get("red_flags"):
            await self.db.set_red_flags(coin_id, ",".join(token_data["red_flags"]))

        has_market = token_data.get("market_cap") and token_data.get("liquidity")
        if has_market:
            await self._alert_token(coin_id, token_data, score_data, tier)

        sm_count = token_data.get("smart_money_count", 0) or 0
        if sm_count >= 1:
            await self._send_matching_wallet_alerts(token_data)

    async def _alert_token(self, coin_id, token_data, score_data, tier):
        guilds = await self.get_all_guilds()
        alerted_any = False
        for guild_id in guilds:
            settings = await self.db.get_guild_settings(guild_id)
            if not settings or not settings["alert_channel_id"]:
                continue
            mode = settings["scan_mode"] or Config.DEFAULT_SCAN_MODE
            if not self.scoring.should_alert(score_data["label"], mode):
                continue
            if not tier and score_data["label"] == "low":
                continue
            try:
                embed = create_token_alert_embed(token_data, score_data, None)
                view = create_link_view(token_data["mint_address"])
                message = await self.bot.send_discord_alert(
                    settings["alert_channel_id"], embed=embed, view=view,
                    is_token_alert=True,
                )
                await self.db.update_coin_alert(
                    coin_id, message.id, settings["alert_channel_id"]
                )
                alerted_any = True
            except Exception as e:
                print(f"Send alert error: {type(e).__name__}: {e}")
        return alerted_any

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
            if "paused" in msg.lower():
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

        market = None
        try:
            market = await self.get_market_data_with_fallback(mint)
        except Exception as e:
            print(f"Migration market fetch failed for {mint[:8]}: {type(e).__name__}: {e}")

        fresh_mc = market.get("market_cap") if market else None

        if fresh_mc is not None and fresh_mc > MIGRATION_MC_CEILING:
            print(
                f"Migration suppressed for {mint[:8]}: "
                f"MC ${fresh_mc:,.0f} > ceiling ${MIGRATION_MC_CEILING:,} "
                f"(likely a replayed event or stale data)"
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
        fresh["first_seen_market_cap"] = None
        fresh["first_seen_price"] = None

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

    async def get_market_data_with_fallback(self, mint):
        data = await self._dexscreener_market_data(mint)
        if data:
            self._dexscreener_consecutive_failures = 0
            return data
        self._dexscreener_consecutive_failures += 1
        if (self._dexscreener_consecutive_failures >= Config.DEXSCREENER_FAILURE_THRESHOLD
                and self._helius_available):
            return await self._helius_market_data(mint)
        return None

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

            return {
                "price": float(best.get("priceUsd", 0) or 0) or None,
                "market_cap": mc_val,
                "liquidity": _liq(best) or None,
                "volume_24h": float((best.get("volume") or {}).get("h24", 0) or 0) or None,
                "image_url": info.get("imageUrl"),
                "source": "dexscreener",
            }
        except Exception as exc:
            print(f"DexScreener error: {type(exc).__name__}: {exc}")
            return None

    async def _helius_market_data(self, mint):
        if self.rpc and self.rpc.is_paused():
            return None
        try:
            url = f"https://mainnet.helius-rpc.com/?api-key={Config.HELIUS_API_KEY}"
            payload = {"jsonrpc": "2.0", "id": 1, "method": "getAsset", "params": {"id": mint}}
            async with self.wallet_tracker.session.post(url, json=payload, timeout=15) as response:
                if response.status != 200:
                    return None
                data = await response.json()
                result = data.get("result") or {}
                content = result.get("content") or {}
                links = content.get("links") or {}
                return {
                    "price": None, "market_cap": None,
                    "liquidity": None, "volume_24h": None,
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

                if not coin.get("alert_message_id") and market.get("market_cap") and market.get("liquidity"):
                    enriched_token = {
                        "mint_address": coin["mint_address"],
                        "program_type": coin.get("program_type") or "pumpfun",
                        "name": coin.get("name"),
                        "symbol": coin.get("symbol"),
                        "uri": coin.get("uri"),
                        "image_url": coin.get("image_url") or market.get("image_url"),
                        "dev_wallet": coin.get("dev_wallet"),
                        "market_cap": market.get("market_cap"),
                        "liquidity": market.get("liquidity"),
                        "volume_24h": market.get("volume_24h"),
                        "holder_count": None,
                        "top10_holder_pct": None,
                        "age_seconds": None,
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
        last_tracked = coin["last_tracked_at"]
        if last_tracked.tzinfo is None:
            last_tracked = last_tracked.replace(tzinfo=timezone.utc)
        expiry = datetime.now(timezone.utc) - timedelta(hours=Config.TRACKING_WINDOW_HOURS)
        return last_tracked < expiry

    async def get_all_guilds(self):
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch("SELECT guild_id FROM guilds")
            return [row["guild_id"] for row in rows]