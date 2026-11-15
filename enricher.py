import time
import re
import asyncio

from config import Config
from dexscreener import parse_pair, pick_best_pair
from rate_limiter import RateLimiter, DailyQuota, QuotaExhausted


_BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

_PLACEHOLDER_NAMES = {
    "wrapped sol", "wrapped solana", "sol", "wsol",
    "unknown", "unknown token", "",
}
_PLACEHOLDER_SYMBOLS = {"sol", "wsol", "unknown", "???"}


def is_valid_solana_address(addr: str) -> bool:
    if not addr or not isinstance(addr, str):
        return False
    return bool(_BASE58_RE.match(addr))


_INVISIBLE_RE = re.compile(r"[\u200b-\u200f\u2060\ufeff]")
_EMPTY_IDENTITY_VALUES = {
    "", "null", "none", "undefined", "nan", "n/a", "na", "unknown",
    "unknown token", "???", "?", "-", "--",
}
# Placeholder the old scanner generated when a token had no name: "Token AbCdEf".
_GENERATED_NAME_RE = re.compile(r"^token [1-9a-hj-np-za-km-z]{6}$", re.IGNORECASE)


def _clean_identity(value) -> str:
    if value is None:
        return ""
    return _INVISIBLE_RE.sub("", str(value)).strip()


def has_valid_identity(name, symbol, mint) -> bool:
    """True only when name, symbol and mint are all real, non-placeholder values."""
    name_c = _clean_identity(name)
    symbol_c = _clean_identity(symbol)
    if not is_valid_solana_address(_clean_identity(mint)):
        return False
    if name_c.lower() in _EMPTY_IDENTITY_VALUES or symbol_c.lower() in _EMPTY_IDENTITY_VALUES:
        return False
    if _GENERATED_NAME_RE.match(name_c):
        return False
    if name_c == _clean_identity(mint) or symbol_c == _clean_identity(mint):
        return False
    return True


class TokenEnricher:
    def __init__(self, wallet_tracker):
        self.wallet_tracker = wallet_tracker
        self._rpc = None
        self._owns_rpc = False

        self.madeonsol_quota = DailyQuota(
            max_per_day=Config.MADEONSOL_DAILY_QUOTA,
            rate_per_minute=Config.MADEONSOL_RATE_PER_MINUTE,
        )
        self.madeonsol_limiter = RateLimiter(rate_per_second=0.17, burst=1)
        self.dexscreener_limiter = RateLimiter(rate_per_second=5.0, burst=5)
        self.helius_limiter = RateLimiter(rate_per_second=8.0, burst=8)
        self.raydium_limiter = RateLimiter(rate_per_second=1.0, burst=2)

        self._cache = {}
        self._cache_ttl = Config.ENRICHER_CACHE_TTL_SECONDS
        self._deferred_queue = {}
        self._holder_lock = asyncio.Lock()

    def _cache_get(self, mint):
        entry = self._cache.get(mint)
        if not entry:
            return None
        ts, data = entry
        if time.time() - ts > self._cache_ttl:
            del self._cache[mint]
            return None
        return data

    def _cache_put(self, mint, data):
        if len(self._cache) > Config.ENRICHER_CACHE_MAX_ENTRIES:
            oldest = min(self._cache.items(), key=lambda x: x[1][0])
            del self._cache[oldest[0]]
        self._cache[mint] = (time.time(), data)

    def _sanitize_name(self, name):
        if not name:
            return None
        if name.strip().lower() in _PLACEHOLDER_NAMES:
            return None
        return name.strip()

    def _sanitize_symbol(self, symbol):
        if not symbol:
            return None
        if symbol.strip().upper() in _PLACEHOLDER_SYMBOLS:
            return None
        return symbol.strip()

    async def enrich(self, mint, priority="normal"):
        if not is_valid_solana_address(mint):
            return {}

        cached = self._cache_get(mint)
        if cached and not cached.get("_madeonsol_deferred"):
            return cached

        result = {
            "name": None,
            "symbol": None,
            "uri": None,
            "price": None,
            "market_cap": None,
            "liquidity": None,
            "volume_24h": None,
            "volume_1h": None,
            "volume_5m": None,
            "buys_1h": 0,
            "sells_1h": 0,
            "buys_5m": 0,
            "sells_5m": 0,
            "price_change_h1": None,
            "image_url": None,
            "holder_count": None,
            "top10_holder_pct": None,
            "deployer_tier": None,
            "deployer_bonded": None,
            "deployer_total": None,
            "kol_buying": None,
            "swarm_3plus_pct": None,
            "source": [],
        }

        ds = await self._fetch_dexscreener(mint)
        if ds:
            for k, v in ds.items():
                if v is not None:
                    result[k] = v
            result["source"].append("dexscreener")

        if not result.get("name") or not result.get("symbol"):
            meta = await self._fetch_metadata(mint)
            if meta:
                if not result.get("name") and meta.get("name"):
                    result["name"] = self._sanitize_name(meta.get("name"))
                if not result.get("symbol") and meta.get("symbol"):
                    result["symbol"] = self._sanitize_symbol(meta.get("symbol"))
                if not result.get("uri") and meta.get("uri"):
                    result["uri"] = meta.get("uri")
                if not result.get("image_url") and meta.get("image_url"):
                    result["image_url"] = meta.get("image_url")
                result["source"].append("helius-metadata")

        result["name"] = self._sanitize_name(result.get("name"))
        result["symbol"] = self._sanitize_symbol(result.get("symbol"))

        try:
            deep_scan_mc = float(result.get("market_cap") or 0)
        except (TypeError, ValueError):
            deep_scan_mc = 0.0
        if (
            Config.HELIUS_API_KEY
            and Config.HELIUS_DEEP_SCAN_MIN_MC <= deep_scan_mc <= Config.HELIUS_DEEP_SCAN_MAX_MC
            and result.get("liquidity")
            and self._rpc is not None
            and not self._rpc.is_paused()
        ):
            async with self._holder_lock:
                holders = await self._fetch_holders(mint)
            if holders:
                for k, v in holders.items():
                    if v is not None:
                        result[k] = v
                result["source"].append("helius")

        madeonsol_deferred = False
        if Config.MADEONSOL_API_KEY:
            if self.madeonsol_quota.can_spend():
                mos = await self._fetch_madeonsol(mint)
                if mos:
                    for k, v in mos.items():
                        if v is not None:
                            result[k] = v
                    result["source"].append("madeonsol")
            elif priority in ("high", "normal"):
                self._deferred_queue[mint] = priority
                madeonsol_deferred = True

        if madeonsol_deferred:
            result["_madeonsol_deferred"] = True

        self._cache_put(mint, result)
        return result

    async def _fetch_dexscreener(self, mint):
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
            async with self.dexscreener_limiter:
                async with self.wallet_tracker.session.get(url, timeout=10) as r:
                    if r.status == 429:
                        self.dexscreener_limiter.record_429()
                        return None
                    if r.status != 200:
                        return None
                    self.dexscreener_limiter.record_success()
                    data = await r.json()

            best = pick_best_pair(data.get("pairs") or [], mint)
            if not best:
                return None
            parsed = parse_pair(best, mint)
            parsed.pop("source", None)
            return parsed
        except Exception as e:
            print(f"DexScreener fetch error: {type(e).__name__}: {e}")
            return None

    async def _fetch_metadata(self, mint):
        if not Config.HELIUS_API_KEY or self._rpc is None or self._rpc.is_paused():
            return None
        try:
            async with self.helius_limiter:
                result = await self._rpc.get_asset(mint) or {}
                self.helius_limiter.record_success()
            content = result.get("content") or {}
            metadata = content.get("metadata") or {}
            links = content.get("links") or {}
            return {
                "name": metadata.get("name"),
                "symbol": metadata.get("symbol"),
                "uri": links.get("json_uri") or content.get("json_uri"),
                "image_url": links.get("image"),
            }
        except Exception as e:
            msg = str(e)
            if "paused" not in msg.lower() and "exhausted" not in msg.lower():
                print(f"Metadata fetch error: {type(e).__name__}: {e}")
            if "429" in msg:
                self.helius_limiter.record_429()
            return None

    async def _fetch_holders(self, mint):
        if not Config.HELIUS_API_KEY or not is_valid_solana_address(mint):
            return None
        try:
            if self._rpc is None:
                from solana_rpc import SolanaRPC
                self._rpc = SolanaRPC()
                self._owns_rpc = True
            if self._rpc.is_paused():
                return None

            async with self.helius_limiter:
                largest = await self._rpc.get_token_largest_accounts(mint)
                supply = await self._rpc.get_token_supply(mint)
                self.helius_limiter.record_success()

            if not largest or not largest.get("value"):
                return None

            accounts = largest["value"]
            top10 = accounts[:10]
            top10_amount = sum(float(a.get("amount", 0) or 0) for a in top10)

            supply_value = (supply or {}).get("value") or {}
            total_supply = float(supply_value.get("amount", 0) or 0)
            top10_pct = (top10_amount / total_supply * 100) if total_supply > 0 else None

            return {
                "holder_count": None,
                "top10_holder_pct": top10_pct,
            }
        except Exception as e:
            msg = str(e)
            if "paused" in msg.lower() or "exhausted" in msg.lower() or "not a Token mint" in msg:
                return None
            print(f"Holder fetch error: {type(e).__name__}: {e}")
            return None

    async def _fetch_madeonsol(self, mint):
        if not Config.MADEONSOL_API_KEY:
            return None
        try:
            url = f"https://api.madeonsol.com/api/v1/token/{mint}"
            headers = {"Authorization": f"Bearer {Config.MADEONSOL_API_KEY}"}
            async with self.madeonsol_quota:
                async with self.madeonsol_limiter:
                    async with self.wallet_tracker.session.get(url, headers=headers, timeout=15) as r:
                        if r.status == 429:
                            self.madeonsol_limiter.record_429()
                            return None
                        if r.status != 200:
                            self.madeonsol_limiter.record_success()
                            return None
                        self.madeonsol_limiter.record_success()
                        data = await r.json()
                        return self._parse_madeonsol(data)
        except QuotaExhausted:
            return None
        except Exception as e:
            print(f"MadeOnSol fetch error: {type(e).__name__}: {e}")
            return None

    def _parse_madeonsol(self, data):
        deployer = data.get("deployer") or {}
        kol = data.get("kol_activity") or {}
        return {
            "deployer_tier": deployer.get("tier"),
            "deployer_bonded": deployer.get("total_bonded"),
            "deployer_total": deployer.get("total_deploys"),
            "deployer_bonding_rate": deployer.get("bonding_rate"),
            "kol_buying": kol.get("buying_kols", 0),
            "swarm_3plus_pct": data.get("swarm_3plus_pct"),
        }

    async def drain_deferred(self):
        if not self._deferred_queue or not self.madeonsol_quota.can_spend():
            return
        mint, priority = next(iter(self._deferred_queue.items()))
        self._deferred_queue.pop(mint, None)
        self._cache.pop(mint, None)
        await self.enrich(mint, priority=priority)

    async def close(self):
        if self._rpc and self._owns_rpc:
            try:
                await self._rpc.close()
            except Exception:
                pass