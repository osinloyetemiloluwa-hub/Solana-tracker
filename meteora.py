"""Meteora discovery through the free Meteora data API.

Same pattern as Raydium: poll for the newest pools, return the non-quote mint
of each new pool, never fetch deeper data. The parser accepts the field names
used by Meteora's DLMM (`token_x`/`token_y`) and DAMM (`token_a`/`token_b`)
pool lists, and raw `mint_*` strings, so a minor API revision degrades to
"no new pools" instead of an exception.
"""
from collections import OrderedDict

from rate_limiter import RateLimiter
from raydium import QUOTE_MINTS

POOLS_URL = "https://dlmm.datapi.meteora.ag/pools"
POOLS_PARAMS = {"page": 1, "page_size": 50, "sort_by": "pool_created_at:desc"}
SEEN_MAX = 5000

_TOKEN_KEYS = ("token_x", "token_y", "token_a", "token_b")
_MINT_KEYS = ("mint_x", "mint_y", "mint_a", "mint_b")


def _pool_list(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "pools", "pairs"):
            if isinstance(payload.get(key), list):
                return payload[key]
    return []


def parse_pools(payload):
    coins = []
    for pool in _pool_list(payload):
        if not isinstance(pool, dict):
            continue
        sides = []
        for key in _TOKEN_KEYS:
            if isinstance(pool.get(key), dict):
                sides.append(pool[key])
        for key in _MINT_KEYS:
            if isinstance(pool.get(key), str):
                sides.append({"address": pool[key]})
        for side in sides:
            address = side.get("address")
            if not isinstance(address, str) or not address or address.lower() in QUOTE_MINTS:
                continue
            coins.append({
                "mint_address": address,
                "program_type": "meteora",
                "name": side.get("name") or None,
                "symbol": side.get("symbol") or None,
                "image_url": None,
                "source": "meteora",
            })
            break
    return coins


class MeteoraDetector:
    def __init__(self, session_getter, limiter=None):
        self._session_getter = session_getter
        self.limiter = limiter or RateLimiter(rate_per_second=1.0, burst=2)
        self._seen = OrderedDict()

    async def fetch_pools(self):
        session = self._session_getter()
        if session is None:
            return None
        async with self.limiter:
            async with session.get(POOLS_URL, params=POOLS_PARAMS, timeout=10) as response:
                if response.status == 429:
                    self.limiter.record_429()
                    return None
                if response.status != 200:
                    return None
                self.limiter.record_success()
                return await response.json(content_type=None)

    async def detect_new_pairs(self):
        try:
            payload = await self.fetch_pools()
        except Exception as exc:
            print(f"Meteora poll error: {type(exc).__name__}: {exc}")
            return []
        fresh = []
        for coin in parse_pools(payload):
            mint = coin["mint_address"]
            if mint in self._seen:
                continue
            self._seen[mint] = True
            while len(self._seen) > SEEN_MAX:
                self._seen.popitem(last=False)
            fresh.append(coin)
        return fresh
