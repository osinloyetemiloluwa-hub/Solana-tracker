"""Raydium discovery through the free api-v3 pool list.

Discovery only: poll for the newest pools, return the non-quote mint of each
new pool, and let the scanner upsert it into `coins`. Market data for those
mints then arrives through the normal batched DexScreener refresh.
"""
from collections import OrderedDict

from rate_limiter import RateLimiter

POOLS_URL = "https://api-v3.raydium.io/pools/info/list"
POOLS_PARAMS = {
    "poolType": "all",
    "poolSortField": "createdTime",
    "sortType": "desc",
    "pageSize": 20,
    "page": 1,
}

# Quote assets are never the token being discovered.
QUOTE_MINTS = {
    "so11111111111111111111111111111111111111112",   # wrapped SOL
    "epjfwdd5aufqssqem2qn1xzybapc8g4weggkzwytdt1v",  # USDC
    "es9vmfrzacermjfrf4h2fyd4kconcky11mcce8benwnyb",  # USDT
}
SEEN_MAX = 5000


def parse_pools(payload):
    """api-v3 pool list payload -> coin-shaped dicts (one per pool, quote mint skipped)."""
    pools = ((payload or {}).get("data") or {}).get("data") if isinstance(payload, dict) else None
    coins = []
    for pool in pools or []:
        if not isinstance(pool, dict):
            continue
        for side in (pool.get("mintA"), pool.get("mintB")):
            if not isinstance(side, dict):
                continue
            address = side.get("address")
            if not isinstance(address, str) or address.lower() in QUOTE_MINTS:
                continue
            coins.append({
                "mint_address": address,
                "program_type": "raydium",
                "name": side.get("name") or None,
                "symbol": side.get("symbol") or None,
                "image_url": side.get("logoURI") or None,
                "source": "raydium",
            })
            break
    return coins


class RaydiumDetector:
    def __init__(self, session_getter, limiter=None):
        """`session_getter` returns the shared aiohttp session (or None)."""
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
        """Mints from pools not returned by an earlier call."""
        try:
            payload = await self.fetch_pools()
        except Exception as exc:
            print(f"Raydium poll error: {type(exc).__name__}: {exc}")
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
