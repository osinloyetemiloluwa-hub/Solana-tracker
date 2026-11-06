"""DexScreener free-API client pieces.

* Discovery feeds (orders, latest boosts, top boosts, latest profiles). Every
  feed owns its rate limiter, its backoff and its consecutive-failure counter;
  a 429 on one feed never slows another feed or the market-data refresh.
* Batched market data: /latest/dex/tokens/{mints}, up to 30 mints per request.
* One pair parser shared by every code path that reads a DexScreener pair.

Missing data is accepted as a gap. Nothing here retries faster, bursts, or
"catches up" after a failure; the caller simply resumes its normal cadence.
"""
import time

from config import Config
from rate_limiter import RateLimiter

BASE_URL = "https://api.dexscreener.com"
CHAIN = "solana"
MAX_BATCH = 30

# (name, path). The four free discovery feeds.
FEEDS = (
    ("orders", "/orders/v1/solana"),
    ("boosts_latest", "/token-boosts/latest/v1"),
    ("boosts_top", "/token-boosts/top/v1"),
    ("profiles_latest", "/token-profiles/latest/v1"),
)


# ---------------------------------------------------------------------------
# Pair parsing
# ---------------------------------------------------------------------------

def _f(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _i(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def pair_liquidity(pair) -> float:
    return _f((pair.get("liquidity") or {}).get("usd")) or 0.0


def pick_best_pair(pairs, mint, base_only=False):
    """Highest-liquidity pair where `mint` is the base (or quote) token."""
    mint_l = (mint or "").lower()
    best = None
    for pair in pairs or []:
        base = ((pair.get("baseToken") or {}).get("address") or "").lower()
        quote = ((pair.get("quoteToken") or {}).get("address") or "").lower()
        if base == mint_l or (not base_only and quote == mint_l):
            if best is None or pair_liquidity(pair) > pair_liquidity(best):
                best = pair
    return best


def group_best_pairs(pairs, mints):
    """{mint: best pair} for a batched response (matches on the base token)."""
    by_lower = {m.lower(): m for m in mints}
    best = {}
    for pair in pairs or []:
        base = ((pair.get("baseToken") or {}).get("address") or "").lower()
        mint = by_lower.get(base)
        if not mint:
            continue
        if mint not in best or pair_liquidity(pair) > pair_liquidity(best[mint]):
            best[mint] = pair
    return best


def parse_pair(pair, mint=None):
    """DexScreener pair -> the market dict used across the scanner."""
    base = pair.get("baseToken") or {}
    quote = pair.get("quoteToken") or {}
    token = base
    if mint and (base.get("address") or "").lower() != mint.lower():
        token = quote
    info = pair.get("info") or {}
    volume = pair.get("volume") or {}
    txns = pair.get("txns") or {}
    h1 = txns.get("h1") or {}
    m5 = txns.get("m5") or {}
    change = pair.get("priceChange") or {}

    mc = pair.get("marketCap")
    if mc is None:
        mc = pair.get("fdv")
    created_ms = _f(pair.get("pairCreatedAt"))

    return {
        "name": token.get("name"),
        "symbol": token.get("symbol"),
        "price": _f(pair.get("priceUsd")) or None,
        "market_cap": _f(mc),
        "liquidity": pair_liquidity(pair) or None,
        "volume_24h": _f(volume.get("h24")) or None,
        "volume_1h": _f(volume.get("h1")) or None,
        "volume_5m": _f(volume.get("m5")) or None,
        "buys_1h": _i(h1.get("buys")),
        "sells_1h": _i(h1.get("sells")),
        "buys_5m": _i(m5.get("buys")),
        "sells_5m": _i(m5.get("sells")),
        "price_change_h1": _f(change.get("h1")),
        "image_url": info.get("imageUrl"),
        "dex_id": pair.get("dexId"),
        "pair_created_at": created_ms / 1000.0 if created_ms else None,
        "source": "dexscreener",
    }


# ---------------------------------------------------------------------------
# Feed items
# ---------------------------------------------------------------------------

def extract_feed_items(payload, feed_name):
    """Solana mints from a discovery-feed payload.

    Boost/profile feeds are multi-chain, so non-Solana items are dropped. The
    payload is either a JSON list or an object wrapping one. Entries without a
    token address are skipped (never guessed).
    """
    if isinstance(payload, dict):
        for key in ("data", "pairs", "orders", "items", "tokens"):
            if isinstance(payload.get(key), list):
                payload = payload[key]
                break
        else:
            payload = []
    items = []
    seen = set()
    for entry in payload if isinstance(payload, list) else []:
        if not isinstance(entry, dict):
            continue
        chain = (entry.get("chainId") or CHAIN)
        if str(chain).lower() != CHAIN:
            continue
        mint = entry.get("tokenAddress") or entry.get("mint") or entry.get("address")
        if not isinstance(mint, str) or not mint or mint in seen:
            continue
        seen.add(mint)
        items.append({
            "mint_address": mint,
            "feed": feed_name,
            "image_url": entry.get("icon") if isinstance(entry.get("icon"), str) else None,
        })
    return items


# ---------------------------------------------------------------------------
# Per-feed state, limiter and backoff
# ---------------------------------------------------------------------------

class FeedState:
    """Everything a single feed owns: limiter, backoff and failure counter."""

    def __init__(self, name, path, limiter=None):
        self.name = name
        self.path = path
        self.limiter = limiter or RateLimiter(rate_per_second=1.0, burst=1)
        self.consecutive_failures = 0
        self.backoff_until = 0.0
        self.total_requests = 0
        self.total_429s = 0
        self.last_ok_at = 0.0
        self.last_count = 0

    def in_backoff(self, now=None):
        return (now if now is not None else time.monotonic()) < self.backoff_until

    def record_success(self, count=0):
        self.consecutive_failures = 0
        self.backoff_until = 0.0
        self.last_ok_at = time.monotonic()
        self.last_count = count

    def record_failure(self, rate_limited=False, retry_after=None, now=None):
        """Back off this feed only. 429s back off immediately; other failures
        only once the consecutive-failure threshold is reached."""
        now = now if now is not None else time.monotonic()
        self.consecutive_failures += 1
        if rate_limited:
            self.total_429s += 1
        threshold = Config.DEXSCREENER_FAILURE_THRESHOLD
        if not rate_limited and self.consecutive_failures < threshold:
            return 0.0
        exponent = max(0, self.consecutive_failures - (1 if rate_limited else threshold))
        delay = min(
            Config.FEED_BACKOFF_INITIAL_SECONDS * (2 ** exponent),
            Config.FEED_BACKOFF_MAX_SECONDS,
        )
        if retry_after:
            delay = min(max(delay, float(retry_after)), Config.FEED_BACKOFF_MAX_SECONDS)
        self.backoff_until = now + delay
        return delay

    def stats(self):
        return {
            "requests": self.total_requests,
            "429s": self.total_429s,
            "failures": self.consecutive_failures,
            "backoff_remaining": max(0.0, round(self.backoff_until - time.monotonic(), 1)),
            "last_count": self.last_count,
        }


def _retry_after(response):
    try:
        return float(response.headers.get("Retry-After"))
    except (TypeError, ValueError, AttributeError):
        return None


class DexScreenerFeeds:
    """The four discovery feeds plus the batched token endpoint."""

    def __init__(self, tokens_limiter=None):
        self.feeds = {name: FeedState(name, path) for name, path in FEEDS}
        # Market-data refresh has its own failure/backoff state; its limiter is
        # shared with other token-lookup calls because they hit the same
        # DexScreener bucket.
        self.tokens = FeedState("tokens", "/latest/dex/tokens", limiter=tokens_limiter)

    async def _get_json(self, session, state, url, timeout=10):
        """GET with per-state limiter/backoff. Returns (ok, payload)."""
        if state.in_backoff():
            return False, None
        state.total_requests += 1
        try:
            async with state.limiter:
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 429:
                        state.record_failure(rate_limited=True, retry_after=_retry_after(response))
                        return False, None
                    if response.status != 200:
                        state.record_failure()
                        return False, None
                    payload = await response.json(content_type=None)
        except Exception:
            state.record_failure()
            return False, None
        return True, payload

    async def poll_feed(self, session, name):
        """One poll of a discovery feed. Returns a list of items ([] on a gap)."""
        state = self.feeds[name]
        ok, payload = await self._get_json(session, state, BASE_URL + state.path)
        if not ok:
            return []
        items = extract_feed_items(payload, name)
        state.record_success(len(items))
        return items

    async def fetch_tokens(self, session, mints):
        """Batched market data. {mint: market dict}; None when the call failed
        or the endpoint is backing off (an unknown, not "no data")."""
        mints = [m for m in dict.fromkeys(mints or []) if m][:MAX_BATCH]
        if not mints:
            return {}
        url = f"{BASE_URL}/latest/dex/tokens/" + ",".join(mints)
        ok, payload = await self._get_json(session, self.tokens, url)
        if not ok:
            return None
        pairs = payload.get("pairs") if isinstance(payload, dict) else None
        self.tokens.record_success(len(pairs or []))
        best = group_best_pairs(pairs or [], mints)
        return {mint: parse_pair(pair, mint) for mint, pair in best.items()}

    def stats(self):
        out = {name: state.stats() for name, state in self.feeds.items()}
        out["tokens"] = self.tokens.stats()
        return out
