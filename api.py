"""Read-only JSON API, served from this bot's own database and an in-process cache.

Endpoints (all GET, UTF-8 JSON):
    /api/tokens/{mint}   one tracked token
    /api/tokens          tracked tokens (?sort=score|market_cap &limit= &offset=)
    /api/milestones      recent milestone hits (?limit=)
    /api/history         recent scans (?limit= &filter=, same grammar as /history)

Per-token shape. `mint` is always a top-level field and `pairs[0]` follows the
DexScreener /latest/dex/tokens/{mint} pair layout (chainId, dexId, baseToken,
priceUsd, marketCap, liquidity.usd, volume.{h24,h1,m5}, txns.{h1,m5},
pairCreatedAt) so a consumer can read it where a DexScreener response would
go. Fields DexScreener does not have live in `tracker`, in snake_case:

    program_type          launch program (pumpfun, pumpswap, raydium, meteora, ...)
    initial_market_cap    market cap when this bot first saw the token
    initial_price         price when first seen
    ath_market_cap        highest market cap seen
    current_multiple      current / initial market cap
    max_multiple          highest multiple reported (milestone high-water mark)
    launch_sol            SOL the dev spent on the launch buy
    dev_wallet            creator wallet
    dev_reputation        {launched, rugged} counts for that dev
    pvp_siblings          up to 10 other tokens with the same name or ticker
    band, score           alert band (low|medium|high) and its score
    red_flags             list of flag strings
    rugged, rug_reason    rug evidence recorded by the bot
    real_buy_volume_1h    buy-side USD volume, 1h (observed trades when available)
    real_buy_volume_5m    buy-side USD volume, 5m
    buy_volume_source     "observed" (PumpPortal trades) or "approx" (volume*buys/(buys+sells))
    buy_volume_1h_approx  the trade-count approximation, always stored alongside
    sniper_count          non-dev buyers inside the first seconds of the launch
    created_at, last_tracked_at   ISO 8601 UTC timestamps
    milestones_hit        [{multiplier, market_cap, hit_at}] highest first

Fields with no data are omitted (never null or 0 placeholders). Internals such
as rejection reasons and retry timings are never exposed. Collection responses
carry `mint` on every item. Errors are JSON: {"error": code, "message": text}.
"""
import json
import time
from collections import OrderedDict, deque
from datetime import datetime, timezone

from aiohttp import web

from cache import TTLCache
from config import Config
from enricher import has_valid_identity, is_valid_solana_address
from history_filter import FilterError, parse_history_filter

SCHEMA_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _num(value):
    """float > 0, else None (zero and missing both mean "no data")."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _iso(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        value = datetime.fromtimestamp(value, tz=timezone.utc)
    if getattr(value, "tzinfo", None) is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _prune(obj):
    """Drop None / empty containers recursively; keeps False and real zeros out of callers' way."""
    if isinstance(obj, dict):
        out = {k: _prune(v) for k, v in obj.items()}
        return {k: v for k, v in out.items() if v is not None and v != {} and v != []}
    if isinstance(obj, list):
        return [x for x in (_prune(i) for i in obj) if x is not None and x != {}]
    return obj


def token_shape(row, dev_rep=None, siblings=(), milestones=()):
    """One coins row (+ optional context) -> the API token object."""
    row = dict(row)
    mint = row["mint_address"]
    named = has_valid_identity(row.get("name"), row.get("symbol"), mint)

    buys_1h, sells_1h = row.get("buys_1h"), row.get("sells_1h")
    buys_5m, sells_5m = row.get("buys_5m"), row.get("sells_5m")
    price = _num(row.get("current_price"))
    pair = {
        "chainId": "solana",
        "dexId": row.get("program_type"),
        "baseToken": {
            "address": mint,
            "name": row["name"].strip() if named else None,
            "symbol": row["symbol"].strip() if named else None,
        },
        "priceUsd": f"{price:.12g}" if price else None,
        "marketCap": _num(row.get("current_market_cap")),
        "liquidity": {"usd": _num(row.get("liquidity"))},
        "volume": {
            "h24": _num(row.get("volume_24h")),
            "h1": _num(row.get("volume_1h")),
            "m5": _num(row.get("volume_5m")),
        },
        "txns": {
            "h1": {"buys": buys_1h, "sells": sells_1h} if (buys_1h or sells_1h) else None,
            "m5": {"buys": buys_5m, "sells": sells_5m} if (buys_5m or sells_5m) else None,
        },
        "pairCreatedAt": (
            int((row["pair_created_at"] if row.get("pair_created_at") else row["created_at"]).timestamp() * 1000)
            if (row.get("pair_created_at") or row.get("created_at")) else None
        ),
    }

    flags = [f for f in (row.get("red_flags") or "").split(",") if f]
    tracker = {
        "program_type": row.get("program_type"),
        "initial_market_cap": _num(row.get("first_seen_market_cap")),
        "initial_price": _num(row.get("first_seen_price")),
        "ath_market_cap": _num(row.get("ath_market_cap")),
        "current_multiple": _num(row.get("current_multiple")),
        "max_multiple": _num(row.get("max_multiple")),
        "launch_sol": _num(row.get("launch_sol")),
        "dev_wallet": row.get("dev_wallet"),
        "dev_reputation": (
            {"launched": dev_rep["launched"], "rugged": dev_rep["rugged"]} if dev_rep else None
        ),
        "pvp_siblings": [
            {
                "mint": sib["mint_address"],
                "name": sib.get("name"),
                "symbol": sib.get("symbol"),
                "market_cap": _num(sib.get("current_market_cap") or sib.get("first_seen_market_cap")),
                "created_at": _iso(sib.get("created_at")),
            }
            for sib in list(siblings)[:10]
        ],
        "band": row.get("score_label"),
        "score": row.get("score") or None,
        "red_flags": flags,
        "rugged": True if row.get("rugged") else None,
        "rug_reason": row.get("rug_reason") if row.get("rugged") else None,
        "real_buy_volume_1h": _num(row.get("buy_volume_1h")),
        "real_buy_volume_5m": _num(row.get("buy_volume_5m")),
        "buy_volume_source": row.get("buy_volume_source"),
        "buy_volume_1h_approx": _num(row.get("buy_volume_1h_approx")),
        "sniper_count": row.get("sniper_count") or None,
        "created_at": _iso(row.get("created_at")),
        "last_tracked_at": _iso(row.get("last_tracked_at")),
        "milestones_hit": [
            {
                "multiplier": _num(m["multiplier"]),
                "market_cap": _num(m.get("market_cap_at_milestone")),
                "hit_at": _iso(m.get("hit_at")),
            }
            for m in milestones
        ],
    }
    return {
        "schemaVersion": SCHEMA_VERSION,
        "mint": mint,
        **_prune({"pairs": [pair], "tracker": tracker}),
    }


def json_response(data, status=200, headers=None):
    return web.Response(
        text=json.dumps(data, ensure_ascii=False, separators=(",", ":"), default=str),
        status=status,
        content_type="application/json",
        charset="utf-8",
        headers=headers,
    )


def error(status, code, message, **extra):
    return json_response({"error": code, "message": message, **extra}, status=status)


# ---------------------------------------------------------------------------
# Per-IP rate limit
# ---------------------------------------------------------------------------

class IPRateLimiter:
    """Sliding one-minute window per client IP, bounded in memory."""

    def __init__(self, per_minute, max_ips=10000, clock=time.monotonic):
        self.per_minute = int(per_minute)
        self.max_ips = max_ips
        self._clock = clock
        self._hits = OrderedDict()

    def check(self, ip):
        """(allowed, retry_after_seconds)."""
        now = self._clock()
        hits = self._hits.pop(ip, None) or deque()
        while hits and now - hits[0] >= 60:
            hits.popleft()
        allowed = len(hits) < self.per_minute
        if allowed:
            hits.append(now)
        retry = 0 if allowed else max(1, int(60 - (now - hits[0])) + 1)
        self._hits[ip] = hits
        while len(self._hits) > self.max_ips:
            self._hits.popitem(last=False)
        return allowed, retry


def client_ip(request):
    """Rightmost X-Forwarded-For hop (added by the platform proxy; the left side
    is client-controlled and spoofable), else the socket peer."""
    forwarded = request.headers.get("X-Forwarded-For", "")
    hops = [h.strip() for h in forwarded.split(",") if h.strip()]
    return hops[-1] if hops else (request.remote or "unknown")


@web.middleware
async def api_middleware(request, handler):
    if not request.path.startswith("/api/"):
        return await handler(request)
    limiter = request.app["api_limiter"]
    allowed, retry = limiter.check(client_ip(request))
    if not allowed:
        return json_response(
            {"error": "rate_limited", "message": "Too many requests. Slow down."},
            status=429, headers={"Retry-After": str(retry)},
        )
    try:
        return await handler(request)
    except web.HTTPException:
        raise
    except Exception as exc:  # never leak internals
        print(f"API error on {request.path}: {type(exc).__name__}: {exc}")
        return error(503, "unavailable", "Temporarily unavailable. Try again shortly.")


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _db(request):
    return request.app["bot"].db


def _int_param(request, name, default, low, high):
    try:
        value = int(request.query.get(name, default))
    except (TypeError, ValueError):
        value = default
    return max(low, min(value, high))


async def _full_shape(db, row):
    row = dict(row)
    dev_rep = siblings = milestones = None
    try:
        dev_rep = await db.get_dev_reputation(row.get("dev_wallet"))
    except Exception:
        pass
    try:
        if row.get("name") and row.get("symbol"):
            siblings = await db.get_pvp_siblings(
                row["mint_address"], row["name"], row["symbol"],
                row.get("created_at") or datetime.now(timezone.utc), limit=10,
            )
    except Exception:
        pass
    try:
        milestones = await db.get_milestones_for_coin(row["id"]) if row.get("id") is not None else None
    except Exception:
        pass
    return token_shape(row, dev_rep, [dict(s) for s in siblings or []], [dict(m) for m in milestones or []])


async def get_token(request):
    mint = request.match_info["mint"].strip()
    if not is_valid_solana_address(mint):
        return error(400, "invalid_mint", "That is not a valid Solana mint address.", mint=mint)
    cache = request.app["api_cache"]
    key = ("token", mint)
    cached = cache.get(key)
    if cached is not None:
        return json_response(cached)
    row = await _db(request).get_coin_by_mint(mint)
    if not row:
        return error(404, "not_tracked", "This mint is not tracked.", mint=mint)
    data = await _full_shape(_db(request), row)
    cache.set(key, data)
    return json_response(data)


async def list_tokens(request):
    sort = request.query.get("sort", "score")
    if sort not in ("score", "market_cap"):
        return error(400, "invalid_sort", "sort must be score or market_cap.")
    limit = _int_param(request, "limit", 20, 1, Config.API_MAX_LIMIT)
    offset = _int_param(request, "offset", 0, 0, 1_000_000)
    cache = request.app["api_cache"]
    key = ("tokens", sort, limit, offset)
    cached = cache.get(key)
    if cached is not None:
        return json_response(cached)
    db = _db(request)
    rows = await db.get_tracked_tokens(sort=sort, limit=limit, offset=offset)
    total = await db.count_tracked_tokens()
    tokens = [await _full_shape(db, r) for r in rows]
    data = {
        "schemaVersion": SCHEMA_VERSION, "sort": sort, "limit": limit,
        "offset": offset, "total": total, "count": len(tokens), "tokens": tokens,
    }
    cache.set(key, data)
    return json_response(data)


async def list_milestones(request):
    limit = _int_param(request, "limit", 10, 1, 25)
    cache = request.app["api_cache"]
    key = ("milestones", limit)
    cached = cache.get(key)
    if cached is not None:
        return json_response(cached)
    rows = await _db(request).get_recent_milestones(limit)
    items = []
    for row in rows:
        row = dict(row)
        if not has_valid_identity(row.get("name"), row.get("symbol"), row.get("mint_address")):
            continue
        items.append(_prune({
            "mint": row["mint_address"],
            "name": row["name"].strip(),
            "symbol": row["symbol"].strip(),
            "multiplier": _num(row.get("multiplier")),
            "market_cap": _num(row.get("market_cap_at_milestone")),
            "initial_market_cap": _num(row.get("first_seen_market_cap")),
            "launch_sol": _num(row.get("launch_sol")),
            "hit_at": _iso(row.get("hit_at")),
        }))
    data = {"schemaVersion": SCHEMA_VERSION, "count": len(items), "milestones": items}
    cache.set(key, data)
    return json_response(data)


async def list_history(request):
    limit = _int_param(request, "limit", 10, 1, 25)
    text = request.query.get("filter", "")
    try:
        conditions = parse_history_filter(text)
    except FilterError as exc:
        return error(400, "invalid_filter", str(exc))
    cache = request.app["api_cache"]
    key = ("history", limit, text.strip().lower())
    cached = cache.get(key)
    if cached is not None:
        return json_response(cached)
    db = _db(request)
    rows = await db.get_history(conditions, limit)
    tokens = [await _full_shape(db, r) for r in rows]
    data = {"schemaVersion": SCHEMA_VERSION, "count": len(tokens), "tokens": tokens}
    cache.set(key, data)
    return json_response(data)


async def api_not_found(request):
    return error(404, "not_found", "Unknown API endpoint.")


def setup_api(app):
    """Attach the API routes, rate limiter and cache to an aiohttp app."""
    app["api_limiter"] = IPRateLimiter(Config.API_RATE_LIMIT_PER_MINUTE)
    app["api_cache"] = TTLCache(256, Config.API_CACHE_TTL_SECONDS)
    app.router.add_get("/api/tokens", list_tokens)
    app.router.add_get("/api/tokens/{mint}", get_token)
    app.router.add_get("/api/milestones", list_milestones)
    app.router.add_get("/api/history", list_history)
    app.router.add_get("/api/{tail:.*}", api_not_found)
