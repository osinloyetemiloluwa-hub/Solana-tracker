import time
import asyncpg
from config import Config
from alert_filters import FILTER_COLUMNS


# Rows with a missing/placeholder name, symbol or mint are never shown anywhere.
_EMPTY_VALUES_SQL = (
    "'', 'null', 'none', 'undefined', 'nan', 'n/a', 'na', 'unknown', "
    "'unknown token', '???', '?', '-', '--'"
)
def identity_sql(prefix: str = "") -> str:
    n, y, m = f"{prefix}name", f"{prefix}symbol", f"{prefix}mint_address"
    return (
        f"({n} IS NOT NULL AND {y} IS NOT NULL AND {m} IS NOT NULL "
        f"AND LOWER(BTRIM({n})) NOT IN ({_EMPTY_VALUES_SQL}) "
        f"AND LOWER(BTRIM({y})) NOT IN ({_EMPTY_VALUES_SQL}) "
        f"AND BTRIM({n}) !~* '^token [a-z0-9]{{6}}$')"
    )


VALID_IDENTITY_SQL = identity_sql()

# A token DexScreener feeds may put back under tracking.
_REVIVABLE_SQL = (
    "NOT COALESCE(coins.rugged, FALSE) "
    "AND COALESCE(coins.red_flags, '') NOT LIKE '%chased%' "
    "AND COALESCE(coins.red_flags, '') NOT LIKE '%duplicate%'"
)

# /history filter keys -> SQL expressions (columns are fixed; values are bound).
_HISTORY_EXPR = {
    "mc": "COALESCE(current_market_cap, first_seen_market_cap)",
    "volume": "volume_1h",
    "vol24": "volume_24h",
    "buyvol": "buy_volume_1h",
    "liq": "liquidity",
    "age": "EXTRACT(EPOCH FROM (NOW() - COALESCE(pair_created_at, created_at)))",
}


def build_history_query(conditions: dict, limit: int):
    """(sql, params) for the filtered history list.

    `conditions` is the output of history_filter.parse_history_filter. With no
    conditions this is the plain newest-first list.
    """
    where = [VALID_IDENTITY_SQL]
    params = []
    for key in ("mc", "volume", "buyvol", "vol24", "liq", "age"):
        bounds = (conditions or {}).get(key)
        if not bounds:
            continue
        lo, hi = bounds
        expr = _HISTORY_EXPR[key]
        if lo is not None:
            params.append(float(lo))
            where.append(f"{expr} >= ${len(params)}::numeric")
        if hi is not None:
            params.append(float(hi))
            where.append(f"{expr} <= ${len(params)}::numeric")
    bands = (conditions or {}).get("band")
    if bands:
        params.append(list(bands))
        where.append(f"score_label = ANY(${len(params)}::text[])")
    params.append(max(1, min(int(limit), 25)))
    sql = (
        f"SELECT * FROM coins WHERE {' AND '.join(where)} "
        f"ORDER BY created_at DESC LIMIT ${len(params)}"
    )
    return sql, params


class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            dsn=Config.DATABASE_URL,
            min_size=1,
            max_size=10,
            command_timeout=60,
            statement_cache_size=0,
        )
        await self.init_tables()

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def init_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS guilds (
                    guild_id BIGINT PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS guild_settings (
                    guild_id BIGINT PRIMARY KEY,
                    alert_channel_id BIGINT,
                    scan_mode TEXT DEFAULT 'medium',
                    dev_tracking BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute(
                "ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS fresh_channel_id BIGINT"
            )
            await conn.execute(
                "ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS launch_alerts BOOLEAN DEFAULT FALSE"
            )
            # Per-guild hard alert filters. NULL = off (existing behaviour).
            for column, sql_type in (
                ("min_buy_volume_usd", "NUMERIC"),
                ("min_volume_5m_usd", "NUMERIC"),
                ("max_snipers", "INTEGER"),
                ("min_age_seconds", "INTEGER"),
                ("min_liquidity_usd", "NUMERIC"),
                ("max_dev_rugs", "INTEGER"),
            ):
                await conn.execute(
                    f"ALTER TABLE guild_settings ADD COLUMN IF NOT EXISTS {column} {sql_type}"
                )
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS watched_wallets (
                    id SERIAL PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    address TEXT NOT NULL,
                    label TEXT,
                    channel_id BIGINT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    last_seen_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(guild_id, address)
                )
            """)
            await conn.execute(
                "ALTER TABLE watched_wallets ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ DEFAULT NOW()"
            )
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS coins (
                    id SERIAL PRIMARY KEY,
                    mint_address TEXT UNIQUE NOT NULL,
                    program_type TEXT NOT NULL,
                    name TEXT,
                    symbol TEXT,
                    uri TEXT,
                    image_url TEXT,
                    first_seen_price NUMERIC,
                    first_seen_market_cap NUMERIC,
                    current_price NUMERIC,
                    current_market_cap NUMERIC,
                    score INTEGER DEFAULT 0,
                    score_label TEXT DEFAULT 'low',
                    alert_message_id BIGINT,
                    alert_channel_id BIGINT,
                    guild_id BIGINT,
                    dev_wallet TEXT,
                    red_flags TEXT,
                    alerted_low BOOLEAN DEFAULT FALSE,
                    alerted_medium BOOLEAN DEFAULT FALSE,
                    alerted_high BOOLEAN DEFAULT FALSE,
                    alert_attempts INTEGER DEFAULT 0,
                    next_alert_retry_at TIMESTAMPTZ,
                    last_reject_reason TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    last_tracked_at TIMESTAMPTZ DEFAULT NOW(),
                    is_active BOOLEAN DEFAULT TRUE,
                    hit_milestone BOOLEAN DEFAULT FALSE
                )
            """)
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS image_url TEXT")
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS red_flags TEXT")
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS alerted_low BOOLEAN DEFAULT FALSE")
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS alerted_medium BOOLEAN DEFAULT FALSE")
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS alerted_high BOOLEAN DEFAULT FALSE")
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS alert_attempts INTEGER DEFAULT 0")
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS next_alert_retry_at TIMESTAMPTZ")
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS last_reject_reason TEXT")
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS launch_sol NUMERIC")
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS ath_market_cap NUMERIC")
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS rugged BOOLEAN DEFAULT FALSE")
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS rug_reason TEXT")
            for column, sql_type in (
                ("liquidity", "NUMERIC"),
                ("volume_24h", "NUMERIC"),
                ("volume_1h", "NUMERIC"),
                ("volume_5m", "NUMERIC"),
                ("buys_1h", "INTEGER"),
                ("sells_1h", "INTEGER"),
                ("buys_5m", "INTEGER"),
                ("sells_5m", "INTEGER"),
                ("buy_volume_1h", "NUMERIC"),
                ("buy_volume_5m", "NUMERIC"),
                ("buy_volume_1h_approx", "NUMERIC"),
                ("buy_volume_5m_approx", "NUMERIC"),
                ("buy_volume_source", "TEXT"),
                ("sniper_count", "INTEGER"),
                ("current_multiple", "NUMERIC"),
                ("max_multiple", "NUMERIC"),
                ("last_trade_at", "TIMESTAMPTZ DEFAULT NOW()"),
                ("is_dead", "BOOLEAN DEFAULT FALSE"),
                ("dead_at", "TIMESTAMPTZ"),
                ("deactivated_at", "TIMESTAMPTZ"),
                ("rugged_at", "TIMESTAMPTZ"),
                ("pair_created_at", "TIMESTAMPTZ"),
                ("discovery_source", "TEXT"),
                ("reactivated_at", "TIMESTAMPTZ"),
            ):
                await conn.execute(
                    f"ALTER TABLE coins ADD COLUMN IF NOT EXISTS {column} {sql_type}"
                )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_coins_dev ON coins(dev_wallet)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_coins_dead ON coins(is_dead, dead_at)")
            # One-time, idempotent backfills for rows that predate these columns.
            await conn.execute(
                "UPDATE coins SET rugged_at = updated_at WHERE rugged AND rugged_at IS NULL"
            )
            await conn.execute(
                "UPDATE coins SET deactivated_at = updated_at "
                "WHERE is_active = FALSE AND deactivated_at IS NULL"
            )
            # Existing tokens must not re-announce every multiple they already
            # reached under the old fixed-list system.
            await conn.execute("""
                UPDATE coins c
                SET max_multiple = ROUND(GREATEST(
                    1,
                    COALESCE((SELECT MAX(multiplier) FROM milestones m WHERE m.coin_id = c.id), 1),
                    CASE WHEN COALESCE(first_seen_market_cap, 0) > 0
                         THEN COALESCE(current_market_cap, 0) / first_seen_market_cap ELSE 1 END
                )::numeric, 2)
                WHERE c.max_multiple IS NULL
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS coin_snapshots (
                    id SERIAL PRIMARY KEY,
                    coin_id INTEGER REFERENCES coins(id) ON DELETE CASCADE,
                    price NUMERIC,
                    market_cap NUMERIC,
                    liquidity NUMERIC,
                    volume_24h NUMERIC,
                    holders INTEGER,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS milestones (
                    id SERIAL PRIMARY KEY,
                    coin_id INTEGER REFERENCES coins(id) ON DELETE CASCADE,
                    multiplier NUMERIC NOT NULL,
                    price_at_milestone NUMERIC,
                    market_cap_at_milestone NUMERIC,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(coin_id, multiplier)
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dev_wallets (
                    id SERIAL PRIMARY KEY,
                    coin_id INTEGER REFERENCES coins(id) ON DELETE CASCADE,
                    wallet_address TEXT NOT NULL,
                    label TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dev_stats (
                    dev_wallet TEXT PRIMARY KEY,
                    launched INTEGER NOT NULL DEFAULT 0,
                    rugged INTEGER NOT NULL DEFAULT 0
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS wallet_buys (
                    id SERIAL PRIMARY KEY,
                    wallet TEXT NOT NULL,
                    mint TEXT NOT NULL,
                    signature TEXT UNIQUE NOT NULL,
                    sol_spent NUMERIC,
                    tokens_received NUMERIC,
                    timestamp BIGINT,
                    slot BIGINT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS system_state (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_coins_mint ON coins(mint_address)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_coins_active ON coins(is_active)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_wallets_guild ON watched_wallets(guild_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_coin ON coin_snapshots(coin_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_wallet_buys_mint ON wallet_buys(mint)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_wallet_buys_wallet ON wallet_buys(wallet)")

    async def get_or_create_guild(self, guild_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO guilds (guild_id) VALUES ($1)
                ON CONFLICT (guild_id) DO NOTHING
            """, guild_id)
            await conn.execute("""
                INSERT INTO guild_settings (guild_id) VALUES ($1)
                ON CONFLICT (guild_id) DO NOTHING
            """, guild_id)

    async def get_guild_settings(self, guild_id: int):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM guild_settings WHERE guild_id = $1", guild_id)

    async def get_all_guilds(self):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT guild_id FROM guilds ORDER BY guild_id")
            return [row["guild_id"] for row in rows]

    async def set_alert_channel(self, guild_id: int, channel_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO guild_settings (guild_id, alert_channel_id)
                VALUES ($1, $2)
                ON CONFLICT (guild_id)
                DO UPDATE SET alert_channel_id = $2, updated_at = NOW()
            """, guild_id, channel_id)

    async def set_fresh_channel(self, guild_id: int, channel_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO guild_settings (guild_id, fresh_channel_id)
                VALUES ($1, $2)
                ON CONFLICT (guild_id)
                DO UPDATE SET fresh_channel_id = $2, updated_at = NOW()
            """, guild_id, channel_id)

    async def set_launch_alerts(self, guild_id: int, enabled: bool):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO guild_settings (guild_id, launch_alerts)
                VALUES ($1, $2)
                ON CONFLICT (guild_id)
                DO UPDATE SET launch_alerts = $2, updated_at = NOW()
            """, guild_id, enabled)

    async def get_fresh_targets(self):
        """Channel ids of guilds with launch alerts ON and a fresh channel set."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT guild_id, fresh_channel_id FROM guild_settings
                WHERE launch_alerts = TRUE AND fresh_channel_id IS NOT NULL
            """)
            return [(r["guild_id"], r["fresh_channel_id"]) for r in rows]

    async def set_scan_mode(self, guild_id: int, mode: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO guild_settings (guild_id, scan_mode)
                VALUES ($1, $2)
                ON CONFLICT (guild_id)
                DO UPDATE SET scan_mode = $2, updated_at = NOW()
            """, guild_id, mode)

    async def set_dev_tracking(self, guild_id: int, enabled: bool):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO guild_settings (guild_id, dev_tracking)
                VALUES ($1, $2)
                ON CONFLICT (guild_id)
                DO UPDATE SET dev_tracking = $2, updated_at = NOW()
            """, guild_id, enabled)

    async def set_guild_filters(self, guild_id: int, values: dict):
        """Set filter columns. A value of None switches that filter off."""
        columns = [c for c in values if c in FILTER_COLUMNS]
        if not columns:
            return
        assignments = ", ".join(f"{c} = ${i + 2}" for i, c in enumerate(columns))
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO guild_settings (guild_id) VALUES ($1) ON CONFLICT (guild_id) DO NOTHING",
                guild_id,
            )
            await conn.execute(
                f"UPDATE guild_settings SET {assignments}, updated_at = NOW() WHERE guild_id = $1",
                guild_id, *[values[c] for c in columns],
            )

    async def reset_guild_filters(self, guild_id: int):
        await self.set_guild_filters(guild_id, {c: None for c in FILTER_COLUMNS})

    async def add_wallet(self, guild_id: int, address: str, label: str = None, channel_id: int = None):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO watched_wallets (guild_id, address, label, channel_id, last_seen_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (guild_id, address)
                DO UPDATE SET label = $3, channel_id = $4, last_seen_at = NOW()
            """, guild_id, address, label, channel_id)

    async def remove_wallet(self, guild_id: int, address: str):
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM watched_wallets WHERE guild_id = $1 AND address = $2",
                guild_id, address,
            )
            return result == "DELETE 1"

    async def clear_wallets(self, guild_id: int) -> int:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM watched_wallets WHERE guild_id = $1", guild_id
            )
            try:
                return int(result.split()[-1])
            except Exception:
                return 0

    async def get_wallets(self, guild_id: int):
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT * FROM watched_wallets WHERE guild_id = $1 ORDER BY created_at DESC
            """, guild_id)

    async def get_all_wallets(self):
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT DISTINCT address, label, guild_id, channel_id FROM watched_wallets
            """)

    async def mark_wallet_seen(self, address: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE watched_wallets
                SET last_seen_at = NOW()
                WHERE LOWER(address) = LOWER($1)
            """, address)

    async def remove_inactive_wallets(self, days: int = 7) -> int:
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM watched_wallets
                WHERE last_seen_at < NOW() - ($1::text || ' days')::interval
                AND address NOT IN (
                    SELECT DISTINCT dev_wallet FROM coins WHERE dev_wallet IS NOT NULL
                )
            """, str(days))
            try:
                return int(result.split()[-1])
            except Exception:
                return 0

    async def insert_coin(self, data: dict):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO coins (
                    mint_address, program_type, name, symbol, uri, image_url,
                    first_seen_price, first_seen_market_cap,
                    score, score_label, guild_id, dev_wallet, launch_sol
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (mint_address)
                DO UPDATE SET
                    program_type = COALESCE($2, coins.program_type),
                    name = COALESCE($3, coins.name),
                    symbol = COALESCE($4, coins.symbol),
                    uri = COALESCE($5, coins.uri),
                    image_url = COALESCE($6, coins.image_url),
                    dev_wallet = COALESCE($12, coins.dev_wallet),
                    launch_sol = COALESCE($13, coins.launch_sol),
                    score = GREATEST(coins.score, $9),
                    score_label = CASE
                        WHEN $10 IS NULL THEN coins.score_label
                        WHEN $10 = 'high' THEN 'high'
                        WHEN $10 = 'medium' AND coins.score_label != 'high' THEN 'medium'
                        ELSE coins.score_label
                    END,
                    updated_at = NOW()
                RETURNING id
            """,
                data.get("mint_address"),
                data.get("program_type"),
                data.get("name"),
                data.get("symbol"),
                data.get("uri"),
                data.get("image_url"),
                data.get("price"),
                data.get("market_cap"),
                data.get("score", 0),
                data.get("score_label"),
                data.get("guild_id"),
                data.get("dev_wallet"),
                data.get("launch_sol"),
            )
            return row["id"]

    async def upsert_discovered(self, mint_address: str, program_type: str, name: str = None,
                                symbol: str = None, image_url: str = None, source: str = None,
                                revive: bool = False):
        """Record a mint found by a discovery source with whatever it gave us.

        Never overwrites existing data. With `revive` (DexScreener feeds: paid
        orders, boosts, profiles) a token that was expired or marked dead is put
        back under tracking with a fresh tracking window; rugged tokens and
        tokens deliberately flagged chased/duplicate stay out.
        Returns (coin_id, inserted).
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO coins (mint_address, program_type, name, symbol, image_url, discovery_source)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (mint_address) DO UPDATE SET
                    name = COALESCE(coins.name, EXCLUDED.name),
                    symbol = COALESCE(coins.symbol, EXCLUDED.symbol),
                    image_url = COALESCE(coins.image_url, EXCLUDED.image_url),
                    reactivated_at = CASE WHEN $7::boolean AND {revivable}
                        AND (coins.is_active = FALSE OR COALESCE(coins.is_dead, FALSE))
                        THEN NOW() ELSE coins.reactivated_at END,
                    last_trade_at = CASE WHEN $7::boolean AND {revivable}
                        AND (coins.is_active = FALSE OR COALESCE(coins.is_dead, FALSE))
                        THEN NOW() ELSE coins.last_trade_at END,
                    dead_at = CASE WHEN $7::boolean AND {revivable} THEN NULL ELSE coins.dead_at END,
                    is_dead = CASE WHEN $7::boolean AND {revivable} THEN FALSE ELSE coins.is_dead END,
                    is_active = CASE WHEN $7::boolean AND {revivable} THEN TRUE ELSE coins.is_active END
                RETURNING id, (xmax = 0) AS inserted
            """.format(revivable=_REVIVABLE_SQL), mint_address, program_type, name, symbol, image_url, source, revive)
            return row["id"], bool(row["inserted"])

    async def update_market(self, coin_id: int, m: dict):
        """Apply one market refresh and return the updated row.

        * initial price / market cap are set the first time they are seen
        * current market cap and price are overwritten on every refresh
        * ATH is the maximum market cap ever seen
        * current_multiple = current / initial market cap, every refresh
        * volume / liquidity / txn counts are a fresh snapshot of the pair
        """
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("""
                UPDATE coins SET
                    current_price = COALESCE($2::numeric, current_price),
                    current_market_cap = COALESCE($3::numeric, current_market_cap),
                    first_seen_price = COALESCE(first_seen_price, $2::numeric),
                    first_seen_market_cap = COALESCE(first_seen_market_cap, $3::numeric),
                    ath_market_cap = CASE
                        WHEN $3::numeric IS NULL OR $3::numeric <= 0 THEN ath_market_cap
                        ELSE GREATEST(COALESCE(ath_market_cap, 0), $3::numeric) END,
                    current_multiple = CASE
                        WHEN $3::numeric IS NOT NULL
                             AND COALESCE(first_seen_market_cap, $3::numeric) > 0
                        THEN $3::numeric / COALESCE(first_seen_market_cap, $3::numeric)
                        ELSE current_multiple END,
                    liquidity = $4::numeric,
                    volume_24h = $5::numeric,
                    volume_1h = $6::numeric,
                    volume_5m = $7::numeric,
                    buys_1h = $8::integer,
                    sells_1h = $9::integer,
                    buys_5m = $10::integer,
                    sells_5m = $11::integer,
                    buy_volume_1h = $12::numeric,
                    buy_volume_5m = $13::numeric,
                    buy_volume_1h_approx = $14::numeric,
                    buy_volume_5m_approx = $15::numeric,
                    buy_volume_source = $16::text,
                    sniper_count = COALESCE($17::integer, sniper_count),
                    name = COALESCE(name, $18::text),
                    symbol = COALESCE(symbol, $19::text),
                    image_url = COALESCE(image_url, $20::text),
                    program_type = CASE
                        WHEN program_type IN ('unknown', 'dexscreener') AND $21::text IS NOT NULL
                        THEN $21::text ELSE program_type END,
                    pair_created_at = COALESCE(pair_created_at, to_timestamp($22::float8)),
                    last_trade_at = CASE WHEN $23::boolean THEN NOW() ELSE last_trade_at END,
                    last_tracked_at = NOW(),
                    updated_at = NOW()
                WHERE id = $1
                RETURNING *
            """,
                coin_id,
                m.get("price"), m.get("market_cap"), m.get("liquidity"),
                m.get("volume_24h"), m.get("volume_1h"), m.get("volume_5m"),
                m.get("buys_1h"), m.get("sells_1h"), m.get("buys_5m"), m.get("sells_5m"),
                m.get("buy_volume_1h"), m.get("buy_volume_5m"),
                m.get("buy_volume_1h_approx"), m.get("buy_volume_5m_approx"),
                m.get("buy_volume_source"), m.get("sniper_count"),
                m.get("name"), m.get("symbol"), m.get("image_url"), m.get("dex_id"),
                m.get("pair_created_at"), bool(m.get("traded")),
            )

    async def get_history(self, conditions: dict, limit: int = 10):
        sql, params = build_history_query(conditions, limit)
        async with self.pool.acquire() as conn:
            return await conn.fetch(sql, *params)

    async def get_tracked_tokens(self, sort: str = "score", limit: int = 20, offset: int = 0):
        """Currently tracked (active, not dead) tokens with a real identity."""
        if sort == "market_cap":
            order = "COALESCE(current_market_cap, first_seen_market_cap) DESC NULLS LAST, id DESC"
        else:
            order = (
                "COALESCE(score, 0) DESC, "
                "COALESCE(current_market_cap, first_seen_market_cap) DESC NULLS LAST, id DESC"
            )
        async with self.pool.acquire() as conn:
            return await conn.fetch(f"""
                SELECT * FROM coins
                WHERE is_active = TRUE AND COALESCE(is_dead, FALSE) = FALSE
                  AND {VALID_IDENTITY_SQL}
                ORDER BY {order}
                LIMIT $1 OFFSET $2
            """, max(1, min(int(limit), Config.API_MAX_LIMIT)), max(0, int(offset)))

    async def count_tracked_tokens(self) -> int:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(f"""
                SELECT COUNT(*) AS n FROM coins
                WHERE is_active = TRUE AND COALESCE(is_dead, FALSE) = FALSE
                  AND {VALID_IDENTITY_SQL}
            """)
            return int(row["n"])

    async def mark_band_alerted(self, coin_id: int, band: str):
        col = f"alerted_{band}"
        if col not in ("alerted_low", "alerted_medium", "alerted_high"):
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"UPDATE coins SET {col} = TRUE, updated_at = NOW() WHERE id = $1",
                coin_id,
            )

    async def reject_alert(self, coin_id: int, band: str, reason: str, retry_seconds: int):
        """Release the in-flight alert lock and persist a bounded retry/backoff."""
        col = f"alerted_{band}"
        if col not in ("alerted_low", "alerted_medium", "alerted_high"):
            return
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(f"""
                UPDATE coins
                SET {col} = FALSE,
                    alert_attempts = alert_attempts + 1,
                    next_alert_retry_at = NOW() + ($2::text || ' seconds')::interval,
                    last_reject_reason = $3,
                    updated_at = NOW()
                WHERE id = $1
                RETURNING alert_attempts
            """, coin_id, str(max(0, int(retry_seconds))), reason[:500])
            return row["alert_attempts"] if row else 0

    async def confirm_alert(self, coin_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE coins
                SET next_alert_retry_at = NULL, last_reject_reason = NULL, updated_at = NOW()
                WHERE id = $1
            """, coin_id)

    async def set_red_flags(self, coin_id: int, flags: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE coins SET red_flags = $2, updated_at = NOW() WHERE id = $1",
                coin_id, flags,
            )

    async def update_coin_alert(self, coin_id: int, message_id: int, channel_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE coins SET alert_message_id = $2, alert_channel_id = $3, updated_at = NOW()
                WHERE id = $1
            """, coin_id, message_id, channel_id)

    async def get_active_coins(self):
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT * FROM coins
                WHERE is_active = TRUE AND COALESCE(is_dead, FALSE) = FALSE
                AND last_tracked_at > NOW() - ($1::text || ' hours')::interval
                ORDER BY last_tracked_at ASC
            """, str(Config.TRACKING_WINDOW_HOURS))

    async def update_coin_snapshot(self, coin_id: int, price: float, market_cap: float):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE coins
                SET current_price = $2::numeric, current_market_cap = $3::numeric,
                    ath_market_cap = CASE
                        WHEN $3::numeric IS NULL OR $3::numeric <= 0 THEN ath_market_cap
                        ELSE GREATEST(COALESCE(ath_market_cap, 0), $3::numeric)
                    END,
                    updated_at = NOW(), last_tracked_at = NOW()
                WHERE id = $1
            """, coin_id, price, market_cap)

    async def insert_snapshot(self, coin_id: int, price: float, market_cap: float):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO coin_snapshots (coin_id, price, market_cap)
                VALUES ($1, $2, $3)
            """, coin_id, price, market_cap)

    async def raise_max_multiple(self, coin_id: int, multiple: float) -> bool:
        """Atomically raise the coin's high-water multiple.

        True only when `multiple` is strictly higher than anything reported
        before (the first report must beat 1x). Lower or equal readings never
        change the stored value, so a drop never lowers the high-water mark.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                UPDATE coins SET max_multiple = $2::numeric, updated_at = NOW()
                WHERE id = $1 AND COALESCE(max_multiple, 1) < $2::numeric
                RETURNING id
            """, coin_id, multiple)
            return row is not None

    async def insert_milestone(self, coin_id: int, multiplier: float, price: float, market_cap: float):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO milestones (coin_id, multiplier, price_at_milestone, market_cap_at_milestone)
                VALUES ($1::integer, $2::numeric, $3::numeric, $4::numeric)
                ON CONFLICT (coin_id, multiplier) DO NOTHING
            """, coin_id, multiplier, price, market_cap)
            # Keep the 20 highest hits per coin; every new hit is a new high.
            await conn.execute("""
                DELETE FROM milestones
                WHERE coin_id = $1::integer AND id NOT IN (
                    SELECT id FROM milestones WHERE coin_id = $1::integer
                    ORDER BY multiplier DESC LIMIT 20
                )
            """, coin_id)

    async def get_milestones_for_coin(self, coin_id: int, limit: int = 20):
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT multiplier, market_cap_at_milestone, price_at_milestone, created_at AS hit_at
                FROM milestones WHERE coin_id = $1 ORDER BY multiplier DESC LIMIT $2
            """, coin_id, max(1, min(int(limit), 20)))

    async def deactivate_coin(self, coin_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE coins SET is_active = FALSE, deactivated_at = COALESCE(deactivated_at, NOW()), "
                "updated_at = NOW() WHERE id = $1",
                coin_id,
            )

    async def _delete_archiving(self, conn, where_sql: str, *args) -> int:
        """DELETE matching coins, rolling launched/rugged counts into dev_stats
        first so dev reputation survives the cleanup."""
        async with conn.transaction():
            rows = await conn.fetch(
                f"DELETE FROM coins WHERE {where_sql} RETURNING dev_wallet, rugged", *args
            )
            per_dev = {}
            for row in rows:
                dev = row["dev_wallet"]
                if not dev:
                    continue
                entry = per_dev.setdefault(dev, [0, 0])
                entry[0] += 1
                entry[1] += 1 if row["rugged"] else 0
            for dev, (launched, rugged) in per_dev.items():
                await conn.execute("""
                    INSERT INTO dev_stats (dev_wallet, launched, rugged) VALUES ($1, $2, $3)
                    ON CONFLICT (dev_wallet) DO UPDATE SET
                        launched = dev_stats.launched + EXCLUDED.launched,
                        rugged = dev_stats.rugged + EXCLUDED.rugged
                """, dev, launched, rugged)
        return len(rows)

    @staticmethod
    def _affected(result: str) -> int:
        try:
            return int(result.split()[-1])
        except Exception:
            return 0

    async def run_lifecycle_cleanup(self) -> dict:
        """Hourly token lifecycle. Returns counts only; nothing is logged per token.

        1. Rugged tokens are deleted RUG_DELETE_HOURS after the rug was recorded.
        2. Tokens with no trades for DEAD_TOKEN_MINUTES are marked dead.
        3. Dead tokens are deleted DEAD_DELETE_HOURS after being marked dead.
        4. Non-moving $100-$3k tokens (no trades for LOWCAP_IDLE_MINUTES) are
           deactivated first, then deleted LOWCAP_DELETE_HOURS later.
        """
        lowcap = (Config.LOWCAP_MIN_MC, Config.LOWCAP_MAX_MC)
        counts = {}
        async with self.pool.acquire() as conn:
            counts["rugged_deleted"] = await self._delete_archiving(
                conn,
                "rugged = TRUE AND rugged_at < NOW() - ($1::text || ' hours')::interval",
                str(Config.RUG_DELETE_HOURS),
            )
            counts["marked_dead"] = self._affected(await conn.execute("""
                UPDATE coins
                SET is_dead = TRUE, is_active = FALSE, dead_at = NOW(),
                    deactivated_at = COALESCE(deactivated_at, NOW()), updated_at = NOW()
                WHERE COALESCE(is_dead, FALSE) = FALSE
                  AND COALESCE(last_trade_at, created_at)
                      < NOW() - ($1::text || ' minutes')::interval
            """, str(Config.DEAD_TOKEN_MINUTES)))
            counts["dead_deleted"] = await self._delete_archiving(
                conn,
                "is_dead = TRUE AND dead_at < NOW() - ($1::text || ' hours')::interval",
                str(Config.DEAD_DELETE_HOURS),
            )
            counts["lowcap_deactivated"] = self._affected(await conn.execute("""
                UPDATE coins
                SET is_active = FALSE, deactivated_at = NOW(), updated_at = NOW()
                WHERE is_active = TRUE AND COALESCE(is_dead, FALSE) = FALSE
                  AND current_market_cap BETWEEN $2::numeric AND $3::numeric
                  AND COALESCE(last_trade_at, created_at)
                      < NOW() - ($1::text || ' minutes')::interval
            """, str(Config.LOWCAP_IDLE_MINUTES), lowcap[0], lowcap[1]))
            counts["lowcap_deleted"] = await self._delete_archiving(
                conn,
                "is_active = FALSE AND COALESCE(is_dead, FALSE) = FALSE "
                "AND current_market_cap BETWEEN $2::numeric AND $3::numeric "
                "AND deactivated_at < NOW() - ($1::text || ' hours')::interval",
                str(Config.LOWCAP_DELETE_HOURS), lowcap[0], lowcap[1],
            )
        return counts

    async def delete_coin_by_mint(self, mint_address: str) -> int:
        """Silently remove a coin row (duplicate hook). 0 when it does not exist."""
        async with self.pool.acquire() as conn:
            return self._affected(await conn.execute(
                "DELETE FROM coins WHERE mint_address = $1", mint_address
            ))

    async def cleanup_old_snapshots(self, hours: int = 48):
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                DELETE FROM coin_snapshots
                WHERE created_at < NOW() - ($1::text || ' hours')::interval
            """, str(hours))
            try:
                return int(result.split()[-1])
            except Exception:
                return 0

    async def get_coin_by_mint(self, mint_address: str):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM coins WHERE mint_address = $1", mint_address)

    async def get_recent_coins(self, limit: int = 10):
        async with self.pool.acquire() as conn:
            return await conn.fetch(f"""
                SELECT mint_address, program_type, name, symbol, image_url,
                       current_market_cap, first_seen_market_cap, score,
                       score_label, dev_wallet, red_flags, created_at,
                       alerted_low, alerted_medium, alerted_high,
                       last_reject_reason, launch_sol, rugged
                FROM coins
                WHERE {VALID_IDENTITY_SQL}
                ORDER BY created_at DESC
                LIMIT $1
            """, max(1, min(int(limit), 25)))

    async def get_top_coins(self, limit: int = 10):
        """Highest-scoring tokens; ties broken by the best market cap seen."""
        async with self.pool.acquire() as conn:
            return await conn.fetch(f"""
                SELECT mint_address, program_type, name, symbol, image_url,
                       current_market_cap, first_seen_market_cap, ath_market_cap,
                       score, score_label, dev_wallet, created_at, launch_sol
                FROM coins
                WHERE {VALID_IDENTITY_SQL} AND COALESCE(score, 0) > 0
                ORDER BY score DESC,
                         GREATEST(COALESCE(ath_market_cap, 0),
                                  COALESCE(current_market_cap, 0)) DESC,
                         created_at DESC
                LIMIT $1
            """, max(1, min(int(limit), 25)))

    async def get_recent_milestones(self, limit: int = 10):
        """Most recent milestone hits, one row per coin showing its best multiple."""
        async with self.pool.acquire() as conn:
            return await conn.fetch(f"""
                SELECT * FROM (
                    SELECT DISTINCT ON (m.coin_id)
                           m.multiplier, m.market_cap_at_milestone,
                           m.created_at AS hit_at,
                           c.mint_address, c.name, c.symbol, c.launch_sol,
                           c.first_seen_market_cap
                    FROM milestones m
                    JOIN coins c ON c.id = m.coin_id
                    WHERE {identity_sql('c.')}
                    ORDER BY m.coin_id, m.multiplier DESC
                ) best
                ORDER BY hit_at DESC
                LIMIT $1
            """, max(1, min(int(limit), 25)))

    async def get_coin_ath(self, coin_id: int):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT GREATEST(
                    COALESCE((SELECT ath_market_cap FROM coins WHERE id = $1), 0),
                    COALESCE((SELECT MAX(market_cap) FROM coin_snapshots WHERE coin_id = $1), 0)
                ) AS ath
            """, coin_id)
            ath = row["ath"] if row else None
            return float(ath) if ath else None

    async def set_rugged(self, mint_address: str, reason: str) -> bool:
        """Record rug evidence. Returns False when the coin row does not exist yet."""
        async with self.pool.acquire() as conn:
            result = await conn.execute("""
                UPDATE coins SET rugged = TRUE, rug_reason = $2,
                    rugged_at = COALESCE(rugged_at, NOW()), updated_at = NOW()
                WHERE mint_address = $1
            """, mint_address, (reason or "")[:200])
            return result.endswith(" 1")

    async def get_dev_reputation(self, dev_wallet: str):
        """Launched/rugged counts for a dev: live rows plus the archived totals of
        rows the lifecycle cleanup already deleted."""
        if not dev_wallet:
            return None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(f"""
                SELECT COUNT(*) AS launched,
                       COUNT(*) FILTER (WHERE rugged) AS rugged
                FROM coins
                WHERE dev_wallet = $1 AND {VALID_IDENTITY_SQL}
            """, dev_wallet)
            archived = await conn.fetchrow(
                "SELECT launched, rugged FROM dev_stats WHERE dev_wallet = $1", dev_wallet
            )
        launched = int(row["launched"] or 0) if row else 0
        rugged = int(row["rugged"] or 0) if row else 0
        if archived:
            launched += int(archived["launched"] or 0)
            rugged += int(archived["rugged"] or 0)
        if not launched:
            return None
        return {"launched": launched, "rugged": rugged}

    async def get_pvp_siblings(self, mint_address: str, name: str, symbol: str,
                               around, limit: int = 10, hours: int = 24):
        """Other tokens with the same name or ticker launched within `hours` of `around`."""
        if not name and not symbol:
            return []
        async with self.pool.acquire() as conn:
            return await conn.fetch(f"""
                SELECT mint_address, name, symbol, current_market_cap,
                       first_seen_market_cap, created_at
                FROM coins
                WHERE mint_address != $1
                  AND (LOWER(BTRIM(name)) = LOWER(BTRIM($2::text))
                       OR LOWER(BTRIM(symbol)) = LOWER(BTRIM($3::text)))
                  AND created_at BETWEEN $4::timestamptz - ($5::text || ' hours')::interval
                                     AND $4::timestamptz + ($5::text || ' hours')::interval
                  AND {VALID_IDENTITY_SQL}
                ORDER BY created_at DESC
                LIMIT $6
            """, mint_address, name or "", symbol or "", around,
                str(hours), max(1, min(int(limit), 10)))

    async def find_recent_duplicate(self, name: str, symbol: str,
                                    exclude_mint: str, hours: int = 24):
        """Same name+symbol, different mint, seen in the last `hours`.
        Used to detect copycat/vamp tokens."""
        if not name or not symbol:
            return None
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("""
                SELECT id, mint_address, name, symbol, created_at
                FROM coins
                WHERE LOWER(name) = LOWER($1)
                  AND LOWER(symbol) = LOWER($2)
                  AND mint_address != $3
                  AND created_at > NOW() - ($4::text || ' hours')::interval
                ORDER BY created_at DESC
                LIMIT 1
            """, name, symbol, exclude_mint, str(hours))

    async def wallet_buy_exists(self, signature: str) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM wallet_buys WHERE signature = $1", signature
            )
            return row is not None

    async def insert_wallet_buy(self, e: dict):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO wallet_buys (
                    wallet, mint, signature, sol_spent, tokens_received, timestamp, slot
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (signature) DO NOTHING
            """,
                e.get("wallet"), e.get("mint"), e.get("signature"),
                e.get("sol_spent"), e.get("tokens_received"),
                e.get("timestamp"), e.get("slot"),
            )

    async def get_wallet_buys_for_mint(self, mint: str, minutes: int = 30):
        async with self.pool.acquire() as conn:
            cutoff = int(time.time()) - (minutes * 60)
            rows = await conn.fetch("""
                SELECT wallet, mint, signature, sol_spent, tokens_received, timestamp, slot
                FROM wallet_buys
                WHERE mint = $1 AND timestamp >= $2
                ORDER BY timestamp ASC
            """, mint, cutoff)
            return [dict(r) for r in rows]