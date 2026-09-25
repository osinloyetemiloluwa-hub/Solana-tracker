import time
import asyncpg
from config import Config


class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            dsn=Config.DATABASE_URL,
            min_size=1,
            max_size=10,
            command_timeout=60,
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
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    last_tracked_at TIMESTAMPTZ DEFAULT NOW(),
                    is_active BOOLEAN DEFAULT TRUE,
                    hit_milestone BOOLEAN DEFAULT FALSE
                )
            """)
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS image_url TEXT")
            await conn.execute("ALTER TABLE coins ADD COLUMN IF NOT EXISTS red_flags TEXT")
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

    async def set_alert_channel(self, guild_id: int, channel_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO guild_settings (guild_id, alert_channel_id)
                VALUES ($1, $2)
                ON CONFLICT (guild_id)
                DO UPDATE SET alert_channel_id = $2, updated_at = NOW()
            """, guild_id, channel_id)

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
                AND LOWER(address) != LOWER($2::text)
            """, str(days), Config.DEFAULT_WALLET)
            try:
                return int(result.split()[-1])
            except Exception:
                return 0

    async def get_state(self, key: str):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT value FROM system_state WHERE key = $1", key
            )
            return row["value"] if row else None

    async def set_state(self, key: str, value: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO system_state (key, value, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (key)
                DO UPDATE SET value = $2, updated_at = NOW()
            """, key, value)

    async def delete_state(self, key: str):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM system_state WHERE key = $1", key)

    async def coin_exists(self, mint_address: str):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT id FROM coins WHERE mint_address = $1", mint_address)
            return row is not None

    async def insert_coin(self, data: dict):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO coins (
                    mint_address, program_type, name, symbol, uri, image_url,
                    first_seen_price, first_seen_market_cap,
                    score, score_label, guild_id, dev_wallet
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (mint_address)
                DO UPDATE SET
                    name = COALESCE($3, coins.name),
                    symbol = COALESCE($4, coins.symbol),
                    uri = COALESCE($5, coins.uri),
                    image_url = COALESCE($6, coins.image_url),
                    score = GREATEST(coins.score, $9),
                    score_label = CASE
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
                data.get("score_label", "low"),
                data.get("guild_id"),
                data.get("dev_wallet"),
            )
            return row["id"]

    async def set_red_flags(self, coin_id: int, flags: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE coins SET red_flags = $2, updated_at = NOW() WHERE id = $1",
                coin_id, flags,
            )

    async def set_first_seen(self, coin_id: int, price: float, market_cap: float, image_url: str = None):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE coins
                SET first_seen_price = CASE
                        WHEN first_seen_price IS NULL AND $2::numeric IS NOT NULL THEN $2::numeric
                        ELSE first_seen_price
                    END,
                    first_seen_market_cap = CASE
                        WHEN first_seen_market_cap IS NULL AND $3::numeric IS NOT NULL THEN $3::numeric
                        ELSE first_seen_market_cap
                    END,
                    image_url = COALESCE(image_url, $4::text),
                    updated_at = NOW()
                WHERE id = $1
            """, coin_id, price, market_cap, image_url)

    async def set_coin_image(self, coin_id: int, image_url: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE coins SET image_url = $2, updated_at = NOW() WHERE id = $1",
                coin_id, image_url,
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
                WHERE is_active = TRUE
                AND last_tracked_at > NOW() - ($1::text || ' hours')::interval
            """, str(Config.TRACKING_WINDOW_HOURS))

    async def update_coin_snapshot(self, coin_id: int, price: float, market_cap: float):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE coins
                SET current_price = $2::numeric, current_market_cap = $3::numeric,
                    updated_at = NOW(), last_tracked_at = NOW()
                WHERE id = $1
            """, coin_id, price, market_cap)

    async def insert_snapshot(self, coin_id: int, price: float, market_cap: float):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO coin_snapshots (coin_id, price, market_cap)
                VALUES ($1, $2, $3)
            """, coin_id, price, market_cap)

    async def milestone_exists(self, coin_id: int, multiplier: float):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id FROM milestones WHERE coin_id = $1::integer AND multiplier = $2::numeric
            """, coin_id, multiplier)
            return row is not None

    async def insert_milestone(self, coin_id: int, multiplier: float, price: float, market_cap: float):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO milestones (coin_id, multiplier, price_at_milestone, market_cap_at_milestone)
                VALUES ($1::integer, $2::numeric, $3::numeric, $4::numeric)
                ON CONFLICT (coin_id, multiplier) DO NOTHING
            """, coin_id, multiplier, price, market_cap)

    async def mark_coin_milestone_hit(self, coin_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE coins SET hit_milestone = TRUE, updated_at = NOW() WHERE id = $1",
                coin_id,
            )

    async def deactivate_coin(self, coin_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE coins SET is_active = FALSE, updated_at = NOW() WHERE id = $1",
                coin_id,
            )

    async def cleanup_old_coins(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM coins
                WHERE is_active = FALSE
                AND hit_milestone = FALSE
                AND created_at < NOW() - INTERVAL '7 days'
            """)

    async def get_coin_by_mint(self, mint_address: str):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM coins WHERE mint_address = $1", mint_address)

    async def add_dev_wallet(self, coin_id: int, wallet_address: str, label: str = None):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO dev_wallets (coin_id, wallet_address, label)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
            """, coin_id, wallet_address, label)

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