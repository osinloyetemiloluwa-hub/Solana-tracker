"""Runtime configuration.

Only values a deployment genuinely needs to change are read from the
environment (see README "Environment variables"). Everything else is a
constant here, or a value computed from one of those variables. Per-guild
alert filters live in the database (guild_settings), never in env vars.
"""
import os
from dotenv import load_dotenv

load_dotenv()

ALLOWED_SCAN_MODES = ("all", "medium", "high")


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


class Config:
    # ------------------------------------------------------------------
    # Environment variables (16)
    # ------------------------------------------------------------------
    DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
    DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
    HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip()
    HELIUS_WEBHOOK_SECRET = os.getenv("HELIUS_WEBHOOK_SECRET", "").strip()
    WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
    SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "").strip()
    SOLANA_RPC_FALLBACK_URL = os.getenv("SOLANA_RPC_FALLBACK_URL", "").strip()
    MADEONSOL_API_KEY = os.getenv("MADEONSOL_API_KEY", "").strip()
    PORT = max(1, _int("PORT", 10000))
    DEFAULT_SCAN_MODE = os.getenv("DEFAULT_SCAN_MODE", "medium").strip().lower()
    SCAN_INTERVAL_SECONDS = max(15, _int("SCAN_INTERVAL_SECONDS", 30))
    TRACKING_WINDOW_HOURS = max(1, _int("TRACKING_WINDOW_HOURS", 24))
    RPC_DAILY_REQUEST_BUDGET = max(100, _int("RPC_DAILY_REQUEST_BUDGET", 2000))
    RPC_MIN_GAP_SECONDS = max(0.2, _float("RPC_MIN_GAP_SECONDS", 1.0))
    DEAD_TOKEN_MINUTES = max(5, _int("DEAD_TOKEN_MINUTES", 60))
    DEAD_DELETE_HOURS = max(1, _int("DEAD_DELETE_HOURS", 24))

    # ------------------------------------------------------------------
    # Computed from the variables above
    # ------------------------------------------------------------------
    # Market-data refresh sweep of every tracked token (batched, 30/request).
    REFRESH_INTERVAL_SECONDS = max(5, SCAN_INTERVAL_SECONDS // 3)
    # Rugged tokens are removed on a shorter clock than ordinary dead tokens.
    RUG_DELETE_HOURS = max(1, DEAD_DELETE_HOURS // 4)
    # Non-moving $100-$3k coins: deactivate after a third of the dead window,
    # then delete after a third of the dead-delete window.
    LOWCAP_IDLE_MINUTES = max(5, DEAD_TOKEN_MINUTES // 3)
    LOWCAP_DELETE_HOURS = max(1, DEAD_DELETE_HOURS // 3)

    @classmethod
    def solana_rpc_url(cls) -> str:
        if cls.SOLANA_RPC_URL:
            return cls.SOLANA_RPC_URL
        if cls.HELIUS_API_KEY:
            return f"https://mainnet.helius-rpc.com/?api-key={cls.HELIUS_API_KEY}"
        return "https://api.mainnet.solana.com"

    @classmethod
    def solana_rpc_fallback_url(cls) -> str:
        if cls.SOLANA_RPC_FALLBACK_URL:
            return cls.SOLANA_RPC_FALLBACK_URL
        if not cls.SOLANA_RPC_URL and not cls.HELIUS_API_KEY:
            return "https://solana-rpc.publicnode.com"
        return ""

    # ------------------------------------------------------------------
    # Constants (no environment override)
    # ------------------------------------------------------------------
    PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

    SCANNER_QUEUE_MAXSIZE = 250
    SCANNER_WORKERS = 3
    MARKET_CACHE_TTL_SECONDS = 8
    REQUEST_TIMEOUT = 20
    MAX_RETRIES = 3

    DISCORD_SEND_DELAY = 1.5
    DISCORD_QUEUE_MAXSIZE = 500
    DISCORD_MAX_RETRIES = 5
    DISCORD_LOGIN_RETRY_INITIAL = 120
    DISCORD_LOGIN_RETRY_MAX = 600
    TOKEN_ALERT_COOLDOWN_SECONDS = 10

    DEXSCREENER_FAILURE_THRESHOLD = 3

    # Pre-send verification hold and retry/backoff (unchanged from 2.2).
    PRE_ALERT_VERIFY_SECONDS = 5
    PRE_ALERT_MAX_MC_DROP_PCT = 25.0
    ALERT_RETRY_BASE_SECONDS = 60
    ALERT_RETRY_MAX_SECONDS = 900

    # PumpPortal trade-flow checks (unchanged).
    FLOW_MAX_TRACKED_MINTS = 500
    FLOW_TRADE_TTL_SECONDS = 300
    FLOW_MIN_TRADES_FOR_FILTER = 5
    FLOW_MIN_BUY_RATIO = 0.35
    FLOW_MAX_MC_DROP_PCT = 50.0
    # Per-mint rolling trade buffer used for observed volume (bounded).
    FLOW_TRADE_BUFFER = 300
    # Non-dev buyers inside this many seconds of the create event are snipers.
    SNIPER_WINDOW_SECONDS = 3.0

    FILTER_MIN_VOL_MC_RATIO = 0.25
    FILTER_MIN_BUY_SELL_RATIO = 0.8
    CHASE_MAX_H1_PCT = 200.0
    LIQUIDITY_FLOOR = 1500

    FAT_BUNDLE_MC_THRESHOLD = 15000
    FAT_BUNDLE_AGE_SECONDS = 120

    TIER_NEW_PAIRS_MAX_AGE_SECONDS = 600
    TIER_NEW_PAIRS_MIN_MC = 5000
    TIER_NEW_PAIRS_MIN_FEES_SOL = 0.2
    TIER_FINAL_STRETCH_MIN_VISITORS = 40
    TIER_FINAL_STRETCH_MIN_FEES_SOL = 1.0
    TIER_MIGRATED_MIN_VISITORS = 100
    TIER_MIGRATED_MIN_MC = 20000
    TIER_MIGRATED_MIN_FEES_SOL = 5.0

    HELIUS_DEEP_SCAN_MIN_MC = 6000
    HELIUS_DEEP_SCAN_MAX_MC = 48000
    EARLY_BUYER_LOOKBACK = 15
    SIMILAR_AMOUNT_TOLERANCE = 0.02
    SEQUENTIAL_GAP_STD_MAX = 1.5

    IDLE_WALLET_MAX_DAYS = 7
    WALLET_BUY_WINDOW_MINUTES = 30
    WALLET_BUY_FAST_WINDOW_SECONDS = 60

    MADEONSOL_DAILY_QUOTA = 200
    MADEONSOL_RATE_PER_MINUTE = 10

    ENRICHER_CACHE_TTL_SECONDS = 60
    ENRICHER_CACHE_MAX_ENTRIES = 500
    BUYER_ANALYSIS_CACHE_TTL_SECONDS = 600
    BUYER_ANALYSIS_CACHE_MAX_ENTRIES = 1000
    SNAPSHOT_RETENTION_HOURS = 48

    # DexScreener discovery feeds: each feed polls on its own cadence.
    FEED_POLL_SECONDS = 3.0
    FEED_BACKOFF_INITIAL_SECONDS = 10.0
    FEED_BACKOFF_MAX_SECONDS = 120.0
    REFRESH_BATCH_SIZE = 30

    # Other discovery sources (poll for new pools, upsert mints, nothing deeper).
    POOL_POLL_INTERVAL_SECONDS = 60
    # PumpFun RPC detector runs only while PumpPortal has been silent this long.
    PUMPFUN_FALLBACK_SILENCE_SECONDS = 120
    PUMPFUN_FALLBACK_POLL_SECONDS = 60

    # Lifecycle thresholds.
    LOWCAP_MIN_MC = 100
    LOWCAP_MAX_MC = 3000
    DUPLICATE_DELETE_WINDOW_MINUTES = 10

    # Read-only API.
    API_RATE_LIMIT_PER_MINUTE = 60
    API_MAX_LIMIT = 100
    API_CACHE_TTL_SECONDS = 3

    @classmethod
    def validate(cls):
        missing = []
        if not cls.DISCORD_TOKEN:
            missing.append("DISCORD_TOKEN")
        if not cls.DATABASE_URL:
            missing.append("DATABASE_URL")
        if missing:
            raise ValueError(f"Missing required env vars: {', '.join(missing)}")
        if cls.DEFAULT_SCAN_MODE not in ALLOWED_SCAN_MODES:
            raise ValueError(
                f"DEFAULT_SCAN_MODE must be one of {', '.join(ALLOWED_SCAN_MODES)} "
                f"(got {cls.DEFAULT_SCAN_MODE!r})"
            )
