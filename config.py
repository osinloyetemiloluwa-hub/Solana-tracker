import os
from dotenv import load_dotenv

load_dotenv()


def _int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return int(default)


def _float(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return float(default)


def _bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


class Config:
    DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
    DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
    HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip()
    SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "").strip()
    SOLANA_RPC_FALLBACK_URL = os.getenv("SOLANA_RPC_FALLBACK_URL", "").strip()
    HELIUS_WEBHOOK_SECRET = os.getenv("HELIUS_WEBHOOK_SECRET", "").strip()

    DEFAULT_WALLET = os.getenv(
        "DEFAULT_WALLET", "7BNaxx6KdUYrjACNQZ9He26NBFoFxujQMAfNLnArLGH5"
    ).strip()

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

    # Conservative RPC budgeting. Public RPC remains usable without any API key;
    # paid/deep enrichment is already disabled automatically when Helius is absent.
    RPC_MIN_GAP_SECONDS = max(0.2, _float("RPC_MIN_GAP_SECONDS", "1.0"))
    RPC_DAILY_REQUEST_BUDGET = max(100, _int("RPC_DAILY_REQUEST_BUDGET", "2000"))

    SCAN_INTERVAL_SECONDS = max(15, _int("SCAN_INTERVAL_SECONDS", "30"))
    SCANNER_QUEUE_MAXSIZE = max(20, _int("SCANNER_QUEUE_MAXSIZE", "250"))
    SCANNER_WORKERS = max(1, min(8, _int("SCANNER_WORKERS", "3")))
    TRACKING_MAX_COINS_PER_CYCLE = max(10, _int("TRACKING_MAX_COINS_PER_CYCLE", "60"))
    MARKET_CACHE_TTL_SECONDS = max(0, _int("MARKET_CACHE_TTL_SECONDS", "8"))
    DEFAULT_SCAN_MODE = os.getenv("DEFAULT_SCAN_MODE", "medium").strip().lower()

    PUMP_FUN_PROGRAM = os.getenv(
        "PUMP_FUN_PROGRAM", "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
    )
    RAYDIUM_AMM_PROGRAM = os.getenv(
        "RAYDIUM_AMM_PROGRAM", "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
    )
    PUMPSWAP_PROGRAM = os.getenv(
        "PUMPSWAP_PROGRAM", "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
    )

    REQUEST_TIMEOUT = max(5, _int("REQUEST_TIMEOUT", "20"))
    MAX_RETRIES = max(1, _int("MAX_RETRIES", "3"))

    DISCORD_SEND_DELAY = max(0.5, _float("DISCORD_SEND_DELAY", "1.5"))
    DISCORD_QUEUE_MAXSIZE = max(50, _int("DISCORD_QUEUE_MAXSIZE", "500"))
    DISCORD_MAX_RETRIES = max(1, _int("DISCORD_MAX_RETRIES", "5"))
    DISCORD_LOGIN_RETRY_INITIAL = max(30, _int("DISCORD_LOGIN_RETRY_INITIAL", "120"))
    DISCORD_LOGIN_RETRY_MAX = max(120, _int("DISCORD_LOGIN_RETRY_MAX", "600"))
    PORT = max(1, _int("PORT", "10000"))
    WIPE_GUILD_COMMANDS = os.getenv("WIPE_GUILD_COMMANDS", "false").lower() == "true"

    TOKEN_ALERT_COOLDOWN_SECONDS = max(3, _int("TOKEN_ALERT_COOLDOWN_SECONDS", "10"))

    MILESTONE_MULTIPLIERS = [2.0, 3.0, 5.0, 10.0, 25.0, 50.0, 100.0]
    TRACKING_WINDOW_HOURS = max(1, _int("TRACKING_WINDOW_HOURS", "24"))
    DEXSCREENER_FAILURE_THRESHOLD = max(1, _int("DEXSCREENER_FAILURE_THRESHOLD", "3"))

    # Pre-send verification
    PRE_ALERT_VERIFY_SECONDS = max(0, _int("PRE_ALERT_VERIFY_SECONDS", "5"))
    PRE_ALERT_MAX_MC_DROP_PCT = _float("PRE_ALERT_MAX_MC_DROP_PCT", "25.0")
    ALERT_RETRY_BASE_SECONDS = max(15, _int("ALERT_RETRY_BASE_SECONDS", "60"))
    ALERT_RETRY_MAX_SECONDS = max(60, _int("ALERT_RETRY_MAX_SECONDS", "900"))

    # Free PumpPortal trade-flow checks. Sparse feeds never cause rejection;
    # ratio/net-flow filters activate only after enough observed trades.
    FLOW_MAX_TRACKED_MINTS = max(50, _int("FLOW_MAX_TRACKED_MINTS", "500"))
    FLOW_TRADE_TTL_SECONDS = max(60, _int("FLOW_TRADE_TTL_SECONDS", "300"))
    FLOW_MIN_TRADES_FOR_FILTER = max(3, _int("FLOW_MIN_TRADES_FOR_FILTER", "5"))
    FLOW_MIN_BUY_RATIO = min(1.0, max(0.0, _float("FLOW_MIN_BUY_RATIO", "0.35")))
    FLOW_MAX_MC_DROP_PCT = min(100.0, max(1.0, _float("FLOW_MAX_MC_DROP_PCT", "50.0")))

    # Dead-token / wash-trade filter (1h volume vs MC, and 1h buys/sells)
    FILTER_MIN_VOL_MC_RATIO = _float("FILTER_MIN_VOL_MC_RATIO", "0.25")
    FILTER_MIN_BUY_SELL_RATIO = _float("FILTER_MIN_BUY_SELL_RATIO", "0.8")

    # Chase guard: if it already pumped more than this % in 1h, skip.
    CHASE_MAX_H1_PCT = _float("CHASE_MAX_H1_PCT", "200.0")

    MIN_MARKET_CAP = max(1000, _int("MIN_MARKET_CAP", "5000"))
    LIQUIDITY_FLOOR = max(500, _int("LIQUIDITY_FLOOR", "1500"))

    # UPPER bound: don't alert on tokens already too big to be an early entry.
    MAX_MARKET_CAP_FOR_ALERT = max(50000, _int("MAX_MARKET_CAP_FOR_ALERT", "500000"))

    FAT_BUNDLE_MC_THRESHOLD = max(5000, _int("FAT_BUNDLE_MC_THRESHOLD", "15000"))
    FAT_BUNDLE_AGE_SECONDS = max(30, _int("FAT_BUNDLE_AGE_SECONDS", "120"))

    TIER_NEW_PAIRS_MAX_AGE_SECONDS = max(60, _int("TIER_NEW_PAIRS_MAX_AGE_SECONDS", "600"))
    TIER_NEW_PAIRS_MIN_MC = max(1000, _int("TIER_NEW_PAIRS_MIN_MC", "5000"))
    TIER_NEW_PAIRS_MIN_FEES_SOL = _float("TIER_NEW_PAIRS_MIN_FEES_SOL", "0.2")

    TIER_FINAL_STRETCH_MIN_VISITORS = max(1, _int("TIER_FINAL_STRETCH_MIN_VISITORS", "40"))
    TIER_FINAL_STRETCH_MIN_FEES_SOL = _float("TIER_FINAL_STRETCH_MIN_FEES_SOL", "1.0")

    TIER_MIGRATED_MIN_VISITORS = max(1, _int("TIER_MIGRATED_MIN_VISITORS", "100"))
    TIER_MIGRATED_MIN_MC = max(1000, _int("TIER_MIGRATED_MIN_MC", "20000"))
    TIER_MIGRATED_MIN_FEES_SOL = _float("TIER_MIGRATED_MIN_FEES_SOL", "5.0")

    HELIUS_DEEP_SCAN_MIN_MC = max(0, _int("HELIUS_DEEP_SCAN_MIN_MC", "6000"))
    HELIUS_DEEP_SCAN_MAX_MC = max(1000, _int("HELIUS_DEEP_SCAN_MAX_MC", "48000"))
    EARLY_BUYER_LOOKBACK = max(5, _int("EARLY_BUYER_LOOKBACK", "15"))
    SIMILAR_AMOUNT_TOLERANCE = max(0.01, _float("SIMILAR_AMOUNT_TOLERANCE", "0.02"))
    SEQUENTIAL_GAP_STD_MAX = max(0.5, _float("SEQUENTIAL_GAP_STD_MAX", "1.5"))

    IDLE_WALLET_MAX_DAYS = max(1, _int("IDLE_WALLET_MAX_DAYS", "7"))

    WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
    HELIUS_WEBHOOK_ID = os.getenv("HELIUS_WEBHOOK_ID", "").strip()
    WALLET_BUY_WINDOW_MINUTES = max(1, _int("WALLET_BUY_WINDOW_MINUTES", "30"))
    WALLET_BUY_FAST_WINDOW_SECONDS = max(10, _int("WALLET_BUY_FAST_WINDOW_SECONDS", "60"))

    MADEONSOL_API_URL = os.getenv("MADEONSOL_API_URL", "").strip()
    MADEONSOL_API_KEY = os.getenv("MADEONSOL_API_KEY", "").strip()
    MADEONSOL_DAILY_QUOTA = max(50, _int("MADEONSOL_DAILY_QUOTA", "200"))
    MADEONSOL_RATE_PER_MINUTE = max(1, _int("MADEONSOL_RATE_PER_MINUTE", "10"))

    ENRICHER_CACHE_TTL_SECONDS = max(30, _int("ENRICHER_CACHE_TTL_SECONDS", "60"))
    ENRICHER_CACHE_MAX_ENTRIES = max(100, _int("ENRICHER_CACHE_MAX_ENTRIES", "500"))
    SNAPSHOT_RETENTION_HOURS = max(24, _int("SNAPSHOT_RETENTION_HOURS", "48"))

    ENABLE_RAYDIUM_DETECTION = os.getenv("ENABLE_RAYDIUM_DETECTION", "true").lower() == "true"
    RAYDIUM_POLL_INTERVAL_SECONDS = max(30, _int("RAYDIUM_POLL_INTERVAL_SECONDS", "60"))

    FILTER_MIN_LIQUIDITY = max(500, _int("FILTER_MIN_LIQUIDITY", "1500"))
    FILTER_MIN_VOLUME = max(1000, _int("FILTER_MIN_VOLUME", "3000"))
    FILTER_MIN_MARKET_CAP = max(1000, _int("FILTER_MIN_MARKET_CAP", "5000"))
    FILTER_MIN_HOLDERS = max(5, _int("FILTER_MIN_HOLDERS", "50"))
    FILTER_MAX_TOP10_PCT = _float("FILTER_MAX_TOP10_PCT", "60.0")
    FILTER_MIN_DEV_MIGRATIONS = max(0, _int("FILTER_MIN_DEV_MIGRATIONS", "1"))
    FILTER_MIN_PRO_TRADERS = max(0, _int("FILTER_MIN_PRO_TRADERS", "10"))
    FILTER_MAX_DEV_HOLDING = _float("FILTER_MAX_DEV_HOLDING", "3.0")
    FILTER_MAX_SNIPERS = _float("FILTER_MAX_SNIPERS", "20.0")
    FILTER_MAX_INSIDERS = _float("FILTER_MAX_INSIDERS", "20.0")
    FILTER_MAX_BUNDLES = _float("FILTER_MAX_BUNDLES", "25.0")

    @classmethod
    def validate(cls):
        missing = []
        if not cls.DISCORD_TOKEN:
            missing.append("DISCORD_TOKEN")
        if not cls.DATABASE_URL:
            missing.append("DATABASE_URL")
        if missing:
            raise ValueError(f"Missing required env vars: {', '.join(missing)}")