from config import Config


class TierFilter:
    """
    New Pairs:     age <= 2 min, MC >= 10K, fees >= 0.2 SOL
    Final Stretch: bonding curve >= 80%, visitors >= 40, fees >= 1 SOL
    Migrated:      PumpSwap, visitors >= 100, MC >= 20K, fees >= 5 SOL
    """

    @staticmethod
    def fees_from_volume_sol(volume_usd, sol_price_usd=150.0):
        if not volume_usd or sol_price_usd <= 0:
            return 0.0
        return (volume_usd / sol_price_usd) * 0.01

    @staticmethod
    def classify(token_data):
        age = token_data.get("age_seconds")
        mc = token_data.get("market_cap") or 0
        volume = token_data.get("volume_24h") or 0
        liquidity = token_data.get("liquidity") or 0
        visitors = token_data.get("recent_visitors") or 0
        bonding_progress = token_data.get("bonding_curve_progress") or 0.0
        program = token_data.get("program_type", "")

        fees_sol = TierFilter.fees_from_volume_sol(volume)

        if program == "pumpswap" or token_data.get("is_migrated"):
            if (visitors >= Config.TIER_MIGRATED_MIN_VISITORS
                    and mc >= Config.TIER_MIGRATED_MIN_MC
                    and fees_sol >= Config.TIER_MIGRATED_MIN_FEES_SOL):
                return "migrated"

        if bonding_progress >= 0.80:
            if (visitors >= Config.TIER_FINAL_STRETCH_MIN_VISITORS
                    and fees_sol >= Config.TIER_FINAL_STRETCH_MIN_FEES_SOL):
                return "final_stretch"

        if (age is not None
                and age <= Config.TIER_NEW_PAIRS_MAX_AGE_SECONDS
                and mc >= Config.TIER_NEW_PAIRS_MIN_MC
                and fees_sol >= Config.TIER_NEW_PAIRS_MIN_FEES_SOL
                and liquidity >= Config.LIQUIDITY_FLOOR):
            return "new_pairs"

        return None