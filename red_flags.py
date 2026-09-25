import statistics


class RedFlagDetector:
    """Pure functions on pre-fetched data. No RPC calls here."""

    @staticmethod
    def similar_buy_amounts(buy_amounts, tolerance=0.02):
        if len(buy_amounts) < 3:
            return False
        for i, amount in enumerate(buy_amounts):
            matches = sum(
                1 for a in buy_amounts[i + 1:]
                if abs(a - amount) / max(amount, 1e-9) <= tolerance
            )
            if matches >= 2:
                return True
        return False

    @staticmethod
    def same_slot_bundle(slot_map, min_wallets=3):
        return any(len(wallets) >= min_wallets for wallets in slot_map.values())

    @staticmethod
    def no_organic_growth(buy_count, sell_count, min_sell_ratio=0.05):
        total = buy_count + sell_count
        if total < 10:
            return False
        return (sell_count / total) < min_sell_ratio

    @staticmethod
    def sequential_buys(timestamps, max_std=1.5):
        if len(timestamps) < 5:
            return False
        gaps = [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]
        if len(gaps) < 3:
            return False
        return statistics.pstdev(gaps) < max_std

    @staticmethod
    def fat_bundle(age_seconds, market_cap, mc_threshold=15000, age_threshold=120):
        if age_seconds is None or market_cap is None:
            return False
        return age_seconds <= age_threshold and market_cap >= mc_threshold

    @staticmethod
    def cluster_concentration(cluster_pct, threshold=0.5):
        return cluster_pct > threshold

    @staticmethod
    def low_views_high_holders(holder_count, unique_buyers):
        if unique_buyers == 0:
            return False
        return holder_count / unique_buyers > 5.0