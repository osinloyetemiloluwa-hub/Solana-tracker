from config import Config


class ScoringEngine:
    def __init__(self):
        self.weights = {
            "watched_wallet_buy": 35,
            "smart_money_consensus": 50,
            "bundle_detected": -80,
            "fat_bundle": -70,
            "bot_farm_detected": -40,
            "liquidity_above_10k": 10,
            "liquidity_above_50k": 20,
            "volume_above_25k": 15,
            "fresh_creation": 15,
            "dev_tracked": 5,
            "red_flag_similar_amounts": -30,
            "red_flag_same_slot_bundle": -50,
            "red_flag_no_organic_growth": -40,
            "red_flag_sequential_buys": -25,
            "red_flag_cluster_concentration": -45,
            "red_flag_low_views_high_holders": -35,
        }

    def calculate_score(self, token_data: dict):
        liquidity = token_data.get("liquidity")
        if liquidity is None:
            liquidity = 0
        if liquidity < Config.FILTER_MIN_LIQUIDITY:
            return {"score": 0, "label": "low",
                    "reasons": [f"FAILED: LP ${liquidity:,.0f} < ${Config.FILTER_MIN_LIQUIDITY:,}"]}

        volume = token_data.get("volume_24h")
        if volume is None:
            volume = 0
        if volume < Config.FILTER_MIN_VOLUME:
            return {"score": 0, "label": "low",
                    "reasons": [f"FAILED: Vol ${volume:,.0f} < ${Config.FILTER_MIN_VOLUME:,}"]}

        market_cap = token_data.get("market_cap")
        if market_cap is None:
            market_cap = 0
        if market_cap < Config.FILTER_MIN_MARKET_CAP:
            return {"score": 0, "label": "low",
                    "reasons": [f"FAILED: MC ${market_cap:,.0f} < ${Config.FILTER_MIN_MARKET_CAP:,}"]}

        # UPPER BOUND: reject tokens already too big to be an early entry.
        if market_cap > Config.MAX_MARKET_CAP_FOR_ALERT:
            return {"score": 0, "label": "low",
                    "reasons": [f"FAILED: MC ${market_cap:,.0f} > ${Config.MAX_MARKET_CAP_FOR_ALERT:,} (too late)"]}

        holders = token_data.get("holder_count")
        if holders is not None and holders < Config.FILTER_MIN_HOLDERS:
            return {"score": 0, "label": "low",
                    "reasons": [f"FAILED: Holders {holders} < {Config.FILTER_MIN_HOLDERS}"]}

        top10 = token_data.get("top10_holder_pct")
        if top10 is not None and top10 > Config.FILTER_MAX_TOP10_PCT:
            return {"score": 0, "label": "low",
                    "reasons": [f"FAILED: Top-10 {top10:.1f}% > {Config.FILTER_MAX_TOP10_PCT:.0f}%"]}

        dev_bonded = token_data.get("deployer_bonded")
        if dev_bonded is not None and dev_bonded < Config.FILTER_MIN_DEV_MIGRATIONS:
            return {"score": 0, "label": "low",
                    "reasons": [f"FAILED: Dev migrations {dev_bonded} < {Config.FILTER_MIN_DEV_MIGRATIONS}"]}

        kols = token_data.get("kol_buying")
        if kols is not None and kols < Config.FILTER_MIN_PRO_TRADERS:
            return {"score": 0, "label": "low",
                    "reasons": [f"FAILED: Pro traders {kols} < {Config.FILTER_MIN_PRO_TRADERS}"]}

        if token_data.get("fat_bundle"):
            return {"score": 0, "label": "low",
                    "reasons": ["FAILED: Fat bundle detected"]}

        score = 0
        reasons = []

        if token_data.get("bundle_detected"):
            score += self.weights["bundle_detected"]
            reasons.append("BUNDLE DETECTED")

        if token_data.get("bot_farm"):
            score += self.weights["bot_farm_detected"]
            reasons.append("Bot farm detected")

        for flag in token_data.get("red_flags", []):
            key = f"red_flag_{flag}"
            if key in self.weights:
                score += self.weights[key]
                reasons.append(flag.replace("_", " ").title())

        sm_count = token_data.get("smart_money_count", 0) or 0
        if sm_count >= 3:
            score += self.weights["smart_money_consensus"]
            reasons.append(f"Smart Money Consensus ({sm_count} wallets)")
        elif sm_count >= 1:
            score += self.weights["watched_wallet_buy"]
            reasons.append(f"Watched wallet involved ({sm_count})")

        if liquidity >= 50000:
            score += self.weights["liquidity_above_50k"]
            reasons.append(f"Strong liquidity (${liquidity:,.0f})")
        elif liquidity >= 10000:
            score += self.weights["liquidity_above_10k"]
            reasons.append(f"Liquidity ${liquidity:,.0f}")

        if volume >= 25000:
            score += self.weights["volume_above_25k"]
            reasons.append(f"Volume ${volume:,.0f}")

        if token_data.get("program_type") in ["pumpfun", "raydium", "pumpswap"]:
            score += self.weights["fresh_creation"]
            reasons.append("New token detected")

        if token_data.get("dev_wallet"):
            score += self.weights["dev_tracked"]
            reasons.append("Dev wallet identified")

        score = max(0, min(score, 100))

        if score >= 70:
            label = "high"
        elif score >= 40:
            label = "medium"
        else:
            label = "low"

        return {"score": score, "label": label, "reasons": reasons}

    def should_alert(self, score_label: str, mode: str):
        if mode == "all":
            return True
        if mode == "medium":
            return score_label in ["medium", "high"]
        if mode == "high":
            return score_label == "high"
        return False