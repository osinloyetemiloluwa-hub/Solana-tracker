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
            "fresh_market": 15,
            "momentum_5m": 10,
            "activity_5m": 10,
            "dev_tracked": 5,
            "red_flag_similar_amounts": -30,
            "red_flag_same_slot_bundle": -50,
            "red_flag_no_organic_growth": -40,
            "red_flag_sequential_buys": -25,
            "red_flag_cluster_concentration": -45,
            "red_flag_low_views_high_holders": -35,
        }

    def calculate_score(self, token_data: dict):
        liquidity = float(token_data.get("liquidity") or 0)
        market_cap = float(token_data.get("market_cap") or 0)
        age = token_data.get("age_seconds")
        program = (token_data.get("program_type") or "").lower()

        # The only universal market gate: real market + enough liquidity.
        # MC is allowed to run above 500k; there is deliberately no upper cap.
        if liquidity < Config.FILTER_MIN_LIQUIDITY:
            return {"score": 0, "label": "low", "reasons": [
                f"FAILED: LP ${liquidity:,.0f} < ${Config.FILTER_MIN_LIQUIDITY:,}"
            ]}
        if market_cap < Config.FILTER_MIN_MARKET_CAP:
            return {"score": 0, "label": "low", "reasons": [
                f"FAILED: MC ${market_cap:,.0f} < ${Config.FILTER_MIN_MARKET_CAP:,}"
            ]}

        # Fresh launches must not be forced through mature-token metrics such as
        # 24h volume, 50 holders, pro-trader count or dev-history evidence.
        fresh = age is not None and age <= Config.TIER_NEW_PAIRS_MAX_AGE_SECONDS
        if fresh and program in {"pumpfun", "raydium", "pumpswap"}:
            return self._fresh_score(token_data, liquidity, market_cap)

        # Mature candidates retain quality gates, but missing optional analytics
        # are treated as unknown rather than automatically as zero.
        score = 25
        reasons = ["Valid market", f"Liquidity ${liquidity:,.0f}"]

        volume = float(token_data.get("volume_24h") or 0)
        if volume >= Config.FILTER_MIN_VOLUME:
            score += 10
            reasons.append(f"24h volume ${volume:,.0f}")
        elif volume > 0:
            score += 3
            reasons.append(f"Early volume ${volume:,.0f}")

        holders = token_data.get("holder_count")
        if holders is not None:
            if holders < Config.FILTER_MIN_HOLDERS:
                score -= 20
                reasons.append(f"Low holders ({holders})")
            else:
                score += 8
                reasons.append(f"Holders {holders}")

        top10 = token_data.get("top10_holder_pct")
        if top10 is not None:
            if top10 > Config.FILTER_MAX_TOP10_PCT:
                score -= 35
                reasons.append(f"Top-10 concentration {top10:.1f}%")
            else:
                score += 5

        dev_bonded = token_data.get("deployer_bonded")
        if dev_bonded is not None:
            if dev_bonded >= Config.FILTER_MIN_DEV_MIGRATIONS:
                score += 5
                reasons.append(f"Dev history {dev_bonded}")
            else:
                score -= 10
                reasons.append("No qualifying dev history")

        kols = token_data.get("kol_buying")
        if kols is not None:
            if kols >= Config.FILTER_MIN_PRO_TRADERS:
                score += 10
                reasons.append(f"Pro traders {kols}")
            else:
                score -= 8

        if liquidity >= 50000:
            score += self.weights["liquidity_above_50k"]
        elif liquidity >= 10000:
            score += self.weights["liquidity_above_10k"]

        if volume >= 25000:
            score += self.weights["volume_above_25k"]

        if token_data.get("bundle_detected"):
            score += self.weights["bundle_detected"]
            reasons.append("BUNDLE DETECTED")
        if token_data.get("fat_bundle"):
            score += self.weights["fat_bundle"]
            reasons.append("Fat bundle")
        if token_data.get("bot_farm"):
            score += self.weights["bot_farm_detected"]
            reasons.append("Bot farm detected")

        for flag in token_data.get("red_flags", []):
            key = f"red_flag_{flag}"
            if key in self.weights:
                score += self.weights[key]
                reasons.append(flag.replace("_", " ").title())

        sm_count = int(token_data.get("smart_money_count") or 0)
        if sm_count >= 3:
            score += self.weights["smart_money_consensus"]
            reasons.append(f"Smart Money Consensus ({sm_count})")
        elif sm_count >= 1:
            score += self.weights["watched_wallet_buy"]
            reasons.append("Watched wallet involved")

        score = max(0, min(score, 100))
        label = "high" if score >= 70 else "medium" if score >= 40 else "low"
        return {"score": score, "label": label, "reasons": reasons}

    def _fresh_score(self, token_data, liquidity, market_cap):
        score = 45
        reasons = ["Fresh launch", "Valid market", f"Liquidity ${liquidity:,.0f}"]

        if liquidity >= 10000:
            score += 15
            reasons.append("Strong launch liquidity")
        elif liquidity >= 3000:
            score += 8

        v5 = float(token_data.get("volume_5m") or 0)
        tx5 = int(token_data.get("txns_5m") or 0)
        pc5 = float(token_data.get("price_change_5m") or 0)
        if v5 >= 3000:
            score += self.weights["momentum_5m"]
            reasons.append(f"5m volume ${v5:,.0f}")
        if tx5 >= 20:
            score += self.weights["activity_5m"]
            reasons.append(f"5m activity {tx5} txns")
        if pc5 >= 10:
            score += 5
            reasons.append(f"5m momentum +{pc5:.1f}%")

        if token_data.get("bundle_detected"):
            score -= 80
            reasons.append("BUNDLE DETECTED")
        if token_data.get("fat_bundle"):
            score -= 70
            reasons.append("Fat bundle")
        if token_data.get("bot_farm"):
            score -= 40
            reasons.append("Bot farm detected")
        for flag in token_data.get("red_flags", []):
            key = f"red_flag_{flag}"
            if key in self.weights:
                score += self.weights[key]
                reasons.append(flag.replace("_", " ").title())

        sm_count = int(token_data.get("smart_money_count") or 0)
        if sm_count >= 3:
            score += 15
            reasons.append(f"Smart-money activity ({sm_count})")
        elif sm_count >= 1:
            score += 8
            reasons.append("Watched-wallet activity")

        score = max(0, min(score, 100))
        label = "high" if score >= 70 else "medium" if score >= 40 else "low"
        return {"score": score, "label": label, "reasons": reasons}

    def should_alert(self, score_label: str, mode: str):
        if mode == "all":
            return True
        if mode == "medium":
            return score_label in ["medium", "high"]
        if mode == "high":
            return score_label == "high"
        return False
