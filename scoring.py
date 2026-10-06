from config import Config


class ScoringEngine:
    """
    Market-cap band scoring.

      low:    6,000 – 11,999 MC   (must be fresh — seconds to minutes old)
      medium: 12,000 – 23,999 MC
      high:   24,000 – 48,000 MC
      else:   no alert

    Red flags, smart money, KOLs do not affect the band. They are only
    shown as warnings on the embed.
    """

    LOW_MIN = 6000
    LOW_MAX = 11999
    MEDIUM_MIN = 12000
    MEDIUM_MAX = 23999
    HIGH_MIN = 24000
    HIGH_MAX = 48000

    LOW_MAX_AGE_SECONDS = 600  # only the low band is age-gated

    def calculate_score(self, token_data: dict):
        mc = token_data.get("market_cap")
        if mc is None:
            return {"score": 0, "label": None, "band": None,
                    "reasons": ["No market cap yet"]}
        try:
            mc = float(mc)
        except (TypeError, ValueError):
            return {"score": 0, "label": None, "band": None,
                    "reasons": ["Invalid market cap"]}

        if mc < self.LOW_MIN:
            return {"score": 0, "label": None, "band": None,
                    "reasons": [f"MC ${mc:,.0f} below floor"]}
        if mc > self.HIGH_MAX:
            return {"score": 0, "label": None, "band": None,
                    "reasons": [f"MC ${mc:,.0f} above ceiling"]}

        if mc <= self.LOW_MAX:
            age = token_data.get("age_seconds")
            if age is None or age > self.LOW_MAX_AGE_SECONDS:
                return {"score": 0, "label": None, "band": None,
                        "reasons": ["Not fresh enough for low band"]}
            return {"score": 30, "label": "low", "band": "low",
                    "reasons": [f"Fresh low-cap (${mc:,.0f})"]}

        if mc <= self.MEDIUM_MAX:
            return {"score": 60, "label": "medium", "band": "medium",
                    "reasons": [f"Mid-cap (${mc:,.0f})"]}

        return {"score": 90, "label": "high", "band": "high",
                "reasons": [f"High-cap (${mc:,.0f})"]}

    def should_alert(self, score_label, mode: str):
        if score_label is None:
            return False
        if mode == "all":
            return True
        if mode == "medium":
            return score_label in ("medium", "high")
        if mode == "high":
            return score_label == "high"
        return False