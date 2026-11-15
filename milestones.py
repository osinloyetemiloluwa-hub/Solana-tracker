import math

from config import Config


def ratio(base_mc, high_mc):
    """high / base market cap, or None when either is missing or not positive."""
    try:
        base, high = float(base_mc), float(high_mc)
    except (TypeError, ValueError):
        return None
    if base <= 0 or high <= 0:
        return None
    return high / base


def rung_for(multiple):
    """Highest milestone rung at or below `multiple`, or None below the first.

    Rungs are 2x, 5x, 10x, 25x, 50x, 100x, then every further 100x
    (200x, 300x, ...). A move only counts when it reaches a new rung, which
    keeps milestone alerts rare.
    """
    if multiple is None or multiple < Config.MILESTONE_LADDER[0]:
        return None
    top = Config.MILESTONE_LADDER[-1]
    if multiple >= top:
        return float(math.floor(multiple / top) * top)
    return float(max(r for r in Config.MILESTONE_LADDER if r <= multiple))


class MilestoneTracker:
    """Ladder milestones measured from the first market cap seen to the ATH.

    Using the ATH (the highest market cap ever sampled) means a rung reached
    between two samples is still reported. The coin's high-water rung
    (`coins.max_multiple`) is raised atomically in SQL, so each rung is
    reported once: a drop sends nothing and never lowers the mark.
    """

    def __init__(self, db):
        self.db = db

    async def check(self, coin_id, initial_mc, ath_mc, current_mc, current_price):
        rung = rung_for(ratio(initial_mc, ath_mc))
        if rung is None or not await self.db.raise_max_multiple(coin_id, rung):
            return None
        await self.db.insert_milestone(coin_id, rung, current_price, current_mc)
        return {"multiplier": rung, "price": current_price, "market_cap": current_mc}
