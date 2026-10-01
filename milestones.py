from config import Config


class MilestoneTracker:
    def __init__(self, db):
        self.db = db

    @staticmethod
    def _milestones_up_to(current_mult):
        # 2, 3, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000 ...
        values = [2.0, 3.0, 5.0, 10.0, 25.0, 50.0, 100.0]
        x = 250.0
        while x <= current_mult:
            values.append(x)
            if x % 1000 == 0:
                x *= 2.5
            elif x % 500 == 0:
                x *= 2
            else:
                x *= 2
        return values

    async def check_milestones(self, coin_id, first_price, first_mc,
                               current_price, current_mc):
        """
        Return AT MOST ONE milestone per call — the highest newly reached.

        - Market cap is the primary basis (MC before -> MC after).
        - Price is only a fallback when MC is missing on either side.
        - Every crossed threshold is still written to the DB so it can never
          fire again, but only the highest is returned for alerting.
        - Duplicate protection comes from:
              * `milestone_exists()` check
              * `UNIQUE(coin_id, multiplier)` on the milestones table
              * `insert_milestone` uses ON CONFLICT DO NOTHING
        """
        current_mult = None

        # MC-first comparison
        if first_mc and current_mc and float(first_mc) > 0 and float(current_mc) > 0:
            current_mult = float(current_mc) / float(first_mc)
        # Price fallback only if we have no usable MC baseline
        elif first_price and current_price and float(first_price) > 0:
            current_mult = float(current_price) / float(first_price)

        if current_mult is None or current_mult < 2.0:
            return []

        newly_reached = []
        for multiplier in self._milestones_up_to(current_mult):
            if await self.db.milestone_exists(coin_id, multiplier):
                continue
            await self.db.insert_milestone(
                coin_id, multiplier, current_price, current_mc
            )
            newly_reached.append(multiplier)

        if not newly_reached:
            return []

        await self.db.mark_coin_milestone_hit(coin_id)

        # Alert only on the single highest threshold crossed this cycle.
        highest = max(newly_reached)
        return [{
            "multiplier": highest,
            "price": current_price,
            "market_cap": current_mc,
        }]