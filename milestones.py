from config import Config


class MilestoneTracker:
    def __init__(self, db):
        self.db = db

    @staticmethod
    def _milestones_up_to(current_mult):
        # Keep the useful early milestones, then expand indefinitely.
        values = [2.0, 3.0, 5.0, 10.0, 25.0, 50.0, 100.0]
        x = 250.0
        while x <= current_mult:
            values.append(x)
            # 250, 500, 1000, 2500, 5000, 10000, ...
            if x % 1000 == 0:
                x *= 2.5
            elif x % 500 == 0:
                x *= 2
            else:
                x *= 2
        return values

    async def check_milestones(self, coin_id, first_price, first_mc, current_price, current_mc):
        if not first_price and not first_mc:
            return []

        candidates = []
        if first_price and current_price and float(first_price) > 0:
            candidates.append(float(current_price) / float(first_price))
        if first_mc and current_mc and float(first_mc) > 0:
            candidates.append(float(current_mc) / float(first_mc))
        if not candidates:
            return []

        current_mult = max(candidates)
        if current_mult < 1.0:
            return []

        hit = []
        for multiplier in self._milestones_up_to(current_mult):
            if await self.db.milestone_exists(coin_id, multiplier):
                continue
            await self.db.insert_milestone(coin_id, multiplier, current_price, current_mc)
            await self.db.mark_coin_milestone_hit(coin_id)
            hit.append({"multiplier": multiplier, "price": current_price, "market_cap": current_mc})
        return hit
