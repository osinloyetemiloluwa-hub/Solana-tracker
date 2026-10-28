from config import Config


class MilestoneTracker:
    def __init__(self, db):
        self.db = db

    async def check_milestones(self, coin_id, first_price, first_mc,
                               current_price, current_mc):
        """
        Fires milestones ONLY at exact multipliers from Config.MILESTONE_MULTIPLIERS.
        Never fires sub-1x. Never computes a dynamic multiple for display.
        """
        if not first_price and not first_mc:
            return []

        price_mult = None
        if first_price and current_price and float(first_price) > 0:
            price_mult = float(current_price) / float(first_price)

        mc_mult = None
        if first_mc and current_mc and float(first_mc) > 0:
            mc_mult = float(current_mc) / float(first_mc)

        candidates = [m for m in (price_mult, mc_mult) if m is not None]
        if not candidates:
            return []

        current_mult = max(candidates)

        if current_mult < 1.0:
            return []

        hit_milestones = []

        for multiplier in Config.MILESTONE_MULTIPLIERS:
            if multiplier <= 1.0:
                continue
            if current_mult < multiplier:
                continue
            if await self.db.milestone_exists(coin_id, multiplier):
                continue

            await self.db.insert_milestone(
                coin_id,
                multiplier,
                current_price,
                current_mc,
            )
            await self.db.mark_coin_milestone_hit(coin_id)
            hit_milestones.append({
                "multiplier": multiplier,
                "price": current_price,
                "market_cap": current_mc,
            })

        return hit_milestones