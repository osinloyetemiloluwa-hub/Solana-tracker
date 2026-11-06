class MilestoneTracker:
    """Dynamic milestones.

    The multiple is current market cap / initial market cap. A milestone fires
    whenever that multiple is higher than the highest multiple the coin has
    ever reported (its high-water mark). There is no fixed list of thresholds:
    every new high is valid. Nothing fires at or below 1x, and a falling
    multiple neither sends anything nor lowers the high-water mark.

    Multiples are compared at the precision they are displayed (2 decimals) so
    a "new high" is always visibly higher than the previous one.
    """

    def __init__(self, db):
        self.db = db

    @staticmethod
    def multiple(initial_mc, current_mc):
        """current / initial market cap rounded to 2 decimals, or None."""
        try:
            initial = float(initial_mc)
            current = float(current_mc)
        except (TypeError, ValueError):
            return None
        if initial <= 0 or current <= 0:
            return None
        return round(current / initial, 2)

    async def check(self, coin_id, initial_mc, current_mc, current_price):
        """Return a milestone dict when a new high was reached, else None."""
        multiple = self.multiple(initial_mc, current_mc)
        if multiple is None or multiple <= 1.0:
            return None
        if not await self.db.raise_max_multiple(coin_id, multiple):
            return None
        await self.db.insert_milestone(coin_id, multiple, current_price, current_mc)
        return {
            "multiplier": multiple,
            "price": current_price,
            "market_cap": current_mc,
        }
