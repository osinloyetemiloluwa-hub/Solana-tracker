"""Silent caller tracking (no slash command, no setup).

Any message in any server the bot can read may contain a Solana contract
address. For each address (max a few per message):

* if the token is not in the database and DexScreener knows it, it is added
  and tracked; addresses DexScreener has no market for (wallets, pair
  addresses, typos) are ignored and not retried for a while;
* the first person to post it in that server is recorded as the first caller,
  with the market cap at the time of the call.

When that token later reaches a milestone rung measured from the call, the
milestone embed credits the caller (see Scanner._send_call_milestone).
The bot never replies to the message.
"""
import re
from datetime import datetime, timezone

from cache import TTLCache
from config import Config
from enricher import is_valid_solana_address
from raydium import QUOTE_MINTS

_B58 = "1-9A-HJ-NP-Za-km-z"
MINT_RE = re.compile(rf"(?<![{_B58}])[{_B58}]{{32,44}}(?![{_B58}])")


def extract_mints(text, limit=None):
    """Distinct valid Solana addresses in `text`, in order of appearance."""
    limit = limit or Config.CALL_MAX_PER_MESSAGE
    found = []
    for match in MINT_RE.findall(text or ""):
        if match in found or match.lower() in QUOTE_MINTS or not is_valid_solana_address(match):
            continue
        found.append(match)
        if len(found) >= limit:
            break
    return found


def message_url(guild_id, channel_id, message_id):
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"


class CallTracker:
    def __init__(self, db, scanner):
        self.db = db
        self.scanner = scanner
        self._misses = TTLCache(2000, Config.CALL_MISS_TTL_SECONDS)

    async def handle_message(self, message):
        if message.author.bot or message.guild is None or not message.content:
            return
        for mint in extract_mints(message.content):
            try:
                await self._record(message, mint)
            except Exception as exc:
                print(f"Call tracking error for {mint[:8]}: {type(exc).__name__}: {exc}")

    async def _record(self, message, mint):
        coin = await self.db.get_coin_by_mint(mint)
        if coin is None and self._misses.get(mint):
            return
        # Always read the market live: a caller's entry price is the market cap at
        # the moment they posted, never an older stored value (and never the
        # token's own initial market cap, which can be lower or higher).
        market = await self.scanner.lookup_market(mint)
        if coin is None:
            if not market:
                self._misses.set(mint, True)
                return
            coin_id, _ = await self.db.upsert_discovered(
                mint, market.get("dex_id") or "dexscreener", name=market.get("name"),
                symbol=market.get("symbol"), image_url=market.get("image_url"),
                source="call", revive=True,
            )
            coin = await self.db.update_market(coin_id, market)
            entry_mc = market.get("market_cap")
        else:
            # A call is a reason to track it again (no-op if it already is).
            await self.db.upsert_discovered(mint, coin["program_type"], source="call", revive=True)
            entry_mc = None
            if market:
                trusted = self.scanner.accept_market(dict(coin), market)
                entry_mc = trusted.get("market_cap")
                if entry_mc:
                    coin = await self.db.update_market(coin["id"], trusted) or coin
            if not entry_mc:
                entry_mc = self._fresh_stored_mc(coin)
        if not coin:
            return
        await self.db.add_call(
            coin["id"], mint, message.guild.id, message.channel.id, message.id,
            message.author.id, getattr(message.author, "display_name", None) or str(message.author),
            entry_mc,
        )

    @staticmethod
    def _fresh_stored_mc(coin):
        """The stored market cap, only if it was refreshed moments ago; else None
        (the first refresh after the call fills in the entry price instead)."""
        stamp = coin.get("last_tracked_at")
        if not stamp or not coin.get("current_market_cap"):
            return None
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - stamp).total_seconds()
        return coin["current_market_cap"] if age <= Config.CALL_FRESH_SECONDS else None
