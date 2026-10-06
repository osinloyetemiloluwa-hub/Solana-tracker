import time

from config import Config


class WalletSignalEngine:
    """Evaluates a fresh wallet buy against recent activity + token context."""

    def __init__(self, bot):
        self.bot = bot

    async def handle_buy(self, event):
        mint = event["mint"]
        now = event["timestamp"] or int(time.time())

        recent = await self.bot.db.get_wallet_buys_for_mint(
            mint, minutes=Config.WALLET_BUY_WINDOW_MINUTES
        )
        distinct_wallets = {b["wallet"] for b in recent}
        wallet_count = len(distinct_wallets)

        fast_window = Config.WALLET_BUY_FAST_WINDOW_SECONDS
        fast_count = sum(
            1 for b in recent
            if (now - (b["timestamp"] or now)) <= fast_window
        )

        market = {}
        try:
            market = await self.bot.scanner.get_market_data_with_fallback(mint) or {}
        except Exception as e:
            print(f"[wallet-signal] market fetch failed: {type(e).__name__}: {e}")

        # --- Token identity fallbacks, in order:
        #   1. DB row (PumpPortal saw it earlier)
        #   2. Enricher (DexScreener, then Helius metadata)
        #   3. Mint prefix (never "Unknown") ---
        name = None
        symbol = None
        try:
            coin = await self.bot.db.get_coin_by_mint(mint)
            if coin:
                name = coin.get("name")
                symbol = coin.get("symbol")
                if not market.get("image_url") and coin.get("image_url"):
                    market["image_url"] = coin.get("image_url")
        except Exception:
            coin = None

        if not name or not symbol:
            try:
                enriched = await self.bot.scanner.enricher.enrich(mint, priority="high")
                if not name:
                    name = enriched.get("name")
                if not symbol:
                    symbol = enriched.get("symbol")
                if not market.get("image_url") and enriched.get("image_url"):
                    market["image_url"] = enriched.get("image_url")
                if not market.get("market_cap") and enriched.get("market_cap"):
                    market["market_cap"] = enriched.get("market_cap")
                if not market.get("liquidity") and enriched.get("liquidity"):
                    market["liquidity"] = enriched.get("liquidity")
            except Exception as e:
                print(f"[wallet-signal] enrich failed: {type(e).__name__}: {e}")

        if not name:
            name = f"Token {mint[:6]}"
        if not symbol:
            symbol = mint[:4].upper()

        event["name"] = name
        event["symbol"] = symbol

        liquidity = market.get("liquidity") or 0
        market_cap = market.get("market_cap") or 0
        volume = market.get("volume_24h") or 0
        image_url = market.get("image_url")
        price = market.get("price")

        reasons = []

        if liquidity and liquidity < Config.LIQUIDITY_FLOOR:
            signal = "AVOID / HIGH RISK"
            reasons.append(f"Liquidity ${liquidity:,.0f} below floor")
        elif wallet_count >= 3:
            signal = "HIGH-CONVICTION SETUP"
            reasons.append(
                f"{wallet_count} tracked wallets in {Config.WALLET_BUY_WINDOW_MINUTES} min"
            )
        elif wallet_count >= 2:
            signal = "STRONG WATCH"
            reasons.append(
                f"{wallet_count} tracked wallets in {Config.WALLET_BUY_WINDOW_MINUTES} min"
            )
        else:
            size = event.get("sol_spent") or 0
            if size >= 5:
                signal = "STRONG WATCH"
                reasons.append(f"Large single buy: {size:.2f} SOL")
            else:
                signal = "WATCH"
                reasons.append(f"Single tracked buy: {size:.2f} SOL")

        if fast_count >= 2:
            reasons.append(f"{fast_count} buys within {fast_window}s")

        payload = {
            "event": event,
            "recent": recent,
            "wallet_count": wallet_count,
            "market": {
                "liquidity": liquidity,
                "market_cap": market_cap,
                "volume_24h": volume,
                "price": price,
                "image_url": image_url,
            },
            "signal": signal,
            "reasons": reasons,
        }

        await self._send_alert(payload)

    async def _send_alert(self, payload):
        event = payload["event"]
        mint = event.get("mint", "?")
        wallet_channel = event.get("channel_id")
        guild_id = event.get("guild_id")

        if wallet_channel:
            await self._dispatch(wallet_channel, payload)
            return

        if guild_id is None or guild_id == -1:
            try:
                guild_ids = await self.bot.db.get_all_guilds()
            except Exception as e:
                print(f"[wallet-signal] get_all_guilds failed: {type(e).__name__}: {e}")
                guild_ids = []

            sent = 0
            for gid in guild_ids:
                try:
                    settings = await self.bot.db.get_guild_settings(gid)
                except Exception as e:
                    print(f"[wallet-signal] settings fetch failed for {gid}: {e}")
                    continue
                if not settings or not settings["alert_channel_id"]:
                    continue
                await self._dispatch(settings["alert_channel_id"], payload)
                sent += 1

            if sent == 0:
                print(
                    f"[wallet-signal] {mint[:8]} matched but no guild has "
                    f"/setchannel configured — alert dropped"
                )
            return

        settings = await self.bot.db.get_guild_settings(guild_id)
        if not settings or not settings["alert_channel_id"]:
            print(
                f"[wallet-signal] {mint[:8]} guild {guild_id} has no "
                f"alert channel set — run /setchannel — alert dropped"
            )
            return
        await self._dispatch(settings["alert_channel_id"], payload)

    async def _dispatch(self, channel_id, payload):
        from embeds import create_wallet_buy_alert_embed, create_link_view

        try:
            embed = create_wallet_buy_alert_embed(payload)
            view = create_link_view(payload["event"]["mint"])

            await self.bot.send_discord_alert(
                channel_id,
                embed=embed,
                view=view,
                is_token_alert=False,
            )
            print(f"[wallet-signal] sent wallet buy alert → channel {channel_id}")
        except Exception as e:
            print(f"[wallet-signal] send failed to {channel_id}: {type(e).__name__}: {e}")