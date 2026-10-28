import time
import discord
from discord import app_commands
from discord.ext import commands
from config import Config
from datetime import datetime, timezone
from enricher import is_valid_solana_address, has_valid_identity
from embeds import (
    create_link_view, dev_wallet_link, fmt_dev_rep, fmt_pvp_lines, fmt_sol,
)


def _fmt_money(value):
    """'$12,345', or None when the value is missing/zero (never a misleading $0)."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return f"${number:,.0f}" if number > 0 else None


def _num(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _valid_rows(rows):
    return [
        r for r in rows or []
        if has_valid_identity(r.get("name"), r.get("symbol"), r.get("mint_address"))
    ]


def _coin_line(row):
    """Shared one-token summary used by /history, /recent and /top."""
    mint = row["mint_address"]
    band = row.get("score_label")
    head = []
    if band:
        head.append(band.upper())
    mc = _fmt_money(row.get("current_market_cap") or row.get("first_seen_market_cap"))
    if mc:
        head.append(mc)
    launch = fmt_sol(row.get("launch_sol"))
    if launch:
        head.append(f"Launch {launch}")
    created = row.get("created_at")
    if created:
        head.append(f"<t:{int(created.timestamp())}:R>")
    value = f"`{mint}`"
    if head:
        value += "\n" + " · ".join(head)
    if row.get("red_flags"):
        value += f"\nFlags: {row['red_flags'][:120]}"
    return value


def _fit_lines(lines, limit=1024):
    out, size = [], 0
    for line in lines:
        if size + len(line) + 1 > limit:
            break
        out.append(line)
        size += len(line) + 1
    return "\n".join(out)


class TrackerCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def is_admin(self, interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            return False
        if not isinstance(interaction.user, discord.Member):
            return False
        perms = interaction.user.guild_permissions
        return bool(perms and perms.administrator)

    async def cog_app_command_error(self, interaction: discord.Interaction, error):
        original = getattr(error, "original", error)
        print(f"Command error: {type(original).__name__}: {original}")
        message = "The command could not be completed. Please try again."
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)

    async def _sync_webhook_safe(self):
        scanner = getattr(self.bot, "scanner", None)
        if scanner:
            try:
                await scanner._refresh_wallet_index()
            except Exception as e:
                print(f"Wallet index refresh after wallet change failed: {e}")
        monitor = getattr(self.bot, "wallet_monitor", None)
        if not monitor:
            return
        try:
            await monitor.sync_webhook()
        except Exception as e:
            print(f"Webhook sync after wallet change failed: {e}")

    @app_commands.command(name="setchannel", description="Set the alert channel for this server")
    @app_commands.describe(channel="The channel to send alerts to")
    async def setchannel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if not self.is_admin(interaction):
            await interaction.response.send_message("Admin only command.", ephemeral=True)
            return
        await self.bot.db.get_or_create_guild(interaction.guild_id)
        await self.bot.db.set_alert_channel(interaction.guild_id, channel.id)
        await interaction.response.send_message(f"Alert channel set to {channel.mention}", ephemeral=True)

    @app_commands.command(
        name="setfreshchannel",
        description="Set the channel for 5-second fresh launch callouts",
    )
    @app_commands.describe(channel="The channel for fresh launch callouts")
    async def setfreshchannel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if not self.is_admin(interaction):
            await interaction.response.send_message("Admin only command.", ephemeral=True)
            return
        await self.bot.db.get_or_create_guild(interaction.guild_id)
        await self.bot.db.set_fresh_channel(interaction.guild_id, channel.id)
        await interaction.response.send_message(
            f"Fresh launch channel set to {channel.mention}", ephemeral=True
        )

    @app_commands.command(
        name="togglelaunchalerts",
        description="Turn 5-second fresh launch callouts on or off",
    )
    @app_commands.describe(enabled="Turn fresh launch callouts on or off")
    async def togglelaunchalerts(self, interaction: discord.Interaction, enabled: bool):
        if not self.is_admin(interaction):
            await interaction.response.send_message("Admin only command.", ephemeral=True)
            return
        await self.bot.db.get_or_create_guild(interaction.guild_id)
        await self.bot.db.set_launch_alerts(interaction.guild_id, enabled)
        message = f"Launch alerts {'ON' if enabled else 'OFF'}"
        if enabled:
            settings = await self.bot.db.get_guild_settings(interaction.guild_id)
            if not settings or not settings.get("fresh_channel_id"):
                message += ". No fresh channel yet: use /setfreshchannel to choose one."
        await interaction.response.send_message(message, ephemeral=True)

    @app_commands.command(name="setmode", description="Set the scan mode for this server")
    @app_commands.describe(mode="Alert mode: all, medium, or high")
    @app_commands.choices(mode=[
        app_commands.Choice(name="All", value="all"),
        app_commands.Choice(name="Medium+", value="medium"),
        app_commands.Choice(name="High only", value="high"),
    ])
    async def setmode(self, interaction: discord.Interaction, mode: app_commands.Choice[str]):
        if not self.is_admin(interaction):
            await interaction.response.send_message("Admin only command.", ephemeral=True)
            return
        await self.bot.db.get_or_create_guild(interaction.guild_id)
        await self.bot.db.set_scan_mode(interaction.guild_id, mode.value)
        await interaction.response.send_message(f"Scan mode set to {mode.name}", ephemeral=True)

    @app_commands.command(name="addwallet", description="Add a wallet to watch")
    @app_commands.describe(
        address="Wallet address to watch",
        label="Optional label for this wallet",
        channel="Optional channel for wallet alerts",
    )
    async def addwallet(
        self,
        interaction: discord.Interaction,
        address: str,
        label: str = None,
        channel: discord.TextChannel = None,
    ):
        if not self.is_admin(interaction):
            await interaction.response.send_message("Admin only command.", ephemeral=True)
            return
        address = (address or "").strip()
        if not is_valid_solana_address(address):
            await interaction.response.send_message("Invalid Solana wallet address.", ephemeral=True)
            return
        await self.bot.db.get_or_create_guild(interaction.guild_id)
        await self.bot.db.add_wallet(
            interaction.guild_id, address, label, channel.id if channel else None
        )
        suffix = f" with label {label}" if label else ""
        await interaction.response.send_message(
            f"Wallet added: {address[:8]}...{address[-8:]}{suffix}",
            ephemeral=True,
        )
        await self._sync_webhook_safe()

    @app_commands.command(name="removewallet", description="Remove a watched wallet")
    @app_commands.describe(address="Wallet address to remove")
    async def removewallet(self, interaction: discord.Interaction, address: str):
        if not self.is_admin(interaction):
            await interaction.response.send_message("Admin only command.", ephemeral=True)
            return
        address = (address or "").strip()
        if not is_valid_solana_address(address):
            await interaction.response.send_message("Invalid Solana wallet address.", ephemeral=True)
            return
        removed = await self.bot.db.remove_wallet(interaction.guild_id, address)
        msg = "Wallet removed" if removed else "Wallet not found"
        await interaction.response.send_message(msg, ephemeral=True)
        if removed:
            await self._sync_webhook_safe()

    @app_commands.command(name="resetwallets", description="Remove all watched wallets for this server")
    async def resetwallets(self, interaction: discord.Interaction):
        if not self.is_admin(interaction):
            await interaction.response.send_message("Admin only command.", ephemeral=True)
            return
        count = await self.bot.db.clear_wallets(interaction.guild_id)
        await interaction.response.send_message(
            f"Removed {count} wallet(s) from this server.",
            ephemeral=True,
        )
        await self._sync_webhook_safe()

    @app_commands.command(name="listwallets", description="List all watched wallets")
    @app_commands.guild_only()
    async def listwallets(self, interaction: discord.Interaction):
        wallets = await self.bot.db.get_wallets(interaction.guild_id)
        if not wallets:
            await interaction.response.send_message("No wallets being watched.", ephemeral=True)
            return
        embed = discord.Embed(title="Watched Wallets", color=discord.Color.blue())
        for w in wallets[:25]:
            label = w["label"] or "No label"
            address = f"{w['address'][:8]}...{w['address'][-8:]}"
            channel = f"<#{w['channel_id']}>" if w["channel_id"] else "Default"
            embed.add_field(name=label, value=f"{address} | {channel}", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(
        name="importwallets",
        description="Import wallets from a JSON or plain-text attachment",
    )
    @app_commands.guild_only()
    async def importwallets(self, interaction: discord.Interaction, attachment: discord.Attachment):
        if not self.is_admin(interaction):
            await interaction.response.send_message("Admin only command.", ephemeral=True)
            return

        fname = (attachment.filename or "").lower()
        if not (fname.endswith(".json") or fname.endswith(".txt")):
            await interaction.response.send_message(
                "Attach a .json or .txt wallet file.", ephemeral=True,
            )
            return
        if attachment.size and attachment.size > 1_000_000:
            await interaction.response.send_message("Wallet file must be 1 MB or smaller.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        try:
            content = await attachment.read()
            text = content.decode("utf-8-sig")
        except Exception as e:
            await interaction.followup.send(
                f"Could not read the attachment: {type(e).__name__}. Expected UTF-8 text.",
                ephemeral=True,
            )
            return

        parser = getattr(self.bot, "wallet_tracker", None)
        wallets = parser.parse_wallet_json(text) if parser else []
        if not wallets:
            await interaction.followup.send(
                "No valid wallets found. Use address, trackedWalletAddress, pubkey, or one address per line.",
                ephemeral=True,
            )
            return

        await self.bot.db.get_or_create_guild(interaction.guild_id)
        imported = 0
        for w in wallets:
            try:
                await self.bot.db.add_wallet(
                    interaction.guild_id, w["address"], w["label"], None
                )
                imported += 1
            except Exception as e:
                print(f"add_wallet failed for {w.get('address')}: {e}")

        await interaction.followup.send(
            f"Imported {imported} unique wallet(s) from {attachment.filename}.",
            ephemeral=True,
        )
        await self._sync_webhook_safe()

    @app_commands.command(name="checktoken", description="Deep dive on a scanned token")
    @app_commands.describe(mint="Solana token mint address")
    async def checktoken(self, interaction: discord.Interaction, mint: str):
        mint = (mint or "").strip()
        if not is_valid_solana_address(mint):
            await interaction.response.send_message("Invalid Solana token mint.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        db = self.bot.db
        row = await db.get_coin_by_mint(mint)
        if not row or not has_valid_identity(row.get("name"), row.get("symbol"), mint):
            await interaction.followup.send(
                "Token not found in the local scan history.", ephemeral=True,
            )
            return
        row = dict(row)
        name = row["name"].strip()
        symbol = row["symbol"].strip()
        band = row.get("score_label") or "unscored"

        # Local data first; the live lookup below is one free DexScreener call.
        dev = row.get("dev_wallet")
        siblings = []
        try:
            siblings = await db.get_pvp_siblings(
                mint, name, symbol, row.get("created_at") or datetime.now(timezone.utc), limit=10,
            )
        except Exception as exc:
            print(f"PVP sibling lookup failed: {type(exc).__name__}: {exc}")
        dev_rep = None
        try:
            dev_rep = await db.get_dev_reputation(dev)
        except Exception as exc:
            print(f"Dev reputation lookup failed: {type(exc).__name__}: {exc}")
        ath_db = None
        try:
            if row.get("id") is not None:
                ath_db = await db.get_coin_ath(row["id"])
        except Exception as exc:
            print(f"ATH lookup failed: {type(exc).__name__}: {exc}")

        live = {}
        scanner = getattr(self.bot, "scanner", None)
        if scanner and hasattr(scanner, "fetch_markets"):
            try:
                live = await scanner.fetch_markets([mint] + [sib["mint_address"] for sib in siblings])
            except Exception as exc:
                print(f"Live market lookup failed: {type(exc).__name__}: {exc}")
        me = live.get(mint) or {}

        embed = discord.Embed(
            title="Token Inspection",
            description=f"**{name}** (`{symbol}`)\n`{mint}`",
            color=discord.Color.green() if band == "high" else discord.Color.gold(),
            timestamp=discord.utils.utcnow(),
        )
        image = row.get("image_url") or me.get("image_url")
        if image:
            embed.set_thumbnail(url=image)
        embed.add_field(name="Band", value=f"**{band.upper()}**", inline=True)
        embed.add_field(name="Score", value=str(row.get("score") or 0), inline=True)
        launch = fmt_sol(row.get("launch_sol"))
        if launch:
            embed.add_field(name="Launch Buy", value=launch, inline=True)

        initial_mc = _num(row.get("first_seen_market_cap"))
        current_mc = _num(me.get("market_cap")) or _num(row.get("current_market_cap"))
        if initial_mc and current_mc:
            change = (current_mc - initial_mc) / initial_mc * 100.0
            embed.add_field(
                name="Initial MC / Current MC",
                value=f"${initial_mc:,.0f} → **${current_mc:,.0f}** ({change:+.1f}%)",
                inline=False,
            )
        elif current_mc:
            embed.add_field(name="Current MC", value=f"${current_mc:,.0f}", inline=True)
        elif initial_mc:
            embed.add_field(name="Initial MC", value=f"${initial_mc:,.0f}", inline=True)

        ath_candidates = [v for v in (_num(ath_db), _num(row.get("ath_market_cap")), current_mc, initial_mc) if v]
        ath = max(ath_candidates) if ath_candidates else None
        if ath:
            embed.add_field(name="ATH", value=f"${ath:,.0f}", inline=True)
        volume = _fmt_money(me.get("volume"))
        if volume:
            embed.add_field(name="Volume", value=volume, inline=True)
        lp = _fmt_money(me.get("liquidity"))
        if lp:
            embed.add_field(name="LP", value=lp, inline=True)

        dev_link = dev_wallet_link(dev)
        if dev_link:
            embed.add_field(name="Dev Wallet", value=dev_link, inline=True)
        rep = fmt_dev_rep(dev_rep)
        if rep:
            embed.add_field(name="Dev History", value=rep, inline=True)
        if row.get("rugged") and row.get("rug_reason"):
            embed.add_field(name="Rug Evidence", value=str(row["rug_reason"])[:1024], inline=False)

        now = datetime.now(timezone.utc)
        sib_data = []
        for sib in siblings:
            live_sib = live.get(sib["mint_address"]) or {}
            created = sib.get("created_at")
            if created is not None and created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            sib_data.append({
                "mint_address": sib["mint_address"],
                "market_cap": _num(live_sib.get("market_cap"))
                or _num(sib.get("current_market_cap")) or _num(sib.get("first_seen_market_cap")),
                "volume": _num(live_sib.get("volume")),
                "liquidity": _num(live_sib.get("liquidity")),
                "age_seconds": (now - created).total_seconds() if created else None,
            })
        pvp_text = _fit_lines(fmt_pvp_lines(sib_data, limit=10))
        if pvp_text:
            embed.add_field(name=f"PVP Siblings ({len(sib_data)})", value=pvp_text, inline=False)

        flags = (row.get("red_flags") or "").replace(",", ", ")
        if flags:
            embed.add_field(name="Red Flags", value=flags[:1024], inline=False)
        if row.get("last_reject_reason"):
            embed.add_field(name="Last Verification", value=row["last_reject_reason"][:1024], inline=False)
        await interaction.followup.send(embed=embed, view=create_link_view(mint), ephemeral=True)

    def _scan_list_embed(self, title, rows, footer):
        embed = discord.Embed(title=title, color=discord.Color.blue(), timestamp=discord.utils.utcnow())
        for row in rows:
            embed.add_field(
                name=f"{row['name'].strip()} ({row['symbol'].strip()})"[:256],
                value=_coin_line(row)[:1024],
                inline=False,
            )
        embed.set_footer(text=footer)
        return embed

    @app_commands.command(name="history", description="Show recent local token scans")
    @app_commands.describe(limit="Number of scans, from 1 to 10")
    async def history(self, interaction: discord.Interaction, limit: int = 8):
        limit = max(1, min(int(limit), 10))
        rows = _valid_rows([dict(r) for r in await self.bot.db.get_recent_coins(limit)])
        if not rows:
            await interaction.response.send_message("No tokens scanned yet.", ephemeral=True)
            return
        embed = self._scan_list_embed(
            "Recent Token Scans", rows, "Solana Meme Tracker | Local history — no API calls",
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="recent", description="Show recent scans with the SOL used to launch")
    @app_commands.describe(
        limit="Number of scans, from 1 to 10",
        ephemeral="True = only you see it, False = post it in the channel",
    )
    async def recent(self, interaction: discord.Interaction, limit: int = 8, ephemeral: bool = True):
        limit = max(1, min(int(limit), 10))
        rows = _valid_rows([dict(r) for r in await self.bot.db.get_recent_coins(limit)])
        if not rows:
            await interaction.response.send_message("No tokens scanned yet.", ephemeral=ephemeral)
            return
        embed = self._scan_list_embed(
            "Recent Scans", rows, "Solana Meme Tracker | Launch = SOL the dev spent at launch",
        )
        await interaction.response.send_message(embed=embed, ephemeral=ephemeral)

    @app_commands.command(name="top", description="Top scanned tokens by score")
    @app_commands.describe(limit="Number of tokens, from 1 to 10")
    async def top(self, interaction: discord.Interaction, limit: int = 10):
        limit = max(1, min(int(limit), 10))
        rows = _valid_rows([dict(r) for r in await self.bot.db.get_top_coins(limit)])
        if not rows:
            await interaction.response.send_message("No scored tokens yet.", ephemeral=True)
            return
        embed = discord.Embed(
            title="Top Tokens by Score", color=discord.Color.gold(), timestamp=discord.utils.utcnow(),
        )
        for i, row in enumerate(rows, 1):
            line = _coin_line(row)
            ath = _fmt_money(row.get("ath_market_cap"))
            if ath:
                line += f"\nATH {ath}"
            embed.add_field(
                name=f"{i}. {row['name'].strip()} ({row['symbol'].strip()})"[:256],
                value=line[:1024],
                inline=False,
            )
        embed.set_footer(text="Solana Meme Tracker | Score band: Low 30 · Medium 60 · High 90")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="milestones", description="Recent 2x, 5x and 10x hits")
    @app_commands.describe(limit="Number of tokens, from 1 to 10")
    async def milestones(self, interaction: discord.Interaction, limit: int = 10):
        limit = max(1, min(int(limit), 10))
        rows = _valid_rows([dict(r) for r in await self.bot.db.get_recent_milestones(limit)])
        if not rows:
            await interaction.response.send_message("No 2x, 5x or 10x hits yet.", ephemeral=True)
            return
        embed = discord.Embed(
            title="Recent Milestones", color=discord.Color.green(), timestamp=discord.utils.utcnow(),
        )
        for row in rows:
            mult = float(row["multiplier"])
            parts = [f"**{mult:g}x**"]
            mc = _fmt_money(row.get("market_cap_at_milestone"))
            if mc:
                parts.append(f"MC {mc}")
            if row.get("hit_at"):
                parts.append(f"<t:{int(row['hit_at'].timestamp())}:R>")
            embed.add_field(
                name=f"{row['name'].strip()} ({row['symbol'].strip()})"[:256],
                value=(f"`{row['mint_address']}`\n" + " · ".join(parts))[:1024],
                inline=False,
            )
        embed.set_footer(text="Solana Meme Tracker | Best of 2x / 5x / 10x per token")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="devtrack", description="Enable or disable dev wallet tracking")
    @app_commands.describe(enabled="Turn dev tracking on or off")
    async def devtrack(self, interaction: discord.Interaction, enabled: bool):
        if not self.is_admin(interaction):
            await interaction.response.send_message("Admin only command.", ephemeral=True)
            return
        await self.bot.db.get_or_create_guild(interaction.guild_id)
        await self.bot.db.set_dev_tracking(interaction.guild_id, enabled)
        status = "enabled" if enabled else "disabled"
        await interaction.response.send_message(f"Dev tracking {status}", ephemeral=True)

    @app_commands.command(name="status", description="Show bot status and settings")
    @app_commands.guild_only()
    async def status(self, interaction: discord.Interaction):
        settings = await self.bot.db.get_guild_settings(interaction.guild_id)
        embed = discord.Embed(
            title="Bot Status",
            color=discord.Color.green(),
            timestamp=discord.utils.utcnow(),
        )
        if settings:
            channel = f"<#{settings['alert_channel_id']}>" if settings["alert_channel_id"] else "Not set"
            mode = settings["scan_mode"] or Config.DEFAULT_SCAN_MODE
            dev_track = "Enabled" if settings["dev_tracking"] else "Disabled"
            embed.add_field(name="Alert Channel", value=channel, inline=True)
            fresh_id = settings.get("fresh_channel_id")
            embed.add_field(
                name="Fresh Channel", value=f"<#{fresh_id}>" if fresh_id else "Not set", inline=True,
            )
            embed.add_field(
                name="Launch Alerts", value="ON" if settings.get("launch_alerts") else "OFF", inline=True,
            )
            embed.add_field(name="Scan Mode", value=mode, inline=True)
            embed.add_field(name="Dev Tracking", value=dev_track, inline=True)
        wallets = await self.bot.db.get_wallets(interaction.guild_id)
        embed.add_field(name="Watched Wallets", value=str(len(wallets)), inline=True)
        active_coins = await self.bot.db.get_active_coins()
        embed.add_field(name="Active Coins", value=str(len(active_coins)), inline=True)
        embed.add_field(name="Scan Interval", value=f"{Config.SCAN_INTERVAL_SECONDS}s", inline=True)

        scanner = getattr(self.bot, "scanner", None)
        if scanner:
            diag = scanner.diagnostics()
            flow = diag["flow"]
            rpc = diag["rpc"] or {}
            trade_age = flow.get("last_trade_age")
            trade_text = "No early trades yet" if trade_age is None else f"Last trade {trade_age}s ago"
            embed.add_field(
                name="Scanner",
                value=(
                    f"WS {'connected' if diag['websocket_connected'] else 'reconnecting'} · "
                    f"Queue {diag['queue_size']} · Verifying {diag['pending_verifications']} · "
                    f"Fresh scans {diag.get('pending_fresh_scans', 0)}"
                ),
                inline=False,
            )
            embed.add_field(
                name="Early Flow",
                value=f"{flow['tracked_mints']} mints · {flow['total_trades']:,} trades · {trade_text}",
                inline=False,
            )
            if rpc:
                embed.add_field(
                    name="RPC Budget",
                    value=(
                        f"{rpc['endpoint']} · {rpc['requests_today']}/{rpc['daily_budget']} today"
                        f"{' · PAUSED' if rpc['paused'] else ''}"
                    ),
                    inline=False,
                )
        queue = getattr(self.bot, "discord_queue", None)
        if queue:
            embed.add_field(name="Discord Send Queue", value=str(queue.queue.qsize()), inline=True)
        started = getattr(self.bot, "started_at", None)
        if started:
            uptime = max(0, int(time.time() - started))
            embed.add_field(name="Uptime", value=f"{uptime // 3600}h {(uptime % 3600) // 60}m", inline=True)
        embed.add_field(
            name="Default Sniper",
            value=f"{Config.DEFAULT_WALLET[:8]}...{Config.DEFAULT_WALLET[-8:]}",
            inline=False,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(TrackerCommands(bot))