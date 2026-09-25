import discord
from discord import app_commands
from discord.ext import commands
from config import Config
from enricher import is_valid_solana_address


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

    async def _sync_webhook_safe(self):
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
        description="Import wallets from a json or txt attachment",
    )
    async def importwallets(self, interaction: discord.Interaction, attachment: discord.Attachment):
        if not self.is_admin(interaction):
            await interaction.response.send_message("Admin only command.", ephemeral=True)
            return

        fname = (attachment.filename or "").lower()
        if not (fname.endswith(".json") or fname.endswith(".txt")):
            await interaction.response.send_message(
                "Attach a json or txt file containing wallet JSON.",
                ephemeral=True,
            )
            return

        content = await attachment.read()
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            try:
                text = content.decode("utf-8-sig")
            except Exception:
                await interaction.response.send_message(
                    "Could not decode file. Expected UTF-8 text.",
                    ephemeral=True,
                )
                return

        parser = getattr(self.bot, "wallet_tracker", None)
        if parser is None:
            await interaction.response.send_message(
                "Wallet tracker not initialized.",
                ephemeral=True,
            )
            return

        wallets = parser.parse_wallet_json(text)
        if not wallets:
            await interaction.response.send_message(
                "No valid wallets found. Expected trackedWalletAddress or address.",
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

        await interaction.response.send_message(
            f"Imported {imported} wallet(s) from {attachment.filename}.",
            ephemeral=True,
        )
        await self._sync_webhook_safe()

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
            embed.add_field(name="Scan Mode", value=mode, inline=True)
            embed.add_field(name="Dev Tracking", value=dev_track, inline=True)
        wallets = await self.bot.db.get_wallets(interaction.guild_id)
        embed.add_field(name="Watched Wallets", value=str(len(wallets)), inline=True)
        active_coins = await self.bot.db.get_active_coins()
        embed.add_field(name="Active Coins", value=str(len(active_coins)), inline=True)
        embed.add_field(name="Scan Interval", value=f"{Config.SCAN_INTERVAL_SECONDS}s", inline=True)
        embed.add_field(
            name="Default Sniper",
            value=f"{Config.DEFAULT_WALLET[:8]}...{Config.DEFAULT_WALLET[-8:]}",
            inline=False,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(TrackerCommands(bot))