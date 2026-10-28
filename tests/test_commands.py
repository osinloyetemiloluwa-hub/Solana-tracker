from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import discord
from discord.ext import commands
import pytest

from commands import TrackerCommands
from config import Config
from wallets import WalletTracker


OTHER = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
EXPECTED = {
    "setchannel", "setmode", "addwallet", "removewallet", "resetwallets",
    "listwallets", "importwallets", "devtrack", "status", "checktoken", "history",
    "setfreshchannel", "togglelaunchalerts", "recent", "top", "milestones",
}


def interaction():
    return SimpleNamespace(
        guild_id=42,
        response=SimpleNamespace(send_message=AsyncMock(), defer=AsyncMock()),
        followup=SimpleNamespace(send=AsyncMock()),
    )


def fake_bot():
    db = SimpleNamespace(
        get_or_create_guild=AsyncMock(), set_alert_channel=AsyncMock(),
        set_scan_mode=AsyncMock(), add_wallet=AsyncMock(), remove_wallet=AsyncMock(return_value=True),
        clear_wallets=AsyncMock(return_value=2), get_wallets=AsyncMock(return_value=[]),
        set_dev_tracking=AsyncMock(), get_guild_settings=AsyncMock(return_value={
            "alert_channel_id": 123, "scan_mode": "medium", "dev_tracking": True,
            "fresh_channel_id": 777, "launch_alerts": True,
        }),
        get_active_coins=AsyncMock(return_value=[]),
        set_fresh_channel=AsyncMock(), set_launch_alerts=AsyncMock(),
    )
    scanner = SimpleNamespace(
        _refresh_wallet_index=AsyncMock(),
        fetch_markets=AsyncMock(return_value={}),
        diagnostics=lambda: {
            "queue_size": 0, "pending_verifications": 0, "websocket_connected": True,
            "flow": {"tracked_mints": 1, "total_trades": 3, "last_trade_age": 2},
            "rpc": {"endpoint": "public", "requests_today": 0, "daily_budget": 2000, "paused": False},
        },
    )
    return SimpleNamespace(
        db=db, scanner=scanner, wallet_monitor=None, wallet_tracker=WalletTracker(),
        discord_queue=SimpleNamespace(queue=SimpleNamespace(qsize=lambda: 0)),
        started_at=1.0,
    )


@pytest.mark.asyncio
async def test_only_intended_commands_register():
    bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())
    await bot.load_extension("commands")
    names = {command.name for command in bot.tree.get_commands()}
    assert names == EXPECTED
    await bot.close()


@pytest.mark.asyncio
async def test_all_admin_command_callbacks():
    bot = fake_bot()
    cog = TrackerCommands(bot)
    cog.is_admin = lambda _interaction: True

    inter = interaction()
    channel = SimpleNamespace(id=123, mention="#alerts")
    await TrackerCommands.setchannel.callback(cog, inter, channel)
    bot.db.set_alert_channel.assert_awaited_with(42, 123)

    inter = interaction()
    choice = SimpleNamespace(value="high", name="High only")
    await TrackerCommands.setmode.callback(cog, inter, choice)
    bot.db.set_scan_mode.assert_awaited_with(42, "high")

    inter = interaction()
    await TrackerCommands.addwallet.callback(cog, inter, Config.DEFAULT_WALLET, "Alpha", channel)
    bot.db.add_wallet.assert_awaited_with(42, Config.DEFAULT_WALLET, "Alpha", 123)

    inter = interaction()
    await TrackerCommands.removewallet.callback(cog, inter, Config.DEFAULT_WALLET)
    bot.db.remove_wallet.assert_awaited_with(42, Config.DEFAULT_WALLET)

    inter = interaction()
    await TrackerCommands.resetwallets.callback(cog, inter)
    bot.db.clear_wallets.assert_awaited_with(42)

    inter = interaction()
    attachment = SimpleNamespace(
        filename="wallets.txt", size=100,
        read=AsyncMock(return_value=f"{Config.DEFAULT_WALLET}\n{OTHER}".encode()),
    )
    await TrackerCommands.importwallets.callback(cog, inter, attachment)
    assert bot.db.add_wallet.await_count == 3  # one above + two imported
    inter.followup.send.assert_awaited()

    inter = interaction()
    await TrackerCommands.devtrack.callback(cog, inter, True)
    bot.db.set_dev_tracking.assert_awaited_with(42, True)

    inter = interaction()
    await TrackerCommands.setfreshchannel.callback(cog, inter, SimpleNamespace(id=777, mention="#fresh"))
    bot.db.set_fresh_channel.assert_awaited_with(42, 777)

    inter = interaction()
    await TrackerCommands.togglelaunchalerts.callback(cog, inter, True)
    bot.db.set_launch_alerts.assert_awaited_with(42, True)
    inter = interaction()
    await TrackerCommands.togglelaunchalerts.callback(cog, inter, False)
    bot.db.set_launch_alerts.assert_awaited_with(42, False)


@pytest.mark.asyncio
async def test_all_read_command_callbacks():
    bot = fake_bot()
    cog = TrackerCommands(bot)
    bot.db.get_wallets.return_value = [{
        "label": "Alpha", "address": Config.DEFAULT_WALLET, "channel_id": 123,
    }]
    await TrackerCommands.listwallets.callback(cog, interaction())

    coin = {
        "id": 1,
        "mint_address": Config.DEFAULT_WALLET, "name": "Token", "symbol": "TOK",
        "image_url": None, "score_label": "high", "score": 90,
        "current_market_cap": 30000, "first_seen_market_cap": 20000,
        "dev_wallet": OTHER, "red_flags": "", "last_reject_reason": None,
        "launch_sol": 1.25, "created_at": datetime.now(timezone.utc),
    }
    sibling = {
        "mint_address": OTHER, "name": "Token", "symbol": "TOK",
        "current_market_cap": 9000, "first_seen_market_cap": None,
        "created_at": datetime.now(timezone.utc),
    }
    bot.db.get_coin_by_mint = AsyncMock(return_value=coin)
    bot.db.get_pvp_siblings = AsyncMock(return_value=[sibling])
    bot.db.get_dev_reputation = AsyncMock(return_value={"launched": 5, "rugged": 2})
    bot.db.get_coin_ath = AsyncMock(return_value=45000.0)
    bot.scanner.fetch_markets = AsyncMock(return_value={
        Config.DEFAULT_WALLET: {"market_cap": 35000.0, "liquidity": 8000.0, "volume": 12000.0},
        OTHER: {"market_cap": 9500.0, "liquidity": 2000.0, "volume": 700.0},
    })
    inter = interaction()
    await TrackerCommands.checktoken.callback(cog, inter, Config.DEFAULT_WALLET)
    embed = inter.followup.send.await_args.kwargs["embed"]
    assert embed.title == "Token Inspection"
    fields = {f.name: f.value for f in embed.fields}
    assert "$20,000" in fields["Initial MC / Current MC"] and "$35,000" in fields["Initial MC / Current MC"]
    assert fields["ATH"] == "$45,000"
    assert fields["Volume"] == "$12,000" and fields["LP"] == "$8,000"
    assert f"https://solscan.io/account/{OTHER}" in fields["Dev Wallet"]
    assert fields["Dev History"] == "Launched **5** | Rugged **2**"
    pvp = fields["PVP Siblings (1)"]
    assert f"https://dexscreener.com/solana/{OTHER}" in pvp
    assert "MC $9,500" in pvp and "Vol $700" in pvp and "LP $2,000" in pvp
    assert f"`{Config.DEFAULT_WALLET}`" in embed.description

    # A null/placeholder token is treated as not found.
    bot.db.get_coin_by_mint = AsyncMock(return_value={**coin, "name": None})
    inter = interaction()
    await TrackerCommands.checktoken.callback(cog, inter, Config.DEFAULT_WALLET)
    assert "not found" in inter.followup.send.await_args.args[0]
    bot.db.get_coin_by_mint = AsyncMock(return_value=coin)

    null_coin = {**coin, "name": None, "mint_address": OTHER}
    bot.db.get_recent_coins = AsyncMock(return_value=[coin, null_coin])
    inter = interaction()
    await TrackerCommands.history.callback(cog, inter, 5)
    embed = inter.response.send_message.await_args.kwargs["embed"]
    assert embed.title == "Recent Token Scans"
    assert len(embed.fields) == 1 and "Launch 1.25 SOL" in embed.fields[0].value

    inter = interaction()
    await TrackerCommands.recent.callback(cog, inter, 5, False)
    assert inter.response.send_message.await_args.kwargs["ephemeral"] is False
    embed = inter.response.send_message.await_args.kwargs["embed"]
    assert len(embed.fields) == 1 and "Launch 1.25 SOL" in embed.fields[0].value
    inter = interaction()
    await TrackerCommands.recent.callback(cog, inter, 5)
    assert inter.response.send_message.await_args.kwargs["ephemeral"] is True

    bot.db.get_top_coins = AsyncMock(return_value=[coin, null_coin])
    inter = interaction()
    await TrackerCommands.top.callback(cog, inter, 5)
    embed = inter.response.send_message.await_args.kwargs["embed"]
    assert len(embed.fields) == 1

    hit = {
        "multiplier": 5, "market_cap_at_milestone": 100000, "hit_at": datetime.now(timezone.utc),
        "mint_address": Config.DEFAULT_WALLET, "name": "Token", "symbol": "TOK",
    }
    bot.db.get_recent_milestones = AsyncMock(return_value=[hit, {**hit, "name": ""}])
    inter = interaction()
    await TrackerCommands.milestones.callback(cog, inter, 5)
    embed = inter.response.send_message.await_args.kwargs["embed"]
    assert len(embed.fields) == 1 and "**5x**" in embed.fields[0].value

    inter = interaction()
    await TrackerCommands.status.callback(cog, inter)
    embed = inter.response.send_message.await_args.kwargs["embed"]
    assert embed.title == "Bot Status"
    assert "Early Flow" in [field.name for field in embed.fields]
