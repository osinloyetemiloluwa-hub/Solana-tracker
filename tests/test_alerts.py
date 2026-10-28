import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from bot import DiscordSendQueue
from config import Config
from embeds import (
    create_fresh_callout_embed, create_token_alert_embed, dev_wallet_link, pvp_ca_link,
)
from enricher import has_valid_identity
from scanner import Scanner


MINT = "So11111111111111111111111111111111111111112"
DEV = Config.DEFAULT_WALLET


def token_data():
    return {
        "mint_address": MINT, "name": "Token", "symbol": "TOK",
        "program_type": "pumpfun", "market_cap": 10000, "liquidity": 5000,
        "volume_24h": 10000, "created_at": 1.0,
    }


def score_data():
    return {"score": 30, "label": "low", "band": "low", "reasons": []}


def market(mc=10000):
    return {
        "market_cap": mc, "liquidity": 5000, "volume_24h": 10000,
        "volume_1h": 5000, "buys_1h": 10, "sells_1h": 2,
        "price_change_h1": 5, "image_url": None,
    }


def fake_db():
    return SimpleNamespace(
        get_coin_by_mint=AsyncMock(return_value={"alert_attempts": 0}),
        reject_alert=AsyncMock(return_value=1), deactivate_coin=AsyncMock(),
        get_all_guilds=AsyncMock(return_value=[42]),
        get_guild_settings=AsyncMock(return_value={"alert_channel_id": 123, "scan_mode": "all"}),
        update_coin_alert=AsyncMock(), confirm_alert=AsyncMock(),
        set_rugged=AsyncMock(return_value=True),
    )


@pytest.mark.asyncio
async def test_verified_send_uses_original_ui_and_confirms(monkeypatch):
    monkeypatch.setattr(Config, "PRE_ALERT_VERIFY_SECONDS", 0)
    db = fake_db()
    bot = SimpleNamespace(send_discord_alert=AsyncMock(return_value=SimpleNamespace(id=999)))
    scanner = Scanner(bot, db)
    scanner.get_market_data_with_fallback = AsyncMock(return_value=market())

    await scanner._verified_send(1, token_data(), score_data())

    db.confirm_alert.assert_awaited_with(1)
    kwargs = bot.send_discord_alert.await_args.kwargs
    assert kwargs["embed"].title == "🚀 New PUMPFUN Token"
    assert kwargs["view"] is not None
    db.update_coin_alert.assert_awaited_with(1, 999, 123)


@pytest.mark.asyncio
async def test_dev_sell_rejects_without_market_api(monkeypatch):
    monkeypatch.setattr(Config, "PRE_ALERT_VERIFY_SECONDS", 0)
    db = fake_db()
    bot = SimpleNamespace(send_discord_alert=AsyncMock())
    scanner = Scanner(bot, db)
    scanner.get_market_data_with_fallback = AsyncMock()
    scanner.flow.register({"mint": MINT, "traderPublicKey": DEV})
    scanner.flow.on_trade({
        "mint": MINT, "txType": "sell", "traderPublicKey": DEV,
        "solAmount": 1, "signature": "rug",
    })

    await scanner._verified_send(1, token_data(), score_data())

    scanner.get_market_data_with_fallback.assert_not_awaited()
    db.reject_alert.assert_awaited()
    db.deactivate_coin.assert_awaited_with(1)
    db.set_rugged.assert_awaited()
    bot.send_discord_alert.assert_not_awaited()


@pytest.mark.asyncio
async def test_mc_drop_is_retryable_not_permanent(monkeypatch):
    monkeypatch.setattr(Config, "PRE_ALERT_VERIFY_SECONDS", 0)
    db = fake_db()
    bot = SimpleNamespace(send_discord_alert=AsyncMock())
    scanner = Scanner(bot, db)
    scanner.get_market_data_with_fallback = AsyncMock(return_value=market(mc=5000))

    await scanner._verified_send(1, token_data(), score_data())

    db.reject_alert.assert_awaited()
    db.deactivate_coin.assert_not_awaited()
    bot.send_discord_alert.assert_not_awaited()


@pytest.mark.asyncio
async def test_discord_sender_disables_mentions():
    channel = SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(id=1)))
    bot = SimpleNamespace(get_channel=lambda _cid: channel)
    queue = DiscordSendQueue(bot)
    result = await queue._send_with_backoff({
        "channel_id": 123, "content": None, "embed": None, "view": None,
    })
    assert result.id == 1
    mentions = channel.send.await_args.kwargs["allowed_mentions"]
    assert mentions.everyone is False and mentions.users is False and mentions.roles is False


BUYER = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
FRESH_CHANNEL = 777


def make_scanner(targets=((42, FRESH_CHANNEL),)):
    db = SimpleNamespace(
        get_fresh_targets=AsyncMock(return_value=list(targets)),
        set_rugged=AsyncMock(return_value=True),
        get_dev_reputation=AsyncMock(return_value={"launched": 4, "rugged": 1}),
        get_pvp_siblings=AsyncMock(return_value=[]),
    )
    bot = SimpleNamespace(send_discord_alert=AsyncMock(return_value=SimpleNamespace(id=1)))
    scanner = Scanner(bot, db)
    scanner._sol_usd = AsyncMock(return_value=100.0)
    scanner._dexscreener_market_data = AsyncMock(return_value=None)
    return scanner, db, bot


def create_event(mcap_sol=30.0, name="Pepe", symbol="PEPE", mint=MINT, sol=1.5):
    return {
        "mint": mint, "txType": "create", "traderPublicKey": DEV,
        "name": name, "symbol": symbol, "marketCapSol": mcap_sol, "solAmount": sol,
    }


def token_from(event):
    return {
        "mint_address": event["mint"], "program_type": "pumpfun",
        "name": event["name"], "symbol": event["symbol"], "dev_wallet": DEV,
        "launch_sol": event.get("solAmount"), "created_at": 1.0, "raw_event": event,
    }


async def run_fresh(scanner, event, trade_mcap=None):
    scanner.flow.register(event)
    if trade_mcap is not None:
        scanner.flow.on_trade({
            "mint": event["mint"], "txType": "buy", "traderPublicKey": BUYER,
            "solAmount": 1.0, "marketCapSol": trade_mcap, "signature": "s1",
        })
    await scanner._maybe_start_fresh_scan(token_from(event))
    if scanner._fresh_tasks:
        await asyncio.gather(*list(scanner._fresh_tasks))


def zero_delay():
    old = Config.PRE_ALERT_VERIFY_SECONDS
    Config.PRE_ALERT_VERIFY_SECONDS = 0
    return old


def test_identity_filter():
    assert has_valid_identity("Pepe", "PEPE", MINT)
    for name, symbol in [
        (None, "X"), ("X", None), ("", "X"), ("  ", "X"), ("null", "X"), ("X", "None"),
        ("undefined", "X"), ("Token AbCdEf", "ABCD"), ("\u200b", "X"), ("X", "???"),
    ]:
        assert not has_valid_identity(name, symbol, MINT), (name, symbol)
    assert not has_valid_identity("Pepe", "PEPE", None)
    assert not has_valid_identity("Pepe", "PEPE", "short")


def test_default_verify_delay_is_five_seconds():
    assert Config.PRE_ALERT_VERIFY_SECONDS == 5


@pytest.mark.asyncio
async def test_growth_sends_callout_with_change():
    old = zero_delay()
    try:
        scanner, db, bot = make_scanner()
        await run_fresh(scanner, create_event(30.0), trade_mcap=33.0)
    finally:
        Config.PRE_ALERT_VERIFY_SECONDS = old
    assert bot.send_discord_alert.await_count == 1
    kwargs = bot.send_discord_alert.await_args
    assert kwargs.args[0] == FRESH_CHANNEL
    embed = kwargs.kwargs["embed"]
    assert "$3,000 → **$3,300** (+10.0%)" == embed.f("Initial MC → Current MC")
    assert f"`{MINT}`" in embed.description and "](" not in embed.description
    assert "solscan.io/account/" + DEV in embed.f("Dev Wallet")
    assert embed.f("Launch Buy") == "1.50 SOL"
    assert embed.f("Dev Rep") == "Launched **4** | Rugged **1**"


@pytest.mark.asyncio
async def test_hold_exactly_sends():
    old = zero_delay()
    try:
        scanner, db, bot = make_scanner()
        await run_fresh(scanner, create_event(30.0), trade_mcap=30.0)
    finally:
        Config.PRE_ALERT_VERIFY_SECONDS = old
    assert bot.send_discord_alert.await_count == 1


@pytest.mark.asyncio
async def test_drop_over_25_percent_is_silent_and_recorded():
    old = zero_delay()
    try:
        scanner, db, bot = make_scanner()
        await run_fresh(scanner, create_event(30.0), trade_mcap=22.0)  # -26.7%
    finally:
        Config.PRE_ALERT_VERIFY_SECONDS = old
    bot.send_discord_alert.assert_not_awaited()
    db.set_rugged.assert_awaited()


@pytest.mark.asyncio
async def test_drop_of_exactly_25_percent_still_sends():
    old = zero_delay()
    try:
        scanner, db, bot = make_scanner()
        await run_fresh(scanner, create_event(40.0), trade_mcap=30.0)  # exactly -25%
    finally:
        Config.PRE_ALERT_VERIFY_SECONDS = old
    assert bot.send_discord_alert.await_count == 1


@pytest.mark.asyncio
async def test_unverifiable_is_suppressed():
    old = zero_delay()
    try:
        scanner, db, bot = make_scanner()
        await run_fresh(scanner, create_event(30.0), trade_mcap=None)  # no trades, no Dex data
    finally:
        Config.PRE_ALERT_VERIFY_SECONDS = old
    bot.send_discord_alert.assert_not_awaited()


@pytest.mark.asyncio
async def test_dexscreener_can_verify_when_no_trades():
    old = zero_delay()
    try:
        scanner, db, bot = make_scanner()
        scanner._dexscreener_market_data = AsyncMock(return_value={
            "market_cap": 3200.0, "liquidity": 4000.0, "volume_24h": 900.0, "image_url": None,
        })
        await run_fresh(scanner, create_event(30.0), trade_mcap=None)
    finally:
        Config.PRE_ALERT_VERIFY_SECONDS = old
    embed = bot.send_discord_alert.await_args.kwargs["embed"]
    assert embed.f("Liquidity") == "$4,000" and embed.f("Volume") == "$900"


@pytest.mark.asyncio
async def test_toggle_off_or_no_channel_sends_nothing():
    old = zero_delay()
    try:
        scanner, db, bot = make_scanner(targets=())
        await run_fresh(scanner, create_event(30.0), trade_mcap=33.0)
    finally:
        Config.PRE_ALERT_VERIFY_SECONDS = old
    assert not scanner._fresh_tasks
    bot.send_discord_alert.assert_not_awaited()


@pytest.mark.asyncio
async def test_null_tokens_are_ignored():
    old = zero_delay()
    try:
        scanner, db, bot = make_scanner()
        for name, symbol in [(None, "X"), ("X", ""), ("null", "X"), ("Token AbCdEf", "ABCD")]:
            await run_fresh(scanner, create_event(name=name, symbol=symbol), trade_mcap=33.0)
    finally:
        Config.PRE_ALERT_VERIFY_SECONDS = old
    bot.send_discord_alert.assert_not_awaited()


@pytest.mark.asyncio
async def test_duplicate_create_events_send_once():
    old = zero_delay()
    try:
        scanner, db, bot = make_scanner()
        event = create_event(30.0)
        scanner.flow.register(event)
        scanner.flow.on_trade({
            "mint": MINT, "txType": "buy", "traderPublicKey": BUYER,
            "solAmount": 1.0, "marketCapSol": 33.0, "signature": "s1",
        })
        for _ in range(3):
            await scanner._maybe_start_fresh_scan(token_from(event))
        await asyncio.gather(*list(scanner._fresh_tasks))
    finally:
        Config.PRE_ALERT_VERIFY_SECONDS = old
    assert bot.send_discord_alert.await_count == 1


@pytest.mark.asyncio
async def test_dev_sell_is_suppressed_and_recorded():
    old = zero_delay()
    try:
        scanner, db, bot = make_scanner()
        event = create_event(30.0)
        scanner.flow.register(event)
        scanner.flow.on_trade({
            "mint": MINT, "txType": "sell", "traderPublicKey": DEV,
            "solAmount": 1.0, "marketCapSol": 31.0, "signature": "dev-sell",
        })
        await scanner._maybe_start_fresh_scan(token_from(event))
        await asyncio.gather(*list(scanner._fresh_tasks))
    finally:
        Config.PRE_ALERT_VERIFY_SECONDS = old
    bot.send_discord_alert.assert_not_awaited()
    db.set_rugged.assert_awaited()


@pytest.mark.asyncio
async def test_sol_denominated_when_price_unavailable():
    old = zero_delay()
    try:
        scanner, db, bot = make_scanner()
        scanner._sol_usd = AsyncMock(return_value=None)
        await run_fresh(scanner, create_event(30.0), trade_mcap=33.0)
    finally:
        Config.PRE_ALERT_VERIFY_SECONDS = old
    embed = bot.send_discord_alert.await_args.kwargs["embed"]
    assert embed.f("Initial MC → Current MC") == "30.0 SOL → **33.0 SOL** (+10.0%)"


def test_link_formatting_rules():
    assert dev_wallet_link(DEV).endswith(f"](https://solscan.io/account/{DEV})")
    assert pvp_ca_link(BUYER).endswith(f"](https://dexscreener.com/solana/{BUYER})")
    assert dev_wallet_link(None) is None


def test_normal_alert_embed_keeps_plain_ca_and_links_dev():
    embed = create_token_alert_embed({
        "mint_address": MINT, "name": "Pepe", "symbol": "PEPE", "program_type": "pumpfun",
        "market_cap": 15000, "liquidity": 5000, "volume_24h": 8000, "dev_wallet": DEV,
        "dev_rep": {"launched": 2, "rugged": 0},
        "pvp_siblings": [{"mint_address": BUYER, "market_cap": 9000.0, "age_seconds": 30}],
    }, {"band": "medium"}, None)
    assert f"`{MINT}`" in embed.description and "](" not in embed.description
    assert f"https://solscan.io/account/{DEV}" in embed.f("Dev Wallet")
    assert f"https://dexscreener.com/solana/{BUYER}" in embed.f("PVP Siblings")
    text = " ".join(str(f.value) for f in embed.fields) + embed.description
    assert "None" not in text and "null" not in text


def test_callout_omits_missing_fields():
    embed = create_fresh_callout_embed({
        "mint_address": MINT, "name": "Pepe", "symbol": "PEPE",
        "initial_mc": 3000, "current_mc": 3300, "mc_unit": "usd", "change_pct": 10.0,
        "liquidity": None, "volume": 0, "launch_sol": None, "dev_wallet": None,
    })
    names = [f.name for f in embed.fields]
    assert names == ["Initial MC → Current MC"]


@pytest.mark.asyncio
async def test_pumpportal_reports_launch_sol_only_for_creates():
    from pumpportal import PumpPortalDetector
    tokens = []

    async def collect(token):
        tokens.append(token)

    detector = PumpPortalDetector(collect, None)
    detector._ws = SimpleNamespace(send=AsyncMock())
    await detector._handle_event(create_event(sol=1.5))
    await detector._handle_event({"mint": MINT, "txType": "migrate"})
    assert tokens[0]["launch_sol"] == 1.5
    assert tokens[1]["launch_sol"] is None
