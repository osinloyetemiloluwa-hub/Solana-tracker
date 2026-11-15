"""Version 2 tests: history filters, dynamic milestones, per-guild filters, API,
cleanup windows, feeds, volume, hygiene fixes."""
import ast
import asyncio
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import api
from alert_filters import evaluate_filters
from cache import TTLCache
from commands import TrackerCommands
from config import Config
from db import Database, build_history_query
from dexscreener import DexScreenerFeeds
from embeds import (
    create_fresh_callout_embed, create_migration_embed, create_milestone_embed,
    create_token_alert_embed, create_wallet_alert_embed, create_wallet_buy_alert_embed,
)
from enricher import has_valid_identity
from history_filter import FilterError, parse_history_filter
from milestones import MilestoneTracker
from scanner import Scanner
from trade_flow import TradeFlowTracker
from volume import approx_buy_volume, compute_volume_metrics

MINT = "So11111111111111111111111111111111111111112"
WSOL = MINT
OTHER = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
DEV = "7BNaxx6KdUYrjACNQZ9He26NBFoFxujQMAfNLnArLGH5"
ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------- history filters

def test_history_filter_grammar():
    assert parse_history_filter(None) == {}
    parsed = parse_history_filter("volume>5k mc:5k-20k band:high age<30m")
    assert parsed == {
        "volume": (5000.0, None), "mc": (5000.0, 20000.0),
        "band": ["high"], "age": (None, 1800.0),
    }
    assert parse_history_filter("high medium buyvol>=$2.5k")["band"] == ["high", "medium"]
    for bad in ("volume", "foo>3", "mc>abc", "band:purple", "age>5x", "mc>10k mc<5k"):
        with pytest.raises(FilterError):
            parse_history_filter(bad)


def test_history_query_is_parameterised():
    sql, params = build_history_query(parse_history_filter("volume>5k mc:5k-20k band:high age<30m"), 8)
    assert "volume_1h >= $3::numeric" in sql and "<= $" in sql and "score_label = ANY(" in sql
    assert "pair_created_at" in sql and sql.strip().endswith(f"LIMIT ${len(params)}")
    assert 5000.0 in params and ["high"] in params and params[-1] == 8
    assert "5000" not in sql  # values are bound, never interpolated
    plain_sql, plain_params = build_history_query({}, 3)
    assert plain_params == [3] and "ANY" not in plain_sql


def history_bot(rows):
    db = SimpleNamespace(
        get_history=AsyncMock(return_value=rows), get_recent_coins=AsyncMock(return_value=rows),
    )
    return SimpleNamespace(db=db)


def coin_row():
    return {
        "mint_address": MINT, "name": "Token", "symbol": "TOK", "score_label": "high",
        "current_market_cap": 30000, "first_seen_market_cap": 20000, "launch_sol": 1.0,
        "created_at": datetime.now(timezone.utc), "red_flags": "",
    }


def inter():
    return SimpleNamespace(
        guild_id=42, response=SimpleNamespace(send_message=AsyncMock()),
        followup=SimpleNamespace(send=AsyncMock()),
    )


@pytest.mark.asyncio
async def test_history_command_filter_and_default():
    bot = history_bot([coin_row()])
    cog = TrackerCommands(bot)

    i = inter()
    await TrackerCommands.history.callback(cog, i, 5, True, "band:high volume>1k")
    bot.db.get_history.assert_awaited_once()
    conditions = bot.db.get_history.await_args.args[0]
    assert conditions["band"] == ["high"] and conditions["volume"] == (1000.0, None)
    bot.db.get_recent_coins.assert_not_awaited()
    assert i.response.send_message.await_args.kwargs["ephemeral"] is True

    # No filter: exactly the old behaviour.
    bot = history_bot([coin_row()])
    cog = TrackerCommands(bot)
    i = inter()
    await TrackerCommands.history.callback(cog, i, 5)
    bot.db.get_recent_coins.assert_awaited_once_with(5)
    bot.db.get_history.assert_not_awaited()
    assert i.response.send_message.await_args.kwargs["embed"].title == "Recent Token Scans"

    # Bad filter: friendly error, no query.
    i = inter()
    await TrackerCommands.history.callback(cog, i, 5, True, "nonsense>1")
    assert "Unknown filter" in i.response.send_message.await_args.args[0]


# ---------------------------------------------------------------- milestone ladder

class MemoryMilestoneDB:
    """Mirrors raise_max_multiple's SQL: only a strictly higher rung raises it."""

    def __init__(self, start=None):
        self.max = start
        self.inserted = []

    async def raise_max_multiple(self, coin_id, multiple):
        if (self.max if self.max is not None else 1) < multiple:
            self.max = multiple
            return True
        return False

    async def insert_milestone(self, coin_id, multiplier, price, mc):
        self.inserted.append(multiplier)


def test_ladder_rungs():
    from milestones import rung_for
    expected = {1.0: None, 1.99: None, 2: 2, 4.9: 2, 5: 5, 9.99: 5, 10: 10, 24.9: 10,
                25: 25, 49: 25, 50: 50, 99.9: 50, 100: 100, 250: 200, 999: 900, 1000: 1000}
    for multiple, rung in expected.items():
        assert rung_for(multiple) == rung, multiple
    assert rung_for(None) is None


@pytest.mark.asyncio
async def test_milestones_fire_once_per_rung_and_never_on_drops():
    db = MemoryMilestoneDB()
    tracker = MilestoneTracker(db)
    fired = []
    ath = 1000
    for step in range(0, 1500):                      # a smooth climb from 1x to 16x
        ath = 1000 + step * 10
        result = await tracker.check(1, 1000, ath, ath, 0.5)
        if result:
            fired.append(result["multiplier"])
    assert fired == [2.0, 5.0, 10.0]                 # three embeds, not hundreds
    assert db.max == 10.0
    assert await tracker.check(1, 1000, ath, 3000, 0.1) is None   # fell back: nothing, mark kept
    assert db.max == 10.0
    assert (await tracker.check(1, 1000, 30000, 29000, 0.1))["multiplier"] == 25.0


@pytest.mark.asyncio
async def test_milestones_use_the_ath_and_need_2x():
    db = MemoryMilestoneDB()
    tracker = MilestoneTracker(db)
    assert await tracker.check(1, 1000, 1990, 1990, 1) is None        # below 2x
    assert await tracker.check(1, None, 5000, 5000, 1) is None        # no initial market cap
    assert await tracker.check(1, 1000, None, 5000, 1) is None        # no ATH yet
    hit = await tracker.check(1, 1000, 2600, 900, 1)                  # spiked to 2.6x, now back below
    assert hit["multiplier"] == 2.0 and hit["market_cap"] == 900
    assert not hasattr(Config, "MILESTONE_MULTIPLIERS")


def test_milestone_embed_footer_title_and_caller():
    coin = {"name": "A", "symbol": "B", "mint_address": MINT}
    embed = create_milestone_embed(coin, 5.0, 0.001, 5000)
    assert embed.footer.text == "Solana Meme Tracker | Milestone"
    assert embed.title == "📈 5x Milestone Hit!"
    assert "First Caller" not in [f.name for f in embed.fields]
    called = create_milestone_embed(coin, 5.0, 0.001, 5000, caller={
        "user_id": 77, "mc_at_call": 1200, "url": "https://discord.com/channels/1/2/3"})
    value = called.f("First Caller")
    assert "<@77>" in value and "$1,200" in value and "channels/1/2/3" in value


def refresh_row(**over):
    row = {
        "id": 7, "mint_address": MINT, "name": "Pepe", "symbol": "PEPE", "program_type": "pumpfun",
        "first_seen_market_cap": 10000, "current_market_cap": 25000, "ath_market_cap": 26000,
        "current_price": 0.01, "created_at": datetime.now(timezone.utc), "pair_created_at": None,
        "dev_wallet": None, "alerted_low": False, "alerted_medium": False, "alerted_high": True,
    }
    row.update(over)
    return row


def refresh_db(row, **extra):
    db = SimpleNamespace(
        update_market=AsyncMock(return_value=row),
        raise_max_multiple=AsyncMock(return_value=True), insert_milestone=AsyncMock(),
        get_all_guilds=AsyncMock(return_value=[42, 43]),
        get_guild_settings=AsyncMock(side_effect=lambda g: {"alert_channel_id": 500 + g, "scan_mode": "all"}),
        deactivate_coin=AsyncMock(),
    )
    for key, value in extra.items():
        setattr(db, key, value)
    return db


async def run_refresh(scanner, calls=None):
    await scanner._apply_refresh({"id": 7, "mint_address": MINT}, {"price": 0.01, "market_cap": 25000}, 100.0, calls)
    await asyncio.gather(*list(scanner._milestone_tasks))


@pytest.mark.asyncio
async def test_milestone_only_announced_for_alerted_tokens():
    bot = SimpleNamespace(send_discord_alert=AsyncMock())
    scanner = Scanner(bot, refresh_db(refresh_row()))
    await run_refresh(scanner)                                         # alerted: 2.6x ATH -> 2x
    assert sorted(c.args[0] for c in bot.send_discord_alert.await_args_list) == [542, 543]
    assert bot.send_discord_alert.await_args.kwargs["embed"].f("Multiplier") == "**2x**"

    quiet_bot = SimpleNamespace(send_discord_alert=AsyncMock())
    quiet_db = refresh_db(refresh_row(alerted_high=False))
    scanner = Scanner(quiet_bot, quiet_db)
    await run_refresh(scanner)                                         # never alerted: stays quiet
    quiet_bot.send_discord_alert.assert_not_awaited()
    quiet_db.raise_max_multiple.assert_awaited_once()                  # but the rung is recorded
    quiet_db.deactivate_coin.assert_not_awaited()


@pytest.mark.asyncio
async def test_first_caller_gets_the_milestone_in_the_channel_they_called_in():
    call = {"id": 3, "mint_address": MINT, "guild_id": 42, "channel_id": 900, "message_id": 5,
            "user_id": 77, "mc_at_call": 4000, "peak_mc": 4000}
    db = refresh_db(
        refresh_row(),
        advance_call=AsyncMock(return_value={**call, "peak_mc": 25000}),
        raise_call_multiple=AsyncMock(return_value=True),
    )
    bot = SimpleNamespace(send_discord_alert=AsyncMock())
    scanner = Scanner(bot, db)
    await run_refresh(scanner, [call])
    sent = {c.args[0]: c.kwargs["embed"] for c in bot.send_discord_alert.await_args_list}
    assert set(sent) == {543, 900}                       # guild 42 skipped for the general embed
    assert sent[900].f("Multiplier") == "**5x**"         # 25000 / 4000 = 6.25x -> 5x rung
    assert "<@77>" in sent[900].f("First Caller")
    assert "First Caller" not in [f.name for f in sent[543].fields]
    db.raise_call_multiple.assert_awaited_with(3, 5.0)


@pytest.mark.asyncio
async def test_call_milestone_falls_back_to_alert_channel():
    call = {"id": 3, "mint_address": MINT, "guild_id": 42, "channel_id": 900, "message_id": 5,
            "user_id": 77, "mc_at_call": 4000, "peak_mc": 25000}
    db = refresh_db(refresh_row(alerted_high=False))
    bot = SimpleNamespace(send_discord_alert=AsyncMock(side_effect=[RuntimeError("no access"), None]))
    scanner = Scanner(bot, db)
    await scanner._send_call_milestone(refresh_row(), call, 5.0, 25000)
    assert [c.args[0] for c in bot.send_discord_alert.await_args_list] == [900, 542]


@pytest.mark.asyncio
async def test_not_yet_priced_call_gets_its_base_filled_in():
    call = {"id": 3, "mint_address": MINT, "guild_id": 42, "channel_id": 900, "message_id": 5,
            "user_id": 77, "mc_at_call": None, "peak_mc": None}
    db = refresh_db(
        refresh_row(alerted_high=False),
        advance_call=AsyncMock(return_value={**call, "mc_at_call": 25000, "peak_mc": 25000}),
        raise_call_multiple=AsyncMock(),
    )
    scanner = Scanner(SimpleNamespace(send_discord_alert=AsyncMock()), db)
    await run_refresh(scanner, [call])
    db.advance_call.assert_awaited_with(3, 25000)
    db.raise_call_multiple.assert_not_awaited()                        # 1x since the call


# ---------------------------------------------------------------- callers

def test_extract_mints():
    from callers import extract_mints
    text = f"ape {OTHER} now! https://pump.fun/coin/{MINT} and again {OTHER}, wsol {WSOL}"
    assert extract_mints(text) == [OTHER]            # WSOL is a quote mint; duplicates collapse
    assert extract_mints(f"x{OTHER}y") == [OTHER] or extract_mints(f"x{OTHER}y") == []
    assert extract_mints("hello world 12345") == [] and extract_mints(None) == []
    many = " ".join(f"{OTHER[:-1]}{c}" for c in "ABCDEFGH")
    assert len(extract_mints(many)) <= Config.CALL_MAX_PER_MESSAGE


def discord_message(text, *, bot=False, guild=42, channel=900, mid=5, uid=77):
    return SimpleNamespace(
        content=text, id=mid, author=SimpleNamespace(bot=bot, id=uid, display_name="anon"),
        guild=SimpleNamespace(id=guild) if guild else None, channel=SimpleNamespace(id=channel),
    )


def call_db(coin=None):
    return SimpleNamespace(
        get_coin_by_mint=AsyncMock(return_value=coin),
        upsert_discovered=AsyncMock(return_value=(9, True)),
        update_market=AsyncMock(return_value={"id": 9, "current_market_cap": 5000, "program_type": "pumpswap"}),
        add_call=AsyncMock(return_value=1),
    )


@pytest.mark.asyncio
async def test_unknown_address_without_market_is_not_added_and_not_retried():
    from callers import CallTracker
    db = call_db(None)
    scanner = SimpleNamespace(lookup_market=AsyncMock(return_value=None))
    tracker = CallTracker(db, scanner)
    await tracker.handle_message(discord_message(f"gm {OTHER}"))
    await tracker.handle_message(discord_message(f"again {OTHER}"))
    db.upsert_discovered.assert_not_awaited()
    db.add_call.assert_not_awaited()
    scanner.lookup_market.assert_awaited_once()                       # negative-cached


@pytest.mark.asyncio
async def test_new_real_token_is_added_and_first_call_recorded():
    from callers import CallTracker
    db = call_db(None)
    market = {"name": "Foo", "symbol": "FOO", "market_cap": 5000, "dex_id": "pumpswap", "image_url": None}
    tracker = CallTracker(db, SimpleNamespace(lookup_market=AsyncMock(return_value=market)))
    await tracker.handle_message(discord_message(f"ca: {OTHER}"))
    args, kwargs = db.upsert_discovered.await_args
    assert args[:2] == (OTHER, "pumpswap") and kwargs["source"] == "call" and kwargs["revive"] is True
    db.update_market.assert_awaited_once_with(9, market)              # initial market cap set now
    assert db.add_call.await_args.args == (9, OTHER, 42, 900, 5, 77, "anon", 5000)


@pytest.mark.asyncio
async def test_existing_token_is_retracked_and_bots_and_dms_are_ignored():
    from callers import CallTracker
    coin = {"id": 4, "program_type": "pumpfun", "current_market_cap": 12000, "first_seen_market_cap": 3000,
            "mint_address": OTHER, "last_tracked_at": datetime.now(timezone.utc) - timedelta(hours=3)}
    db = call_db(coin)
    scanner = Scanner(SimpleNamespace(), db)
    scanner.lookup_market = AsyncMock(return_value={"market_cap": 12500.0, "price": 1.0})
    tracker = CallTracker(db, scanner)
    await tracker.handle_message(discord_message(OTHER))
    scanner.lookup_market.assert_awaited_once()                       # always read live
    assert db.upsert_discovered.await_args.kwargs["revive"] is True
    assert db.add_call.await_args.args[-1] == 12500.0                 # the live price at the call
    db.add_call.reset_mock()
    await tracker.handle_message(discord_message(OTHER, bot=True))
    await tracker.handle_message(discord_message(OTHER, guild=None))
    await tracker.handle_message(discord_message(""))
    db.add_call.assert_not_awaited()


@pytest.mark.asyncio
async def test_call_entry_price_is_the_callers_own_not_the_tokens_initial():
    """Token started at $3k, trades at $50k when called: the call records $50k."""
    from callers import CallTracker
    coin = {"id": 4, "program_type": "pumpfun", "current_market_cap": 49000, "first_seen_market_cap": 3000,
            "mint_address": OTHER, "last_tracked_at": datetime.now(timezone.utc)}
    db = call_db(coin)
    scanner = Scanner(SimpleNamespace(), db)
    scanner.lookup_market = AsyncMock(return_value={"market_cap": 50000.0, "price": 1.0})
    await CallTracker(db, scanner).handle_message(discord_message(OTHER))
    assert db.add_call.await_args.args[-1] == 50000.0
    assert coin["first_seen_market_cap"] == 3000                       # the token's own record is untouched


@pytest.mark.asyncio
async def test_call_price_falls_back_only_to_a_fresh_stored_value():
    from callers import CallTracker
    base = {"id": 4, "program_type": "pumpfun", "current_market_cap": 8000, "mint_address": OTHER}
    for age, expected in ((5, 8000), (600, None)):
        coin = {**base, "last_tracked_at": datetime.now(timezone.utc) - timedelta(seconds=age)}
        db = call_db(coin)
        scanner = Scanner(SimpleNamespace(), db)
        scanner.lookup_market = AsyncMock(return_value=None)           # DexScreener gap
        await CallTracker(db, scanner).handle_message(discord_message(OTHER))
        assert db.add_call.await_args.args[-1] == expected             # None -> filled by the next refresh


@pytest.mark.asyncio
async def test_call_at_a_glitch_print_does_not_record_the_glitch():
    from callers import CallTracker
    coin = {"id": 4, "program_type": "pumpfun", "current_market_cap": 10000, "mint_address": OTHER,
            "last_tracked_at": datetime.now(timezone.utc)}
    db = call_db(coin)
    scanner = Scanner(SimpleNamespace(), db)
    scanner.lookup_market = AsyncMock(return_value={"market_cap": 900000.0, "price": 9.0})  # 90x bad tick
    await CallTracker(db, scanner).handle_message(discord_message(OTHER))
    assert db.add_call.await_args.args[-1] == 10000                    # fresh stored value instead
    db.update_market.assert_not_awaited()


# ---------------------------------------------------------------- market accuracy

def test_pair_choice_prefers_a_trading_pair_over_a_dead_deep_one():
    from dexscreener import pick_best_pair
    dead = {"baseToken": {"address": MINT}, "quoteToken": {"address": WSOL}, "liquidity": {"usd": 900000},
            "volume": {"h24": 0}, "priceUsd": "9", "marketCap": 9000000}
    live = {"baseToken": {"address": MINT}, "quoteToken": {"address": WSOL}, "liquidity": {"usd": 20000},
            "volume": {"h24": 5000}, "priceUsd": "1", "marketCap": 1000000}
    exotic = {"baseToken": {"address": MINT}, "quoteToken": {"address": OTHER}, "liquidity": {"usd": 90000},
              "volume": {"h24": 5000}, "priceUsd": "1.1", "marketCap": 1100000}
    assert pick_best_pair([dead, live, exotic], MINT) is live
    deeper = {**live, "liquidity": {"usd": 70000}}
    assert pick_best_pair([live, deeper], MINT) is deeper


def guard_scanner():
    return Scanner(SimpleNamespace(), SimpleNamespace())


def test_bad_tick_is_held_then_accepted_only_when_confirmed():
    scanner = guard_scanner()
    coin = {"mint_address": OTHER, "current_market_cap": 10000}
    ok = scanner.accept_market(coin, {"market_cap": 14000, "price": 1.4, "volume_1h": 5})
    assert ok["market_cap"] == 14000                                   # normal move: untouched
    spike = {"market_cap": 400000, "price": 40.0, "volume_1h": 5}
    held = scanner.accept_market(coin, spike)
    assert held["market_cap"] is None and held["price"] is None and held["volume_1h"] == 5
    assert scanner.market_stats["held"] == 1
    again = scanner.accept_market(coin, {"market_cap": 410000, "price": 41.0})   # repeats: real
    assert again["market_cap"] == 410000 and scanner.market_stats["confirmed"] == 1
    # a one-off glitch that does not repeat never gets through
    scanner2 = guard_scanner()
    scanner2.accept_market(coin, {"market_cap": 400000, "price": 40.0})
    back = scanner2.accept_market(coin, {"market_cap": 11000, "price": 1.1})
    assert back["market_cap"] == 11000


def test_jump_confirmed_by_live_trades_and_collapse_guard():
    scanner = guard_scanner()
    coin = {"mint_address": OTHER, "current_market_cap": 10000}
    assert scanner.accept_market(coin, {"market_cap": 300000}, observed_mc=280000)["market_cap"] == 300000
    held = scanner.accept_market(coin, {"market_cap": 50, "price": 0.001})       # collapse to ~0
    assert held["market_cap"] is None
    assert scanner.accept_market({"mint_address": "X"}, {"market_cap": 5})["market_cap"] == 5   # first sample


@pytest.mark.asyncio
async def test_held_tick_never_reaches_ath_or_milestones():
    row = refresh_row()
    db = refresh_db(row)
    bot = SimpleNamespace(send_discord_alert=AsyncMock())
    scanner = Scanner(bot, db)
    coin = {"id": 7, "mint_address": MINT, "current_market_cap": 10000}
    await scanner._apply_refresh(coin, {"price": 9.0, "market_cap": 900000.0, "liquidity": 1}, 100.0)
    written = db.update_market.await_args.args[1]
    assert written["market_cap"] is None and written["price"] is None
    bot.send_discord_alert.assert_not_awaited()


@pytest.mark.asyncio
async def test_cleanup_reports_held_ticks(capsys):
    db = SimpleNamespace(run_lifecycle_cleanup=AsyncMock(return_value={}), remove_inactive_wallets=AsyncMock(return_value=0))
    scanner = Scanner(SimpleNamespace(), db)
    scanner.market_stats["held"] = 3
    await scanner.run_cleanup()
    assert "bad_ticks_held=3" in capsys.readouterr().out


def test_calls_have_no_slash_command_and_bot_listens_to_messages():
    assert "calls" not in [n for n in re.findall(r'name="([a-z]+)"', (ROOT / "commands.py").read_text())]
    assert "async def on_message" in (ROOT / "bot.py").read_text()


def test_ath_is_single_sourced():
    text = (ROOT / "db.py").read_text()
    assert "GREATEST(" in text and "COALESCE(first_seen_market_cap, 0)" in text
    for name in ("insert_snapshot", "update_coin_snapshot", "get_coin_ath", "cleanup_old_snapshots"):
        assert not hasattr(Database, name), name
    assert "INSERT INTO coin_snapshots" not in text


# ---------------------------------------------------------------- per-guild filters

def test_filter_rules():
    assert evaluate_filters({}, {}) == (True, None)                       # all off
    s = {"min_buy_volume_usd": 500, "max_snipers": 5}
    assert evaluate_filters(s, {"buy_volume_1h": 800, "sniper_count": 2})[0]
    assert not evaluate_filters(s, {"buy_volume_1h": 100})[0]
    assert not evaluate_filters(s, {})[0]                                  # unknown minimum fails
    assert evaluate_filters({"max_snipers": 5}, {})[0]                     # unknown maximum passes
    assert not evaluate_filters({"max_snipers": 5}, {"sniper_count": 9})[0]
    assert not evaluate_filters({"min_age_seconds": 600}, {"age_seconds": 30})[0]


def verified_db(settings_by_guild):
    return SimpleNamespace(
        get_coin_by_mint=AsyncMock(return_value={"alert_attempts": 0}),
        reject_alert=AsyncMock(return_value=1), deactivate_coin=AsyncMock(),
        get_all_guilds=AsyncMock(return_value=list(settings_by_guild)),
        get_guild_settings=AsyncMock(side_effect=lambda gid: settings_by_guild[gid]),
        update_coin_alert=AsyncMock(), confirm_alert=AsyncMock(), set_rugged=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_guild_filters_suppress_only_that_guilds_alert(monkeypatch):
    monkeypatch.setattr(Config, "PRE_ALERT_VERIFY_SECONDS", 0)
    guilds = {
        1: {"alert_channel_id": 111, "scan_mode": "all"},                          # no filters
        2: {"alert_channel_id": 222, "scan_mode": "all", "min_buy_volume_usd": 10000},  # too strict
        3: {"alert_channel_id": 333, "scan_mode": "all", "min_buy_volume_usd": 1000,
            "min_volume_5m_usd": 100, "max_snipers": 3},                            # passes
    }
    db = verified_db(guilds)
    bot = SimpleNamespace(send_discord_alert=AsyncMock(return_value=SimpleNamespace(id=1)))
    scanner = Scanner(bot, db)
    market = {
        "market_cap": 10000, "liquidity": 5000, "volume_24h": 10000, "volume_1h": 5000,
        "buys_1h": 10, "sells_1h": 2, "volume_5m": 400, "buys_5m": 3, "sells_5m": 1,
        "price_change_h1": 5, "image_url": None,
    }
    scanner.get_market_data_with_fallback = AsyncMock(return_value=market)
    token = {
        "mint_address": MINT, "name": "Token", "symbol": "TOK", "program_type": "pumpfun",
        "market_cap": 10000, "liquidity": 5000, "volume_24h": 10000, "created_at": 1.0,
        "age_seconds": 120,
    }
    await scanner._verified_send(1, token, {"score": 30, "label": "low", "band": "low", "reasons": []})
    channels = sorted(c.args[0] for c in bot.send_discord_alert.await_args_list)
    assert channels == [111, 333]          # guild 2 suppressed, token stays tracked
    db.deactivate_coin.assert_not_awaited()
    db.confirm_alert.assert_awaited_with(1)
    assert token["buy_volume_1h"] == pytest.approx(5000 * 10 / 12)


@pytest.mark.asyncio
async def test_only_first_band_alert_per_token():
    db = SimpleNamespace(
        get_coin_by_mint=AsyncMock(return_value={"alerted_low": True, "alert_attempts": 0}),
        mark_band_alerted=AsyncMock(),
    )
    scanner = Scanner(SimpleNamespace(), db)
    data = {"mint_address": MINT, "name": "Token", "symbol": "TOK"}
    assert await scanner._alert_token(1, data, {"band": "high", "label": "high"}, None) is False
    db.mark_band_alerted.assert_not_awaited()


@pytest.mark.asyncio
async def test_setmode_sets_filters_and_zero_turns_off():
    db = SimpleNamespace(
        get_or_create_guild=AsyncMock(), set_scan_mode=AsyncMock(),
        set_guild_filters=AsyncMock(), reset_guild_filters=AsyncMock(),
        get_guild_settings=AsyncMock(return_value={"min_buy_volume_usd": 1500}),
    )
    cog = TrackerCommands(SimpleNamespace(db=db))
    cog.is_admin = lambda _i: True
    i = inter()
    await TrackerCommands.setmode.callback(cog, i, None, 1500, None, 0, 10, None, None, False)
    values = db.set_guild_filters.await_args.args[1]
    assert values == {"min_buy_volume_usd": 1500, "max_snipers": None, "min_age_seconds": 600}
    db.set_scan_mode.assert_not_awaited()
    assert "$1,500" in i.response.send_message.await_args.args[0]

    i = inter()
    await TrackerCommands.setmode.callback(cog, i, None, reset_filters=True)
    db.reset_guild_filters.assert_awaited_with(42)


# ---------------------------------------------------------------- volume

def test_buy_volume_formula_and_observed_preference():
    assert approx_buy_volume(1000, 30, 10) == 750
    assert approx_buy_volume(1000, 0, 0) is None
    market = {"volume_1h": 1000, "buys_1h": 30, "sells_1h": 10, "volume_5m": 200, "buys_5m": 1, "sells_5m": 1}
    approx = compute_volume_metrics(market)
    assert approx["buy_volume_1h"] == 750 and approx["buy_volume_source"] == "approx"
    assert approx["buy_volume_5m"] == 100
    observed = compute_volume_metrics(market, {"volume": 500, "buy_volume": 400, "buys": 5, "sells": 2})
    assert observed["buy_volume_1h"] == 400 and observed["buy_volume_source"] == "observed"
    assert observed["buy_volume_1h_approx"] == 750      # both figures are kept


def test_flow_observed_window_and_snipers():
    flow = TradeFlowTracker()
    flow.register({"mint": MINT, "traderPublicKey": DEV, "marketCapSol": 30})
    flow.on_trade({"mint": MINT, "txType": "buy", "traderPublicKey": OTHER, "solAmount": 2.0, "signature": "a"})
    flow.on_trade({"mint": MINT, "txType": "sell", "traderPublicKey": "X", "solAmount": 1.0, "signature": "b"})
    assert flow.snapshot(MINT)["snipers"] == 1
    stats = flow.window_stats(MINT, 300, 100.0)
    assert stats == {"volume": 300.0, "buy_volume": 200.0, "buys": 1, "sells": 1}
    assert flow.window_stats(MINT, 300, None) is None


# ---------------------------------------------------------------- feeds

def test_feed_backoff_is_per_feed():
    feeds = DexScreenerFeeds()
    feeds.feeds["orders"].record_failure(rate_limited=True, now=100.0)
    assert feeds.feeds["orders"].in_backoff(101.0)
    for name in ("boosts_latest", "boosts_top", "profiles_latest"):
        assert not feeds.feeds[name].in_backoff(101.0)
    assert not feeds.tokens.in_backoff(101.0)
    assert len({id(s.limiter) for s in feeds.feeds.values()}) == 4
    assert set(feeds.feeds) == {"orders", "boosts_latest", "boosts_top", "profiles_latest"}


class FakeResponse:
    def __init__(self, status, payload=None):
        self.status, self._payload, self.headers = status, payload, {}

    async def json(self, content_type=None):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def get(self, url, **kwargs):
        self.calls.append(url)
        for suffix, response in self.routes.items():
            if suffix in url:
                return response
        return FakeResponse(404)


@pytest.mark.asyncio
async def test_429_on_one_feed_does_not_affect_others():
    feeds = DexScreenerFeeds()
    session = FakeSession({
        "/orders/v1/solana": FakeResponse(429),
        "/token-boosts/latest/v1": FakeResponse(200, [{"chainId": "solana", "tokenAddress": MINT}]),
    })
    assert await feeds.poll_feed(session, "orders") == []
    assert feeds.feeds["orders"].in_backoff()
    items = await feeds.poll_feed(session, "boosts_latest")
    assert [i["mint_address"] for i in items] == [MINT]
    before = len(session.calls)
    assert await feeds.poll_feed(session, "orders") == []     # backing off: no request
    assert len(session.calls) == before


@pytest.mark.asyncio
async def test_batched_token_fetch_caps_at_30_and_parses():
    feeds = DexScreenerFeeds()
    pair = {"baseToken": {"address": MINT, "name": "Foo", "symbol": "FOO"}, "priceUsd": "1.5",
            "marketCap": 9000, "liquidity": {"usd": 3000}, "volume": {"h24": 10, "h1": 5, "m5": 1},
            "txns": {"h1": {"buys": 3, "sells": 1}}}
    session = FakeSession({MINT: FakeResponse(200, {"pairs": [pair]})})
    mints = [MINT] + [f"M{i}" for i in range(40)]
    result = await feeds.fetch_tokens(session, mints)
    assert result[MINT]["market_cap"] == 9000 and result[MINT]["volume_5m"] == 1
    assert session.calls[0].count(",") == 29
    assert await DexScreenerFeeds().fetch_tokens(FakeSession({}), [MINT]) is None   # failure -> unknown


@pytest.mark.asyncio
async def test_feed_items_are_upserted_once():
    db = SimpleNamespace(upsert_discovered=AsyncMock(return_value=(1, True)))
    scanner = Scanner(SimpleNamespace(), db)
    items = [{"mint_address": OTHER, "feed": "boosts_top", "image_url": None},
             {"mint_address": MINT, "feed": "boosts_top"}]       # wrapped SOL is a quote mint
    assert await scanner._upsert_discovered(items) == 1
    assert await scanner._upsert_discovered(items) == 0
    db.upsert_discovered.assert_awaited_once()
    assert db.upsert_discovered.await_args.args[:2] == (OTHER, "dexscreener")


# ---------------------------------------------------------------- API

def request(path="/api/tokens", match=None, query=None, headers=None, remote="1.2.3.4", app=None):
    return SimpleNamespace(
        path=path, match_info=match or {}, query=query or {}, headers=headers or {},
        remote=remote, app=app,
    )


def api_app(db):
    app = {"bot": SimpleNamespace(db=db)}
    app["api_limiter"] = api.IPRateLimiter(3)
    app["api_cache"] = TTLCache(16, 60)
    return app


def full_row(**over):
    row = {
        "id": 1, "mint_address": MINT, "name": "Pepe", "symbol": "PEPE", "program_type": "pumpfun",
        "first_seen_market_cap": 8000, "current_market_cap": 24000, "ath_market_cap": 30000,
        "first_seen_price": 0.0001, "current_price": 0.0003, "launch_sol": 1.5,
        "liquidity": 5000, "volume_24h": 90000, "volume_1h": 4000, "volume_5m": None,
        "buys_1h": 30, "sells_1h": 10, "buys_5m": 0, "sells_5m": 0,
        "buy_volume_1h": 3000, "buy_volume_5m": None, "buy_volume_source": "approx",
        "buy_volume_1h_approx": 3000, "sniper_count": None, "dev_wallet": DEV,
        "score": 60, "score_label": "medium", "red_flags": "fat_bundle,chased",
        "rugged": False, "rug_reason": None, "current_multiple": 3.0, "max_multiple": 3.0,
        "created_at": datetime(2026, 11, 1, tzinfo=timezone.utc), "pair_created_at": None,
        "last_tracked_at": datetime(2026, 11, 2, tzinfo=timezone.utc),
        "last_reject_reason": "secret", "alert_attempts": 3, "next_alert_retry_at": datetime.now(),
    }
    row.update(over)
    return row


def api_db(row=None):
    return SimpleNamespace(
        get_coin_by_mint=AsyncMock(return_value=row),
        get_dev_reputation=AsyncMock(return_value={"launched": 4, "rugged": 1}),
        get_pvp_siblings=AsyncMock(return_value=[{
            "mint_address": OTHER, "name": "Pepe", "symbol": "PEPE",
            "current_market_cap": 9000, "first_seen_market_cap": None,
            "created_at": datetime(2026, 11, 1, tzinfo=timezone.utc)}]),
        get_milestones_for_coin=AsyncMock(return_value=[
            {"multiplier": 3.0, "market_cap_at_milestone": 24000, "hit_at": datetime(2026, 11, 2, tzinfo=timezone.utc)}]),
        get_tracked_tokens=AsyncMock(return_value=[row] if row else []),
        count_tracked_tokens=AsyncMock(return_value=1 if row else 0),
        get_recent_milestones=AsyncMock(return_value=[{
            "multiplier": 3.0, "market_cap_at_milestone": 24000, "hit_at": datetime(2026, 11, 2, tzinfo=timezone.utc),
            "mint_address": MINT, "name": "Pepe", "symbol": "PEPE", "launch_sol": None,
            "first_seen_market_cap": 8000}]),
        get_history=AsyncMock(return_value=[row] if row else []),
    )


@pytest.mark.asyncio
async def test_api_token_shape_omits_missing_and_hides_internals():
    app = api_app(api_db(full_row()))
    resp = await api.get_token(request(match={"mint": MINT}, app=app))
    assert resp.status == 200 and resp.content_type == "application/json" and resp.charset == "utf-8"
    data = json.loads(resp.text)
    assert data["mint"] == MINT
    pair = data["pairs"][0]
    assert pair["baseToken"] == {"address": MINT, "name": "Pepe", "symbol": "PEPE"}
    assert pair["marketCap"] == 24000 and pair["liquidity"] == {"usd": 5000}
    assert pair["volume"] == {"h24": 90000, "h1": 4000}              # 5m had no data: omitted
    assert pair["txns"] == {"h1": {"buys": 30, "sells": 10}}
    t = data["tracker"]
    assert t["initial_market_cap"] == 8000 and t["ath_market_cap"] == 30000 and t["launch_sol"] == 1.5
    assert t["real_buy_volume_1h"] == 3000 and t["dev_reputation"] == {"launched": 4, "rugged": 1}
    assert t["pvp_siblings"][0]["mint"] == OTHER and t["red_flags"] == ["fat_bundle", "chased"]
    assert t["milestones_hit"][0]["multiplier"] == 3.0 and t["band"] == "medium"
    for missing in ("sniper_count", "rugged", "rug_reason", "real_buy_volume_5m"):
        assert missing not in t
    text = resp.text
    assert "secret" not in text and "alert_attempts" not in text and "retry" not in text.lower()
    assert "null" not in text


@pytest.mark.asyncio
async def test_api_token_without_market_data_returns_stored_shape():
    row = full_row(current_market_cap=None, current_price=None, liquidity=None, volume_24h=None,
                   volume_1h=None, buys_1h=None, sells_1h=None, first_seen_market_cap=None,
                   ath_market_cap=None, first_seen_price=None, launch_sol=None, dev_wallet=None,
                   score=0, score_label=None, red_flags=None, buy_volume_1h=None, buy_volume_source=None,
                   buy_volume_1h_approx=None, current_multiple=None, max_multiple=None)
    app = api_app(api_db(row))
    resp = await api.get_token(request(match={"mint": MINT}, app=app))
    data = json.loads(resp.text)
    assert resp.status == 200 and data["mint"] == MINT
    assert data["pairs"][0]["baseToken"]["address"] == MINT
    assert "marketCap" not in data["pairs"][0] and "liquidity" not in data["pairs"][0]


@pytest.mark.asyncio
async def test_api_404_400_and_unknown_path():
    app = api_app(api_db(None))
    resp = await api.get_token(request(match={"mint": MINT}, app=app))
    assert resp.status == 404 and json.loads(resp.text)["mint"] == MINT
    assert json.loads(resp.text)["error"] == "not_tracked"
    resp = await api.get_token(request(match={"mint": "nope"}, app=app))
    assert resp.status == 400
    resp = await api.api_not_found(request("/api/zzz"))
    assert resp.status == 404 and resp.content_type == "application/json"


@pytest.mark.asyncio
async def test_api_collection_endpoints():
    app = api_app(api_db(full_row()))
    resp = await api.list_tokens(request(query={"sort": "market_cap", "limit": "5"}, app=app))
    data = json.loads(resp.text)
    assert data["count"] == 1 and data["total"] == 1 and data["tokens"][0]["mint"] == MINT
    app["bot"].db.get_tracked_tokens.assert_awaited_with(sort="market_cap", limit=5, offset=0)
    assert (await api.list_tokens(request(query={"sort": "bogus"}, app=app))).status == 400

    data = json.loads((await api.list_milestones(request(app=app))).text)
    assert data["milestones"][0] == {
        "mint": MINT, "name": "Pepe", "symbol": "PEPE", "multiplier": 3.0, "market_cap": 24000,
        "initial_market_cap": 8000, "hit_at": "2026-11-02T00:00:00+00:00",
    }

    resp = await api.list_history(request(query={"filter": "band:medium mc>5k", "limit": "3"}, app=app))
    assert json.loads(resp.text)["count"] == 1
    conditions = app["bot"].db.get_history.await_args.args[0]
    assert conditions["band"] == ["medium"] and conditions["mc"] == (5000.0, None)
    bad = await api.list_history(request(query={"filter": "zzz>1"}, app=app))
    assert bad.status == 400 and json.loads(bad.text)["error"] == "invalid_filter"


@pytest.mark.asyncio
async def test_api_rate_limit_per_ip():
    app = api_app(api_db(None))
    ok = AsyncMock(return_value=api.json_response({"ok": True}))
    statuses = []
    for _ in range(5):
        statuses.append((await api.api_middleware(request(app=app), ok)).status)
    assert statuses == [200, 200, 200, 429, 429]
    other = await api.api_middleware(request(app=app, remote="9.9.9.9"), ok)
    assert other.status == 200
    limited = await api.api_middleware(request(app=app), ok)
    assert "Retry-After" in limited.headers
    # non-API paths are never limited
    assert (await api.api_middleware(request("/health", app=app), ok)).status == 200


@pytest.mark.asyncio
async def test_api_never_leaks_internal_errors():
    db = api_db(None)
    db.get_coin_by_mint = AsyncMock(side_effect=RuntimeError("password=hunter2"))
    app = api_app(db)

    async def handler(req):
        return await api.get_token(req)

    resp = await api.api_middleware(request(match={"mint": MINT}, app=app), handler)
    assert resp.status == 503 and "hunter2" not in resp.text


# ---------------------------------------------------------------- lifecycle cleanup

class FakeConn:
    def __init__(self):
        self.executed, self.fetched = [], []

    async def execute(self, sql, *args):
        self.executed.append((" ".join(sql.split()), args))
        return "UPDATE 2"

    async def fetch(self, sql, *args):
        self.fetched.append((" ".join(sql.split()), args))
        return [{"dev_wallet": DEV, "rugged": True}, {"dev_wallet": DEV, "rugged": False}]

    def transaction(self):
        class Tx:
            async def __aenter__(s): return s
            async def __aexit__(s, *a): return False
        return Tx()


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        conn = self.conn

        class Ctx:
            async def __aenter__(s): return conn
            async def __aexit__(s, *a): return False
        return Ctx()


@pytest.mark.asyncio
async def test_cleanup_windows_and_dev_rollup():
    conn = FakeConn()
    db = Database()
    db.pool = FakePool(conn)
    counts = await db.run_lifecycle_cleanup()
    assert counts == {
        "rugged_deleted": 2, "marked_dead": 2, "dead_deleted": 2,
        "lowcap_deactivated": 2, "lowcap_deleted": 2,
    }
    deletes = [sql for sql, _ in conn.fetched]
    assert any("rugged = TRUE" in q and "rugged_at" in q for q in deletes)
    assert any("is_dead = TRUE" in q and "dead_at" in q for q in deletes)
    assert any("deactivated_at" in q and "BETWEEN" in q for q in deletes)
    args = {sql[:30]: a for sql, a in conn.fetched}
    assert (str(Config.RUG_DELETE_HOURS),) in [a for _, a in conn.fetched]
    assert (str(Config.DEAD_DELETE_HOURS),) in [a for _, a in conn.fetched]
    updates = {sql: a for sql, a in conn.executed if sql.startswith("UPDATE")}
    mark = next(a for sql, a in updates.items() if "is_dead = TRUE" in sql)
    assert mark == (str(Config.DEAD_TOKEN_MINUTES),)
    low = next(a for sql, a in updates.items() if "lowcap" in sql or "BETWEEN" in sql)
    assert low == (str(Config.LOWCAP_IDLE_MINUTES), Config.LOWCAP_MIN_MC, Config.LOWCAP_MAX_MC)
    rollups = [a for sql, a in conn.executed if "INSERT INTO dev_stats" in sql]
    assert rollups and rollups[0] == (DEV, 2, 1)
    assert Config.RUG_DELETE_HOURS < Config.DEAD_DELETE_HOURS
    assert Config.LOWCAP_MIN_MC == 100 and Config.LOWCAP_MAX_MC == 3000


@pytest.mark.asyncio
async def test_cleanup_prints_one_summary_line(capsys):
    db = SimpleNamespace(
        run_lifecycle_cleanup=AsyncMock(return_value={"marked_dead": 40, "dead_deleted": 12}),
        remove_inactive_wallets=AsyncMock(return_value=0),
    )
    scanner = Scanner(SimpleNamespace(), db)
    await scanner.run_cleanup()
    lines = [l for l in capsys.readouterr().out.splitlines() if l.strip()]
    assert len(lines) == 1 and lines[0].startswith("Cleanup:") and "marked_dead=40" in lines[0]


@pytest.mark.asyncio
async def test_recent_duplicate_is_deleted_silently(capsys):
    db = SimpleNamespace(
        get_coin_by_mint=AsyncMock(return_value=None),
        find_recent_duplicate=AsyncMock(return_value={
            "mint_address": OTHER, "name": "Pepe", "symbol": "PEPE",
            "created_at": datetime.now(timezone.utc) - timedelta(minutes=2)}),
        delete_coin_by_mint=AsyncMock(), insert_coin=AsyncMock(),
    )
    scanner = Scanner(SimpleNamespace(), db)
    scanner.enricher.enrich = AsyncMock(return_value={"name": "Pepe", "symbol": "PEPE", "price_change_h1": None})
    await scanner._process_token_locked(MINT, {"mint_address": MINT, "name": "Pepe", "symbol": "PEPE"})
    db.delete_coin_by_mint.assert_awaited_with(MINT)
    db.insert_coin.assert_not_awaited()
    assert capsys.readouterr().out == ""

    # Older duplicate keeps the existing flag-and-deactivate behaviour.
    db.find_recent_duplicate.return_value["created_at"] = datetime.now(timezone.utc) - timedelta(hours=5)
    db.insert_coin = AsyncMock(return_value=9)
    db.set_red_flags, db.deactivate_coin = AsyncMock(), AsyncMock()
    db.delete_coin_by_mint.reset_mock()
    await scanner._process_token_locked(MINT, {"mint_address": MINT, "name": "Pepe", "symbol": "PEPE"})
    db.delete_coin_by_mint.assert_not_awaited()
    db.deactivate_coin.assert_awaited_with(9)


# ---------------------------------------------------------------- hygiene / config / footers

def test_default_wallet_is_gone():
    assert not hasattr(Config, "DEFAULT_WALLET")
    for name in ("scanner.py", "wallet_signals.py", "wallet_monitor.py", "db.py", "commands.py", "config.py"):
        text = (ROOT / name).read_text()
        assert "DEFAULT_WALLET" not in text and "Default Sniper" not in text, name
        assert "== -1" not in text, name


@pytest.mark.asyncio
async def test_wallet_alerts_only_reach_guilds_that_added_the_wallet():
    from wallet_signals import WalletSignalEngine
    db = SimpleNamespace(get_all_guilds=AsyncMock(), get_guild_settings=AsyncMock())
    bot = SimpleNamespace(db=db, send_discord_alert=AsyncMock())
    engine = WalletSignalEngine(bot)
    engine._dispatch = AsyncMock()
    await engine._send_alert({"event": {"mint": MINT, "guild_id": None, "channel_id": None}})
    db.get_all_guilds.assert_not_awaited()
    engine._dispatch.assert_not_awaited()
    await engine._send_alert({"event": {"mint": MINT, "guild_id": 5, "channel_id": 99}})
    engine._dispatch.assert_awaited_once()


def test_placeholder_name_is_not_a_placeholder():
    src = (ROOT / "wallet_signals.py").read_text()
    assert 'f"Token {mint' not in src
    name = f"{MINT[:6]}...{MINT[-4:]}"
    assert has_valid_identity(name, MINT[:4].upper(), MINT)


def test_buyer_cache_is_bounded_and_expires():
    clock = [0.0]
    cache = TTLCache(2, 10, clock=lambda: clock[0])
    cache.set("a", 1); cache.set("b", 2); cache.set("c", 3)
    assert cache.get("a") is None and cache.get("c") == 3 and len(cache) == 2
    clock[0] = 11
    assert cache.get("c") is None
    scanner = Scanner(SimpleNamespace(), SimpleNamespace())
    assert isinstance(scanner._buyer_analysis_cache, TTLCache)


def test_scan_mode_is_validated(monkeypatch):
    monkeypatch.setattr(Config, "DISCORD_TOKEN", "t")
    monkeypatch.setattr(Config, "DATABASE_URL", "d")
    monkeypatch.setattr(Config, "DEFAULT_SCAN_MODE", "medium")
    Config.validate()
    monkeypatch.setattr(Config, "DEFAULT_SCAN_MODE", "bogus")
    with pytest.raises(ValueError, match="DEFAULT_SCAN_MODE"):
        Config.validate()


@pytest.mark.asyncio
async def test_pumpportal_clean_close_still_sleeps(monkeypatch):
    import pumpportal

    class EmptyWS:
        async def send(self, *_): pass
        def __aiter__(self): return self
        async def __anext__(self): raise StopAsyncIteration

    class Conn:
        async def __aenter__(self): return EmptyWS()
        async def __aexit__(self, *a): return False

    sleeps = []
    detector = pumpportal.PumpPortalDetector(AsyncMock())
    detector._running = True

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        detector._running = False

    monkeypatch.setattr(pumpportal.websockets, "connect", lambda *a, **k: Conn())
    monkeypatch.setattr(pumpportal.asyncio, "sleep", fake_sleep)
    await detector._run()
    assert sleeps and sleeps[0] >= 1


def test_footers_are_exact():
    alert = create_token_alert_embed({"mint_address": MINT, "name": "A", "symbol": "B", "program_type": "pumpfun"}, {"band": "low"})
    fresh = create_fresh_callout_embed({"mint_address": MINT, "name": "A", "symbol": "B",
                                        "initial_mc": 1, "current_mc": 2, "mc_unit": "usd", "change_pct": 1.0})
    migrated = create_migration_embed({"mint_address": MINT, "name": "A", "symbol": "B"}, {})
    milestone = create_milestone_embed({"mint_address": MINT, "name": "A", "symbol": "B"}, 2.0, 1.0, 1.0)
    wallet = create_wallet_alert_embed(DEV, None, {"mint_address": MINT, "name": "A", "symbol": "B"})
    buy = create_wallet_buy_alert_embed({"event": {"mint": MINT, "name": "A", "symbol": "B"}, "recent": [],
                                         "market": {}, "signal": "WATCH", "reasons": [], "wallet_count": 1})
    assert [e.footer.text for e in (alert, fresh, migrated, milestone, wallet, buy)] == [
        "Solana Meme Tracker | Alert", "Solana Meme Tracker | Fresh launch",
        "Solana Meme Tracker | Migrated", "Solana Meme Tracker | Milestone",
        "Solana Meme Tracker | Wallet", "Solana Meme Tracker | Wallet",
    ]
    assert "Verified at T+5s" not in (ROOT / "embeds.py").read_text()


def test_command_manifest_matches_readme():
    tree = ast.parse((ROOT / "commands.py").read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "command":
            for kw in node.keywords:
                if kw.arg == "name":
                    names.add(kw.value.value)
    assert len(names) == 15 and "recent" not in names
    readme = (ROOT / "README.md").read_text()
    for name in names:
        assert f"`/{name}`" in readme, name
    assert "exactly 15 global slash commands" in readme


def test_config_env_vars_are_documented():
    env = set(re.findall(r'(?:os\.getenv|_int|_float)\("([A-Z_]+)"', (ROOT / "config.py").read_text()))
    readme, render = (ROOT / "README.md").read_text(), (ROOT / "render.yaml").read_text()
    assert len(env) == 16
    for var in env:
        assert var in readme and var in render, var


# ---------------------------------------------------------------- always monitored

@pytest.mark.asyncio
async def test_websocket_launches_are_recorded_even_if_later_rejected_or_dropped():
    db = SimpleNamespace(upsert_discovered=AsyncMock(return_value=(1, True)))
    scanner = Scanner(SimpleNamespace(), db)
    scanner._maybe_start_fresh_scan = AsyncMock()
    scanner._token_queue = asyncio.Queue(maxsize=1)
    scanner._token_queue.put_nowait({"filler": True})              # queue full: launch is dropped
    event = {"mint_address": OTHER, "program_type": "pumpfun", "name": "Token AbCdEf",
             "symbol": "X", "raw_event": {"txType": "create", "mint": OTHER}}
    await scanner._on_new_token_from_ws(event)
    db.upsert_discovered.assert_awaited_once()
    args, kwargs = db.upsert_discovered.await_args
    assert args[0] == OTHER and args[1] == "pumpfun"
    assert kwargs["name"] is None and kwargs["symbol"] is None      # placeholder identity not stored
    assert kwargs["source"] == "pumpportal" and kwargs["revive"] is False


@pytest.mark.asyncio
async def test_dexscreener_feed_sightings_retrack_tokens():
    db = SimpleNamespace(upsert_discovered=AsyncMock(return_value=(1, False)))
    scanner = Scanner(SimpleNamespace(), db)
    await scanner._upsert_discovered([{"mint_address": OTHER, "feed": "boosts_latest"}])
    assert db.upsert_discovered.await_args.kwargs["revive"] is True
    assert db.upsert_discovered.await_args.kwargs["source"] == "boosts_latest"
    await scanner._upsert_discovered([{"mint_address": OTHER, "feed": "boosts_latest"}])
    assert db.upsert_discovered.await_count == 1                     # deduped within the hour


def test_reactivated_tokens_get_a_fresh_tracking_window():
    scanner = Scanner(SimpleNamespace(), SimpleNamespace())
    old = datetime.now(timezone.utc) - timedelta(days=5)
    assert scanner.is_expired({"created_at": old})
    assert not scanner.is_expired({"created_at": old, "reactivated_at": datetime.now(timezone.utc)})


def test_revive_sql_skips_rugged_chased_and_duplicate():
    import db as dbmod
    sql = dbmod._REVIVABLE_SQL
    assert "rugged" in sql and "chased" in sql and "duplicate" in sql
