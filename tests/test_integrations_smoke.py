"""Live smoke tests for the five integrations.

Not part of the default run (they need the network). Run explicitly:

    pytest -m integration -v
"""
import aiohttp
import pytest

import verify_integrations as v

pytestmark = pytest.mark.integration


async def _run(check):
    async with aiohttp.ClientSession() as session:
        summary = await check(session)
    print(summary)
    assert summary


@pytest.mark.asyncio
async def test_dexscreener_feeds_and_known_mint():
    await _run(v.check_dexscreener)


@pytest.mark.asyncio
async def test_raydium_pools_parse_into_coins():
    await _run(v.check_raydium)


@pytest.mark.asyncio
async def test_meteora_pools_parse_into_coins():
    await _run(v.check_meteora)


@pytest.mark.asyncio
async def test_pumpfun_rpc_and_known_create_tx():
    await _run(v.check_pumpfun)


@pytest.mark.asyncio
async def test_pumpportal_create_event_parses_into_coin():
    await _run(v.check_pumpportal)
