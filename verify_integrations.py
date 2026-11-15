"""Manual integration check: runs each integration once against the live
service and prints success/failure with a summary of the real response.

    python verify_integrations.py            # all five
    python verify_integrations.py meteora    # one

Exit status is non-zero if any check fails. The same functions back the
`integration`-marked pytest smoke tests (pytest -m integration).
"""
import asyncio
import json
import sys
import time
from types import SimpleNamespace

import aiohttp
import websockets

from config import Config
from dexscreener import DexScreenerFeeds
from enricher import is_valid_solana_address
from meteora import MeteoraDetector, parse_pools as parse_meteora
from pumpfun import PumpFunDetector
from pumpportal import PumpPortalDetector
from raydium import RaydiumDetector, parse_pools as parse_raydium
from solana_rpc import SolanaRPC

BONK = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"   # long-lived, liquid mint
WSOL = "So11111111111111111111111111111111111111112"
PUMPPORTAL_WAIT_SECONDS = 45


def _assert_coin_shape(coin, program=None):
    assert is_valid_solana_address(coin["mint_address"]), f"bad mint {coin['mint_address']!r}"
    assert coin.get("program_type"), "no program_type"
    if program:
        assert coin["program_type"] == program, coin["program_type"]


async def check_dexscreener(session):
    feeds = DexScreenerFeeds()
    markets = await feeds.fetch_tokens(session, [BONK])
    assert markets and BONK in markets, "batched /latest/dex/tokens returned nothing for BONK"
    market = markets[BONK]
    assert market["market_cap"] and market["market_cap"] > 0 and market["symbol"], market
    parts = []
    for name in feeds.feeds:
        items = await feeds.poll_feed(session, name)
        state = feeds.feeds[name]
        assert state.consecutive_failures == 0, f"feed {name} failed (see {state.path})"
        for item in items:
            _assert_coin_shape({**item, "program_type": "dexscreener"})
        parts.append(f"{name}={len(items)}")
    return (
        f"BONK mc=${market['market_cap']:,.0f} symbol={market['symbol']} "
        f"liq=${market['liquidity'] or 0:,.0f}; feeds: {', '.join(parts)}"
    )


async def check_raydium(session):
    detector = RaydiumDetector(lambda: session)
    payload = await detector.fetch_pools()
    assert payload, "no/failed response from api-v3.raydium.io"
    coins = parse_raydium(payload)
    assert coins, "response parsed but contained no non-quote mints"
    for coin in coins:
        _assert_coin_shape(coin, "raydium")
    first = coins[0]
    return f"{len(coins)} pools; first {first['mint_address'][:8]}… {first.get('symbol')}"


async def check_meteora(session):
    detector = MeteoraDetector(lambda: session)
    payload = await detector.fetch_pools()
    assert payload, "no/failed response from the Meteora data API"
    coins = parse_meteora(payload)
    assert coins, f"response parsed but no mints found; top-level keys: {list(payload)[:6] if isinstance(payload, dict) else type(payload).__name__}"
    for coin in coins:
        _assert_coin_shape(coin, "meteora")
    first = coins[0]
    return f"{len(coins)} pools; first {first['mint_address'][:8]}… {first.get('symbol')}"


FIXTURE_CREATE_TX = {
    "meta": {"innerInstructions": []},
    "transaction": {
        "signatures": ["fixture-signature"],
        "message": {
            "accountKeys": [{"pubkey": "7BNaxx6KdUYrjACNQZ9He26NBFoFxujQMAfNLnArLGH5"}],
            "instructions": [{"parsed": {"type": "initializeMint2", "info": {"mint": WSOL}}}],
        },
    },
}


async def check_pumpfun(_session):
    detector = PumpFunDetector(rpc=None)
    parsed = detector.parse_token_creation(FIXTURE_CREATE_TX)
    assert parsed and parsed["mint_address"] == WSOL, "known-good create tx did not parse"
    _assert_coin_shape(parsed, "pumpfun")
    rpc = SolanaRPC()
    try:
        signatures = await rpc.get_recent_signatures(Config.PUMP_FUN_PROGRAM, limit=3)
    finally:
        await rpc.close()
    assert isinstance(signatures, list) and signatures and signatures[0].get("signature"), \
        "RPC returned no signatures for the pump.fun program"
    return f"fixture parsed to {parsed['mint_address'][:8]}…; RPC returned {len(signatures)} signature(s)"


async def check_pumpportal(_session):
    received = []

    async def on_token(token):
        received.append(token)

    detector = PumpPortalDetector(on_token, None)
    detector._ws = SimpleNamespace(send=_noop)
    deadline = time.monotonic() + PUMPPORTAL_WAIT_SECONDS
    async with websockets.connect(PumpPortalDetector.WS_URL, open_timeout=15) as ws:
        await ws.send(json.dumps({"method": "subscribeNewToken"}))
        while time.monotonic() < deadline and not received:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.monotonic())
            except asyncio.TimeoutError:
                break
            event = json.loads(raw)
            if isinstance(event, dict) and event.get("txType") == "create":
                await detector._handle_event(event)
    assert received, f"no create event within {PUMPPORTAL_WAIT_SECONDS}s"
    token = received[0]
    _assert_coin_shape(token, "pumpfun")
    return f"create event: {token.get('symbol')} {token['mint_address'][:8]}… launch_sol={token.get('launch_sol')}"


async def _noop(*_a, **_k):
    return None


CHECKS = {
    "dexscreener": check_dexscreener,
    "raydium": check_raydium,
    "meteora": check_meteora,
    "pumpfun": check_pumpfun,
    "pumpportal": check_pumpportal,
}


async def main(selected):
    failures = 0
    async with aiohttp.ClientSession() as session:
        for name in selected:
            started = time.monotonic()
            try:
                summary = await CHECKS[name](session)
                print(f"[ OK ] {name:<11} {summary}  ({time.monotonic() - started:.1f}s)")
            except Exception as exc:
                failures += 1
                print(f"[FAIL] {name:<11} {type(exc).__name__}: {exc}  ({time.monotonic() - started:.1f}s)")
    print(f"\n{len(selected) - failures}/{len(selected)} integrations working")
    return 1 if failures else 0


if __name__ == "__main__":
    wanted = [a.lower() for a in sys.argv[1:]] or list(CHECKS)
    unknown = [a for a in wanted if a not in CHECKS]
    if unknown:
        raise SystemExit(f"unknown integration(s): {', '.join(unknown)}; choose from {', '.join(CHECKS)}")
    raise SystemExit(asyncio.run(main(wanted)))
