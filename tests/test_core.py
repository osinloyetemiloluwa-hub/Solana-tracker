import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import httpx

from config import Config
from pumpportal import PumpPortalDetector
from rate_limiter import DailyQuota, QuotaExhausted
from solana_rpc import SolanaRPC
from trade_flow import TradeFlowTracker
from wallets import WalletTracker


MINT = "So11111111111111111111111111111111111111112"
DEV = Config.DEFAULT_WALLET
BUYER = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


def test_trade_flow_normalizes_lamports_and_deduplicates():
    flow = TradeFlowTracker()
    flow.register({"mint": MINT, "traderPublicKey": DEV, "marketCapSol": 30})
    event = {
        "mint": MINT, "txType": "buy", "traderPublicKey": BUYER,
        "solAmount": 2_000_000_000, "marketCapSol": 32, "signature": "sig-1",
    }
    flow.on_trade(event)
    flow.on_trade(event)
    snap = flow.snapshot(MINT)
    assert snap["buys"] == 1
    assert snap["net_sol"] == 2.0
    assert snap["non_dev_buyers"] == 1
    assert flow.rejection_reason(MINT) is None


def test_trade_flow_dev_sell_is_hard_evidence():
    flow = TradeFlowTracker()
    flow.register({"mint": MINT, "traderPublicKey": DEV})
    flow.on_trade({
        "mint": MINT, "txType": "sell", "traderPublicKey": DEV,
        "solAmount": 0.5, "signature": "dev-sell",
    })
    assert "Dev sold" in flow.rejection_reason(MINT)


def test_trade_flow_sparse_data_does_not_reject():
    flow = TradeFlowTracker()
    flow.register({"mint": MINT, "traderPublicKey": DEV})
    assert flow.rejection_reason(MINT) is None


def test_wallet_parser_supports_json_text_and_deduplicates():
    parser = WalletTracker()
    parsed = parser.parse_wallet_json(
        f'{{"wallets":[{{"address":"{DEV}","name":" Alpha "}},'
        f'{{"trackedWalletAddress":"{DEV}"}},{{"address":"bad"}}]}}'
    )
    assert parsed == [{"address": DEV, "label": "Alpha"}]
    plain = parser.parse_wallet_json(f"{DEV},Main\n{BUYER}\n# comment\n")
    assert [w["address"] for w in plain] == [DEV, BUYER]


@pytest.mark.asyncio
async def test_pumpportal_routes_create_trade_and_migration():
    tokens = []
    trades = []
    detector = PumpPortalDetector(tokens.append, trades.append)
    detector._ws = SimpleNamespace(send=AsyncMock())

    await detector._handle_event({
        "mint": MINT, "txType": "create", "traderPublicKey": DEV,
        "name": "Token", "symbol": "TOK",
    })
    await detector._handle_event({"mint": MINT, "txType": "buy", "signature": "s1"})
    await detector._handle_event({"mint": MINT, "txType": "migrate"})

    assert len(tokens) == 2
    assert tokens[0]["program_type"] == "pumpfun"
    assert tokens[1]["program_type"] == "pumpswap"
    assert trades[0]["signature"] == "s1"
    assert MINT not in detector._subscribed_mints


@pytest.mark.asyncio
async def test_rpc_budget_fails_fast_without_network():
    rpc = SolanaRPC()
    try:
        rpc._requests_today = Config.RPC_DAILY_REQUEST_BUDGET
        with pytest.raises(RuntimeError, match="budget exhausted"):
            rpc._spend_budget()
        assert rpc.stats()["requests_today"] == Config.RPC_DAILY_REQUEST_BUDGET
    finally:
        await rpc.close()


@pytest.mark.asyncio
async def test_rpc_rotates_only_on_network_failure(monkeypatch):
    monkeypatch.setattr(Config, "MAX_RETRIES", 2)
    rpc = SolanaRPC()
    original_client = rpc.client
    response = SimpleNamespace(
        status_code=200, headers={}, text="",
        raise_for_status=lambda: None, json=lambda: {"result": {"ok": True}},
    )
    rpc.client = SimpleNamespace(post=AsyncMock(side_effect=[httpx.ConnectTimeout("timeout"), response]))
    try:
        result = await rpc._request("getLatestBlockhash", [])
        assert result == {"ok": True}
        assert rpc.url == Config.solana_rpc_fallback_url()
    finally:
        await original_client.aclose()


@pytest.mark.asyncio
async def test_daily_quota_stops_at_cap():
    quota = DailyQuota(max_per_day=1, rate_per_minute=10)
    await quota.acquire()
    assert quota.remaining() == 0
    with pytest.raises(QuotaExhausted):
        await quota.acquire()
