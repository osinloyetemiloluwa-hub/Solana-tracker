import asyncio
from collections import OrderedDict
import json
import time

import websockets

from config import Config
from trade_flow import _sol_amount


class PumpPortalDetector:
    """Free PumpPortal launch, migration, and bounded early-trade feed."""

    WS_URL = "wss://pumpportal.fun/api/data"

    def __init__(self, on_token_callback, on_trade_callback=None):
        self.on_token_callback = on_token_callback
        self.on_trade_callback = on_trade_callback
        self._ws = None
        self._task = None
        self._running = False
        self._subscribed_mints = OrderedDict()
        self.last_event_at = 0.0

    @property
    def connected(self):
        return self._ws is not None

    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="pumpportal-ws")

    async def stop(self):
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        self._ws = None

    async def _run(self):
        backoff = 2
        while self._running:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=1000,
                ) as ws:
                    self._ws = ws
                    backoff = 2
                    print("PumpPortal WebSocket connected")
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    await ws.send(json.dumps({"method": "subscribeMigration"}))
                    await self._resubscribe_all(ws)

                    async for raw in ws:
                        if not self._running:
                            return
                        try:
                            event = json.loads(raw)
                        except (TypeError, ValueError):
                            continue
                        if isinstance(event, dict):
                            await self._handle_event(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(
                    f"PumpPortal WS error: {type(exc).__name__}: {exc}; "
                    f"reconnecting in {backoff}s"
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            finally:
                self._ws = None

    async def _resubscribe_all(self, ws):
        mints = list(self._subscribed_mints)
        for offset in range(0, len(mints), 100):
            batch = mints[offset:offset + 100]
            await ws.send(json.dumps({
                "method": "subscribeTokenTrade",
                "keys": batch,
            }))
        if mints:
            print(f"PumpPortal restored {len(mints)} trade subscription(s)")

    async def _subscribe_trades(self, ws, mint):
        while mint in self._subscribed_mints:
            self._subscribed_mints.pop(mint, None)
        self._subscribed_mints[mint] = time.time()
        while len(self._subscribed_mints) > Config.FLOW_MAX_TRACKED_MINTS:
            old_mint, _ = self._subscribed_mints.popitem(last=False)
            try:
                await ws.send(json.dumps({
                    "method": "unsubscribeTokenTrade",
                    "keys": [old_mint],
                }))
            except Exception:
                pass
        await ws.send(json.dumps({
            "method": "subscribeTokenTrade",
            "keys": [mint],
        }))

    async def _unsubscribe_trades(self, ws, mint):
        self._subscribed_mints.pop(mint, None)
        try:
            await ws.send(json.dumps({
                "method": "unsubscribeTokenTrade",
                "keys": [mint],
            }))
        except Exception:
            pass

    @staticmethod
    def _launch_sol(event: dict):
        """SOL the dev spent on the launch buy; None when the feed omits it."""
        raw = event.get("solAmount")
        if raw is None:
            return None
        try:
            return abs(_sol_amount(raw))
        except (TypeError, ValueError):
            return None

    async def _handle_event(self, event: dict):
        mint = event.get("mint") or event.get("tokenAddress") or event.get("mintAddress")
        if not mint:
            return
        self.last_event_at = time.time()
        tx_type = event.get("txType")

        if tx_type in ("buy", "sell"):
            if self.on_trade_callback:
                try:
                    result = self.on_trade_callback(event)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as exc:
                    print(f"PumpPortal trade callback error: {type(exc).__name__}: {exc}")
            return

        if tx_type not in ("create", "migrate"):
            return

        if tx_type == "create":
            try:
                await self._subscribe_trades(self._ws, mint)
            except Exception as exc:
                print(f"Trade subscription failed for {mint[:8]}: {exc}")
        else:
            await self._unsubscribe_trades(self._ws, mint)

        token_data = {
            "mint_address": mint,
            "program_type": "pumpswap" if tx_type == "migrate" else "pumpfun",
            "name": event.get("name"),
            "symbol": event.get("symbol"),
            "uri": event.get("uri") or event.get("metadataUri"),
            "dev_wallet": event.get("traderPublicKey") or event.get("creator"),
            "signature": event.get("signature"),
            "launch_sol": self._launch_sol(event) if tx_type == "create" else None,
            "created_at": time.time(),
            "raw_event": event,
        }
        try:
            await self.on_token_callback(token_data)
        except Exception as exc:
            print(f"PumpPortal callback error: {type(exc).__name__}: {exc}")
