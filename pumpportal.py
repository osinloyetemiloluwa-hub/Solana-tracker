import asyncio
import json
import websockets


class PumpPortalDetector:
    """
    Free, real-time pump.fun / PumpSwap token creation feed.

    Docs: https://pumpportal.fun/data-api/real-time/
    Endpoint: wss://pumpportal.fun/api/data
    No API key required for subscribeNewToken.
    """

    WS_URL = "wss://pumpportal.fun/api/data"

    def __init__(self, on_token_callback):
        self.on_token_callback = on_token_callback
        self._ws = None
        self._task = None
        self._running = False

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

    async def _run(self):
        backoff = 2
        while self._running:
            try:
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                ) as ws:
                    self._ws = ws
                    backoff = 2
                    print("PumpPortal WebSocket connected")

                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    await ws.send(json.dumps({"method": "subscribeMigration"}))

                    async for raw in ws:
                        if not self._running:
                            return
                        try:
                            event = json.loads(raw)
                        except Exception:
                            continue
                        await self._handle_event(event)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"PumpPortal WS error: {type(exc).__name__}: {exc}; reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            finally:
                self._ws = None

    async def _handle_event(self, event: dict):
        # PumpPortal sends different message shapes. Token creations include
        # a mint address and usually name/symbol. Migrations tell us a token
        # graduated to PumpSwap.
        mint = (
            event.get("mint")
            or event.get("tokenAddress")
            or event.get("mintAddress")
        )
        if not mint:
            return

        token_data = {
            "mint_address": mint,
            "program_type": "pumpswap" if event.get("txType") == "migrate" else "pumpfun",
            "name": event.get("name"),
            "symbol": event.get("symbol"),
            "uri": event.get("uri") or event.get("metadataUri"),
            "dev_wallet": event.get("traderPublicKey") or event.get("creator"),
            "signature": event.get("signature"),
            "raw_event": event,
        }

        try:
            await self.on_token_callback(token_data)
        except Exception as exc:
            print(f"PumpPortal callback error: {type(exc).__name__}: {exc}")