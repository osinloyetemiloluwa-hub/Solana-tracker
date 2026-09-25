import asyncio
import time
import httpx
from config import Config


class SolanaRPC:
    """
    Helius-aware RPC client.
      - Throttle: 0.4s min gap between calls
      - Transient 429: exponential backoff (up to 3 tries)
      - 3 consecutive 429s OR "-32429 max usage reached": HARD PAUSE
      - During pause: all calls fail instantly (no retries, no delay)
    """

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=Config.REQUEST_TIMEOUT)
        self.url = Config.solana_rpc_url()
        self._lock = asyncio.Lock()
        self._last_request_at = 0.0
        self._min_gap = 0.4
        self._paused_until = 0.0
        self._pause_logged = False
        self._consecutive_429s = 0
        self._hard_quota = False

    async def close(self):
        await self.client.aclose()

    def is_paused(self) -> bool:
        return time.monotonic() < self._paused_until

    def _pause_for(self, seconds: int, reason: str):
        self._paused_until = time.monotonic() + seconds
        if not self._pause_logged:
            print(f"[RPC] PAUSED for {seconds}s — {reason}")
            self._pause_logged = True
        self._consecutive_429s = 0
        self._hard_quota = "account limit" in reason.lower() or "max usage" in reason.lower()

    async def _throttle(self):
        async with self._lock:
            loop = asyncio.get_event_loop()
            now = loop.time()
            wait = self._min_gap - (now - self._last_request_at)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_at = loop.time()

    async def _request(self, method: str, params: list):
        # Fail fast when paused — no retries, no delay
        if self.is_paused():
            remaining = max(1, int(self._paused_until - time.monotonic()))
            if self._hard_quota:
                raise RuntimeError(f"Helius account quota paused ({remaining}s remaining)")
            raise RuntimeError(f"RPC paused ({remaining}s remaining)")

        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}

        last_error = None
        for attempt in range(Config.MAX_RETRIES):
            await self._throttle()
            try:
                response = await self.client.post(self.url, json=payload)

                # --- 429 handling ---
                if response.status_code == 429:
                    body = response.text or ""
                    # Hard account limit
                    if "-32429" in body or "max usage reached" in body:
                        self._pause_for(1800, "Helius account limit reached")
                        raise RuntimeError("Helius account exhausted")

                    # Plain rate limit — count consecutive
                    self._consecutive_429s += 1
                    if self._consecutive_429s >= 3:
                        self._pause_for(600, "3 consecutive 429s")
                        raise RuntimeError("RPC rate limited — pausing")

                    # Single retry attempt
                    retry_after = response.headers.get("Retry-After")
                    try:
                        delay = float(retry_after) if retry_after else min(2 ** (attempt + 1), 8)
                    except ValueError:
                        delay = min(2 ** (attempt + 1), 8)
                    await asyncio.sleep(min(max(delay, 1.0), 10.0))
                    continue

                response.raise_for_status()
                data = response.json()

                if "error" in data:
                    err = data["error"]
                    if isinstance(err, dict):
                        code = err.get("code")
                        msg = err.get("message", "")
                        if code == -32429 or "max usage reached" in msg:
                            self._pause_for(1800, "Helius account limit reached")
                            raise RuntimeError("Helius account exhausted")
                    raise Exception(f"RPC error: {err}")

                # Success — reset counters
                self._consecutive_429s = 0
                self._pause_logged = False
                return data.get("result")

            except httpx.HTTPStatusError as exc:
                last_error = exc
                if exc.response.status_code == 429:
                    self._consecutive_429s += 1
                    if self._consecutive_429s >= 3:
                        self._pause_for(600, "3 consecutive 429s (httpx)")
                        raise RuntimeError("RPC rate limited — pausing")
                    await asyncio.sleep(min(2 ** (attempt + 1), 8))
                    continue
                if attempt == Config.MAX_RETRIES - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
            except Exception as exc:
                last_error = exc
                if "paused" in str(exc) or "exhausted" in str(exc):
                    raise
                if attempt == Config.MAX_RETRIES - 1:
                    raise
                await asyncio.sleep(2 ** attempt)

        raise last_error or RuntimeError("Solana RPC request failed")

    async def get_recent_signatures(self, program_id: str, limit: int = 50):
        return await self._request("getSignaturesForAddress", [program_id, {"limit": limit}]) or []

    async def get_transaction(self, signature: str):
        return await self._request("getTransaction", [
            signature,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1},
        ])

    async def get_account_info(self, address: str):
        return await self._request("getAccountInfo", [address, {"encoding": "jsonParsed"}])

    async def get_token_supply(self, mint: str):
        return await self._request("getTokenSupply", [mint])

    async def get_token_largest_accounts(self, mint: str):
        return await self._request("getTokenLargestAccounts", [mint])

    async def get_multiple_accounts(self, addresses: list):
        return await self._request("getMultipleAccounts", [addresses, {"encoding": "jsonParsed"}])