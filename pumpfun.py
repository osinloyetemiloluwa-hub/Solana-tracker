"""pump.fun launch detection straight from the chain (RPC).

PumpPortal's WebSocket is the primary launch feed. This detector is the
backstop: the scanner polls it only while PumpPortal has been silent, and with
small limits, so it stays inside the daily RPC budget.
"""
from collections import OrderedDict

from config import Config
from enricher import is_valid_solana_address

SEEN_MAX = 500
MAX_TX_PER_POLL = 10


class PumpFunDetector:
    def __init__(self, rpc):
        self.rpc = rpc
        self.program_id = Config.PUMP_FUN_PROGRAM
        self._seen = OrderedDict()

    async def detect_new_tokens(self, limit: int = MAX_TX_PER_POLL):
        try:
            signatures = await self.rpc.get_recent_signatures(self.program_id, limit=limit)
            new_tokens = []
            fetched = 0
            for sig_info in signatures or []:
                signature = sig_info.get("signature")
                if not signature or signature in self._seen:
                    continue
                if sig_info.get("err"):
                    continue
                self._seen[signature] = True
                while len(self._seen) > SEEN_MAX:
                    self._seen.popitem(last=False)
                if fetched >= MAX_TX_PER_POLL:
                    break
                fetched += 1
                tx = await self.rpc.get_transaction(signature)
                if not tx:
                    continue
                token = self.parse_token_creation(tx)
                if token:
                    new_tokens.append(token)
            return new_tokens
        except Exception as exc:
            print(f"PumpFun detection error: {type(exc).__name__}: {exc}")
            return []

    def parse_token_creation(self, tx: dict):
        """A pump.fun create transaction -> coin-shaped dict, else None.

        Only a parsed mint-initialisation (or a mintTo of a `...pump` address)
        counts. Account keys are never guessed at.
        """
        try:
            meta = tx.get("meta") or {}
            message = (tx.get("transaction") or {}).get("message") or {}

            account_keys = []
            for key in message.get("accountKeys") or []:
                account_keys.append(key.get("pubkey", "") if isinstance(key, dict) else key)

            instructions = list(message.get("instructions") or [])
            for inner in meta.get("innerInstructions") or []:
                instructions.extend(inner.get("instructions") or [])

            for ix in instructions:
                parsed = ix.get("parsed")
                if not isinstance(parsed, dict):
                    continue
                kind = str(parsed.get("type") or "")
                mint = (parsed.get("info") or {}).get("mint")
                if not isinstance(mint, str) or not is_valid_solana_address(mint):
                    continue
                if kind.startswith("initializeMint") or (kind == "mintTo" and mint.endswith("pump")):
                    signatures = (tx.get("transaction") or {}).get("signatures") or [None]
                    return {
                        "mint_address": mint,
                        "program_type": "pumpfun",
                        "name": None,
                        "symbol": None,
                        "image_url": None,
                        "dev_wallet": account_keys[0] if account_keys else None,
                        "signature": signatures[0],
                        "source": "pumpfun",
                    }
            return None
        except Exception as exc:
            print(f"PumpFun parse error: {type(exc).__name__}: {exc}")
            return None
