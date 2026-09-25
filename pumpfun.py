from config import Config


class PumpFunDetector:
    def __init__(self, rpc):
        self.rpc = rpc
        self.program_id = Config.PUMP_FUN_PROGRAM
        self.seen_signatures = set()

    async def detect_new_tokens(self):
        try:
            signatures = await self.rpc.get_recent_signatures(
                self.program_id,
                limit=30
            )

            new_tokens = []

            for sig_info in signatures:
                signature = sig_info.get("signature")
                if not signature or signature in self.seen_signatures:
                    continue

                self.seen_signatures.add(signature)

                if len(self.seen_signatures) > 500:
                    self.seen_signatures = set(list(self.seen_signatures)[-250:])

                tx = await self.rpc.get_transaction(signature)
                if not tx:
                    continue

                token_data = self._parse_token_creation(tx)
                if token_data:
                    new_tokens.append(token_data)

            return new_tokens

        except Exception as e:
            print(f"PumpFun detection error: {e}")
            return []

    def _parse_token_creation(self, tx: dict):
        try:
            meta = tx.get("meta", {})
            transaction = tx.get("transaction", {})
            message = transaction.get("message", {})

            account_keys = []
            for key in message.get("accountKeys", []):
                if isinstance(key, dict):
                    account_keys.append(key.get("pubkey", ""))
                else:
                    account_keys.append(key)

            instructions = message.get("instructions", [])
            inner_instructions = meta.get("innerInstructions", [])

            all_instructions = list(instructions)
            for inner in inner_instructions:
                all_instructions.extend(inner.get("instructions", []))

            for ix in all_instructions:
                parsed = ix.get("parsed", {})
                if not parsed:
                    continue

                if parsed.get("type") == "mintTo":
                    info = parsed.get("info", {})
                    mint = info.get("mint")
                    if mint:
                        return {
                            "mint_address": mint,
                            "program_type": "pumpfun",
                            "name": None,
                            "symbol": None,
                            "uri": None,
                            "dev_wallet": account_keys[0] if account_keys else None,
                            "signature": tx.get("transaction", {}).get("signatures", [None])[0]
                        }

            for key in account_keys:
                if len(key) == 44 and key != self.program_id:
                    return {
                        "mint_address": key,
                        "program_type": "pumpfun",
                        "name": None,
                        "symbol": None,
                        "uri": None,
                        "dev_wallet": account_keys[0] if account_keys else None,
                        "signature": tx.get("transaction", {}).get("signatures", [None])[0]
                    }

            return None

        except Exception as e:
            print(f"Parse error: {e}")
            return None
