from config import Config


class RaydiumDetector:
    def __init__(self, rpc):
        self.rpc = rpc
        self.program_id = Config.RAYDIUM_AMM_PROGRAM
        self.seen_signatures = set()

    async def detect_new_pairs(self):
        try:
            signatures = await self.rpc.get_recent_signatures(
                self.program_id,
                limit=30
            )

            new_pairs = []

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

                pair_data = self._parse_pair_creation(tx)
                if pair_data:
                    new_pairs.append(pair_data)

            return new_pairs

        except Exception as e:
            print(f"Raydium detection error: {e}")
            return []

    def _parse_pair_creation(self, tx: dict):
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

            log_messages = meta.get("logMessages", [])

            for log in log_messages:
                if "initialize" in log.lower() or "create" in log.lower():
                    for key in account_keys:
                        if len(key) == 44 and key != self.program_id:
                            return {
                                "mint_address": key,
                                "program_type": "raydium",
                                "name": None,
                                "symbol": None,
                                "uri": None,
                                "dev_wallet": account_keys[0] if account_keys else None,
                                "signature": tx.get("transaction", {}).get("signatures", [None])[0]
                            }

            return None

        except Exception as e:
            print(f"Raydium parse error: {e}")
            return None
