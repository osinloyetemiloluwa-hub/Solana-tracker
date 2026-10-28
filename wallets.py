import json
import aiohttp

from enricher import is_valid_solana_address


class WalletTracker:
    def __init__(self):
        self.session = None

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()

    async def get_wallet_label(self, wallet_address: str):
        return None

    def parse_wallet_json(self, json_content: str):
        """Parse wallet JSON or plain text, validate base58, and de-duplicate."""
        try:
            data = json.loads(json_content)
        except (json.JSONDecodeError, TypeError):
            data = None

        if isinstance(data, dict):
            wallets = data.get("wallets") or data.get("trackedWallets") or []
        elif isinstance(data, list):
            wallets = data
        elif data is None:
            wallets = []
            for line in (json_content or "").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = [part.strip() for part in line.split(",", 1)]
                wallets.append({
                    "address": parts[0],
                    "label": parts[1] if len(parts) > 1 and parts[1] else None,
                })
        else:
            return []

        parsed = []
        seen = set()
        skipped_disabled = 0
        skipped_invalid = 0

        for w in wallets:
            if isinstance(w, str):
                w = {"address": w}
            if not isinstance(w, dict):
                skipped_invalid += 1
                continue
            if w.get("alertsOn") is False:
                skipped_disabled += 1
                continue

            address = (
                w.get("trackedWalletAddress") or w.get("address")
                or w.get("pubkey") or w.get("wallet") or ""
            )
            address = address.strip() if isinstance(address, str) else ""
            if not is_valid_solana_address(address) or address in seen:
                skipped_invalid += 1
                continue

            name = w.get("name") or w.get("label")
            emoji = w.get("emoji")
            if emoji and name:
                label = f"{emoji} {name}"
            else:
                label = name or emoji or None
            if label:
                label = str(label).strip()[:100] or None

            parsed.append({"address": address, "label": label})
            seen.add(address)
            if len(parsed) >= 1000:
                print("Wallet import capped at 1000 unique addresses")
                break

        if skipped_disabled:
            print(f"Wallet import: skipped {skipped_disabled} disabled wallet(s)")
        if skipped_invalid:
            print(f"Wallet import: skipped {skipped_invalid} invalid/duplicate entrie(s)")
        return parsed