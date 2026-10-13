import json
import aiohttp


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
        """
        Accepts three shapes:
        1. [{"trackedWalletAddress": "...", "name": "...", "emoji": "🥥", "alertsOn": true}, ...]
        2. [{"address": "...", "label": "..."}, ...]
        3. ["<addr1>", "<addr2>", ...]
        """
        try:
            data = json.loads(json_content)
        except Exception as e:
            print(f"JSON parse error: {e}")
            return []

        if isinstance(data, dict):
            wallets = data.get("wallets") or data.get("trackedWallets") or []
        elif isinstance(data, list):
            wallets = data
        else:
            return []

        parsed = []
        skipped_disabled = 0
        skipped_invalid = 0

        for w in wallets:
            if isinstance(w, str):
                if len(w) >= 32:
                    parsed.append({"address": w, "label": None})
                else:
                    skipped_invalid += 1
                continue

            if not isinstance(w, dict):
                skipped_invalid += 1
                continue

            address = (
                w.get("trackedWalletAddress")
                or w.get("address")
                or w.get("pubkey")
                or w.get("wallet")
            )

            if not address or not isinstance(address, str) or len(address) < 32:
                skipped_invalid += 1
                continue

            if w.get("alertsOn") is False:
                skipped_disabled += 1
                continue

            name = w.get("name") or w.get("label")
            emoji = w.get("emoji")

            if emoji and name:
                label = f"{emoji} {name}"
            elif name:
                label = name
            elif emoji:
                label = emoji
            else:
                label = None

            parsed.append({"address": address, "label": label})

        if skipped_disabled:
            print(f"parse_wallet_json: skipped {skipped_disabled} wallet(s) with alertsOn=false")
        if skipped_invalid:
            print(f"parse_wallet_json: skipped {skipped_invalid} invalid entry/entries")

        return parsed