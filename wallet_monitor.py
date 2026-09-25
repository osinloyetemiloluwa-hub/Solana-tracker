import time
import aiohttp

from config import Config


HELIUS_BASE = "https://api.helius.xyz/v0/webhooks"

_LAST_DEBUG = [0.0]
_DEBUG_INTERVAL = 3.0


def _dbg(msg: str):
    now = time.monotonic()
    if now - _LAST_DEBUG[0] >= _DEBUG_INTERVAL:
        _LAST_DEBUG[0] = now
        print(f"[webhook] {msg}")


class WalletMonitor:
    """Receives Helius enhanced webhook events for tracked wallets."""

    def __init__(self, bot):
        self.bot = bot
        self.session = None
        self.current_webhook_id = Config.HELIUS_WEBHOOK_ID or None

    async def start(self):
        if not self.session:
            self.session = aiohttp.ClientSession()
        if Config.HELIUS_API_KEY and Config.WEBHOOK_BASE_URL:
            try:
                await self.sync_webhook()
            except Exception as e:
                print(f"Webhook sync on boot failed: {type(e).__name__}: {e}")

    async def close(self):
        if self.session:
            await self.session.close()

    async def sync_webhook(self):
        if not Config.HELIUS_API_KEY:
            print("Webhook sync skipped: no HELIUS_API_KEY")
            return
        if not Config.WEBHOOK_BASE_URL:
            print("Webhook sync skipped: no WEBHOOK_BASE_URL")
            return

        addresses = await self._collect_addresses()
        if not addresses:
            print("Webhook sync skipped: no addresses to watch")
            return

        url = f"{HELIUS_BASE}?api-key={Config.HELIUS_API_KEY}"
        target = Config.WEBHOOK_BASE_URL.rstrip("/") + "/webhook/helius"

        try:
            async with self.session.get(url) as r:
                existing = await r.json() if r.status == 200 else []
        except Exception:
            existing = []

        if not isinstance(existing, list):
            existing = []

        mine = None
        for w in existing:
            if not isinstance(w, dict):
                continue
            if (w.get("webhookURL") or "").rstrip("/") == target:
                mine = w
                break

        payload = {
            "webhookURL": target,
            "transactionTypes": ["SWAP"],
            "accountAddresses": addresses,
            "webhookType": "enhanced",
        }

        try:
            if mine:
                wid = mine.get("webhookID")
                async with self.session.put(
                    f"{HELIUS_BASE}/{wid}?api-key={Config.HELIUS_API_KEY}",
                    json=payload,
                ) as r:
                    if r.status in (200, 204):
                        self.current_webhook_id = wid
                        print(f"Webhook updated: {wid} ({len(addresses)} addresses)")
                    else:
                        body = await r.text()
                        if r.status == 429 and ("-32429" in body or "max usage" in body.lower()):
                            print("Webhook update deferred: Helius account usage limit reached")
                        elif r.status == 429:
                            retry_after = r.headers.get("Retry-After", "unknown")
                            print(f"Webhook update rate-limited; Retry-After={retry_after}s")
                        else:
                            print(f"Webhook update failed: HTTP {r.status} - {body[:200]}")
            else:
                async with self.session.post(url, json=payload) as r:
                    if r.status in (200, 201):
                        data = await r.json()
                        self.current_webhook_id = data.get("webhookID")
                        print(f"Webhook created: {self.current_webhook_id} ({len(addresses)} addresses)")
                    else:
                        body = await r.text()
                        if r.status == 429 and ("-32429" in body or "max usage" in body.lower()):
                            print("Webhook sync deferred: Helius account usage limit reached")
                        elif r.status == 429:
                            retry_after = r.headers.get("Retry-After", "unknown")
                            print(f"Webhook sync rate-limited; Retry-After={retry_after}s")
                        else:
                            print(f"Webhook create failed: HTTP {r.status} - {body[:200]}")
        except Exception as e:
            print(f"Webhook sync error: {type(e).__name__}: {e}")

    async def _collect_addresses(self):
        wallets = await self.bot.db.get_all_wallets()
        addrs = set()
        for w in wallets:
            a = (w["address"] or "").strip()
            if a:
                addrs.add(a)
        if Config.DEFAULT_WALLET:
            addrs.add(Config.DEFAULT_WALLET)
        return list(addrs)

    async def process_webhook(self, payload):
        if not isinstance(payload, list):
            return
        for tx in payload:
            try:
                await self._handle_transaction(tx)
            except Exception as e:
                print(f"Webhook tx parse error: {type(e).__name__}: {e}")

    async def _handle_transaction(self, tx):
        if not isinstance(tx, dict):
            return

        sig = tx.get("signature") or "?"
        tx_type = tx.get("type")
        source = tx.get("source")
        fee_payer = tx.get("feePayer") or tx.get("fee_payer")

        if not sig or sig == "?":
            return

        short = sig[:8]

        if tx_type not in ("SWAP", "BUY", "BUY_NFT", "UNKNOWN"):
            _dbg(f"{short} type={tx_type} filtered")
            return

        if tx.get("transactionError"):
            _dbg(f"{short} failed tx; skipping")
            return

        if await self.bot.db.wallet_buy_exists(sig):
            return

        scanner = getattr(self.bot, "scanner", None)
        wallet_index = getattr(scanner, "_wallet_index", {}) or {}
        if not wallet_index:
            _dbg(f"{short} wallet index empty; skipping")
            return

        account_data = tx.get("accountData") or []

        matched = []
        for entry in account_data:
            account = entry.get("account")
            if account and account.lower() in wallet_index:
                matched.append(entry)

        if not matched and fee_payer and fee_payer.lower() in wallet_index:
            for entry in account_data:
                if (entry.get("account") or "").lower() == fee_payer.lower():
                    matched.append(entry)
                    break
            if not matched:
                _dbg(f"{short} fee payer {fee_payer[:8]} tracked but no accountData entry")
                return

        if not matched:
            _dbg(f"{short} no tracked wallet in accountData (type={tx_type} source={source})")
            return

        for entry in matched:
            account = entry.get("account")
            native_change = entry.get("nativeBalanceChange")
            if native_change is None:
                native_change = 0
            try:
                native_change = int(native_change)
            except (TypeError, ValueError):
                native_change = 0

            if native_change >= 0:
                _dbg(f"{short} {account[:8]} nativeChange={native_change} (not a buy)")
                continue

            token_changes = entry.get("tokenBalanceChanges") or []
            for tc in token_changes:
                mint = tc.get("mint")
                raw_amount = (tc.get("rawTokenAmount") or {}).get("tokenAmount")
                if not mint or raw_amount is None:
                    continue
                try:
                    tokens_recv = float(raw_amount)
                except (TypeError, ValueError):
                    continue
                if tokens_recv <= 0:
                    continue

                sol_spent = abs(native_change) / 1e9
                print(
                    f"TRACKED WALLET BUY DETECTED: wallet={account[:8]}... "
                    f"mint={mint[:8]}... sol={sol_spent:.3f} "
                    f"type={tx_type} source={source}"
                )

                event = {
                    "wallet": account,
                    "mint": mint,
                    "signature": sig,
                    "sol_spent": sol_spent,
                    "tokens_received": tokens_recv,
                    "timestamp": tx.get("timestamp") or int(time.time()),
                    "slot": tx.get("slot"),
                }
                await self._dispatch_buy(event)
                return

        _dbg(f"{short} tracked wallet had no positive token change")

    async def _dispatch_buy(self, event):
        await self.bot.db.insert_wallet_buy(event)
        await self.bot.db.mark_wallet_seen(event["wallet"])
        scanner = getattr(self.bot, "scanner", None)
        wallet_match = scanner._match_wallet(event["wallet"]) if scanner else None
        if wallet_match:
            event["wallet_label"] = wallet_match.get("label")
            event["guild_id"] = wallet_match.get("guild_id")
            event["channel_id"] = wallet_match.get("channel_id")
        else:
            event["guild_id"] = None
            event["channel_id"] = None
        await self.bot.wallet_signals.handle_buy(event)