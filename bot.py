import asyncio
import time
import contextlib
from aiohttp import web
import discord
from discord.ext import commands

from config import Config
from db import Database
from scanner import Scanner
from wallets import WalletTracker
from wallet_monitor import WalletMonitor
from wallet_signals import WalletSignalEngine


class DiscordSendQueue:
    """Single FIFO sender with a cooldown between token alerts."""

    def __init__(self, bot):
        self.bot = bot
        self.queue = asyncio.Queue()
        self.worker_task = None
        self.running = False
        self._last_token_alert_at = 0.0

    async def start(self):
        if self.worker_task and not self.worker_task.done():
            return
        self.running = True
        self.worker_task = asyncio.create_task(self._worker(), name="discord-send-queue")

    async def stop(self):
        self.running = False
        if self.worker_task:
            await self.queue.put(None)
            with contextlib.suppress(asyncio.CancelledError):
                await self.worker_task
            self.worker_task = None

    async def enqueue(self, channel_id: int, *, embed=None, view=None, content=None,
                      is_token_alert: bool = False):
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        await self.queue.put({
            "channel_id": int(channel_id),
            "embed": embed,
            "view": view,
            "content": content,
            "future": future,
            "is_token_alert": is_token_alert,
        })
        return await future

    async def _worker(self):
        while True:
            job = await self.queue.get()
            try:
                if job is None:
                    return

                if job.get("is_token_alert"):
                    now = time.monotonic()
                    elapsed = now - self._last_token_alert_at
                    cooldown = Config.TOKEN_ALERT_COOLDOWN_SECONDS
                    if elapsed < cooldown:
                        wait = cooldown - elapsed
                        print(f"Token alert cooldown: waiting {wait:.1f}s")
                        await asyncio.sleep(wait)
                    self._last_token_alert_at = time.monotonic()

                try:
                    result = await self._send_with_backoff(job)
                    if not job["future"].done():
                        job["future"].set_result(result)
                except Exception as exc:
                    if not job["future"].done():
                        job["future"].set_exception(exc)
                    print(f"Discord queue job failed: {type(exc).__name__}: {exc}")
            finally:
                self.queue.task_done()
                if job is not None:
                    await asyncio.sleep(Config.DISCORD_SEND_DELAY)

    async def _send_with_backoff(self, job):
        channel = self.bot.get_channel(job["channel_id"])
        if channel is None:
            channel = await self.bot.fetch_channel(job["channel_id"])

        last_error = None
        for attempt in range(Config.DISCORD_MAX_RETRIES + 1):
            try:
                return await channel.send(
                    content=job["content"],
                    embed=job["embed"],
                    view=job["view"],
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            except discord.HTTPException as exc:
                last_error = exc
                if exc.status == 429:
                    retry_after = getattr(exc, "retry_after", None)
                    if retry_after is None:
                        retry_after = min(2 ** min(attempt + 1, 6), 120)
                    retry_after = min(max(float(retry_after), 1.0), 120.0)
                    await asyncio.sleep(retry_after)
                    continue
                if 500 <= exc.status < 600 and attempt < Config.DISCORD_MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 30))
                    continue
                raise
            except (discord.ConnectionClosed, asyncio.TimeoutError) as exc:
                last_error = exc
                if attempt >= Config.DISCORD_MAX_RETRIES:
                    raise
                await asyncio.sleep(min(2 ** attempt, 30))

        raise last_error or RuntimeError("Discord send failed")


class MemeTrackerBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        super().__init__(command_prefix="!", intents=intents, help_command=None)

        self.db = Database()
        self.scanner = None
        self.wallet_tracker = WalletTracker()
        self.discord_queue = DiscordSendQueue(self)
        self.bg_task = None

        self.wallet_monitor = None
        self.wallet_signals = None

    async def setup_hook(self):
        await self.db.connect()
        print("Database connected")

        self.scanner = Scanner(self, self.db)
        await self.scanner.initialize()

        self.wallet_monitor = WalletMonitor(self)
        self.wallet_signals = WalletSignalEngine(self)
        await self.wallet_monitor.start()

        await self.load_extension("commands")
        await self.discord_queue.start()

        # Global sync only. Old guild-scoped commands are not cleared on
        # every boot because doing one API sync per guild can itself trigger
        # Discord rate limits. Set WIPE_GUILD_COMMANDS=true once if a cleanup
        # is actually needed.
        if getattr(Config, "WIPE_GUILD_COMMANDS", False):
            await self._wipe_guild_scoped_commands()

        # Single global sync — the only sync we ever do.
        try:
            synced = await self.tree.sync()
            print(f"Global sync: {len(synced)} command(s) registered app-wide")
        except discord.HTTPException as exc:
            if exc.status == 429:
                print("Global command sync rate-limited (HTTP 429); will retry on next boot")
            else:
                print(f"Global command sync failed: HTTP {exc.status}: {exc}")
        except Exception as exc:
            print(f"Global command sync failed: {type(exc).__name__}: {exc}")

        self.bg_task = asyncio.create_task(self.scanner.start(), name="solana-scanner")

    async def _wipe_guild_scoped_commands(self):
        for guild in self.guilds:
            try:
                self.tree.clear_commands(guild=guild)
                await self.tree.sync(guild=guild)
                print(f"Cleared guild-scoped commands for {guild.name} ({guild.id})")
            except Exception as exc:
                print(f"Guild-scoped cleanup failed for {guild.id}: {type(exc).__name__}: {exc}")

    async def on_ready(self):
        print(f"Logged in as {self.user}")
        print(f"Connected to {len(self.guilds)} guilds")

        try:
            await self.change_presence(
                activity=discord.Activity(
                    type=discord.ActivityType.watching, name="Solana memes"
                )
            )
        except discord.HTTPException as exc:
            print(f"Presence update skipped: HTTP {exc.status}")

        for guild in self.guilds:
            try:
                await self.db.get_or_create_guild(guild.id)
            except Exception as exc:
                print(f"Guild initialization failed for {guild.id}: {exc}")

    async def on_guild_join(self, guild):
        await self.db.get_or_create_guild(guild.id)
        print(f"Joined guild: {guild.name} ({guild.id})")

    async def send_discord_alert(self, channel_id, *, embed=None, view=None, content=None,
                                 is_token_alert: bool = False):
        return await self.discord_queue.enqueue(
            channel_id, embed=embed, view=view, content=content,
            is_token_alert=is_token_alert,
        )

    async def close(self):
        if self.scanner:
            await self.scanner.stop()
        if self.discord_queue:
            await self.discord_queue.stop()
        if self.scanner:
            await self.scanner.close()
        if self.wallet_monitor:
            await self.wallet_monitor.close()
        await self.db.close()
        await super().close()


async def health(_request):
    return web.json_response({"status": "ok", "service": "solana-meme-tracker"})


async def root(_request):
    return web.json_response({"status": "online", "service": "solana-meme-tracker"})


async def webhook_handler(request):
    bot = request.app.get("bot")
    if not bot or not bot.wallet_monitor:
        return web.json_response({"status": "ignored"}, status=200)
    try:
        data = await request.json()
        await bot.wallet_monitor.process_webhook(data)
    except Exception as e:
        print(f"Webhook handler error: {type(e).__name__}: {e}")
        return web.json_response({"status": "error"}, status=500)
    return web.json_response({"status": "ok"})


async def start_web_server(bot):
    app = web.Application()
    app["bot"] = bot
    app.router.add_get("/", root)
    app.router.add_get("/health", health)
    app.router.add_get("/healthz", health)
    app.router.add_post("/webhook/helius", webhook_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
    await site.start()
    print(f"HTTP health server listening on 0.0.0.0:{Config.PORT}")
    return runner


async def run_discord_forever(bot):
    delay = Config.DISCORD_LOGIN_RETRY_INITIAL
    while True:
        try:
            await bot.start(Config.DISCORD_TOKEN, reconnect=True)
            print("Discord connection ended; reconnecting in 15s")
            delay = Config.DISCORD_LOGIN_RETRY_INITIAL
            await asyncio.sleep(15)
        except discord.HTTPException as exc:
            if exc.status == 429:
                print(
                    "Discord/Cloudflare rate limit during login (HTTP 429/1015). "
                    f"Waiting {delay}s before trying again."
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, Config.DISCORD_LOGIN_RETRY_MAX)
                continue
            print(f"Discord HTTP error during startup: {exc}. Retrying in 30s")
            await asyncio.sleep(30)
        except (discord.LoginFailure, discord.PrivilegedIntentsRequired) as exc:
            print(f"Discord configuration error: {exc}")
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Discord connection error: {type(exc).__name__}: {exc}. Retrying in 30s")
            await asyncio.sleep(30)


async def main():
    Config.validate()
    bot = MemeTrackerBot()
    web_runner = await start_web_server(bot)
    try:
        await run_discord_forever(bot)
    finally:
        with contextlib.suppress(Exception):
            await bot.close()
        with contextlib.suppress(Exception):
            await web_runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())