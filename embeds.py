import time
import discord


def _fmt_usd(v):
    if v is None:
        return None
    try:
        return f"${float(v):,.0f}"
    except (TypeError, ValueError):
        return None


def _fmt_age(seconds):
    if seconds is None:
        return None
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        return None
    if seconds < 60:
        return f"{int(seconds)}s old"
    if seconds < 3600:
        return f"{int(seconds / 60)}m old"
    return f"{int(seconds / 3600)}h old"


def solscan_account_url(address: str) -> str:
    return f"https://solscan.io/account/{address}"


def dexscreener_url(mint: str) -> str:
    return f"https://dexscreener.com/solana/{mint}"


def _short(addr: str, head: int = 6, tail: int = 4) -> str:
    return f"{addr[:head]}...{addr[-tail:]}" if addr and len(addr) > head + tail else (addr or "")


def dev_wallet_link(dev_wallet):
    """Dev wallet as a clickable Solscan link, or None when there is no wallet."""
    if not dev_wallet:
        return None
    return f"[{_short(dev_wallet, 8, 8)}]({solscan_account_url(dev_wallet)})"


def pvp_ca_link(mint: str) -> str:
    """PVP sibling CA, hyperlinked straight to DexScreener."""
    return f"[{_short(mint, 6, 4)}]({dexscreener_url(mint)})"


def fmt_dev_rep(dev_rep):
    """'Launched 12 | Rugged 3'; None when there is no reputation data."""
    if not dev_rep:
        return None
    try:
        launched = int(dev_rep.get("launched") or 0)
        rugged = int(dev_rep.get("rugged") or 0)
    except (TypeError, ValueError, AttributeError):
        return None
    if launched <= 0:
        return None
    return f"Launched **{launched}** | Rugged **{rugged}**"


def fmt_sol(value):
    if value is None:
        return None
    try:
        return f"{float(value):,.2f} SOL"
    except (TypeError, ValueError):
        return None


def fmt_pvp_lines(siblings, limit: int = 10):
    """One line per sibling; fields with no data are left out, never shown as 0/None."""
    lines = []
    for sib in (siblings or [])[:limit]:
        mint = sib.get("mint_address")
        if not mint:
            continue
        parts = [pvp_ca_link(mint)]
        for label, key in (("MC", "market_cap"), ("Vol", "volume"), ("LP", "liquidity")):
            val = _fmt_usd(sib.get(key))
            if val and sib.get(key):
                parts.append(f"{label} {val}")
        age = _fmt_age(sib.get("age_seconds"))
        if age:
            parts.append(age)
        lines.append("• " + " · ".join(parts))
    return lines


def create_token_alert_embed(token_data: dict, score_data: dict, safety_data: dict = None):
    mint = token_data.get("mint_address", "Unknown")
    program = token_data.get("program_type", "unknown").upper()
    band = (score_data.get("band") or "low").lower()

    color_map = {
        "high": discord.Color.green(),
        "medium": discord.Color.gold(),
        "low": discord.Color.greyple(),
    }

    name = token_data.get("name") or "Unknown"
    symbol = token_data.get("symbol") or "???"

    age_str = _fmt_age(token_data.get("age_seconds"))
    if not age_str and token_data.get("created_at"):
        age_str = _fmt_age(time.time() - token_data["created_at"])

    # Short description: name/symbol, then age · CA.
    meta_parts = []
    if age_str:
        meta_parts.append(age_str)
    if mint and mint != "Unknown":
        meta_parts.append(f"`{mint}`")

    description = f"**{name}** (`{symbol}`)"
    if meta_parts:
        description += "\n" + " · ".join(meta_parts)

    embed = discord.Embed(
        title=f"🚀 New {program} Token",
        description=description,
        color=color_map.get(band, discord.Color.blue()),
        timestamp=discord.utils.utcnow(),
    )

    image_url = token_data.get("image_url")
    if image_url:
        embed.set_thumbnail(url=image_url)

    embed.add_field(name="Band", value=f"**{band.upper()}**", inline=True)

    mc = _fmt_usd(token_data.get("market_cap"))
    if mc:
        embed.add_field(name="Market Cap", value=mc, inline=True)

    lp = _fmt_usd(token_data.get("liquidity"))
    if lp:
        embed.add_field(name="Liquidity", value=lp, inline=True)

    vol = _fmt_usd(token_data.get("volume_24h"))
    if vol:
        embed.add_field(name="Volume 24h", value=vol, inline=True)

    embed.add_field(name="Program", value=program, inline=True)

    dev_link = dev_wallet_link(token_data.get("dev_wallet"))
    if dev_link:
        embed.add_field(name="Dev Wallet", value=dev_link, inline=True)

    launch_sol = fmt_sol(token_data.get("launch_sol"))
    if launch_sol:
        embed.add_field(name="Launch Buy", value=launch_sol, inline=True)

    dev_rep = fmt_dev_rep(token_data.get("dev_rep"))
    if dev_rep:
        embed.add_field(name="Dev Rep", value=dev_rep, inline=True)

    pvp_lines = fmt_pvp_lines(token_data.get("pvp_siblings"), limit=3)
    if pvp_lines:
        embed.add_field(name="PVP Siblings", value="\n".join(pvp_lines)[:1024], inline=False)

    if safety_data:
        safety_status = []
        if safety_data.get("mint_authority_revoked"):
            safety_status.append("✅ Mint Revoked")
        else:
            safety_status.append("⚠️ Mint Active")
        if safety_data.get("freeze_authority_revoked"):
            safety_status.append("✅ Freeze Revoked")
        else:
            safety_status.append("⚠️ Freeze Active")
        if safety_data.get("lp_burned"):
            safety_status.append("✅ LP Burned")
        embed.add_field(name="Safety Checks", value="\n".join(safety_status), inline=False)

    embed.set_footer(text="Solana Meme Tracker | DYOR")
    return embed


def _fmt_mc_value(value, unit):
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if unit == "sol":
        return f"{v:,.1f} SOL"
    return f"${v:,.0f}"


def create_fresh_callout_embed(callout: dict):
    """Fresh-launch callout sent after the T+5 verification.

    `callout` carries: mint_address, name, symbol, initial_mc, current_mc, mc_unit
    ("usd"/"sol"), change_pct, and optional image_url, launch_sol, liquidity,
    volume, band, dev_wallet, dev_rep, pvp_siblings, age_seconds.
    """
    mint = callout["mint_address"]
    band = (callout.get("band") or "").lower()
    color = {
        "high": discord.Color.green(),
        "medium": discord.Color.gold(),
        "low": discord.Color.greyple(),
    }.get(band, discord.Color.blue())

    description = f"**{callout['name']}** (`{callout['symbol']}`)\n`{mint}`"
    embed = discord.Embed(
        title="⚡ Fresh Launch",
        description=description,
        color=color,
        timestamp=discord.utils.utcnow(),
    )
    if callout.get("image_url"):
        embed.set_thumbnail(url=callout["image_url"])

    unit = callout.get("mc_unit") or "usd"
    initial = _fmt_mc_value(callout.get("initial_mc"), unit)
    current = _fmt_mc_value(callout.get("current_mc"), unit)
    pct = callout.get("change_pct")
    if initial and current and pct is not None:
        embed.add_field(
            name="Initial MC → Current MC",
            value=f"{initial} → **{current}** ({pct:+.1f}%)",
            inline=False,
        )

    if band:
        embed.add_field(name="Band", value=f"**{band.upper()}**", inline=True)
    launch_sol = fmt_sol(callout.get("launch_sol"))
    if launch_sol:
        embed.add_field(name="Launch Buy", value=launch_sol, inline=True)
    lp = _fmt_usd(callout.get("liquidity"))
    if lp and callout.get("liquidity"):
        embed.add_field(name="Liquidity", value=lp, inline=True)
    vol = _fmt_usd(callout.get("volume"))
    if vol and callout.get("volume"):
        embed.add_field(name="Volume", value=vol, inline=True)

    dev_link = dev_wallet_link(callout.get("dev_wallet"))
    if dev_link:
        embed.add_field(name="Dev Wallet", value=dev_link, inline=True)
    dev_rep = fmt_dev_rep(callout.get("dev_rep"))
    if dev_rep:
        embed.add_field(name="Dev Rep", value=dev_rep, inline=True)

    pvp_lines = fmt_pvp_lines(callout.get("pvp_siblings"), limit=3)
    if pvp_lines:
        embed.add_field(name="PVP Siblings", value="\n".join(pvp_lines)[:1024], inline=False)

    embed.set_footer(text="Solana Meme Tracker | Verified at T+5s | DYOR")
    return embed


def create_milestone_embed(coin_data: dict, multiplier: float, current_price: float, current_mc: float):
    pct = (float(multiplier) - 1.0) * 100.0

    embed = discord.Embed(
        title=f"📈 {multiplier}x Milestone Hit!",
        description=f"**{coin_data.get('name', 'Unknown')}** (`{coin_data.get('symbol', '???')}`)",
        color=discord.Color.gold(),
        timestamp=discord.utils.utcnow(),
    )

    image_url = coin_data.get("image_url")
    if image_url:
        embed.set_thumbnail(url=image_url)

    embed.add_field(name="Contract", value=f"`{coin_data.get('mint_address', 'Unknown')}`", inline=False)
    embed.add_field(name="Multiplier", value=f"**{multiplier}x**", inline=True)
    embed.add_field(name="Gain", value=f"**+{pct:,.0f}%**", inline=True)
    embed.add_field(
        name="Current Price",
        value=f"${current_price:.10f}" if current_price else "Unknown",
        inline=True,
    )
    embed.add_field(
        name="Market Cap",
        value=f"${current_mc:,.0f}" if current_mc else "Unknown",
        inline=True,
    )
    embed.set_footer(text="Solana Meme Tracker")
    return embed


def create_migration_embed(coin_data: dict, token_data: dict):
    mint = coin_data.get("mint_address") or token_data.get("mint_address") or "Unknown"
    embed = discord.Embed(
        title="🚀 MIGRATED to PumpSwap",
        description=f"**{coin_data.get('name', 'Unknown')}** (`{coin_data.get('symbol', '???')}`)",
        color=discord.Color.green(),
        timestamp=discord.utils.utcnow(),
    )

    image_url = coin_data.get("image_url") or token_data.get("image_url")
    if image_url:
        embed.set_thumbnail(url=image_url)

    embed.add_field(name="Contract", value=f"`{mint}`", inline=False)

    current_mc = coin_data.get("current_market_cap")
    if current_mc:
        embed.add_field(name="Market Cap", value=f"${float(current_mc):,.0f}", inline=True)

    first_mc = coin_data.get("first_seen_market_cap")
    if first_mc:
        embed.add_field(name="First MC", value=f"${float(first_mc):,.0f}", inline=True)
        try:
            mult = float(current_mc) / float(first_mc)
            embed.add_field(name="Multiple", value=f"**{mult:.2f}x**", inline=True)
        except Exception:
            pass

    embed.set_footer(text="Solana Meme Tracker | Graduated from pump.fun")
    return embed


def create_wallet_alert_embed(wallet_address: str, label: str, token_data: dict):
    display = label or (wallet_address[:8] + "..." if wallet_address else "Unknown")
    embed = discord.Embed(
        title="👀 Watched Wallet Activity",
        description=f"Wallet **{display}** interacted with new token",
        color=discord.Color.purple(),
        timestamp=discord.utils.utcnow(),
    )

    image_url = token_data.get("image_url")
    if image_url:
        embed.set_thumbnail(url=image_url)

    embed.add_field(
        name="Token",
        value=f"**{token_data.get('name', 'Unknown')}** (`{token_data.get('symbol', '???')}`)",
        inline=False,
    )
    embed.add_field(
        name="Contract",
        value=f"`{token_data.get('mint_address', 'Unknown')}`",
        inline=False,
    )
    if wallet_address:
        embed.add_field(name="Wallet", value=f"`{wallet_address}`", inline=False)

    return embed


def create_wallet_buy_alert_embed(payload: dict):
    event = payload["event"]
    recent = payload["recent"]
    market = payload["market"]
    signal = payload["signal"]
    reasons = payload["reasons"]

    mint = event.get("mint", "Unknown")
    symbol = event.get("symbol") or ""
    name = event.get("name") or ""

    color_map = {
        "HIGH-CONVICTION SETUP": discord.Color.green(),
        "STRONG WATCH": discord.Color.gold(),
        "WATCH": discord.Color.blue(),
        "AVOID / HIGH RISK": discord.Color.red(),
    }

    embed = discord.Embed(
        title="🚨 Tracked Wallet Buy",
        description=f"**{name or 'Unknown'}** (`{symbol or '???'}`)",
        color=color_map.get(signal, discord.Color.blurple()),
        timestamp=discord.utils.utcnow(),
    )

    image_url = market.get("image_url")
    if image_url:
        embed.set_thumbnail(url=image_url)

    embed.add_field(name="Contract", value=f"`{mint}`", inline=False)
    embed.add_field(name="Signal", value=f"**{signal}**", inline=True)

    mc = market.get("market_cap") or 0
    if mc:
        embed.add_field(name="Market Cap", value=f"${mc:,.0f}", inline=True)
    liq = market.get("liquidity") or 0
    if liq:
        embed.add_field(name="Liquidity", value=f"${liq:,.0f}", inline=True)

    buyers_lines = []
    for b in recent[:5]:
        size = b.get("sol_spent") or 0
        wallet = b.get("wallet") or "?"
        label = wallet[:8] + "..." + wallet[-6:]
        buyers_lines.append(f"• `{label}` — {size:.2f} SOL")
    if buyers_lines:
        embed.add_field(
            name=f"Buyers ({payload['wallet_count']})",
            value="\n".join(buyers_lines),
            inline=False,
        )

    if reasons:
        embed.add_field(
            name="Why Flagged",
            value="\n".join([f"• {r}" for r in reasons[:5]]),
            inline=False,
        )

    embed.set_footer(text="Solana Meme Tracker | Wallet Intelligence")
    return embed


def create_link_view(mint_address: str):
    view = discord.ui.View(timeout=None)
    view.add_item(discord.ui.Button(
        label="Dexscreener",
        url=f"https://dexscreener.com/solana/{mint_address}",
        style=discord.ButtonStyle.link,
        emoji="📊",
    ))
    view.add_item(discord.ui.Button(
        label="Birdeye",
        url=f"https://birdeye.so/token/{mint_address}?chain=solana",
        style=discord.ButtonStyle.link,
        emoji="🦅",
    ))
    view.add_item(discord.ui.Button(
        label="Solscan",
        url=f"https://solscan.io/token/{mint_address}",
        style=discord.ButtonStyle.link,
        emoji="🔍",
    ))
    return view