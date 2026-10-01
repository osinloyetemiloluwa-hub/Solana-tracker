import discord


def create_token_alert_embed(token_data: dict, score_data: dict, safety_data: dict = None):
    mint = token_data.get("mint_address", "Unknown")
    program = token_data.get("program_type", "unknown").upper()
    score = score_data.get("score", 0)
    label = score_data.get("label", "low")
    reasons = score_data.get("reasons", [])

    color_map = {
        "high": discord.Color.green(),
        "medium": discord.Color.gold(),
        "low": discord.Color.greyple(),
    }

    embed = discord.Embed(
        title=f"🚀 New {program} Token",
        description=f"**{token_data.get('name', 'Unknown')}** (`{token_data.get('symbol', '???')}`)",
        color=color_map.get(label, discord.Color.blue()),
        timestamp=discord.utils.utcnow(),
    )

    image_url = token_data.get("image_url")
    if image_url:
        embed.set_thumbnail(url=image_url)

    embed.add_field(name="Contract Address", value=f"`{mint}`", inline=False)
    embed.add_field(name="Score", value=f"**{score}/100** ({label.upper()})", inline=True)
    embed.add_field(name="Program", value=program, inline=True)

    if token_data.get("dev_wallet"):
        embed.add_field(
            name="Dev Wallet",
            value=f"`{token_data['dev_wallet'][:8]}...{token_data['dev_wallet'][-8:]}`",
            inline=True,
        )

    if reasons:
        embed.add_field(
            name="Signals",
            value="\n".join([f"• {r}" for r in reasons[:5]]),
            inline=False,
        )

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