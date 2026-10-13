# Solana Meme Tracker — Render Free Web

Discord bot that scans Solana (pump.fun + Raydium) for new tokens and posts alerts.

## Render configuration

- Service type: **Web Service**
- Plan: **Free**
- Runtime: **Python 3.11.9**
- Build: `pip install -r requirements.txt`
- Start: `python bot.py`
- Health check: `/health`

## Required environment variables

```text
DISCORD_TOKEN=your_bot_token
DATABASE_URL=your_postgres_connection_string