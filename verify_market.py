"""Market-data accuracy report: a real number instead of a feeling.

Compares the market cap stored in the database for currently tracked tokens
with a fresh DexScreener reading taken right now and prints the difference.
Read-only.

    python verify_market.py            # 30 most recently refreshed tokens
    python verify_market.py --limit 60

Stored values are at most one refresh interval old, so small differences on
moving tokens are normal. Large ones mean stale or wrong data.
"""
import argparse
import asyncio
import statistics

import aiohttp
import asyncpg

from config import Config
from dexscreener import DexScreenerFeeds

BIG_DIFF_PCT = 15.0


async def main(limit):
    pool = await asyncpg.create_pool(dsn=Config.DATABASE_URL, min_size=1, max_size=2)
    try:
        rows = await pool.fetch(
            """
            SELECT mint_address, symbol, current_market_cap, last_tracked_at
            FROM coins
            WHERE is_active = TRUE AND COALESCE(is_dead, FALSE) = FALSE
              AND current_market_cap IS NOT NULL
            ORDER BY last_tracked_at DESC LIMIT $1
            """,
            limit,
        )
    finally:
        await pool.close()
    if not rows:
        print("No tracked tokens with a stored market cap yet.")
        return 0

    feeds = DexScreenerFeeds()
    live = {}
    async with aiohttp.ClientSession() as session:
        for start in range(0, len(rows), 30):
            batch = [r["mint_address"] for r in rows[start:start + 30]]
            live.update(await feeds.fetch_tokens(session, batch) or {})

    diffs, missing = [], 0
    for row in rows:
        now = (live.get(row["mint_address"]) or {}).get("market_cap")
        stored = float(row["current_market_cap"])
        if not now:
            missing += 1
            print(f"  {row['symbol'] or row['mint_address'][:6]:<10} stored ${stored:>12,.0f}   live: no data")
            continue
        pct = (stored - now) / now * 100.0
        diffs.append(abs(pct))
        flag = "  <-- check" if abs(pct) > BIG_DIFF_PCT else ""
        print(f"  {row['symbol'] or row['mint_address'][:6]:<10} stored ${stored:>12,.0f}   live ${now:>12,.0f}   {pct:+7.1f}%{flag}")

    print()
    if diffs:
        big = sum(1 for d in diffs if d > BIG_DIFF_PCT)
        print(
            f"compared {len(diffs)} tokens: median diff {statistics.median(diffs):.1f}%, "
            f"worst {max(diffs):.1f}%, {big} over {BIG_DIFF_PCT:g}%, {missing} with no live data"
        )
    else:
        print(f"nothing comparable ({missing} tokens had no live data)")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=30)
    raise SystemExit(asyncio.run(main(max(1, min(parser.parse_args().limit, 300)))))
