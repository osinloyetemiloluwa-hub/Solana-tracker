"""Buy-side volume.

DexScreener exposes total USD volume and buy/sell *counts* per window, not
buy-side USD volume. The approximation is

    buy_volume = volume * buys / (buys + sells)

When PumpPortal trades were observed for a mint, the rolling calculation from
those trades is preferred because it uses real per-trade amounts.
"""


def _num(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def approx_buy_volume(volume, buys, sells):
    """Trade-count approximation of buy-side USD volume; None when not computable."""
    vol = _num(volume)
    b = _num(buys)
    s = _num(sells)
    if vol is None or vol <= 0 or b is None or s is None:
        return None
    total = b + s
    if total <= 0:
        return None
    return vol * b / total


def compute_volume_metrics(market, observed_1h=None, observed_5m=None):
    """Combine a DexScreener market dict with optional observed-trade windows.

    `observed_*` is the dict from TradeFlowTracker.window_stats(...) already
    converted to USD ({"volume", "buy_volume", "buys", "sells"}) or None.
    Returns only the keys that have data. `buy_volume_source` is "observed"
    when the 1h figure came from PumpPortal trades, else "approx".
    """
    market = market or {}
    out = {}

    vol_1h = market.get("volume_1h")
    approx_1h = approx_buy_volume(vol_1h, market.get("buys_1h"), market.get("sells_1h"))
    vol_5m = market.get("volume_5m")
    approx_5m = approx_buy_volume(vol_5m, market.get("buys_5m"), market.get("sells_5m"))

    if approx_1h is not None:
        out["buy_volume_1h_approx"] = approx_1h
    if approx_5m is not None:
        out["buy_volume_5m_approx"] = approx_5m

    if observed_1h:
        out["volume_1h"] = observed_1h["volume"]
        out["buy_volume_1h"] = observed_1h["buy_volume"]
        out["buys_1h"] = observed_1h["buys"]
        out["sells_1h"] = observed_1h["sells"]
        out["buy_volume_source"] = "observed"
    else:
        if vol_1h is not None:
            out["volume_1h"] = vol_1h
        if approx_1h is not None:
            out["buy_volume_1h"] = approx_1h
            out["buy_volume_source"] = "approx"
        for key in ("buys_1h", "sells_1h"):
            if market.get(key) is not None:
                out[key] = market[key]

    if observed_5m:
        out["volume_5m"] = observed_5m["volume"]
        out["buy_volume_5m"] = observed_5m["buy_volume"]
    else:
        if vol_5m is not None:
            out["volume_5m"] = vol_5m
        if approx_5m is not None:
            out["buy_volume_5m"] = approx_5m
    for key in ("buys_5m", "sells_5m"):
        if market.get(key) is not None:
            out[key] = market[key]
    return out
