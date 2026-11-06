"""Per-guild hard alert filters (stored in guild_settings, all default off).

A filter that is NULL/unset is off, so guilds that never configure anything
behave exactly as before. Failing tokens stay in the database and keep being
tracked; only that guild's alert is suppressed.

Missing data: a *minimum* filter treats an unknown metric as failing (the
token cannot be shown to meet the bar); a *maximum* filter treats an unknown
metric as passing (nothing shows the token exceeds the cap).
"""

# column -> (metric key in the token dict, "min" | "max", label, unit)
FILTERS = {
    "min_buy_volume_usd": ("buy_volume_1h", "min", "Min real buy-side volume (1h)", "$"),
    "min_volume_5m_usd": ("volume_5m", "min", "Min 5m volume", "$"),
    "max_snipers": ("sniper_count", "max", "Max snipers", ""),
    "min_age_seconds": ("age_seconds", "min", "Min token age", "s"),
    "min_liquidity_usd": ("liquidity", "min", "Min liquidity", "$"),
    "max_dev_rugs": ("dev_rugged", "max", "Max dev rugs", ""),
}
FILTER_COLUMNS = tuple(FILTERS)


def _get(settings, key):
    if settings is None:
        return None
    try:
        return settings.get(key)
    except AttributeError:
        try:
            return settings[key]
        except (KeyError, IndexError, TypeError):
            return None


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def active_filters(settings):
    """{column: float} for every filter that is switched on."""
    out = {}
    for column in FILTER_COLUMNS:
        value = _as_float(_get(settings, column))
        if value is not None:
            out[column] = value
    return out


def evaluate_filters(settings, metrics):
    """Return (True, None) when the token passes, else (False, reason)."""
    metrics = metrics or {}
    for column, threshold in active_filters(settings).items():
        key, kind, label, _unit = FILTERS[column]
        value = _as_float(metrics.get(key))
        if kind == "min":
            if value is None or value < threshold:
                return False, f"{label} {_show(value)} < {threshold:g}"
        else:
            if value is not None and value > threshold:
                return False, f"{label} {_show(value)} > {threshold:g}"
    return True, None


def _show(value):
    return "unknown" if value is None else f"{value:g}"


def describe_filters(settings):
    """Human-readable lines for the active filters ('' when none are on)."""
    lines = []
    for column, threshold in active_filters(settings).items():
        _key, _kind, label, unit = FILTERS[column]
        if column == "min_age_seconds":
            text = f"{threshold / 60:g} min"
        elif unit == "$":
            text = f"${threshold:,.0f}"
        else:
            text = f"{threshold:g}"
        lines.append(f"{label}: {text}")
    return "\n".join(lines)
