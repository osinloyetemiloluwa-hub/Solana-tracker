"""Parser for the /history and /api/history `filter` string.

Conditions are separated by spaces or commas and are AND-ed together:

    volume>5k            1h USD volume at or above 5,000
    buyvol>2k            real buy-side 1h USD volume at or above 2,000
    vol24>50k            24h USD volume at or above 50,000
    liq>3k               liquidity at or above 3,000
    mc>5k  mc<50k        market cap bounds
    mc:5k-20k            market cap range (also mc=5k-20k)
    band:high            band name; bare `high`, `medium`, `low` also work.
                         Several band words are OR-ed:  high medium
    age<30m  age>2h      token age bounds (s, m, h, d)
    age:10m-1h           age window

Numbers accept k / m / b suffixes and an optional leading $. `>`/`>=` set an
inclusive minimum and `<`/`<=` an inclusive maximum.
"""
import re

BANDS = ("low", "medium", "high")

SYNTAX_HELP = (
    "Examples: `volume>5k`, `mc:5k-20k`, `band:high`, `age<30m`, `buyvol>2k`. "
    "Keys: volume, buyvol, vol24, liq, mc, band, age."
)

_ALIASES = {
    "volume": "volume", "vol": "volume", "vol1h": "volume", "volume1h": "volume",
    "vol24": "vol24", "volume24h": "vol24", "vol24h": "vol24",
    "buyvol": "buyvol", "buyvolume": "buyvol", "realvolume": "buyvol",
    "liq": "liq", "liquidity": "liq", "lp": "liq",
    "mc": "mc", "marketcap": "mc", "cap": "mc",
    "age": "age",
    "band": "band",
}
_NUMBER_KEYS = ("volume", "vol24", "buyvol", "liq", "mc")

_COND_RE = re.compile(r"^(?P<key>[a-z0-9]+)(?P<op>>=|<=|>|<|=|:)(?P<val>.+)$")
_NUM_RE = re.compile(r"^\$?(\d+(?:\.\d+)?)([kmb]?)$")
_AGE_RE = re.compile(r"^(\d+(?:\.\d+)?)([smhd])$")
_MULT = {"": 1.0, "k": 1e3, "m": 1e6, "b": 1e9}
_AGE_MULT = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


class FilterError(ValueError):
    """Raised with a user-facing message when a filter string cannot be parsed."""


def _parse_number(text: str) -> float:
    match = _NUM_RE.match(text.strip().lower())
    if not match:
        raise FilterError(f"`{text}` is not a number. {SYNTAX_HELP}")
    return float(match.group(1)) * _MULT[match.group(2)]


def _parse_age(text: str) -> float:
    match = _AGE_RE.match(text.strip().lower())
    if not match:
        raise FilterError(f"`{text}` is not an age like 30m, 2h or 1d. {SYNTAX_HELP}")
    return float(match.group(1)) * _AGE_MULT[match.group(2)]


def _bound(result, key, lo=None, hi=None):
    cur_lo, cur_hi = result.get(key, (None, None))
    if lo is not None:
        cur_lo = lo if cur_lo is None else max(cur_lo, lo)
    if hi is not None:
        cur_hi = hi if cur_hi is None else min(cur_hi, hi)
    if cur_lo is not None and cur_hi is not None and cur_lo > cur_hi:
        raise FilterError(f"The `{key}` conditions contradict each other. {SYNTAX_HELP}")
    result[key] = (cur_lo, cur_hi)


def parse_history_filter(text):
    """Return {"mc": (min, max), "age": (min_s, max_s), ..., "band": [..]}.

    Empty / None input returns {}. Raises FilterError on anything unparseable.
    """
    result = {}
    if text is None or not str(text).strip():
        return result

    for raw in re.split(r"[\s,]+", str(text).strip().lower()):
        if not raw:
            continue
        if raw in BANDS:
            result.setdefault("band", [])
            if raw not in result["band"]:
                result["band"].append(raw)
            continue

        match = _COND_RE.match(raw)
        if not match:
            raise FilterError(f"Could not understand `{raw}`. {SYNTAX_HELP}")
        key = _ALIASES.get(match.group("key"))
        if key is None:
            raise FilterError(f"Unknown filter `{match.group('key')}`. {SYNTAX_HELP}")
        op, val = match.group("op"), match.group("val")

        if key == "band":
            if op not in (":", "="):
                raise FilterError(f"Use `band:high`. {SYNTAX_HELP}")
            for band in val.split("|"):
                if band not in BANDS:
                    raise FilterError(f"Unknown band `{band}` (low, medium or high).")
                result.setdefault("band", [])
                if band not in result["band"]:
                    result["band"].append(band)
            continue

        parse = _parse_number if key in _NUMBER_KEYS else _parse_age
        if op in (":", "=") and "-" in val:
            low_text, _, high_text = val.partition("-")
            lo, hi = parse(low_text), parse(high_text)
            if lo > hi:
                lo, hi = hi, lo
            _bound(result, key, lo=lo, hi=hi)
        elif op in (">", ">="):
            _bound(result, key, lo=parse(val))
        elif op in ("<", "<="):
            _bound(result, key, hi=parse(val))
        else:
            value = parse(val)
            _bound(result, key, lo=value, hi=value)
    return result


def describe_filter(parsed) -> str:
    """Short human-readable summary used in command responses."""
    if not parsed:
        return ""
    parts = []
    for key in ("mc", "volume", "buyvol", "vol24", "liq", "age"):
        if key not in parsed:
            continue
        lo, hi = parsed[key]
        if key == "age":
            fmt = _fmt_age
        else:
            fmt = _fmt_num
        if lo is not None and hi is not None:
            parts.append(f"{key} {fmt(lo)}-{fmt(hi)}")
        elif lo is not None:
            parts.append(f"{key} ≥ {fmt(lo)}")
        elif hi is not None:
            parts.append(f"{key} ≤ {fmt(hi)}")
    if parsed.get("band"):
        parts.append("band " + "/".join(parsed["band"]))
    return ", ".join(parts)


def _fmt_num(value):
    for suffix, size in (("b", 1e9), ("m", 1e6), ("k", 1e3)):
        if value >= size:
            return f"{value / size:g}{suffix}"
    return f"{value:g}"


def _fmt_age(seconds):
    for suffix, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size:
            return f"{seconds / size:g}{suffix}"
    return f"{seconds:g}s"
