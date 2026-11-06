"""Bounded, API-free early trade intelligence from PumpPortal events."""

from collections import OrderedDict, deque
import time

from config import Config


def _number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _sol_amount(value):
    """PumpPortal commonly reports lamports; tolerate providers returning SOL."""
    amount = _number(value)
    return amount / 1_000_000_000 if abs(amount) >= 1_000_000 else amount


class TradeFlowTracker:
    def __init__(self):
        self._states = OrderedDict()
        self._last_prune = 0.0
        self.total_trades = 0
        self.last_trade_at = 0.0

    def register(self, event: dict):
        mint = event.get("mint")
        if not mint:
            return
        now = time.time()
        state = self._states.get(mint)
        if state is None:
            mcap = _number(event.get("marketCapSol"))
            state = {
                "mint": mint,
                "dev": event.get("traderPublicKey") or event.get("creator") or "",
                "created_at": now,
                "initial_mcap_sol": mcap,
                "last_mcap_sol": mcap,
                "buys": 0,
                "sells": 0,
                "sol_in": 0.0,
                "sol_out": 0.0,
                "buyers": set(),
                "non_dev_buyers": set(),
                "dev_sold": False,
                "signatures": set(),
                "snipers": set(),
                # (timestamp, is_buy, sol_amount): bounded rolling buffer
                "trades": deque(maxlen=Config.FLOW_TRADE_BUFFER),
                "trades_dropped": False,
            }
            self._states[mint] = state
        else:
            state["dev"] = state["dev"] or event.get("traderPublicKey") or ""
            self._states.move_to_end(mint)
        self._prune(now)

    def on_trade(self, event: dict):
        mint = event.get("mint")
        if not mint:
            return
        state = self._states.get(mint)
        if state is None:
            return

        signature = event.get("signature")
        if signature:
            if signature in state["signatures"]:
                return
            state["signatures"].add(signature)
            if len(state["signatures"]) > 1000:
                state["signatures"] = set(list(state["signatures"])[-500:])

        self.total_trades += 1
        self.last_trade_at = time.time()
        self._states.move_to_end(mint)

        tx_type = event.get("txType")
        trader = event.get("traderPublicKey") or ""
        sol = abs(_sol_amount(event.get("solAmount")))
        if tx_type not in ("buy", "sell"):
            return
        now = time.time()
        if len(state["trades"]) == state["trades"].maxlen:
            state["trades_dropped"] = True
        state["trades"].append((now, tx_type == "buy", sol))
        if tx_type == "buy":
            state["buys"] += 1
            state["sol_in"] += sol
            if trader:
                state["buyers"].add(trader)
                if trader != state["dev"]:
                    state["non_dev_buyers"].add(trader)
                    if now - state["created_at"] <= Config.SNIPER_WINDOW_SECONDS:
                        state["snipers"].add(trader)
        else:
            state["sells"] += 1
            state["sol_out"] += sol
            if trader and trader == state["dev"]:
                state["dev_sold"] = True

        mcap = _number(event.get("marketCapSol"))
        if mcap > 0:
            state["last_mcap_sol"] = mcap
        self._prune(time.time())

    def snapshot(self, mint: str):
        state = self._states.get(mint)
        if not state:
            return None
        buys = state["buys"]
        sells = state["sells"]
        trades = buys + sells
        return {
            "buys": buys,
            "sells": sells,
            "trades": trades,
            "buyers": len(state["buyers"]),
            "non_dev_buyers": len(state["non_dev_buyers"]),
            "snipers": len(state["snipers"]),
            "net_sol": state["sol_in"] - state["sol_out"],
            "buy_ratio": buys / trades if trades else None,
            "dev_sold": state["dev_sold"],
            "initial_mcap_sol": state["initial_mcap_sol"],
            "last_mcap_sol": state["last_mcap_sol"],
            "age_seconds": max(0, int(time.time() - state["created_at"])),
        }

    def window_stats(self, mint: str, seconds: float, sol_usd: float):
        """Observed trade volume in USD over the last `seconds`.

        Returns {"volume", "buy_volume", "buys", "sells"} or None when there is
        no trustworthy answer: unknown mint, no SOL price, no trades, or the
        bounded buffer has already dropped trades inside the window (so the
        total would be an under-count). Callers fall back to the DexScreener
        trade-count approximation in that case.
        """
        state = self._states.get(mint)
        if not state or not sol_usd or sol_usd <= 0 or not state["trades"]:
            return None
        cutoff = time.time() - seconds
        if state["trades_dropped"] and state["trades"][0][0] > cutoff:
            return None
        buy_sol = sell_sol = 0.0
        buys = sells = 0
        for ts, is_buy, sol in state["trades"]:
            if ts < cutoff:
                continue
            if is_buy:
                buy_sol += sol
                buys += 1
            else:
                sell_sol += sol
                sells += 1
        if not buys and not sells:
            return None
        return {
            "volume": (buy_sol + sell_sol) * sol_usd,
            "buy_volume": buy_sol * sol_usd,
            "buys": buys,
            "sells": sells,
        }

    def rejection_reason(self, mint: str):
        """Return only evidence-backed risks; missing/sparse data is never fatal."""
        flow = self.snapshot(mint)
        if not flow:
            return None
        if flow["dev_sold"]:
            return "Dev sold during the pre-alert hold"

        initial = flow["initial_mcap_sol"]
        current = flow["last_mcap_sol"]
        if initial > 0 and current > 0:
            drop_pct = (initial - current) / initial * 100.0
            if drop_pct >= Config.FLOW_MAX_MC_DROP_PCT:
                return f"Early market cap fell {drop_pct:.0f}%"

        if flow["trades"] >= Config.FLOW_MIN_TRADES_FOR_FILTER:
            if flow["buy_ratio"] < Config.FLOW_MIN_BUY_RATIO:
                return f"Early buy ratio {flow['buy_ratio']:.2f} is too weak"
            if flow["net_sol"] < 0:
                return f"Early net flow is negative ({flow['net_sol']:.2f} SOL)"
            if flow["buys"] >= 6 and flow["non_dev_buyers"] <= 1:
                return "Early activity has at most one independent buyer"
        return None

    def _prune(self, now):
        if now - self._last_prune < 15 and len(self._states) <= Config.FLOW_MAX_TRACKED_MINTS:
            return
        self._last_prune = now
        cutoff = now - Config.FLOW_TRADE_TTL_SECONDS
        for mint, state in list(self._states.items()):
            if state["created_at"] < cutoff:
                self._states.pop(mint, None)
        while len(self._states) > Config.FLOW_MAX_TRACKED_MINTS:
            self._states.popitem(last=False)

    def stats(self):
        return {
            "tracked_mints": len(self._states),
            "total_trades": self.total_trades,
            "last_trade_age": (
                int(time.time() - self.last_trade_at) if self.last_trade_at else None
            ),
        }
