from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_DOWN
import math
from .config import Risk


@dataclass(frozen=True)
class Allocation:
    accepted: bool
    reason: str
    notional: float = 0
    base_amount: float = 0
    leverage: float = 1


class Portfolio:
    def __init__(self, config: Risk, store=None):
        self.config, self.store = config, store
        previous = store.get("paper_portfolio") if store else None
        self.equity = previous["equity"] if previous else config.initial_balance
        self.peak = previous["peak"] if previous else self.equity
        self.positions = previous["positions"] if previous else {}
        self.suspended = previous.get("suspended", False) if previous else False

    def save(self):
        if self.store:
            self.store.set("paper_portfolio", self.snapshot())

    def snapshot(self):
        return {
            "equity": self.equity,
            "peak": self.peak,
            "positions": self.positions,
            "suspended": self.suspended,
        }

    @property
    def gross(self):
        return sum(p["notional"] for p in self.positions.values())

    def allocate(self, symbol, price, instrument, forecast, cap=float("inf")):
        numbers = [
            price,
            forecast["mean"],
            forecast["lower_mean"],
            forecast["q05"],
            forecast["adverse"],
            forecast["slippage_probability"],
            self.equity,
        ]
        if not all(math.isfinite(x) for x in numbers) or price <= 0 or self.equity <= 0:
            return Allocation(False, "invalid_input")
        if self.suspended or self.equity <= self.peak * (1 - self.config.max_drawdown_fraction):
            self.suspended = True
            self.save()
            return Allocation(False, "drawdown_suspension")
        if symbol in self.positions:
            return Allocation(False, "existing_position")
        if forecast["lower_mean"] <= self.config.minimum_edge_bps:
            return Allocation(False, "uncertain_or_negative_edge")
        if forecast["slippage_probability"] > 0.2:
            return Allocation(False, "slippage_risk")
        downside = max(forecast["adverse"], -forecast["q05"], 5) / 1e4
        spare = self.equity * self.config.max_gross_leverage - self.gross
        # Margin reserve limits exposure, including concurrent positions.
        margin_spare = (
            self.equity * (1 - self.config.margin_reserve_fraction)
            - self.gross / self.config.max_gross_leverage
        ) * self.config.max_gross_leverage
        notional = min(
            self.equity * self.config.max_market_fraction,
            self.equity * self.config.max_loss_fraction / downside,
            spare,
            margin_spare,
            cap,
        )
        if notional <= 0:
            return Allocation(False, "portfolio_capacity")
        quantum = Decimal(1).scaleb(-int(instrument["size_decimals"]))
        quantity = (Decimal(str(notional)) / Decimal(str(price))).quantize(
            quantum, rounding=ROUND_DOWN
        )
        actual = float(quantity) * price
        if quantity < Decimal(instrument["min_base"]) or actual < float(instrument["min_quote"]):
            return Allocation(False, "exchange_minimum")
        if actual <= 0:
            return Allocation(False, "rounded_to_zero")
        # Leverage is bounded by the configured policy; rejected small orders
        # never cause the policy to increase leverage.
        return Allocation(True, "accepted", actual, float(quantity), self.config.max_gross_leverage)

    def open(self, symbol, allocation, details):
        if not allocation.accepted or symbol in self.positions:
            raise ValueError("Invalid portfolio transition")
        self.positions[symbol] = {**details, **asdict(allocation)}
        self.save()

    def close(self, symbol, net_bps):
        position = self.positions.pop(symbol)
        pnl = position["notional"] * net_bps / 1e4
        self.equity += pnl
        self.peak = max(self.peak, self.equity)
        if self.equity <= self.peak * (1 - self.config.max_drawdown_fraction):
            self.suspended = True
        self.save()
        return pnl
