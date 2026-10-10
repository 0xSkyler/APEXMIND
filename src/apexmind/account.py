from dataclasses import dataclass
from decimal import Decimal
import math
import re
import subprocess
import time


class AccountError(RuntimeError):
    pass


def number(value):
    result = float(Decimal(str(value)))
    if not math.isfinite(result):
        raise AccountError("Nonfinite exchange account field")
    return result


@dataclass(frozen=True)
class Position:
    symbol: str
    market_id: int
    quantity: float
    side: int
    entry_price: float
    notional: float
    unrealized_pnl: float
    liquidation_price: float
    margin_mode: int
    initial_margin_fraction: float


@dataclass(frozen=True)
class AccountSnapshot:
    account_index: int
    collateral: float
    available: float
    equity: float
    initial_margin: float
    maintenance_margin: float
    positions: dict[str, Position]
    observed_mono_ns: int
    observed_wall_ns: int
    exchange_time_ms: int
    tier: str

    @classmethod
    def parse(cls, raw, index, tier="unknown"):
        if int(raw["account_index"]) != index or int(raw["index"]) != index:
            raise AccountError("Account response identity mismatch")
        if raw.get("account_trading_mode") != 0:
            raise AccountError("This runtime requires a classic USDC perpetual account")
        positions = {}
        for p in raw["positions"]:
            q = abs(number(p["position"]))
            if not q:
                continue
            if p["sign"] not in (-1, 1) or p["symbol"] in positions:
                raise AccountError("Invalid or duplicate exchange position")
            positions[p["symbol"]] = Position(
                p["symbol"],
                int(p["market_id"]),
                q,
                int(p["sign"]),
                number(p["avg_entry_price"]),
                abs(number(p["position_value"])),
                number(p["unrealized_pnl"]),
                number(p["liquidation_price"]),
                int(p["margin_mode"]),
                number(p["initial_margin_fraction"]),
            )
        collateral = number(raw["collateral"])
        available = number(raw["available_balance"])
        im = number(raw["cross_initial_margin_requirement"])
        mm = number(raw["cross_maintenance_margin_requirement"])
        if min(collateral, im, mm) < 0:
            raise AccountError("Invalid collateral or margin requirement")
        return cls(
            index,
            collateral,
            available,
            collateral + sum(p.unrealized_pnl for p in positions.values()),
            im,
            mm,
            positions,
            time.monotonic_ns(),
            time.time_ns(),
            int(raw["transaction_time"]),
            tier,
        )


def clock_health(max_error_ms=50):
    try:
        result = subprocess.run(["chronyc", "tracking"], capture_output=True, text=True, timeout=2)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {"healthy": False, "reason": "chrony clock-error bound unavailable"}
    if result.returncode or not re.search(r"Leap status\s*:\s*Normal", result.stdout):
        return {"healthy": False, "reason": "host clock is not synchronized"}

    def seconds(label):
        match = re.search(rf"{label}\s*:\s*([-+0-9.eE]+)", result.stdout)
        if not match:
            raise ValueError("Missing clock bound")
        return abs(float(match[1]))

    try:
        bound = (
            seconds("System time") + seconds("Root dispersion") + seconds("Root delay") / 2
        ) * 1000
    except ValueError:
        return {"healthy": False, "reason": "clock bound is incomplete"}
    return {
        "healthy": math.isfinite(bound) and bound <= max_error_ms,
        "error_bound_ms": bound,
        "checked_wall_ns": time.time_ns(),
        "scope": "local host NTP; exchange clocks require feed validation",
    }
