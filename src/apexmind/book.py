from dataclasses import dataclass, field
import math


class BookError(ValueError):
    pass


@dataclass
class Book:
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    sequence: int | None = None
    valid: bool = False
    sequence_verified: bool = False

    def update(
        self, bids, asks, sequence: int, *, snapshot: bool = False, previous: int | None = None
    ):
        if not snapshot:
            if not self.valid:
                raise BookError("Delta before snapshot")
            if sequence <= self.sequence:
                return False
            if previous is not None and previous != self.sequence:
                self.valid = False
                raise BookError("Sequence gap; resnapshot required")
        new_bids, new_asks = ({}, {}) if snapshot else (dict(self.bids), dict(self.asks))
        for source, target in ((bids, new_bids), (asks, new_asks)):
            for level in source:
                price, size = (
                    (float(level["price"]), float(level["size"]))
                    if isinstance(level, dict)
                    else map(float, level[:2])
                )
                if not math.isfinite(price) or not math.isfinite(size) or price <= 0 or size < 0:
                    self.valid = False
                    raise BookError("Invalid order-book level")
                if size == 0:
                    target.pop(price, None)
                else:
                    target[price] = size
        if not new_bids or not new_asks or max(new_bids) >= min(new_asks):
            self.valid = False
            raise BookError("Empty or crossed order book")
        self.bids, self.asks = new_bids, new_asks
        self.sequence, self.valid = sequence, True
        self.sequence_verified = snapshot or previous is not None
        return True

    @property
    def mid(self):
        return (max(self.bids) + min(self.asks)) / 2

    def payload(self, depth: int = 100):
        return {
            "bids": sorted(self.bids.items(), reverse=True)[:depth],
            "asks": sorted(self.asks.items())[:depth],
            "sequence": self.sequence,
            "sequence_verified": self.sequence_verified,
        }


def vwap(levels, base_amount: float) -> float:
    if base_amount <= 0:
        raise BookError("Order amount must be positive")
    left, value = base_amount, 0.0
    for price, size in levels:
        take = min(left, float(size))
        value += take * float(price)
        left -= take
        if left <= base_amount * 1e-10:
            return value / base_amount
    raise BookError("Insufficient captured depth; do not extrapolate liquidity")
