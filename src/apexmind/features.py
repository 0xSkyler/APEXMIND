from collections import defaultdict, deque
from dataclasses import dataclass
import math
import numpy as np
from .config import Feeds
from .store import Event

FEATURES = [
    "basis_bps",
    "basis_z",
    "lag_250ms",
    "lag_1s",
    "lag_5s",
    "reference_250ms",
    "reference_1s",
    "reference_5s",
    "lighter_250ms",
    "lighter_1s",
    "lighter_5s",
    "imbalance",
    "flow_imbalance",
    "aggressive_ratio",
    "spread_bps",
    "depth_usd",
    "impact_bps",
    "realized_vol_bps",
    "replenishment",
    "net_depth_removal",
    "funding_bps_hour",
    "reference_age_ms",
    "lighter_age_ms",
    "relative_age_ms",
    "feed_delay_ms",
    "execution_latency_ms",
    "regime",
]


@dataclass
class Decision:
    run: str
    symbol: str
    mono_ns: int
    wall_ns: int
    x: list[float]
    book: dict
    funding_bps_hour: float


class FeatureEngine:
    def __init__(self, feeds: Feeds, notional: float = 10, latency_ms: float = 0):
        self.feeds, self.notional, self.latency_ms = feeds, notional, latency_ms
        self.run = None
        self._reset()

    def _reset(self):
        self.books = {}
        self.history = defaultdict(deque)
        self.trades = defaultdict(deque)
        self.funding = {}
        self.flow = defaultdict(float)
        self.depth_change = defaultdict(lambda: (0.0, 0.0))
        self.last_decision = defaultdict(int)
        self.basis_history = defaultdict(deque)

    def update(self, event: Event) -> Decision | None:
        if event.run != self.run:
            self._reset()
            self.run = event.run
        key, now, p = (event.venue, event.symbol), event.mono_ns, event.payload
        if event.kind == "disconnect":
            self.books.pop(key, None)
            self.history[key].clear()
            return None
        if event.kind == "funding":
            self.funding[event.symbol] = (float(p["rate_bps_hour"]), now)
        if event.kind == "trade":
            self.trades[key].append((now, float(p["size"]), int(p["side"])))
        if event.kind != "book":
            return None
        bids, asks = p["bids"], p["asks"]
        if not bids or not asks or bids[0][0] >= asks[0][0] or not p.get("clock_healthy", True):
            self.books.pop(key, None)
            return None
        old = self.books.get(key)
        if old:
            ob, oa = old.payload["bids"][0], old.payload["asks"][0]
            b, a = bids[0], asks[0]
            # Cont-Kukanov-Stoikov top-of-book order flow, based only on arrivals.
            self.flow[key] = (
                (b[1] if b[0] >= ob[0] else 0)
                - (ob[1] if b[0] <= ob[0] else 0)
                - (a[1] if a[0] <= oa[0] else 0)
                + (oa[1] if a[0] >= oa[0] else 0)
            )
            previous = sum(q for _, q in old.payload["bids"][:10] + old.payload["asks"][:10])
            current = sum(q for _, q in bids[:10] + asks[:10])
            self.depth_change[key] = (max(current - previous, 0), max(previous - current, 0))
        self.books[key] = event
        self.history[key].append((now, (bids[0][0] + asks[0][0]) / 2))
        for history in (self.history[key], self.trades[key]):
            while history and history[0][0] < now - 10_000_000_000:
                history.popleft()
        if event.venue != "lighter":
            return None
        other = self.books.get(("reference", event.symbol))
        if other is None:
            return None
        la, ra = now - event.mono_ns, now - other.mono_ns
        if min(la, ra) < 0 or max(la, ra) > self.feeds.max_freshness_ms * 1e6:
            return None
        if now - self.last_decision[event.symbol] < self.feeds.decision_interval_ms * 1e6:
            return None
        returns = []
        for venue in ("reference", "lighter"):
            history = self.history[(venue, event.symbol)]
            for seconds in (0.25, 1, 5):
                target = now - int(seconds * 1e9)
                candidates = [r for r in history if r[0] <= target]
                if not candidates or target - candidates[-1][0] > self.feeds.max_freshness_ms * 1e6:
                    return None
                returns.append(math.log(history[-1][1] / candidates[-1][1]) * 1e4)
        funding = self.funding.get(event.symbol)
        if funding is None or now - funding[1] > 120 * 1e9:
            return None
        ref, lit = returns[:3], returns[3:]
        mid = (bids[0][0] + asks[0][0]) / 2
        refmid = (other.payload["bids"][0][0] + other.payload["asks"][0][0]) / 2
        basis = (refmid / mid - 1) * 1e4
        prior = self.basis_history[event.symbol]
        while prior and prior[0][0] < now - 30_000_000_000:
            prior.popleft()
        values = [v for _, v in prior]
        sigma = float(np.std(values)) if len(values) > 2 else 0
        basis_z = (basis - float(np.mean(values))) / sigma if sigma > 1e-8 else 0
        bq, aq = sum(q for _, q in bids[:10]), sum(q for _, q in asks[:10])
        trade_window = [r for r in self.trades[key] if r[0] >= now - 1e9]
        volume = sum(r[1] for r in trade_window)
        signed = sum(r[1] * r[2] for r in trade_window)
        hist = np.array([v for _, v in self.history[key]])
        vol = float(np.std(np.diff(np.log(hist))) * 1e4) if len(hist) > 2 else 0
        depth = sum(price * q for price, q in bids[:10] + asks[:10])
        best_depth = min(bids[0][1] * bids[0][0], asks[0][1] * asks[0][0])
        replen, removal = self.depth_change[key]
        delay = ((event.wall_ns - event.exchange_ns) / 1e6) if event.exchange_ns else 0
        x = [
            basis,
            basis_z,
            *[a - b for a, b in zip(ref, lit, strict=True)],
            *ref,
            *lit,
            (bq - aq) / (bq + aq),
            self.flow[key] / (bq + aq),
            signed / volume if volume else 0,
            (asks[0][0] - bids[0][0]) / mid * 1e4,
            depth,
            self.notional / max(best_depth, 1e-9) * (asks[0][0] - bids[0][0]) / mid * 1e4,
            vol,
            replen / (bq + aq),
            removal / (bq + aq),
            funding[0],
            ra / 1e6,
            la / 1e6,
            (ra - la) / 1e6,
            delay,
            self.latency_ms,
            float(vol > 2),
        ]
        if not np.isfinite(x).all():
            return None
        self.last_decision[event.symbol] = now
        prior.append((now, basis))
        return Decision(event.run, event.symbol, now, event.wall_ns, x, p, funding[0])
