"""Forward paper execution on causal events, using measured fill-delay samples.

This engine does not contact any trading endpoint. All fills are hypothetical.
"""

import hashlib
import numpy as np
from .book import BookError, vwap
from .features import FeatureEngine
from .risk import Portfolio


class PaperEngine:
    def __init__(self, config, latency, model, store, instruments):
        self.config, self.latency, self.model, self.store = config, latency, model, store
        self.instruments = {i["symbol"]: i for i in instruments}
        self.features = FeatureEngine(
            config.feeds, config.research.notional, float(np.median(latency.entry_ms))
        )
        self.portfolio = Portfolio(config.risk)
        state = store.get("stream_paper", {})
        for attr in ("equity", "peak", "positions", "suspended"):
            if attr in state:
                setattr(self.portfolio, attr, state[attr])
        self.run = state.get("run")
        self.last_sequence = {}
        self.rng = np.random.default_rng(config.research.seed)
        if "rng_state" in state:
            self.rng.bit_generator.state = state["rng_state"]
        self.completed = state.get("completed", 0)

    def commit(self, order=None, execution=None):
        state = {
            **self.portfolio.snapshot(),
            "run": self.run,
            "completed": self.completed,
            "mode": "HYPOTHETICAL_FORWARD_PAPER",
            "rng_state": self.rng.bit_generator.state,
        }
        self.store.stream_commit(state, order, execution)

    def cancel_pending(self, symbol):
        p = self.portfolio.positions.get(symbol)
        if p and p["phase"] == "entry_pending":
            self.portfolio.positions.pop(symbol)
            self.commit((p["client_id"], symbol, "cancelled", p))

    def on_event(self, event):
        symbol, payload = event.symbol, event.payload
        if event.run != self.run:
            # Monotonic deadlines do not survive process restarts. Cancel only
            # hypothetical pending entries; recover filled positions at a new
            # fresh quote and schedule their delayed protective exit.
            for s in list(self.portfolio.positions):
                self.cancel_pending(s)
            for p in self.portfolio.positions.values():
                p["phase"] = "recovering"
            self.last_sequence.clear()
            self.run = event.run
            self.commit()
        if event.kind == "disconnect":
            self.last_sequence.pop((event.venue, symbol), None)
            self.cancel_pending(symbol)
            self.features.update(event)
            return
        if event.kind == "book":
            key = (event.venue, symbol)
            seq = int(payload["sequence"])
            if seq <= self.last_sequence.get(key, -1):
                return
            self.last_sequence[key] = seq
        decision = self.features.update(event)
        if event.kind != "book" or event.venue != "lighter":
            return
        if not payload.get("clock_healthy", True):
            self.portfolio.suspended = True
            self.commit()
            return
        p = self.portfolio.positions.get(symbol)
        if p:
            self.process_position(event, p)
        if decision is None or symbol in self.portfolio.positions or self.portfolio.suspended:
            return
        if decision.wall_ns <= getattr(self.model, "calibrated_until_wall_ns", 0):
            return
        x = np.array([decision.x + [side] for side in (1, -1)])
        prediction = self.model.predict(x)
        choice = int(np.argmax(prediction["lower_mean"]))
        forecast = {k: float(v[choice]) for k, v in prediction.items()}
        if forecast["lower_mean"] <= self.model.threshold:
            return
        allocation = self.portfolio.allocate(
            symbol,
            (payload["bids"][0][0] + payload["asks"][0][0]) / 2,
            self.instruments[symbol],
            forecast,
            self.config.research.notional,
        )
        if not allocation.accepted:
            return
        client_id = (
            "paper:"
            + hashlib.sha256(f"{event.run}:{event.mono_ns}:{symbol}".encode()).hexdigest()[:20]
        )
        delay = float(self.rng.choice(self.latency.entry_ms))
        position = {
            "client_id": client_id,
            "phase": "entry_pending",
            "side": (1, -1)[choice],
            "quantity": allocation.base_amount,
            "ready_ns": event.mono_ns + int(delay * 1e6),
            "forecast": forecast,
            "funding_bps_hour": decision.funding_bps_hour,
            "holding_seconds": float(
                np.clip(forecast["holding_seconds"], 0.1, 2 * self.config.research.holding_seconds)
            ),
            "decision_price": payload["asks" if choice == 0 else "bids"][0][0],
            "entry_delay_ms": delay,
            "mode": "HYPOTHETICAL_FORWARD_PAPER",
        }
        self.portfolio.open(symbol, allocation, position)
        self.commit((client_id, symbol, "pending", self.portfolio.positions[symbol]))

    def schedule_exit(self, event, p, reason):
        p["phase"] = "exit_pending"
        p["ready_ns"] = event.mono_ns + int(float(self.rng.choice(self.latency.exit_ms)) * 1e6)
        p["exit_reason"] = reason
        self.commit((p["client_id"], event.symbol, "exit_pending", p))

    def process_position(self, event, p):
        side = p["side"]
        if p["phase"] == "recovering":
            self.schedule_exit(event, p, "restart_reconciliation")
            return
        if p["phase"] in ("entry_pending", "exit_pending") and event.mono_ns < p["ready_ns"]:
            return
        try:
            price = vwap(
                event.payload[
                    "asks"
                    if (side == 1 and p["phase"] == "entry_pending")
                    or (side == -1 and p["phase"] != "entry_pending")
                    else "bids"
                ],
                p["quantity"],
            )
        except BookError:
            if p["phase"] == "entry_pending":
                self.cancel_pending(event.symbol)
            # Never invent an exit fill when captured depth is insufficient.
            return
        if p["phase"] == "entry_pending":
            if (
                event.mono_ns - p["ready_ns"] > self.config.research.max_execution_gap_ms * 1e6
                or abs(price / p["decision_price"] - 1) * 1e4 > self.config.risk.max_slippage_bps
            ):
                self.cancel_pending(event.symbol)
                return
            p["entry_price"] = price * (1 + side * self.config.research.extra_slippage_bps / 1e4)
            p["entry_ns"], p["entry_wall_ns"] = event.mono_ns, event.wall_ns
            p["phase"] = "filled"
            self.commit((p["client_id"], event.symbol, "paper_open", p))
            return
        marked_bps = side * (price / p["entry_price"] - 1) * 1e4
        marked_pnl = p["notional"] * marked_bps / 1e4
        p["marked_pnl"] = marked_pnl
        if p["phase"] == "exit_pending":
            exit_price = price * (1 - side * self.config.research.extra_slippage_bps / 1e4)
            holding = (event.wall_ns - p["entry_wall_ns"]) / 1e9
            if holding < 0:
                self.portfolio.suspended = True
                self.commit()
                return
            gross = side * (exit_price / p["entry_price"] - 1) * 1e4
            fees = self.config.research.taker_fee_bps * (1 + exit_price / p["entry_price"])
            funding = (
                max(abs(p["funding_bps_hour"]), self.config.research.funding_reserve_bps_per_hour)
                * holding
                / 3600
            )
            record = {
                **p,
                "exit_price": exit_price,
                "exit_wall_ns": event.wall_ns,
                "net_bps": gross - fees - funding,
                "holding_seconds_realized": holding,
                "funding_cost_basis": "conservative reserve; not settled account funding",
            }
            record["pnl"] = self.portfolio.close(event.symbol, record["net_bps"])
            self.completed += 1
            self.commit((p["client_id"], event.symbol, "filled", record), (p["client_id"], record))
            return
        marked_equity = self.portfolio.equity + sum(
            pos.get("marked_pnl", 0) for pos in self.portfolio.positions.values()
        )
        if marked_equity <= self.portfolio.peak * (1 - self.config.risk.max_drawdown_fraction):
            self.portfolio.suspended = True
        if (
            marked_pnl <= -self.portfolio.equity * self.config.risk.max_loss_fraction
            or self.portfolio.suspended
        ):
            self.schedule_exit(event, p, "downside_or_margin_protection")
        elif event.mono_ns - p["entry_ns"] >= p["holding_seconds"] * 1e9:
            self.schedule_exit(event, p, "forecast_holding_horizon")
