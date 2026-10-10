"""Continuous live operation with a separate exposure watchdog and durable recovery."""

import asyncio
from collections import Counter
from dataclasses import asdict
import fcntl
import json
import logging
from pathlib import Path
import signal
import time
import joblib
import numpy as np

from .account import clock_health
from .broker import LiveBroker, TERMINAL, margin_reason
from .evidence import runtime_gate, forward_metrics
from .execution import ExecutionBlocked, execution_choice
from .features import FeatureEngine
from .feeds import Collector, discover
from .latency import Latency
from .native import NativeGateway
from .risk import Portfolio
from .store import Store

LOG = logging.getLogger(__name__)


class LedgerLock:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.file = open(str(path) + ".lock", "a")
        try:
            fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.file.close()
            raise ExecutionBlocked("Another process owns this live ledger") from None

    def close(self):
        self.file.close()


class LiveEngine:
    def __init__(
        self, config, broker, model, report, model_path, latency, stage="production", registry=None
    ):
        self.config, self.broker, self.model = config, broker, model
        self.store, self.report, self.model_path = broker.store, report, model_path
        self.latency, self.stage = latency, stage
        self.registry = Path(registry) if registry else None
        self.features = FeatureEngine(
            config.feeds,
            report["evaluation_config"]["research"]["notional"],
            float(np.median(latency.entry_ms)),
        )
        self.queue = asyncio.Queue(maxsize=config.live.queue_capacity)
        self.latest = {}
        self.lock = asyncio.Lock()
        self.stop = asyncio.Event()
        self.suspended = self.store.get("live_suspension")
        self.clock = {"healthy": False}
        self.gate = {"allowed": False, "reasons": ["startup validation pending"]}
        self.rejections = Counter(self.store.get("live_rejections", {}))
        self.last_gate_ns = 0
        self.started_mono_ns = time.monotonic_ns()

    def adopt_candidate(self):
        if (
            not self.registry
            or not self.registry.exists()
            or self.broker.snapshot.positions
            or self.broker.active
        ):
            return False
        if self.store.pending_transactions() or self.store.unresolved():
            return False
        if self.suspended and self.suspended != "live_advantage_disappeared":
            return False
        try:
            registry = json.loads(self.registry.read_text())
            candidate = registry.get("challenger" if self.stage == "pilot" else "champion")
            if not candidate or candidate["sha256"] == self.report["model_sha256"]:
                return False
            report = json.loads(Path(candidate["report"]).read_text())
            if report["model_sha256"] != candidate["sha256"]:
                raise ExecutionBlocked("Registry artifact identity mismatch")
            gate = runtime_gate(
                report,
                candidate["model"],
                self.config,
                self.store,
                self.broker.snapshot,
                self.broker.gateway.limits,
                self.latency,
                self.stage,
            )
            if not gate["allowed"] or margin_reason(
                self.broker.snapshot, self.config, self.store.get("live_peak_equity")
            ):
                return False
            episode = self.store.get("live_episode")
            if episode and set(episode["fill_ids"]) != {f["id"] for f in self.store.fills()}:
                return False
            model = joblib.load(candidate["model"])
            self.model, self.report, self.model_path = model, report, candidate["model"]
            self.features = FeatureEngine(
                self.config.feeds,
                report["evaluation_config"]["research"]["notional"],
                float(np.median(self.latency.entry_ms)),
            )
            while not self.queue.empty():
                self.queue.get_nowait()
                self.queue.task_done()
            self.suspended = None
            self.store.set("live_suspension", None)
            self.store.set("live_episode", None)
            self.store.set(
                "live_model_transition",
                {"wall_ns": time.time_ns(), "sha256": candidate["sha256"], "stage": self.stage},
            )
            self.gate, self.last_gate_ns = gate, time.monotonic_ns()
            LOG.info("Adopted validated model while flat: %s", report["champion"])
            return True
        except (OSError, ValueError, KeyError, RuntimeError) as error:
            self.store.set("candidate_rejection", str(error))
            return False

    def suspend(self, reason):
        if self.suspended == reason:
            return
        self.suspended = reason
        self.store.set("live_suspension", reason)
        LOG.error("Live strategy suspended: %s", reason)

    def on_event(self, event):
        key = (event.venue, event.symbol)
        if event.kind == "book":
            self.latest[key] = event
        elif event.kind == "disconnect":
            self.latest.pop(key, None)
        decision = self.features.update(event)
        if decision:
            try:
                self.queue.put_nowait(decision)
            except asyncio.QueueFull:
                self.suspend("decision_queue_overflow")

    def quote(self, symbol):
        now = time.monotonic_ns()
        for venue in ("lighter", "reference"):
            event = self.latest.get((venue, symbol))
            if (
                event is None
                or now - event.mono_ns > self.config.feeds.max_freshness_ms * 1e6
                or not event.payload.get("clock_healthy")
                or not event.payload.get("sequence_verified")
            ):
                raise ExecutionBlocked("Fresh continuous books from both venues are required")
            if event.exchange_ns is None or abs(event.wall_ns - event.exchange_ns) > (
                self.config.feeds.max_freshness_ms * 1e6
            ):
                raise ExecutionBlocked("Exchange timestamp delay or clock offset exceeds policy")
        return self.latest[("lighter", symbol)].payload

    def preflight(self, decision, forecast):
        if self.suspended or self.store.get("live_suspension") or not self.gate["allowed"]:
            raise ExecutionBlocked("Strategy evidence or operational suspension prevents entry")
        if not self.clock["healthy"]:
            raise ExecutionBlocked("Measured host clock synchronization is required")
        if time.time_ns() - self.clock.get("checked_wall_ns", 0) > 60e9:
            raise ExecutionBlocked("Clock health observation expired")
        self.broker.account_fresh()
        if self.audit_due(reserve_entry=True):
            raise ExecutionBlocked("Portfolio cash audit checkpoint prevents new exposure")
        if margin_reason(self.broker.snapshot, self.config, self.store.get("live_peak_equity")):
            raise ExecutionBlocked("Margin or drawdown policy prevents entry")
        age = (time.monotonic_ns() - decision.mono_ns) / 1e6
        lifetime = forecast["holding_seconds"] * 1000
        delay = max(forecast["execution_delay_ms"], float(np.quantile(self.latency.entry_ms, 0.95)))
        if age + delay >= lifetime:
            raise ExecutionBlocked(
                "Remaining signal lifetime cannot cover measured execution delay"
            )
        self.quote(decision.symbol)

    def audit_due(self, reserve_entry=False):
        episode = self.store.get("live_episode")
        if not episode:
            return reserve_entry
        count = self.store.db.execute(
            "SELECT COUNT(*) FROM live_transactions WHERE "
            "json_extract(payload,'$.submitted_wall_ns')>=?",
            (episode["start_wall_ns"],),
        ).fetchone()[0]
        pending_entries = sum(
            r["payload"]["role"] == "entry" and r["status"] not in TERMINAL
            for r in self.store.orders()
        )
        # Reserve exit and cancellation capacity before the 100-transaction
        # native history window can make an episode impossible to audit.
        reserved = (
            count
            + 2 * (len(self.broker.snapshot.positions) + pending_entries)
            + (3 if reserve_entry else 0)
        )
        return reserved >= self.config.live.episode_transaction_budget or (
            time.time_ns() - episode["start_wall_ns"] >= self.config.live.max_episode_seconds * 1e9
        )

    async def decisions(self):
        while not self.stop.is_set():
            try:
                decision = await asyncio.wait_for(self.queue.get(), timeout=1)
            except TimeoutError:
                continue
            async with self.lock:
                try:
                    if decision.wall_ns <= self.model.calibrated_until_wall_ns:
                        raise ExecutionBlocked("Decision precedes model calibration")
                    if decision.symbol not in self.report["metadata"]["symbols"]:
                        raise ExecutionBlocked("Market is outside model evidence")
                    x = np.array([decision.x + [side] for side in (1, -1)])
                    predictions = self.model.predict(x)
                    side_index = int(np.argmax(predictions["lower_mean"]))
                    forecast = {k: float(v[side_index]) for k, v in predictions.items()}
                    if not all(np.isfinite(v) for v in forecast.values()):
                        raise ExecutionBlocked("Nonfinite forecast")
                    if forecast["lower_mean"] < self.model.threshold:
                        raise ExecutionBlocked("Learned entry threshold rejected opportunity")
                    self.preflight(decision, forecast)
                    book = self.quote(decision.symbol)
                    spread = (
                        (book["asks"][0][0] - book["bids"][0][0])
                        / ((book["asks"][0][0] + book["bids"][0][0]) / 2)
                        * 1e4
                    )
                    mode = execution_choice(forecast, spread, forecast["holding_seconds"] * 1000)
                    if mode["mode"] != "aggressive":
                        raise ExecutionBlocked("Execution optimizer rejected opportunity")
                    snapshot = self.broker.snapshot
                    portfolio = Portfolio(self.config.risk)
                    portfolio.equity = snapshot.equity
                    portfolio.peak = self.store.get("live_peak_equity", snapshot.equity)
                    portfolio.positions = {
                        s: {"notional": p.notional} for s, p in snapshot.positions.items()
                    }
                    side = 1 if side_index == 0 else -1
                    price = book["asks"][0][0] if side == 1 else book["bids"][0][0]
                    # Never extrapolate liquidity beyond the size actually labelled.
                    reserve = (
                        snapshot.available
                        - snapshot.equity * self.config.risk.margin_reserve_fraction
                    )
                    cap = min(self.report["evaluation_config"]["research"]["notional"], reserve)
                    allocation = portfolio.allocate(
                        decision.symbol,
                        price,
                        self.broker.instruments[decision.symbol],
                        forecast,
                        cap,
                    )
                    if not allocation.accepted:
                        raise ExecutionBlocked(allocation.reason)
                    # Stop distance, slippage bound, and fees must also fit the
                    # portfolio loss budget; model MAE alone is insufficient.
                    stop_bps = max(
                        self.config.live.min_stop_bps, forecast["adverse"], -forecast["q05"]
                    )
                    loss = (
                        allocation.notional
                        * (
                            stop_bps
                            + self.config.live.protective_slippage_bps
                            + 2 * self.config.research.taker_fee_bps
                        )
                        / 1e4
                    )
                    if loss > snapshot.equity * self.config.risk.max_loss_fraction:
                        raise ExecutionBlocked("Native stop loss exceeds portfolio loss budget")
                    await self.broker.enter(
                        decision.symbol,
                        side,
                        allocation.base_amount,
                        book,
                        forecast,
                        self.report["model_sha256"],
                        lambda d=decision, f=forecast: self.preflight(d, f),
                    )
                except ExecutionBlocked as error:
                    self.rejections[str(error)] += 1
                    self.store.set("live_rejections", dict(self.rejections))
                finally:
                    self.queue.task_done()

    async def protection(self, shutdown=False):
        snapshot = self.broker.snapshot
        peak = max(snapshot.equity, self.store.get("live_peak_equity", snapshot.equity))
        self.store.set("live_peak_equity", peak)
        self.store.set(
            "live_maximum_drawdown",
            max(
                self.store.get("live_maximum_drawdown", 0),
                max(0, 1 - snapshot.equity / peak) if peak > 0 else 1,
            ),
        )
        reason = margin_reason(snapshot, self.config, peak)
        if reason:
            self.suspend(reason)
        # Pending entries time out and are cancelled, never blindly replaced.
        now_wall = time.time_ns()
        for r in self.store.orders():
            if r["status"] in TERMINAL:
                continue
            p = r["payload"]
            if p["role"] == "entry" and (
                shutdown
                or self.suspended
                or now_wall - p["submitted_wall_ns"] > self.config.live.order_timeout_seconds * 1e9
            ):
                try:
                    await self.broker.cancel(r)
                except ExecutionBlocked as error:
                    LOG.warning("Entry cancellation awaits reconciliation: %s", error)
            elif p["role"] == "stop" and r["symbol"] not in snapshot.positions:
                entries = {o["client_id"]: o for o in self.store.orders()}
                if entries[p["parent_id"]]["status"] in TERMINAL:
                    try:
                        await self.broker.cancel(r)
                    except ExecutionBlocked as error:
                        LOG.warning("Orphan stop cancellation awaits reconciliation: %s", error)
        for symbol in list(snapshot.positions):
            entries = [
                r
                for r in self.store.orders()
                if r["symbol"] == symbol
                and r["payload"]["role"] == "entry"
                and r["payload"].get("filled_quantity", 0)
            ]
            if not entries:
                raise ExecutionBlocked("Owned position has no entry history")
            entry = entries[-1]["payload"]
            start = min(
                entry["first_fill_wall_ns"],
                entry.get("first_fill_exchange_wall_ns", entry["first_fill_wall_ns"]),
            )
            # Targets include delayed exit execution. Schedule the submission
            # before that duration, accounting for measured exit observation.
            hold = max(
                0.01, entry["holding_seconds"] - float(np.median(self.latency.exit_ms)) / 1000
            )
            deadline = start + int(hold * 1e9)
            stale = any(
                (event := self.latest.get((venue, symbol))) is None
                or time.monotonic_ns() - event.mono_ns > self.config.live.feed_outage_seconds * 1e9
                for venue in ("lighter", "reference")
            )
            if (
                shutdown
                or self.suspended
                or reason
                or not self.broker.protected(symbol)
                or stale
                or now_wall >= deadline
            ):
                event = self.latest.get(("lighter", symbol))
                price = (
                    ((event.payload["asks"][0][0] + event.payload["bids"][0][0]) / 2)
                    if event
                    else None
                )
                try:
                    await self.broker.close_position(
                        symbol,
                        price,
                        str(shutdown)
                        if isinstance(shutdown, str)
                        else "shutdown"
                        if shutdown
                        else reason or self.suspended or "holding_or_protection_exit",
                    )
                except ExecutionBlocked as error:
                    LOG.error("Protective reduction awaits reconciliation: %s", error)

    async def watchdog(self):
        while not self.stop.is_set():
            async with self.lock:
                try:
                    await self.broker.reconcile()
                    if time.monotonic_ns() - self.last_gate_ns > 30e9:
                        self.clock = clock_health(self.config.live.max_clock_error_ms)
                        self.gate = runtime_gate(
                            self.report,
                            self.model_path,
                            self.config,
                            self.store,
                            self.broker.snapshot,
                            self.broker.gateway.limits,
                            self.latency,
                            self.stage,
                        )
                        self.last_gate_ns = time.monotonic_ns()
                    episodes = [
                        e
                        for e in self.store.episodes()
                        if not e["integration"] and e["model_sha256"] == self.report["model_sha256"]
                    ]
                    if len(episodes) >= self.config.live.retirement_window:
                        recent = episodes[-self.config.live.retirement_window :]
                        metrics = forward_metrics(
                            recent, self.report["model_sha256"], self.config.live.retirement_window
                        )
                        ci = metrics["expectancy_ci_bps_99pct"]
                        if metrics["net_pnl"] <= 0 or ci[0] is None or ci[0] <= 0:
                            self.suspend("live_advantage_disappeared")
                    if not self.gate["allowed"] and self.broker.snapshot.positions:
                        self.suspend("deployment_evidence_revoked")
                    checkpoint = self.audit_due()
                    await self.protection(shutdown="cash_audit_checkpoint" if checkpoint else False)
                    await self.broker.settle_episode(self.report["model_sha256"])
                    episode = self.store.get("live_episode")
                    if (
                        checkpoint
                        and not self.broker.snapshot.positions
                        and not self.store.unresolved()
                        and episode
                    ):
                        if set(episode["fill_ids"]) == {f["id"] for f in self.store.fills()}:
                            self.store.set("live_episode", None)
                            await self.broker.settle_episode(self.report["model_sha256"])
                    self.adopt_candidate()
                    self.store.set(
                        "live_status",
                        {
                            **self.broker.status(),
                            "stage": self.stage,
                            "suspended": self.suspended,
                            "gate": self.gate,
                            "clock": self.clock,
                            "heartbeat_wall_ns": time.time_ns(),
                            "rejections": dict(self.rejections),
                        },
                    )
                except (RuntimeError, ValueError, KeyError) as error:
                    # Keep running reconciliation and the native protection path.
                    # A failed poll never makes a previous account snapshot fresh.
                    self.gate = {"allowed": False, "reasons": [str(error)]}
                    LOG.error("Watchdog blocked: %s", error)
            try:
                await asyncio.wait_for(
                    self.stop.wait(), timeout=self.config.live.account_poll_seconds
                )
            except TimeoutError:
                pass

    async def run(self, collector, seconds=None):
        await self.broker.reconcile()
        if self.store.get("live_episode") is None:
            await self.broker.settle_episode(self.report["model_sha256"])
        self.clock = clock_health(self.config.live.max_clock_error_ms)
        self.gate = runtime_gate(
            self.report,
            self.model_path,
            self.config,
            self.store,
            self.broker.snapshot,
            self.broker.gateway.limits,
            self.latency,
            self.stage,
        )
        if not self.gate["allowed"]:
            # Recovery still runs before returning the blocked startup result.
            await self.protection(shutdown=True)
            raise ExecutionBlocked("Deployment gate: " + "; ".join(self.gate["reasons"]))
        if not self.clock["healthy"]:
            await self.protection(shutdown=True)
            raise ExecutionBlocked("Host clock synchronization must be verified")
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stop.set)
        tasks = [
            asyncio.create_task(self.watchdog()),
            asyncio.create_task(self.decisions()),
            asyncio.create_task(collector.run()),
        ]
        waiter = asyncio.create_task(self.stop.wait())
        try:
            done, _ = await asyncio.wait(
                [*tasks, waiter], timeout=seconds, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task.result()
        finally:
            self.stop.set()
            for task in [*tasks, waiter]:
                task.cancel()
            await asyncio.gather(*tasks, waiter, return_exceptions=True)
            # A restart resumes any unresolved close. The original native stop
            # remains in place until the account is proven flat.
            try:
                await self.broker.reconcile()
                await self.protection(shutdown=True)
                await self.broker.reconcile()
            except (ExecutionBlocked, ValueError, KeyError) as error:
                self.store.set("shutdown_reconciliation_required", str(error))
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(sig)
        return {
            "stage": self.stage,
            "status": self.broker.status(),
            "suspended": self.suspended,
            "rejections": dict(self.rejections),
        }


async def run_live(
    config, report_path, model_path, latency_path, stage, seconds=None, registry=None
):
    if seconds is not None and seconds <= 0:
        raise ValueError("Run duration must be positive")
    report = json.loads(Path(report_path).read_text())
    from .evidence import research_gate

    gate = research_gate(report, model_path, config)
    if not gate["allowed"]:
        ledger = Store(config.live.ledger)
        try:
            needs_recovery = bool(
                ledger.unresolved() or ledger.get("last_account_snapshot", {}).get("positions")
            )
        finally:
            ledger.close()
        if needs_recovery:
            from .integration import recover

            await recover(config)
        raise ExecutionBlocked("Research gate: " + "; ".join(gate["reasons"]))
    latency = Latency.load(latency_path)
    instruments = await discover(config)
    lease = LedgerLock(config.live.ledger)
    store, events = Store(config.live.ledger), Store(config.storage.database)
    gateway = None
    try:
        gateway = await NativeGateway.from_environment(config.feeds.lighter_rest, store, config)
        broker = LiveBroker(gateway, config, [asdict(i) for i in instruments])
        engine = LiveEngine(
            config, broker, joblib.load(model_path), report, model_path, latency, stage, registry
        )
        collector = Collector(config, events, instruments, engine.on_event)
        return await engine.run(collector, seconds)
    finally:
        if gateway:
            await gateway.close()
        store.close()
        events.close()
        lease.close()
