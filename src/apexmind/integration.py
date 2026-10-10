"""Explicit user-run native lifecycle tests. These commands can incur real costs."""

import asyncio
from dataclasses import asdict
import time
from decimal import Decimal

from .account import clock_health
from .broker import LiveBroker, TERMINAL, margin_reason
from .evidence import export_latency, integration_summary, source_digest
from .execution import ExecutionBlocked
from .feeds import Collector, discover, Instrument
from .live import LedgerLock
from .native import NativeGateway
from .risk import Portfolio
from .store import Store

FORECAST = dict(
    mean=20.0,
    lower_mean=10.0,
    q05=-10.0,
    q50=20.0,
    q95=30.0,
    adverse=10.0,
    favorable_probability=0.7,
    slippage_probability=0.0,
    execution_delay_ms=1.0,
    holding_seconds=2.0,
)


async def connected(config, callback, public_feeds=False):
    lease = LedgerLock(config.live.ledger)
    store = Store(config.live.ledger)
    gateway = None
    try:
        gateway = await NativeGateway.from_environment(config.feeds.lighter_rest, store, config)
        if public_feeds:
            instruments = await discover(config)
        else:
            # Account access and risk reduction remain independent of the
            # reference venue. Reference availability cannot prevent recovery.
            catalog = await gateway.request("/api/v1/orderBooks")
            instruments = [
                Instrument(
                    m["symbol"],
                    int(m["market_id"]),
                    "",
                    m["min_base_amount"],
                    m["min_quote_amount"],
                    int(m["supported_size_decimals"]),
                    int(m["supported_price_decimals"]),
                )
                for m in catalog["order_books"]
                if m.get("market_type") == "perp" and Decimal(str(m.get("multiplier", "1"))) == 1
            ]
        broker = LiveBroker(gateway, config, [asdict(i) for i in instruments])
        return await callback(broker, instruments)
    finally:
        if gateway:
            await gateway.close()
        store.close()
        lease.close()


async def auth_check(config):
    async def check(broker, instruments):
        snapshot = await broker.gateway.snapshot()
        orders = await broker.gateway.active_orders()
        return {
            "authorized": True,
            "account_index": snapshot.account_index,
            "account_tier": snapshot.tier,
            "equity": snapshot.equity,
            "available_balance": snapshot.available,
            "positions": len(snapshot.positions),
            "active_orders": len(orders),
            "clock": clock_health(config.live.max_clock_error_ms),
            "fee_ticks": {
                k: broker.gateway.limits[k]
                for k in ("current_maker_fee_tick", "current_taker_fee_tick")
            },
            "mode": "read_only; no transactions signed or submitted",
        }

    return await connected(config, check)


async def recover(config, timeout=120):
    async def close_owned(broker, instruments):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            await broker.reconcile()
            for record in broker.store.orders():
                if record["status"] not in TERMINAL and record["payload"]["role"] != "stop":
                    await broker.cancel(record)
            for symbol in list(broker.snapshot.positions):
                await broker.close_position(symbol, reason="explicit_recovery")
            if not broker.snapshot.positions:
                for record in broker.store.orders():
                    if record["status"] not in TERMINAL:
                        await broker.cancel(record)
                if not broker.store.unresolved() and not broker.store.pending_transactions():
                    return {"flat": True, "status": broker.status()}
            await asyncio.sleep(config.live.account_poll_seconds)
        raise ExecutionBlocked(
            "Recovery timed out; native stops retained and unresolved IDs preserved"
        )

    return await connected(config, close_owned)


async def integration_test(
    config, symbol, notional, roundtrips=20, latency_out="data/latency.json"
):
    if notional <= 0 or not 1 <= roundtrips <= 100:
        raise ValueError("Positive probe notional and 1–100 roundtrips required")
    if not clock_health(config.live.max_clock_error_ms)["healthy"]:
        raise ExecutionBlocked("Integration requires verified host clock synchronization")

    async def exercise(broker, instruments):
        store = broker.store
        await broker.reconcile()
        if broker.snapshot.positions or broker.active or store.pending_transactions():
            raise ExecutionBlocked(
                "Integration requires a reconciled flat account without active orders"
            )
        if symbol not in broker.instruments:
            raise ValueError("Integration symbol is not eligible")
        # Keep synthetic hypotheses out of the episode ledger.
        await broker.settle_episode("integration", integration=True)
        events = Store(config.storage.database)
        latest = {}
        coverage = set()

        def receive(event):
            if event.kind == "disconnect":
                latest.pop((event.venue, event.symbol), None)
            elif event.kind == "book":
                latest[(event.venue, event.symbol)] = event
            coverage.add((event.venue, event.symbol, event.kind))

        collector = Collector(config, events, instruments, receive)
        task = asyncio.create_task(collector.run())

        def quote():
            now = time.monotonic_ns()
            for venue in ("lighter", "reference"):
                e = latest.get((venue, symbol))
                if (
                    e is None
                    or now - e.mono_ns > config.feeds.max_freshness_ms * 1e6
                    or not e.payload.get("sequence_verified")
                    or not e.payload.get("clock_healthy")
                    or e.exchange_ns is None
                    or abs(e.wall_ns - e.exchange_ns) > config.feeds.max_freshness_ms * 1e6
                ):
                    raise ExecutionBlocked(
                        "Native feeds lack fresh verified sequence/timestamp coverage"
                    )
            return latest[("lighter", symbol)].payload

        def ready():
            quote()
            broker.account_fresh()
            if margin_reason(broker.snapshot, config):
                raise ExecutionBlocked("Integration account fails margin policy")
            if not clock_health(config.live.max_clock_error_ms)["healthy"]:
                raise ExecutionBlocked("Integration clock became unhealthy")

        async def wait_for(predicate, timeout=120):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if task.done():
                    task.result()
                    raise ExecutionBlocked("Integration collector stopped")
                await broker.reconcile()
                if predicate():
                    return
                await asyncio.sleep(config.live.account_poll_seconds)
            raise ExecutionBlocked("Native lifecycle did not complete before timeout")

        try:
            until = time.monotonic() + 30
            while time.monotonic() < until:
                try:
                    quote()
                    if ("lighter", symbol, "funding") in coverage:
                        break
                except ExecutionBlocked:
                    pass
                if task.done():
                    task.result()
                await asyncio.sleep(0.1)
            else:
                raise ExecutionBlocked("Required public feed coverage was not established")
            book = quote()
            portfolio = Portfolio(config.risk)
            portfolio.equity = portfolio.peak = broker.snapshot.equity
            allocation = portfolio.allocate(
                symbol, book["asks"][0][0], broker.instruments[symbol], FORECAST, cap=notional
            )
            if not allocation.accepted:
                raise ExecutionBlocked("Integration sizing rejected: " + allocation.reason)
            quantity = allocation.base_amount
            # Verify a resting post-only order and cancellation without taking
            # liquidity. It is still genuine potential exposure and is bounded.
            identifier, protection_id = store.allocate_ids(2)
            native = broker.order_payload(identifier, symbol, 1, quantity, book["bids"][0][0] * 0.9)
            native.update(Type=0, TimeInForce=2, OrderExpiry=int(time.time() * 1000) + 30_000)
            stop_price = book["bids"][0][0] * 0.899
            protective = broker.order_payload(
                protection_id,
                symbol,
                -1,
                quantity,
                stop_price * (1 - config.live.protective_slippage_bps / 1e4),
                stop=stop_price,
                reduce=True,
            )
            protective["BaseAmount"] = 0
            payload = {"orders": [native, protective]}
            record = broker.record(
                identifier,
                symbol,
                "entry",
                1,
                quantity,
                integration=True,
                model_sha256="integration",
                holding_seconds=2,
                probe=True,
                stop_id=protection_id,
            )
            child = broker.record(
                protection_id,
                symbol,
                "stop",
                -1,
                quantity,
                integration=True,
                parent_id=identifier,
                model_sha256="integration",
                probe=True,
                position_tied=True,
            )
            await broker.gateway.signed_transaction("group", payload, [record, child], ready)
            await wait_for(
                lambda: (
                    next(r for r in store.orders() if r["client_id"] == identifier)["status"]
                    == "open"
                )
            )
            await broker.cancel(next(r for r in store.orders() if r["client_id"] == identifier))
            await wait_for(
                lambda: (
                    next(r for r in store.orders() if r["client_id"] == identifier)["status"]
                    == "cancelled"
                )
            )
            await broker.cancel(next(r for r in store.orders() if r["client_id"] == protection_id))
            await wait_for(lambda: not store.pending_transactions())
            # A separate triggered native stop proves the protective lifecycle.
            # Follow with ordinary IOC entry/exit pairs for latency calibration.
            for cycle in range(roundtrips + 1):
                await broker.reconcile()
                await broker.settle_episode("integration", integration=True)
                ready()
                book = quote()
                side = 1 if cycle % 2 == 0 else -1
                anchor = book["asks"][0][0] if side == 1 else book["bids"][0][0]
                stop = anchor * (1 + side * 0.001) if cycle == 0 else None
                result = await broker.enter(
                    symbol,
                    side,
                    quantity,
                    book,
                    FORECAST,
                    "integration",
                    ready,
                    integration=True,
                    stop_override=stop,
                )
                entry_id, stop_id = result["client_ids"]
                await wait_for(
                    lambda e=entry_id: (
                        next(r for r in store.orders() if r["client_id"] == e)["status"] in TERMINAL
                    )
                )
                entry = next(r for r in store.orders() if r["client_id"] == entry_id)
                if not entry["payload"].get("filled_quantity"):
                    raise ExecutionBlocked("Integration entry did not fill; do not invent latency")
                if cycle == 0:
                    await wait_for(
                        lambda s=stop_id: (
                            next(r for r in store.orders() if r["client_id"] == s)["status"]
                            == "filled"
                        ),
                        30,
                    )
                else:
                    if not broker.protected(symbol):
                        raise ExecutionBlocked("Native stop coverage could not be verified")
                    fresh = quote()
                    price = fresh["bids"][0][0] if side == 1 else fresh["asks"][0][0]
                    await broker.close_position(symbol, price, "integration_roundtrip")
                    await wait_for(lambda: symbol not in broker.snapshot.positions)
                for r in store.orders():
                    if r["payload"]["role"] == "stop" and r["status"] not in TERMINAL:
                        await broker.cancel(r)
                await wait_for(lambda: not store.pending_transactions() and not store.unresolved())
                await wait_for(lambda: not broker.snapshot.positions)
                deadline = time.monotonic() + 120
                while await broker.settle_episode("integration", integration=True) is None:
                    if time.monotonic() > deadline:
                        raise ExecutionBlocked("Settled cash evidence did not complete")
                    await asyncio.sleep(config.live.account_poll_seconds)
                    await broker.reconcile()
            # Margin refusal logic is exercised against account-derived amounts;
            # the actual native stop and reduce-only fills above prove exits.
            from dataclasses import replace

            s = broker.snapshot
            if not margin_reason(replace(s, equity=s.maintenance_margin * 0.5), config):
                raise ExecutionBlocked("Margin watchdog negative scenario failed")
            store.set(
                "native_integration_proof",
                {
                    "account_index": s.account_index,
                    "verified_wall_ns": time.time_ns(),
                    "source_sha256": source_digest(),
                    "network": broker.gateway.url,
                    "symbol": symbol,
                    "native_feed_coverage": sorted(coverage),
                    "fee_fingerprint": {
                        k: broker.gateway.limits[k]
                        for k in ("user_tier", "current_maker_fee_tick", "current_taker_fee_tick")
                    },
                    "margin_test_scope": "account-derived refusal scenario plus real native stop/reduce-only fills",
                },
            )
            summary = integration_summary(store, config)
            if summary["allowed"]:
                summary["latency"] = export_latency(store, latency_out, s.tier)
            return summary
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            # Best-effort cleanup has no entry capability. Never cancel the
            # native stop for an open position if reduction cannot be verified.
            try:
                await broker.reconcile()
                for s in list(broker.snapshot.positions):
                    await broker.close_position(s, reason="integration_cleanup")
                await broker.reconcile()
                if not broker.snapshot.positions:
                    for r in store.orders():
                        if r["status"] not in TERMINAL:
                            await broker.cancel(r)
                    await broker.reconcile()
            finally:
                events.close()

    return await connected(config, exercise, public_feeds=True)
