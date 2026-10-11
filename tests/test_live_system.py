import asyncio
from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
import json
from types import SimpleNamespace
import time

import pytest

from apexmind.account import AccountError, AccountSnapshot, Position, clock_health
from apexmind.broker import LiveBroker, margin_reason
from apexmind.config import Config
from apexmind.evidence import (
    forward_metrics,
    integration_summary,
    research_gate,
    source_digest,
    runtime_gate,
)
from apexmind.execution import ExecutionBlocked
from apexmind.integration import feed_problems
from apexmind.live import LedgerLock, LiveEngine
from apexmind.native import NativeGateway, units
from apexmind.store import Store

INSTRUMENT = dict(
    symbol="BTC", market_id=1, min_base="0.01", min_quote="5", size_decimals=2, price_decimals=2
)
FORECAST = dict(
    mean=20.0,
    lower_mean=10.0,
    q05=-10.0,
    q50=20.0,
    q95=30.0,
    adverse=10.0,
    favorable_probability=0.7,
    slippage_probability=0.01,
    execution_delay_ms=10.0,
    holding_seconds=2.0,
)
BOOK = {"bids": [[99.99, 10]], "asks": [[100.01, 10]]}


def test_native_feed_diagnostics_identify_missing_and_stale_evidence():
    now = 10_000_000_000
    event = SimpleNamespace(
        mono_ns=now - 800_000_000,
        wall_ns=20_000_000_000,
        exchange_ns=19_000_000_000,
        payload={"sequence_verified": True, "clock_healthy": True},
    )
    problems = feed_problems({("lighter", "BTC"): event}, set(), "BTC", 500, now)
    assert problems == [
        "lighter book stale (800 ms)",
        "lighter timestamp offset 1000 ms",
        "reference book missing",
        "lighter funding missing",
    ]


def snapshot(quantity=0, collateral=100, side=1):
    positions = (
        {
            "BTC": Position(
                "BTC", 1, quantity, side, 100, quantity * 100, 0, 50 if side == 1 else 150, 0, 10000
            )
        }
        if quantity
        else {}
    )
    return AccountSnapshot(
        123,
        collateral,
        collateral - quantity * 100,
        collateral,
        quantity * 100,
        quantity * 5,
        positions,
        time.monotonic_ns(),
        time.time_ns(),
        time.time_ns() // 1_000_000,
        "standard",
    )


def raw_order(identifier="1", side=1, quantity=".10", filled="0", status="open", role="entry"):
    return dict(
        client_order_index=int(identifier),
        order_index=int(identifier) + 100,
        owner_account_index=123,
        market_index=1,
        is_ask=side == -1,
        initial_base_amount=quantity,
        filled_base_amount=filled,
        remaining_base_amount=str(float(quantity) - float(filled)),
        status=status,
        reduce_only=role != "entry",
        type="stop-loss" if role == "stop" else "market",
        trigger_status="mark-price" if role == "stop" else "na",
    )


def trade(identifier="1", size=".04", price="100", side=1, tid="10"):
    return dict(
        bid_account_id=123 if side == 1 else 456,
        ask_account_id=123 if side == -1 else 456,
        bid_client_id=int(identifier) if side == 1 else 99,
        ask_client_id=int(identifier) if side == -1 else 99,
        bid_id=int(identifier) + 100 if side == 1 else 199,
        ask_id=int(identifier) + 100 if side == -1 else 199,
        market_id=1,
        size=size,
        price=price,
        trade_id=int(tid),
        trade_id_str=tid,
        timestamp=time.time_ns() // 1_000_000,
        tx_hash="tx" + identifier,
    )


class Exchange:
    def __init__(self, store):
        self.store, self.account_index, self.key_index = store, 123, 2
        self.run = "run"
        self.native_orders, self.native_trades = [], {}
        self.current = snapshot()
        self.calls = []
        self.limits = {
            "user_tier": "standard",
            "current_maker_fee_tick": 0,
            "current_taker_fee_tick": 0,
        }
        self.cash = [
            {
                k: 0
                for k in (
                    "inflow",
                    "outflow",
                    "pool_inflow",
                    "pool_outflow",
                    "spot_inflow",
                    "spot_outflow",
                    "staking_inflow",
                    "staking_outflow",
                    "staking_pnl",
                    "pool_pnl",
                    "trade_spot_pnl",
                )
            }
        ]
        self.cash[0]["timestamp"] = time.time_ns() // 1_000_000
        self.history = []

    async def snapshot(self):
        return replace(
            self.current, observed_mono_ns=time.monotonic_ns(), observed_wall_ns=time.time_ns()
        )

    async def orders(self, identifiers):
        return deepcopy(
            [o for o in self.native_orders if str(o["client_order_index"]) in identifiers]
        )

    async def active_orders(self):
        return deepcopy([o for o in self.native_orders if o["status"] in ("open", "pending")])

    async def trades(self, identifier):
        return deepcopy(self.native_trades.get(identifier, []))

    async def transaction_status(self, tx_hash):
        tx = next(t for t in self.store.pending_transactions() if t["hash"] == tx_hash)
        return dict(
            hash=tx_hash,
            account_index=123,
            api_key_index=2,
            nonce=tx["nonce"],
            executed_at=1,
            status=1,
        )

    async def account_transactions(self):
        return self.history

    async def cash_history(self, start, end):
        return self.cash

    async def funding(self, start, end):
        return []

    async def signed_transaction(
        self, operation, payload, records=(), preflight=None, risk_reducing=False
    ):
        if preflight:
            preflight()
        self.calls.append((operation, payload, risk_reducing))
        for r in records:
            self.store.order(
                r["client_id"],
                r["symbol"],
                "submitting",
                {
                    **r["payload"],
                    "run": self.run,
                    "submitted_mono_ns": time.monotonic_ns(),
                    "submitted_wall_ns": time.time_ns(),
                },
            )
        return {"client_ids": [r["client_id"] for r in records]}


@pytest.fixture
def broker(tmp_path):
    store = Store(tmp_path / "live.sqlite")
    exchange = Exchange(store)
    result = LiveBroker(exchange, Config(), [INSTRUMENT])
    result.snapshot = exchange.current
    yield result
    store.close()


def pending(broker, identifier="1", side=1, quantity=0.1, role="entry", **extra):
    broker.store.order(
        identifier,
        "BTC",
        "unknown",
        {
            "role": role,
            "quantity": quantity,
            "side": side,
            "run": "run",
            "submitted_mono_ns": time.monotonic_ns() - 100_000_000,
            "submitted_wall_ns": time.time_ns() - 100_000_000,
            "integration": False,
            "model_sha256": "model",
            "holding_seconds": 2,
            "tx_hash": "tx" + identifier,
            **extra,
        },
    )


def test_native_bracket_is_atomic_and_symmetric(broker):
    for side in (1, -1):
        asyncio.run(broker.enter("BTC", side, 0.1, BOOK, FORECAST, "model"))
        operation, payload, reducing = broker.gateway.calls[-1]
        parent, stop = payload["orders"]
        assert operation == "group" and not reducing
        assert parent["IsAsk"] == (side == -1) and parent["TimeInForce"] == 0
        assert stop["Type"] == 2 and stop["ReduceOnly"] == 1 and stop["IsAsk"] == (side == 1)
        assert stop["OrderExpiry"] == -1 and parent["OrderExpiry"] == 0
        assert parent["BaseAmount"] == 10 and stop["BaseAmount"] == 0
        assert (stop["TriggerPrice"] < parent["Price"]) == (side == 1)
        # Simulate local rejection before transmission for next direction.
        for r in broker.store.orders():
            broker.store.order(r["client_id"], r["symbol"], "rejected", r["payload"])


def test_tick_rounding_keeps_ioc_prices_inside_the_slippage_bound(broker):
    buy = broker.order_payload("1", "BTC", 1, 0.1, 100.019)
    sell = broker.order_payload("2", "BTC", -1, 0.1, 99.981)
    assert buy["Price"] == 10001 and sell["Price"] == 9999
    stop = broker.order_payload("3", "BTC", -1, 0.1, 98.001, stop=99.001, reduce=True)
    assert stop["Price"] == 9801 and stop["TriggerPrice"] == 9901


def test_partial_cancel_keeps_actual_position_and_fills_are_idempotent(broker):
    pending(broker)
    broker.gateway.native_orders = [raw_order(filled=".04", status="canceled-not-enough-liquidity")]
    broker.gateway.native_trades = {"101": [trade()]}
    broker.gateway.current = snapshot(0.04)
    asyncio.run(broker.reconcile())
    assert broker.store.orders()[0]["status"] == "cancelled"
    assert len(broker.store.fills()) == 1 and broker.snapshot.positions["BTC"].quantity == 0.04
    # A process restart uses persisted final fills; old orders may be absent.
    broker.gateway.native_orders = []
    broker.gateway.run = "restarted"
    asyncio.run(broker.reconcile())
    assert len(broker.store.fills()) == 1


def test_repeated_partial_updates_do_not_duplicate_fills(broker):
    pending(broker)
    broker.gateway.native_orders = [raw_order(filled=".04")]
    broker.gateway.native_trades = {"101": [trade()]}
    broker.gateway.current = snapshot(0.04)
    for _ in range(2):
        asyncio.run(broker.reconcile())
    assert broker.store.orders()[0]["status"] == "partial" and len(broker.store.fills()) == 1
    broker.gateway.native_orders = [raw_order(filled=".10", status="filled")]
    broker.gateway.native_trades["101"].append(trade(size=".06", tid="11"))
    broker.gateway.current = snapshot(0.1)
    asyncio.run(broker.reconcile())
    assert broker.store.orders()[0]["status"] == "filled" and len(broker.store.fills()) == 2


@pytest.mark.parametrize(
    "failure", ["absent", "wrong_owner", "wrong_trade", "missing_fill", "external_position"]
)
def test_unverified_exchange_effects_block_reconciliation(broker, failure):
    pending(broker)
    broker.gateway.native_orders = [raw_order(filled=".04")]
    broker.gateway.native_trades = {"101": [trade()]}
    broker.gateway.current = snapshot(0.04)
    if failure == "absent":
        broker.gateway.native_orders = []
    if failure == "wrong_owner":
        broker.gateway.native_orders[0]["owner_account_index"] = 999
    if failure == "wrong_trade":
        broker.gateway.native_trades["101"][0]["bid_account_id"] = 999
    if failure == "missing_fill":
        broker.gateway.native_trades["101"] = []
    if failure == "external_position":
        broker.gateway.current = snapshot(0.08)
    with pytest.raises(ExecutionBlocked):
        asyncio.run(broker.reconcile())


def test_partial_position_is_closed_with_actual_size_and_stop_is_retained(broker):
    pending(broker, role="entry")
    pending(broker, identifier="2", side=-1, role="stop", parent_id="1")
    broker.gateway.native_orders = [
        raw_order(filled=".04", status="canceled"),
        raw_order("2", side=-1, role="stop"),
    ]
    broker.gateway.native_trades = {"101": [trade()]}
    broker.gateway.current = snapshot(0.04)
    asyncio.run(broker.reconcile())
    assert broker.protected("BTC")
    stop = next(r for r in broker.store.orders() if r["client_id"] == "2")
    with pytest.raises(ExecutionBlocked, match="Cannot cancel protection"):
        asyncio.run(broker.cancel(stop))
    asyncio.run(broker.close_position("BTC", 99))
    op, payload, reducing = broker.gateway.calls[-1]
    assert op == "order" and reducing and payload["base_amount"] == 4 and payload["reduce_only"]
    assert payload["is_ask"]
    with pytest.raises(ExecutionBlocked, match="exit already"):
        asyncio.run(broker.close_position("BTC", 99))


def test_parent_waiting_stop_is_not_protection(broker):
    pending(broker, role="stop", side=-1)
    raw = raw_order(side=-1, role="stop")
    raw["trigger_status"] = "parent-order"
    record = broker.store.orders()[0]
    broker.store.order("1", "BTC", "open", {**record["payload"], "exchange": raw})
    broker.snapshot = snapshot(0.04)
    assert not broker.protected("BTC")


def test_position_tied_stop_covers_partial_fill_only_with_parent_identity(broker):
    pending(broker)
    parent = raw_order(filled=".04", status="canceled")
    original = broker.store.orders()[0]
    broker.store.order("1", "BTC", "cancelled", {**original["payload"], "exchange": parent})
    pending(broker, identifier="2", role="stop", side=-1, parent_id="1", position_tied=True)
    stop = raw_order("2", side=-1, quantity="0", role="stop")
    stop["parent_order_index"] = parent["order_index"]
    r = next(r for r in broker.store.orders() if r["client_id"] == "2")
    broker._identity(r, stop)
    broker.store.order("2", "BTC", "open", {**r["payload"], "exchange": stop})
    broker.snapshot = snapshot(0.04)
    assert broker.protected("BTC")
    stop["parent_order_index"] = 999
    broker.store.order("2", "BTC", "open", {**r["payload"], "exchange": stop})
    assert not broker.protected("BTC")


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"equity": 0}, "insolvent"),
        ({"maintenance_margin": 60}, "maintenance"),
        ({"available": 10}, "reserve"),
        ({"equity": 80}, "drawdown"),
    ],
)
def test_margin_and_drawdown_watchdog(change, reason):
    assert reason in margin_reason(replace(snapshot(), **change), Config(), peak=100)


def test_watchdog_reduces_even_when_evidence_revoked(broker):
    pending(broker)
    broker.gateway.native_orders = [raw_order(filled=".1", status="filled")]
    broker.gateway.native_trades = {"101": [trade(size=".1")]}
    broker.gateway.current = snapshot(0.1)
    asyncio.run(broker.reconcile())
    latency = SimpleNamespace(entry_ms=(10,) * 20, exit_ms=(10,) * 20)
    engine = LiveEngine(
        Config(),
        broker,
        None,
        {"evaluation_config": {"research": {"notional": 10}}},
        "model",
        latency,
    )
    engine.gate = {"allowed": False}
    engine.suspend("evidence_revoked")
    asyncio.run(engine.protection())
    assert broker.gateway.calls[-1][2] and broker.gateway.calls[-1][1]["reduce_only"]


class Response:
    def __init__(self, data, status=200, fail=False):
        self.data, self.status, self.fail = data, status, fail

    async def __aenter__(self):
        if self.fail:
            raise TimeoutError()
        return self

    async def __aexit__(self, *args):
        pass

    async def json(self):
        return self.data


class HTTP:
    def __init__(self, store, fail=False):
        self.store, self.fail, self.posts, self.nonce = store, fail, 0, 1

    def get(self, url, **kwargs):
        return Response({"nonce": self.nonce})

    def post(self, url, **kwargs):
        assert self.store.pending_transactions() and self.store.orders()
        self.posts += 1
        return Response({"code": 200, "tx_hash": "signed-hash"}, fail=self.fail)


class Signer:
    def sign_create_order(self, **kwargs):
        return 14, "signed-content", "signed-hash", None

    def sign_create_grouped_orders(self, kind, orders, **kwargs):
        assert kind == 1 and len(orders) == 2
        return 28, "signed-content", "signed-hash", None

    async def close(self):
        pass


def gateway_fixture(tmp_path, fail=False):
    store = Store(tmp_path / "native.sqlite")
    http = HTTP(store, fail)
    gateway = NativeGateway(
        "https://mainnet.zklighter.elliot.ai",
        Signer(),
        store,
        123,
        2,
        request_rate=10000,
        transaction_rate=10000,
        http=http,
    )
    return gateway, http, store


def test_post_timeout_is_durable_and_never_retried(tmp_path):
    gateway, http, store = gateway_fixture(tmp_path, True)
    record = {"client_id": "1", "symbol": "BTC", "payload": {"role": "entry"}}
    with pytest.raises(ExecutionBlocked, match="unknown"):
        asyncio.run(gateway.signed_transaction("order", {}, [record]))
    assert store.pending_transactions()[0]["status"] == "unknown"
    with pytest.raises(ExecutionBlocked, match="Uncertain"):
        asyncio.run(gateway.signed_transaction("order", {}, [record]))
    assert http.posts == 1
    store.close()


def test_final_preflight_failure_never_posts_or_leaves_ambiguous_nonce(tmp_path):
    gateway, http, store = gateway_fixture(tmp_path)
    calls = []

    def preflight():
        calls.append(1)
        if len(calls) == 2:
            raise ExecutionBlocked("signal expired")

    with pytest.raises(ExecutionBlocked, match="expired"):
        asyncio.run(
            gateway.signed_transaction(
                "order", {}, [{"client_id": "1", "symbol": "BTC", "payload": {}}], preflight
            )
        )
    assert http.posts == 0 and not store.pending_transactions()
    assert store.orders()[0]["status"] == "rejected"
    store.close()


def test_acknowledgement_is_not_recorded_as_fill(tmp_path):
    gateway, http, store = gateway_fixture(tmp_path)
    result = asyncio.run(
        gateway.signed_transaction(
            "order", {}, [{"client_id": "1", "symbol": "BTC", "payload": {}}]
        )
    )
    assert result["status"] == "acknowledged" and store.orders()[0]["status"] == "submitting"
    assert not store.fills() and http.posts == 1
    assert "signed-content" not in json.dumps(store.orders())
    store.close()


def test_process_lock_and_account_binding_prevent_two_writers(tmp_path):
    path = tmp_path / "state.sqlite"
    lease = LedgerLock(path)
    with pytest.raises(ExecutionBlocked, match="Another process"):
        LedgerLock(path)
    lease.close()
    other = LedgerLock(path)
    other.close()
    store = Store(path)
    NativeGateway("https://mainnet.zklighter.elliot.ai", Signer(), store, 123, 2)
    with pytest.raises(ExecutionBlocked, match="another account"):
        NativeGateway("https://mainnet.zklighter.elliot.ai", Signer(), store, 456, 2)
    store.close()


def test_cash_episode_uses_actual_balance_including_costs(broker):
    asyncio.run(broker.settle_episode("model"))
    pending(broker)
    pending(broker, identifier="2", side=-1, role="exit")
    broker.gateway.native_orders = [
        raw_order(filled=".1", status="filled"),
        raw_order("2", side=-1, filled=".1", status="filled", role="exit"),
    ]
    broker.gateway.native_trades = {
        "101": [trade(size=".1")],
        "102": [trade("2", size=".1", price="101", side=-1, tid="11")],
    }
    broker.gateway.current = snapshot(collateral=100.08)
    broker.gateway.history = [
        dict(
            hash="tx" + identifier,
            transaction_time=time.time_ns() // 1_000_000,
            committed_at=1,
            executed_at=1,
        )
        for identifier in ("1", "2")
    ]
    asyncio.run(broker.reconcile())
    episode = asyncio.run(broker.settle_episode("model"))
    assert episode["gross_pnl"] == pytest.approx(0.1)
    assert episode["net_pnl"] == pytest.approx(0.08)
    assert episode["all_in_cost_bps_per_fill"] == pytest.approx(0.02 / 20.1 * 10000)
    assert episode["source"] == "exchange_reconciled_flat_to_flat_cash"
    assert len(broker.store.episodes()) == 1
    assert asyncio.run(broker.settle_episode("model")) is None


def test_unexplained_flat_cash_change_is_blocked(broker):
    asyncio.run(broker.settle_episode("model"))
    broker.snapshot = snapshot(collateral=101)
    with pytest.raises(ExecutionBlocked, match="Unexplained cash"):
        asyncio.run(broker.settle_episode("model"))


def test_integration_flags_cannot_bypass_actual_lifecycle(broker):
    broker.store.set(
        "native_integration_proof",
        {
            "account_index": 123,
            "source_sha256": source_digest(),
            "verified_wall_ns": time.time_ns(),
            "passed": True,
        },
    )
    result = integration_summary(broker.store, Config())
    assert not result["allowed"] and "no native protective stop fill" in result["reasons"]


def test_forward_promotion_and_retirement_use_actual_costed_episodes():
    now = time.time_ns()
    rows = [
        dict(
            model_sha256="model",
            integration=False,
            reconciled=True,
            net_pnl=0.01,
            net_bps=2.0,
            end_wall_ns=now - i * 12 * 3600 * 10**9,
        )
        for i in range(20)
    ]
    assert forward_metrics(rows, "model")["allowed"]
    assert not forward_metrics(rows, "different")["allowed"]
    assert not forward_metrics([{**r, "net_pnl": -0.01, "net_bps": -2} for r in rows], "model")[
        "allowed"
    ]
    assert not forward_metrics([{**r, "integration": True} for r in rows], "model")["allowed"]


def test_research_gate_accepts_valid_evidence_and_rejects_tampering(tmp_path):
    path = tmp_path / "model"
    path.write_bytes(b"local-model")
    report = {
        "metadata": {"synthetic": False, "latency": {"synthetic": False}},
        "model_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "source_manifest_sha256": source_digest(),
        "beats_simple_baselines_after_costs": True,
        "champion_expectancy_ci_bps_family_adjusted": [1.0, 3.0],
        "positive_out_of_sample_periods": 2,
        "champion": "cross_venue_lag",
        "results": [
            {
                "model": "cross_venue_lag",
                "net_pnl": 1,
                "trades": [{"net_bps": 2} for _ in range(20)],
            }
        ]
        * 2,
    }
    assert research_gate(report, path)["allowed"]
    path.write_bytes(b"tampered")
    assert not research_gate(report, path)["allowed"]


def test_clock_bound_includes_offset_dispersion_and_half_delay(monkeypatch):
    monkeypatch.setattr(
        "apexmind.account.subprocess.run",
        lambda *a, **k: SimpleNamespace(
            returncode=0,
            stdout="Leap status : Normal\nSystem time : 0.001 seconds slow\nRoot dispersion : 0.002\nRoot delay : 0.006\n",
        ),
    )
    assert clock_health(7)["healthy"] and clock_health(7)["error_bound_ms"] == pytest.approx(6)
    assert not clock_health(5)["healthy"]


def test_precision_and_account_nonfinite_rejection():
    assert units(".0199", 2) == 1 and units(".0199", 2, up=True) == 2
    raw = {
        "account_index": 123,
        "index": 123,
        "account_trading_mode": 0,
        "positions": [],
        "collateral": "NaN",
        "available_balance": "100",
        "cross_initial_margin_requirement": "0",
        "cross_maintenance_margin_requirement": "0",
        "transaction_time": 1,
    }
    with pytest.raises(AccountError, match="Nonfinite"):
        AccountSnapshot.parse(raw, 123)
    raw["account_trading_mode"] = 1
    with pytest.raises(AccountError, match="classic"):
        AccountSnapshot.parse(raw, 123)


def test_durable_client_ids_do_not_repeat_after_restart(tmp_path):
    path = tmp_path / "ids.sqlite"
    store = Store(path)
    first = store.allocate_ids(2)
    store.close()
    store = Store(path)
    second = store.allocate_ids(2)
    store.close()
    assert not set(first) & set(second) and max(map(int, first)) < min(map(int, second))


def test_account_snapshot_records_are_serializable():
    assert json.loads(json.dumps(asdict(snapshot(0.1))))["positions"]["BTC"]["side"] == 1


def test_native_reduce_order_can_clip_to_partial_position(broker):
    pending(broker, role="stop", side=-1)
    record = broker.store.orders()[0]
    raw = raw_order(side=-1, quantity=".04", role="stop")
    broker._identity(record, raw)
    raw["initial_base_amount"] = ".20"
    with pytest.raises(ExecutionBlocked, match="quantity mismatch"):
        broker._identity(record, raw)


def test_existing_deposit_is_a_baseline_and_new_deposit_is_not_strategy_profit(broker):
    broker.gateway.cash[0]["inflow"] = 100
    asyncio.run(broker.settle_episode("model"))
    broker.gateway.cash[0]["inflow"] = 110
    broker.snapshot = snapshot(collateral=110)
    assert asyncio.run(broker.settle_episode("model")) is None
    assert not broker.store.episodes()
    assert broker.store.get("external_capital_changes")[0]["cash_change"] == 10


def qualified_fixture(broker, tmp_path):
    from apexmind.latency import Latency

    path = tmp_path / "model"
    path.write_bytes(b"trusted-test-artifact")
    latency = Latency((50.0,) * 20, (50.0,) * 20, "fixture-ledger", "standard", 1)
    config = replace(Config(), feeds=replace(Config().feeds, symbols=["BTC"]))
    report = {
        "model_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "source_manifest_sha256": source_digest(),
        "evaluation_config": asdict(config),
        "champion": "cross_venue_lag",
        "positive_out_of_sample_periods": 2,
        "beats_simple_baselines_after_costs": True,
        "champion_expectancy_ci_bps_family_adjusted": [1.0, 3.0],
        "metadata": {"synthetic": False, "latency": latency.summary(), "symbols": ["BTC"]},
        "results": [
            {
                "model": "cross_venue_lag",
                "net_pnl": 1,
                "trades": [{"net_bps": 2.0} for _ in range(20)],
            }
        ]
        * 2,
    }
    for role in ("entry", "exit"):
        for i in range(20):
            identifier = role + str(i)
            broker.store.order(
                identifier,
                "BTC",
                "filled",
                {
                    "integration": True,
                    "role": role,
                    "filled_quantity": 0.1,
                    "observed_fill_delay_ms": 50.0,
                    "exchange": {"reduce_only": role == "exit"},
                },
            )
    broker.store.order(
        "stop", "BTC", "filled", {"integration": True, "role": "stop", "filled_quantity": 0.1}
    )
    broker.store.order(
        "probe", "BTC", "cancelled", {"integration": True, "role": "entry", "probe": True}
    )
    broker.store.episode(
        "integration",
        dict(
            integration=True,
            reconciled=True,
            all_in_cost_bps_per_fill=0.5,
            model_sha256="integration",
        ),
    )
    broker.store.set(
        "native_integration_proof",
        dict(
            account_index=123,
            source_sha256=source_digest(),
            verified_wall_ns=time.time_ns(),
            fee_fingerprint=broker.gateway.limits,
        ),
    )
    return config, report, path, latency


def test_pilot_can_gather_forward_evidence_but_production_requires_it(broker, tmp_path):
    config, report, path, latency = qualified_fixture(broker, tmp_path)
    pilot = runtime_gate(
        report, path, config, broker.store, broker.snapshot, broker.gateway.limits, latency, "pilot"
    )
    production = runtime_gate(
        report,
        path,
        config,
        broker.store,
        broker.snapshot,
        broker.gateway.limits,
        latency,
        "production",
    )
    assert pilot["allowed"]
    assert (
        not production["allowed"]
        and "actual forward advantage has not been established" in production["reasons"]
    )
    for i in range(20):
        broker.store.episode(
            str(i),
            dict(
                integration=False,
                reconciled=True,
                model_sha256=report["model_sha256"],
                net_pnl=0.01,
                net_bps=2.0,
                end_wall_ns=time.time_ns() - i * 12 * 3600 * 10**9,
            ),
        )
    assert runtime_gate(
        report,
        path,
        config,
        broker.store,
        broker.snapshot,
        broker.gateway.limits,
        latency,
        "production",
    )["allowed"]
    changed = {**broker.gateway.limits, "current_taker_fee_tick": 1}
    assert not runtime_gate(
        report, path, config, broker.store, broker.snapshot, changed, latency, "production"
    )["allowed"]


def test_registry_failure_preserves_incumbent_and_refuses_open_exposure(broker, tmp_path):
    latency = SimpleNamespace(entry_ms=(50.0,) * 20, exit_ms=(50.0,) * 20)
    registry = tmp_path / "registry.json"
    registry.write_text(
        json.dumps({"challenger": {"sha256": "different", "report": "missing", "model": "missing"}})
    )
    report = {"model_sha256": "incumbent", "evaluation_config": {"research": {"notional": 12}}}
    model = object()
    engine = LiveEngine(Config(), broker, model, report, "incumbent", latency, "pilot", registry)
    assert not engine.adopt_candidate() and engine.model is model
    broker.snapshot = snapshot(0.1)
    assert not engine.adopt_candidate() and engine.report is report


def test_portfolio_checkpoints_reserve_auditable_exit_capacity(broker):
    engine = LiveEngine(
        Config(),
        broker,
        None,
        {"evaluation_config": {"research": {"notional": 12}}},
        "model",
        SimpleNamespace(entry_ms=(50.0,) * 20, exit_ms=(50.0,) * 20),
    )
    assert engine.audit_due(reserve_entry=True)
    start = time.time_ns()
    broker.store.set("live_episode", {"start_wall_ns": start})
    assert not engine.audit_due(reserve_entry=True)
    for i in range(57):
        broker.store.transaction(str(i), i, "resolved", {"submitted_wall_ns": start + 1})
    assert engine.audit_due(reserve_entry=True)
    broker.store.set("live_episode", {"start_wall_ns": start - 301 * 10**9})
    assert engine.audit_due()


def test_account_access_does_not_depend_on_reference_discovery(tmp_path, monkeypatch):
    from apexmind.integration import connected

    config = replace(Config(), live=replace(Config().live, ledger=str(tmp_path / "native.sqlite")))

    async def native(url, store, configuration):
        exchange = Exchange(store)

        async def request(path):
            assert path == "/api/v1/orderBooks"
            return {
                "order_books": [
                    dict(
                        symbol="BTC",
                        market_id=1,
                        market_type="perp",
                        min_base_amount=".01",
                        min_quote_amount="5",
                        supported_size_decimals=2,
                        supported_price_decimals=2,
                    )
                ]
            }

        async def close():
            pass

        exchange.request, exchange.close = request, close
        return exchange

    async def forbidden(configuration):
        raise AssertionError("Reference discovery must not gate account recovery")

    monkeypatch.setattr("apexmind.integration.NativeGateway.from_environment", native)
    monkeypatch.setattr("apexmind.integration.discover", forbidden)

    async def inspect(broker, instruments):
        await broker.reconcile()
        return broker.snapshot.account_index

    assert asyncio.run(connected(config, inspect)) == 123
