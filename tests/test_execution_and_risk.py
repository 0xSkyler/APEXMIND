import asyncio
from dataclasses import replace
from types import SimpleNamespace
import numpy as np
import pytest
from apexmind.config import Config
from apexmind.evaluation import replay
from apexmind.execution import ExecutionBlocked, LighterBroker, execution_choice, read_signing_key
from apexmind.risk import Portfolio
from apexmind.store import Store

INSTRUMENT = {
    "symbol": "BTC",
    "market_id": 1,
    "min_base": "0.00001",
    "min_quote": "5",
    "size_decimals": 5,
    "price_decimals": 2,
}
FORECAST = {
    "mean": 20.0,
    "lower_mean": 10.0,
    "q05": -10.0,
    "q50": 20.0,
    "q95": 40.0,
    "adverse": 10.0,
    "slippage_probability": 0.01,
    "favorable_probability": 0.7,
    "execution_delay_ms": 50.0,
    "holding_seconds": 2.0,
}


def test_ten_dollar_account_uses_feasible_order_and_never_increases_leverage():
    portfolio = Portfolio(Config().risk)
    allocation = portfolio.allocate("BTC", 30000, INSTRUMENT, FORECAST)
    assert allocation.accepted and 5 <= allocation.notional <= 6
    assert allocation.leverage == 1
    impossible = {**INSTRUMENT, "min_quote": "20"}
    rejected = portfolio.allocate("BTC", 30000, impossible, FORECAST)
    assert not rejected.accepted and rejected.reason == "exchange_minimum"
    assert portfolio.config.max_gross_leverage == 1


def test_concurrent_positions_respect_portfolio_capacity():
    c = replace(Config().risk, initial_balance=100, max_market_fraction=0.3)
    p = Portfolio(c)
    for symbol in ("BTC", "ETH"):
        a = p.allocate(symbol, 100, INSTRUMENT, FORECAST)
        assert a.accepted
        p.open(symbol, a, {})
    assert len(p.positions) == 2 and p.gross <= 70
    assert not p.allocate("BTC", 100, INSTRUMENT, FORECAST).accepted


def test_downside_drawdown_and_invalid_forecasts_are_rejected():
    p = Portfolio(Config().risk)
    assert not p.allocate("BTC", 30000, INSTRUMENT, {**FORECAST, "mean": float("nan")}).accepted
    assert not p.allocate("BTC", 30000, INSTRUMENT, {**FORECAST, "lower_mean": 0}).accepted
    a = p.allocate("BTC", 30000, INSTRUMENT, FORECAST)
    p.open("BTC", a, {})
    p.close("BTC", -5000)
    assert p.suspended
    assert p.allocate("ETH", 100, INSTRUMENT, FORECAST).reason == "drawdown_suspension"


def test_persistent_paper_replay_does_not_duplicate_orders(dataset, config, tmp_path):
    _, data, metadata = dataset
    pred = {k: np.full(len(data["x"]), v) for k, v in FORECAST.items()}
    store = Store(tmp_path / "paper.sqlite")
    instruments = {i["symbol"]: i for i in metadata["instruments"]}
    first = replay(data, pred, config, instruments, 1, store)
    assert first["executed"] > 0
    equity = store.get("paper_portfolio")["equity"]
    second = replay(data, pred, config, instruments, 1, store)
    assert second["executed"] == 0
    assert store.get("paper_portfolio")["equity"] == equity
    assert not store.unresolved()
    store.close()


def test_execution_optimizer_requires_passive_measurements():
    assert execution_choice(FORECAST, 1, 20)["mode"] == "reject"
    assert execution_choice(FORECAST, 1, 1000)["mode"] == "aggressive"
    measured = {
        "fill_probability": 0.99,
        "delay_ms": 10,
        "adverse_selection_bps": 0,
        "measured_samples": 1000,
    }
    assert execution_choice(FORECAST, 5, 1000, measured)["mode"] == "passive"


def test_local_signer_permissions_checked_without_logging_key(tmp_path, monkeypatch):
    path = tmp_path / "signing.key"
    path.write_text("f" * 64)
    path.chmod(0o644)
    monkeypatch.setenv("LIGHTER_SIGNING_KEY_FILE", str(path))
    with pytest.raises(ExecutionBlocked, match="0600"):
        read_signing_key()
    path.chmod(0o600)
    assert read_signing_key() == "f" * 64


def test_submission_ack_is_not_a_fill_and_partial_fills_remain_unresolved(tmp_path, monkeypatch):
    store = Store(tmp_path / "account.sqlite")

    class Client:
        async def create_market_order_if_slippage(self, **kwargs):
            return None, SimpleNamespace(code=200, tx_hash="hash"), None

    monkeypatch.setattr("apexmind.execution.validate_evidence", lambda *args: {"allowed": True})
    broker = LighterBroker(Client(), store, {"BTC": INSTRUMENT}, 123)
    intent = {
        "client_id": "42",
        "symbol": "BTC",
        "mode": "aggressive",
        "risk_approved": True,
        "base_units": 100,
        "side": 1,
        "max_slippage_bps": 5,
    }
    result = asyncio.run(broker.submit(intent, "report", "model"))
    assert result["status"] == "acknowledged" and store.unresolved()[0]["status"] == "acknowledged"
    trade = {"account_index": 123, "trade_id": "one", "cumulative_base_units": 50}
    assert broker.reconcile_fill("42", trade)
    assert store.unresolved()[0]["status"] == "partial"
    assert not broker.reconcile_fill("42", trade)
    assert broker.reconcile_fill("42", {**trade, "trade_id": "two", "cumulative_base_units": 100})
    assert not store.unresolved()
    store.close()


def test_ambiguous_submission_blocks_automatic_retry(tmp_path, monkeypatch):
    store = Store(tmp_path / "account.sqlite")

    class Client:
        async def create_market_order_if_slippage(self, **kwargs):
            raise TimeoutError()

    monkeypatch.setattr("apexmind.execution.validate_evidence", lambda *args: {"allowed": True})
    broker = LighterBroker(Client(), store, {"BTC": INSTRUMENT}, 123)
    intent = {
        "client_id": "43",
        "symbol": "BTC",
        "mode": "aggressive",
        "risk_approved": True,
        "base_units": 100,
        "side": 1,
        "max_slippage_bps": 5,
    }
    with pytest.raises(ExecutionBlocked, match="unknown"):
        asyncio.run(broker.submit(intent, "report", "model"))
    with pytest.raises(ExecutionBlocked, match="Unreconciled"):
        asyncio.run(broker.submit(intent, "report", "model"))
    assert store.unresolved()[0]["status"] == "unknown"
    store.close()
