from dataclasses import replace
import numpy as np
from apexmind.config import Config
from apexmind.demo import generate
from apexmind.paper import PaperEngine
from apexmind.store import Store


class TestForecast:
    __test__ = False
    threshold = 1
    calibrated_until_wall_ns = 0

    def predict(self, x):
        rows = len(x)
        return {
            "mean": np.full(rows, 50.0),
            "lower_mean": np.full(rows, 30.0),
            "q05": np.full(rows, -10.0),
            "adverse": np.full(rows, 10.0),
            "slippage_probability": np.zeros(rows),
            "holding_seconds": np.full(rows, 0.2),
        }


def test_forward_delayed_execution_protection_and_recovery(tmp_path, latency):
    market = Store(tmp_path / "market.sqlite")
    ledger = Store(tmp_path / "ledger.sqlite")
    generate(market, seconds=12)
    config = replace(Config(), risk=replace(Config().risk, initial_balance=100))
    events = list(market.events())
    engine = PaperEngine(config, latency, TestForecast(), ledger, market.get("instruments"))
    for event in events:
        engine.on_event(event)
    assert engine.completed > 0
    rows = list(ledger.db.execute("SELECT payload FROM executions"))
    assert rows
    for (raw,) in rows:
        import json

        fill = json.loads(raw)
        assert fill["holding_seconds_realized"] >= 0.2
        assert fill["mode"] == "HYPOTHETICAL_FORWARD_PAPER"
        assert fill["entry_delay_ms"] > 0
    saved = ledger.get("stream_paper")
    restarted = PaperEngine(config, latency, TestForecast(), ledger, market.get("instruments"))
    assert restarted.completed == engine.completed
    assert restarted.portfolio.equity == saved["equity"]
    last = events[-1]
    restarted.on_event(
        replace(
            last, run="new-process", mono_ns=1_000_000_000, wall_ns=last.wall_ns + 1_000_000_000
        )
    )
    for p in restarted.portfolio.positions.values():
        assert p["phase"] in ("recovering", "exit_pending")
    market.close()
    ledger.close()


def test_duplicate_book_does_not_repeat_paper_execution(tmp_path, latency):
    market = Store(tmp_path / "market.sqlite")
    ledger = Store(tmp_path / "ledger.sqlite")
    generate(market, seconds=8)
    c = replace(Config(), risk=replace(Config().risk, initial_balance=100))
    engine = PaperEngine(c, latency, TestForecast(), ledger, market.get("instruments"))
    events = list(market.events())
    for event in events:
        engine.on_event(event)
    before = (engine.completed, engine.portfolio.equity)
    for event in events[-8:]:
        if event.kind == "book":
            engine.on_event(event)
    assert (engine.completed, engine.portfolio.equity) == before
    market.close()
    ledger.close()
