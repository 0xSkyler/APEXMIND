from dataclasses import replace
import json
import numpy as np
import pytest
from threadpoolctl import threadpool_limits
from apexmind.dataset import build_dataset
from apexmind.evaluation import chronological_folds, research
from apexmind.governance import register_challenger, validate_evidence
from apexmind.latency import Latency, import_measurements
from apexmind.models import ForecastModel, directed
from apexmind.store import Store
from apexmind.demo import generate


def test_latency_requires_measurements_not_zero_delay():
    with pytest.raises(ValueError, match="20 positive"):
        Latency((0.0,) * 20, (1.0,) * 20, "fixture", "standard", 0)
    with pytest.raises(ValueError):
        Latency((1.0,) * 19, (1.0,) * 20, "fixture", "standard", 0)


def test_latency_import_checks_tier_and_uses_fill_not_ack(tmp_path):
    path = tmp_path / "latency.csv"
    header = (
        "role,account_tier,venue,submitted_mono_ns,observed_fill_mono_ns,observed_fill_wall_ns\n"
    )
    rows = "".join(
        f"{role},standard,lighter,1000000,51000000,1000000000\n"
        for role in ("entry", "exit")
        for _ in range(20)
    )
    path.write_text(header + rows)
    report = import_measurements(path, tmp_path / "latency.json", "standard")
    assert report["entry_median_ms"] == 50
    with pytest.raises(ValueError, match="Mixed"):
        import_measurements(path, tmp_path / "wrong.json", "premium")


def test_labels_are_delayed_executable_and_costed(dataset, config):
    _, data, meta = dataset
    assert meta["synthetic"]
    assert len(data["x"]) > 100
    assert np.all(data["label_end_ns"] > data["decision_ns"])
    np.testing.assert_allclose(
        data["gross_bps"] - data["net_bps"],
        data["fee_bps"] + data["funding_bps"] + data["slippage_reserve_bps"],
    )
    assert np.all(data["holding_seconds"] >= config.research.holding_seconds)
    assert np.all(data["entry_delay_ms"] > 0)
    assert meta["rejected_labels"]["exit_feed_gap"] > 0


def test_latency_information_cannot_come_from_after_research(tmp_path, config, latency):
    store = Store(tmp_path / "market.sqlite")
    generate(store, seconds=8)
    late = replace(latency, synthetic=False, measured_until_wall_ns=1_800_000_000_000_000_000)
    with pytest.raises(ValueError, match="precede"):
        build_dataset(store, config, late, str(tmp_path / "bad.npz"))
    store.close()


def test_calibration_capture_is_excluded_from_following_research(tmp_path, config, latency):
    store = Store(tmp_path / "capture.sqlite")
    generate(store, seconds=60)
    cutoff = 1_700_000_000_000_000_000 + 20 * 10**9
    measured = replace(latency, synthetic=False, measured_until_wall_ns=cutoff)
    result = build_dataset(
        store, config, measured, str(tmp_path / "after.npz"), after_wall_ns=cutoff
    )
    assert result["excluded_pre_measurement_events"] > 0
    assert result["data_period_wall_ns"][0] > cutoff
    assert result["synthetic"]  # Synthetic prices remain marked despite filtering.
    store.close()


def test_purging_keeps_labels_out_of_validation_and_test(dataset, config):
    _, data, _ = dataset
    folds = list(chronological_folds(data, config))
    assert len(folds) == config.research.folds
    previous_end = 0
    for train, val, test in folds:
        assert data["label_end_wall_ns"][train].max() < data["wall_ns"][val].min()
        assert data["label_end_wall_ns"][val].max() < data["wall_ns"][test].min()
        assert data["wall_ns"][test].min() > previous_end
        previous_end = data["label_end_wall_ns"][test].max()
        assert not set(train) & set(test)


def test_side_interaction_and_train_only_selection(dataset):
    _, data, _ = dataset
    x = data["x"][:100]
    z = directed(x)
    np.testing.assert_allclose(z[:, 0], x[:, 0] * x[:, -1])
    td = {k: v[:100] for k, v in data.items()}
    model = ForecastModel("regularized").fit(x, td)
    selected = model.selected.copy()
    pred = model.predict(data["x"][100:120])
    assert np.all(pred["q05"] <= pred["q95"])
    assert np.all((pred["favorable_probability"] >= 0) & (pred["favorable_probability"] <= 1))
    np.testing.assert_array_equal(model.selected, selected)


def test_end_to_end_report_is_honest_and_models_share_periods(dataset, config, tmp_path):
    path, _, _ = dataset
    out = tmp_path / "report"
    with threadpool_limits(limits=2):
        result = research(path, config, out)
    report = json.loads((out / "report.json").read_text())
    assert result["status"] == "SYNTHETIC_SOFTWARE_VALIDATION"
    assert not report["profitability_proven"] and not report["live_deployment_allowed"]
    assert len(report["results"]) == 6 * config.research.folds
    for fold in range(config.research.folds):
        rows = [r for r in report["results"] if r["fold"] == fold]
        assert len({tuple(r["test_period_wall_ns"]) for r in rows}) == 1
        assert sum(r["selected_champion"] for r in rows) == 1
    check = validate_evidence(out / "report.json", out / "champion.joblib")
    assert not check["allowed"] and "synthetic data" in check["reasons"]
    registry = tmp_path / "registry.json"
    registry.write_text(json.dumps({"champion": {"sha256": "existing-incumbent"}}))
    register_challenger(out / "report.json", out / "champion.joblib", registry)
    assert json.loads(registry.read_text())["champion"]["sha256"] == "existing-incumbent"
    assert (out / "report.md").is_file()
