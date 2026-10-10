from collections import Counter, defaultdict
from dataclasses import asdict
import hashlib
import importlib.metadata
import json
from pathlib import Path
import sys
import joblib
import numpy as np
from . import __version__
from .config import Config
from .dataset import load_dataset
from .models import MODELS, ForecastModel, block_mean_ci
from .risk import Portfolio


def chronological_folds(data, config):
    times = np.unique(data["wall_ns"])
    if len(times) < config.research.min_train_rows:
        raise ValueError(
            "Insufficient independent decision timestamps for chronological evaluation"
        )
    boundaries = np.linspace(0.6, 1, config.research.folds + 1)
    for left, right in zip(boundaries, boundaries[1:], strict=False):
        test_start = times[min(len(times) - 1, int(len(times) * left))]
        test_end = (
            times[min(len(times) - 1, int(len(times) * right))] if right < 1 else times[-1] + 1
        )
        val_start = times[int(len(times) * left * 0.8)]
        train = np.flatnonzero(data["label_end_wall_ns"] < val_start)
        val = np.flatnonzero(
            (data["wall_ns"] >= val_start) & (data["label_end_wall_ns"] < test_start)
        )
        test = np.flatnonzero(
            (data["wall_ns"] >= test_start) & (data["label_end_wall_ns"] < test_end)
        )
        if min(len(train), len(val), len(test)) < 20:
            raise ValueError("A purged fold is too small; collect more data")
        yield train, val, test


def subset(data, indices):
    return {k: v[indices] for k, v in data.items()}


def metrics(trades, initial_balance, final_equity, peak_drawdown):
    returns = np.array([t["net_bps"] for t in trades])
    pnl = np.array([t["pnl"] for t in trades])
    gains, losses = float(pnl[pnl > 0].sum()), float(-pnl[pnl < 0].sum())
    tail = returns[returns <= np.quantile(returns, 0.05)] if len(returns) else np.array([])
    return {
        "executed": len(trades),
        "gross_pnl": sum(t["gross_pnl"] for t in trades),
        "net_pnl": float(pnl.sum()),
        "net_return_fraction": (final_equity / initial_balance - 1),
        "final_equity": final_equity,
        "profit_factor": gains / losses if losses > 0 else None,
        "expectancy_bps": float(returns.mean()) if len(returns) else None,
        "expectancy_ci_bps": block_mean_ci(returns),
        "maximum_drawdown_fraction": peak_drawdown,
        "expected_shortfall_95_bps": float(tail.mean()) if len(tail) else None,
        "fee_cost": sum(t["fee_cost"] for t in trades),
        "funding_reserve_cost": sum(t["funding_cost"] for t in trades),
        "extra_slippage_reserve_cost": sum(t["slippage_cost"] for t in trades),
    }


def replay(data, predictions, config, instruments, threshold, store=None):
    portfolio = Portfolio(config.risk, store)
    # Paper ledger transitions below are committed atomically with portfolio
    # state. Prevent intermediate automatic saves from splitting a transaction.
    portfolio.store = None
    initial_balance = portfolio.equity
    rejection, trades = Counter(), []
    active = {s: p["record"] for s, p in portfolio.positions.items()}
    maximum_drawdown = 0.0
    watermark = store.get("paper_watermark_ns", 0) if store else 0

    def settle(now):
        nonlocal maximum_drawdown
        for symbol, record in list(active.items()):
            if record["end"] > now:
                continue
            pnl = portfolio.close(symbol, record["net_bps"])
            record["pnl"] = pnl
            trades.append(record)
            if store:
                store.paper_close(record, portfolio.snapshot())
            active.pop(symbol)
            maximum_drawdown = max(maximum_drawdown, 1 - portfolio.equity / portfolio.peak)

    # Decide between both sides using predictions. Never use realized labels to
    # choose direction. Competing symbols are ranked by predicted portfolio edge.
    for now in np.unique(data["wall_ns"]):
        if int(now) <= watermark:
            continue
        settle(int(now))
        indices = np.flatnonzero(data["wall_ns"] == now)
        ranking = sorted(indices, key=lambda i: predictions["lower_mean"][i], reverse=True)
        considered = set()
        for i in ranking:
            symbol = str(data["symbol"][i])
            if symbol in considered:
                rejection["opposite_side"] += 1
                continue
            considered.add(symbol)
            forecast = {k: float(v[i]) for k, v in predictions.items()}
            if forecast["lower_mean"] <= threshold:
                rejection["learned_threshold"] += 1
                continue
            instrument = instruments.get(symbol)
            if instrument is None:
                rejection["missing_market_rules"] += 1
                continue
            # Use observation-time book mid from features only indirectly: the
            # immutable dataset stores decision top; future entry is never used
            # for sizing. Research notional caps label scaling conservatively.
            price = float(data["decision_price"][i])
            allocation = portfolio.allocate(
                symbol, price, instrument, forecast, config.research.notional
            )
            if not allocation.accepted:
                rejection[allocation.reason] += 1
                continue
            client_id = hashlib.sha256(
                f"paper:{data['run'][i]}:{now}:{symbol}".encode()
            ).hexdigest()[:24]
            notional = allocation.notional
            record = {
                "client_id": client_id,
                "symbol": symbol,
                "side": int(data["side"][i]),
                "start": int(now),
                "end": int(data["label_end_wall_ns"][i]),
                "net_bps": float(data["net_bps"][i]),
                "notional": notional,
                "gross_pnl": notional * float(data["gross_bps"][i]) / 1e4,
                "fee_cost": notional * float(data["fee_bps"][i]) / 1e4,
                "funding_cost": notional * float(data["funding_bps"][i]) / 1e4,
                "slippage_cost": notional * float(data["slippage_reserve_bps"][i]) / 1e4,
                "regime": int(data["x"][i, -2]),
                "forecast": forecast,
                "mode": "hypothetical_depth_fill",
            }
            portfolio.open(symbol, allocation, {"record": record})
            if store and not store.paper_entry(record, portfolio.snapshot()):
                portfolio.positions.pop(symbol)
                rejection["already_processed"] += 1
                continue
            active[symbol] = record
        if store:
            store.set("paper_watermark_ns", int(now))
    settle(2**63 - 1)
    by_regime = defaultdict(list)
    for trade in trades:
        by_regime[str(trade["regime"])].append(trade["net_bps"])
    return {
        **metrics(trades, initial_balance, portfolio.equity, maximum_drawdown),
        "tested_sides": len(data["x"]),
        "rejected": dict(rejection),
        "regime_breakdown": {
            k: {"trades": len(v), "mean_net_bps": float(np.mean(v))} for k, v in by_regime.items()
        },
        "trades": trades,
        "drawdown_basis": "realized equity at closes; intratrade drawdown is not measured",
    }


def train_threshold(model, data, config, instruments):
    predictions = model.predict(data["x"])
    finite = predictions["lower_mean"][np.isfinite(predictions["lower_mean"])]
    candidates = sorted(
        set([config.risk.minimum_edge_bps] + list(np.quantile(finite, [0.25, 0.5, 0.75, 0.9])))
    )
    best = (0.0, float("inf"))
    for threshold in candidates:
        threshold = max(config.risk.minimum_edge_bps, float(threshold))
        result = replay(data, predictions, config, instruments, threshold)
        # Net contribution learned on validation, with at least five fills.
        score = result["net_pnl"] if result["executed"] >= 5 else -float("inf")
        if score > best[0]:
            best = (score, threshold)
    model.threshold = best[1]
    return model.threshold


def research(dataset_path, config: Config, out):
    data, metadata = load_dataset(dataset_path)
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    instruments = {i["symbol"]: i for i in metadata["instruments"]}
    results, artifacts, champions = [], {}, []
    for fold, (train, val, test) in enumerate(chronological_folds(data, config)):
        td, vd, od = subset(data, train), subset(data, val), subset(data, test)
        validation_scores = {}
        validation_curves = {}
        fold_results = []
        for name in MODELS:
            model = ForecastModel(
                name,
                config.research.seed,
                config.research.max_features,
                config.risk.max_slippage_bps,
            ).fit(td["x"], td)
            model.calibrate(vd["x"], vd["net_bps"], config.research.bootstrap_samples)
            model.calibrated_until_wall_ns = int(vd["label_end_wall_ns"].max())
            threshold = train_threshold(model, vd, config, instruments)
            validation = replay(vd, model.predict(vd["x"]), config, instruments, threshold)
            result = replay(od, model.predict(od["x"]), config, instruments, threshold)
            scores = {
                "fold": fold,
                "model": name,
                "train_rows": len(train),
                "validation_rows": len(val),
                "test_rows": len(test),
                "train_label_end_max": int(td["label_end_wall_ns"].max()),
                "validation_period_wall_ns": [int(vd["wall_ns"].min()), int(vd["wall_ns"].max())],
                "test_period_wall_ns": [int(od["wall_ns"].min()), int(od["wall_ns"].max())],
                "selected_features": model.selected_names(),
                "entry_threshold_bps": threshold if np.isfinite(threshold) else None,
                **result,
            }
            fold_results.append(scores)
            validation_scores[name] = validation["net_pnl"]
            profits = defaultdict(float)
            for trade in validation["trades"]:
                profits[trade["start"]] += trade["pnl"]
            validation_curves[name] = np.array([profits[int(t)] for t in np.unique(vd["wall_ns"])])
            artifacts[name] = model
        selected = max(MODELS, key=lambda n: validation_scores[n])
        if selected in MODELS[4:]:
            simple = max(MODELS[:4], key=lambda n: validation_scores[n])
            difference = validation_curves[selected] - validation_curves[simple]
            interval = block_mean_ci(
                difference,
                config.research.seed,
                config.research.bootstrap_samples,
                alpha=0.05 / len(MODELS),
            )
            if interval[0] is None or interval[0] <= 0:
                selected = simple
        champions.append(selected)
        for row in fold_results:
            row["selected_champion"] = row["model"] == selected
            results.append(row)
    # Select separately at every boundary, using only preceding validation.
    # Never use later folds to select a model retrospectively for earlier tests.
    champion_name = champions[-1]
    model_path = out / "champion.joblib"
    joblib.dump(artifacts[champion_name], model_path)
    totals = {name: sum(r["net_pnl"] for r in results if r["model"] == name) for name in MODELS}
    baselines = MODELS[:4]
    champion_results = [r for r in results if r["selected_champion"]]
    champion_trades = [t for r in champion_results for t in r["trades"]]
    confidence = block_mean_ci(
        [t["net_bps"] for t in champion_trades],
        config.research.seed,
        config.research.bootstrap_samples,
        alpha=0.05 / len(MODELS),
    )
    selected_pnl = sum(r["net_pnl"] for r in champion_results)
    baseline_pnl = max(totals[n] for n in baselines)
    beats_baselines = selected_pnl > baseline_pnl or (
        champion_name in baselines and selected_pnl > 0 and selected_pnl == baseline_pnl
    )
    positive_periods = sum(r["net_pnl"] > 0 for r in champion_results)
    report = {
        "schema": 1,
        "status": "SYNTHETIC_SOFTWARE_VALIDATION" if metadata["synthetic"] else "RESEARCH_ONLY",
        "profitability_proven": False,
        "live_deployment_allowed": False,
        "dataset_sha256": hashlib.sha256(Path(dataset_path).read_bytes()).hexdigest(),
        "model_sha256": hashlib.sha256(model_path.read_bytes()).hexdigest(),
        "code_version": __version__,
        "source_manifest_sha256": hashlib.sha256(
            b"".join(
                f.name.encode() + b"\0" + f.read_bytes()
                for f in sorted(Path(__file__).parent.glob("*.py"))
            )
        ).hexdigest(),
        "python_version": sys.version.split()[0],
        "dependency_versions": {
            name: importlib.metadata.version(name)
            for name in ("numpy", "scipy", "scikit-learn", "joblib", "aiohttp")
        },
        "metadata": metadata,
        "champion": champion_name,
        "evaluation_config": asdict(config),
        "champions_by_period": champions,
        "sequential_champion_net_pnl": selected_pnl,
        "selection_basis": "preceding validation PnL; complex models require positive paired bootstrap advantage",
        "model_net_pnl": totals,
        "beats_simple_baselines_after_costs": beats_baselines,
        "champion_expectancy_ci_bps_family_adjusted": confidence,
        "positive_out_of_sample_periods": positive_periods,
        "tested_model_configurations": len(results),
        "out_of_sample_periods": config.research.folds,
        "results": results,
        "deployment_blockers": [
            "No authorized account integration evidence",
            "No reconciled live execution or margin protection evidence",
            "Funding and account-tier fees remain assumptions",
            "Exchange clock synchronization and Lighter sequence contract require validation",
            "Backtesting alone cannot establish profitability",
        ],
    }
    if metadata["synthetic"]:
        report["deployment_blockers"].insert(0, "Synthetic market data and synthetic latency")
    if confidence[0] is None or confidence[0] <= 0:
        report["deployment_blockers"].append("Positive expectancy is not statistically supported")
    if not beats_baselines:
        report["deployment_blockers"].append("Champion does not outperform the simple baselines")
    if positive_periods < 2:
        report["deployment_blockers"].append("Insufficient positive separate out-of-sample periods")
    (out / "report.json").write_text(json.dumps(report, indent=2, allow_nan=False))
    lines = [
        "# APEX MIND execution-aware evaluation",
        "",
        f"Status: **{report['status']}**",
        "",
        "Profitability has not been established. Live deployment is blocked.",
        "",
        f"Dataset SHA256: `{report['dataset_sha256']}`",
        f"Champion: {champion_name}",
        f"Markets: {', '.join(metadata['symbols'])}",
        f"Data period (Unix nanoseconds): {metadata['data_period_wall_ns']}",
        f"Tested decision sides: {metadata['total_tested_sides']}; labelled: {metadata['labelled_sides']}",
        f"Execution delay: {metadata['latency']['source']}; median {metadata['latency']['entry_median_ms']:.2f} ms",
        "",
        "| Model | Hypothetical net PnL |",
        "|---|---:|",
    ]
    lines += [f"| {name} | {value:.6f} |" for name, value in totals.items()]
    lines += [
        "",
        "Each fold resets its portfolio; totals are sums of independent fold PnL.",
        "All models use identical chronological held-out periods. Labels are purged at boundaries.",
        "The champion was selected using validation scores. Trade histories below are hypothetical.",
        "Drawdown measures realized equity at closes; intratrade drawdown remains unmeasured.",
        "",
        "## Required evidence",
        "",
    ] + [f"- {b}" for b in report["deployment_blockers"]]
    lines += ["", "## Fill and cost assumptions", ""] + [f"- {b}" for b in metadata["assumptions"]]
    (out / "report.md").write_text("\n".join(lines) + "\n")
    return {
        "report": str(out / "report.json"),
        "champion": champion_name,
        "status": report["status"],
        "model_net_pnl": totals,
        "profitability_proven": False,
        "live_deployment_allowed": False,
    }
