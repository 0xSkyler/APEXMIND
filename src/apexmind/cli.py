import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import logging
import os
import sys
import time
import aiohttp
import joblib
import numpy as np
from threadpoolctl import threadpool_limits
from .config import load_config
from .dataset import build_dataset, load_dataset
from .demo import demo
from .evaluation import replay, research, subset
from .execution import credential_presence
from .feeds import Collector, discover
from .governance import register_challenger, validate_evidence
from .lab import laboratory
from .latency import Latency, import_measurements
from .paper import PaperEngine
from .store import Store


def parser():
    p = argparse.ArgumentParser(
        description="APEX MIND execution-aware research (live trading gated)"
    )
    p.add_argument("--config", default=os.environ.get("APEX_CONFIG", "config/default.toml"))
    sub = p.add_subparsers(dest="command", required=True)
    doctor = sub.add_parser("doctor", help="Inspect local readiness and optional public API access")
    doctor.add_argument("--network", action="store_true")
    sub.add_parser("discover", help="Discover exact eligible market mappings")
    collector = sub.add_parser(
        "collect", help="Capture public feeds; indefinitely if no duration given"
    )
    collector.add_argument("--seconds", type=float)
    latency = sub.add_parser("import-latency", help="Import measured submit-to-fill observations")
    latency.add_argument("--csv", required=True)
    latency.add_argument("--tier", required=True)
    latency.add_argument("--out", required=True)
    dataset = sub.add_parser("dataset", help="Build causal features and delayed executable labels")
    dataset.add_argument("--latency", required=True)
    dataset.add_argument("--out", required=True)
    evaluation = sub.add_parser("research", help="Purged chronological model comparison")
    evaluation.add_argument("--dataset", required=True)
    evaluation.add_argument("--out", required=True)
    fixture = sub.add_parser(
        "demo", help="Synthetic end-to-end software validation; never alpha evidence"
    )
    fixture.add_argument("--out", required=True)
    fixture.add_argument("--seconds", type=int, default=180)
    paper = sub.add_parser("paper", help="Persist hypothetical out-of-sample portfolio replay")
    paper.add_argument("--dataset", required=True)
    paper.add_argument("--model", required=True)
    paper.add_argument("--ledger", default="data/paper.sqlite")
    forward = sub.add_parser(
        "paper-stream", help="Forward hypothetical execution on live public feeds"
    )
    forward.add_argument("--model", required=True)
    forward.add_argument("--report", required=True)
    forward.add_argument("--latency", required=True)
    forward.add_argument("--ledger", default="data/forward-paper.sqlite")
    forward.add_argument("--seconds", type=float)
    for name in ("register", "live-check"):
        command = sub.add_parser(name)
        command.add_argument("--report", required=True)
        command.add_argument("--model", required=True)
        if name == "register":
            command.add_argument("--registry", required=True)
        else:
            command.add_argument("--integration")
            command.add_argument(
                "--account", action="store_true", help="Verify against configured native account"
            )
            command.add_argument("--latency")
            command.add_argument("--stage", choices=("pilot", "production"), default="production")
    sub.add_parser("auth-check", help="Read-only account authentication, balances, and limits")
    native = sub.add_parser(
        "integration-test", help="User-run native orders and latency calibration; incurs costs"
    )
    native.add_argument("--symbol", required=True)
    native.add_argument("--notional", required=True, type=float)
    native.add_argument("--roundtrips", type=int, default=20)
    native.add_argument("--latency-out", default="data/latency.json")
    recovery = sub.add_parser(
        "recover", help="Cancel owned entries and reduce owned positions to flat"
    )
    recovery.add_argument("--timeout", type=float, default=120)
    sub.add_parser(
        "live-status", help="Read local runtime, executions, and actual cash performance"
    )
    live = sub.add_parser(
        "live", help="Continuous native trading with measured evidence and protection"
    )
    live.add_argument("--report", required=True)
    live.add_argument("--model", required=True)
    live.add_argument("--latency", required=True)
    live.add_argument("--stage", choices=("pilot", "production"), default="production")
    live.add_argument("--seconds", type=float)
    live.add_argument("--registry", help="Adopt qualified registry candidates only while flat")
    promote = sub.add_parser(
        "promote", help="Register a champion only after current account verification"
    )
    for field in ("report", "model", "latency", "registry"):
        promote.add_argument("--" + field, required=True)
    promote.add_argument("--stage", choices=("pilot", "production"), default="production")
    resume = sub.add_parser(
        "resume", help="Clear an operational suspension after flat account/evidence verification"
    )
    for field in ("report", "model", "latency"):
        resume.add_argument("--" + field, required=True)
    resume.add_argument("--stage", choices=("pilot", "production"), default="production")
    ui = sub.add_parser("ui", help="Local API setup with Save and guarded Start controls")
    ui.add_argument("--port", type=int, default=8765)
    ui.add_argument("--state-dir", default=os.environ.get("APEX_UI_STATE", "secrets/ui"))
    ui.add_argument(
        "--report", default=os.environ.get("APEX_REPORT", "reports/cvifa-001/report.json")
    )
    ui.add_argument(
        "--model", default=os.environ.get("APEX_MODEL", "reports/cvifa-001/champion.joblib")
    )
    ui.add_argument("--latency", default=os.environ.get("APEX_LATENCY", "data/latency.json"))
    ui.add_argument(
        "--registry", default=os.environ.get("APEX_REGISTRY", "artifacts/registry.json")
    )
    ui.add_argument(
        "--stage",
        choices=("pilot", "production"),
        default=os.environ.get("APEX_STAGE", "production"),
    )
    lab = sub.add_parser("lab", help="Bounded periodic champion-challenger research")
    lab.add_argument("--latency", required=True)
    lab.add_argument("--out", required=True)
    lab.add_argument("--interval", type=float, default=3600)
    lab.add_argument("--cycles", type=int)
    return p


async def run(args):
    config = load_config(args.config)
    if args.command == "ui":
        from .ui import Settings, serve

        await serve(
            Settings(
                args.config,
                args.state_dir,
                args.report,
                args.model,
                args.latency,
                args.stage,
                args.registry,
            ),
            args.port,
        )
        return {"ui": "stopped"}
    if args.command == "doctor":
        result = {
            "python": sys.version.split()[0],
            "config_valid": True,
            "credential_names_present": credential_presence(),
            "live_execution_available": True,
            "live_trading_enabled": False,
            "exchange_clock_sync": "unverified",
            "public_feeds": "unverified",
        }
        from .account import clock_health

        result["host_clock"] = clock_health(config.live.max_clock_error_ms)
        if args.network:
            result["instruments"] = [asdict(i) for i in await discover(config)]
            result["public_rest"] = "verified"
        return result
    if args.command == "discover":
        return [asdict(i) for i in await discover(config)]
    if args.command == "collect":
        if args.seconds is not None and args.seconds <= 0:
            raise ValueError("Collection duration must be positive")
        instruments = await discover(config)
        store = Store(config.storage.database)
        try:
            return await Collector(config, store, instruments).run(args.seconds)
        finally:
            store.close()
    if args.command == "import-latency":
        return import_measurements(args.csv, args.out, args.tier)
    if args.command == "dataset":
        latency = Latency.load(args.latency)
        store = Store(config.storage.database)
        try:
            return build_dataset(
                store,
                config,
                latency,
                args.out,
                after_wall_ns=latency.measured_until_wall_ns if not latency.synthetic else None,
            )
        finally:
            store.close()
    if args.command == "research":
        return research(args.dataset, config, args.out)
    if args.command == "demo":
        return demo(config, args.out, args.seconds)
    if args.command == "paper":
        # Load only trusted locally generated model artifacts. joblib/pickle
        # is not an interchange format for models from untrusted sources.
        model = joblib.load(args.model)
        data, metadata = load_dataset(args.dataset)
        selected = np.flatnonzero(data["wall_ns"] > model.calibrated_until_wall_ns)
        if not len(selected):
            raise ValueError("Paper dataset must include observations after model calibration")
        data = subset(data, selected)
        store = Store(args.ledger)
        try:
            result = replay(
                data,
                model.predict(data["x"]),
                config,
                {i["symbol"]: i for i in metadata["instruments"]},
                model.threshold,
                store,
            )
        finally:
            store.close()
        return {**result, "synthetic": metadata["synthetic"], "mode": "hypothetical_paper_replay"}
    if args.command == "paper-stream":
        from pathlib import Path

        report = json.loads(Path(args.report).read_text())
        if report["metadata"]["synthetic"]:
            raise ValueError("A synthetic model cannot start forward market research")
        if hashlib.sha256(Path(args.model).read_bytes()).hexdigest() != report["model_sha256"]:
            raise ValueError("Model hash does not match report")
        latency = Latency.load(args.latency)
        if latency.synthetic:
            raise ValueError("Forward research requires measured account-tier latency")
        instruments = await discover(config)
        store, ledger = Store(config.storage.database), Store(args.ledger)
        try:
            model = joblib.load(args.model)
            engine = PaperEngine(config, latency, model, ledger, [asdict(i) for i in instruments])
            result = await Collector(config, store, instruments, engine.on_event).run(args.seconds)
            return {
                **result,
                "paper_state": ledger.get("stream_paper"),
                "mode": "HYPOTHETICAL_FORWARD_PAPER",
            }
        finally:
            store.close()
            ledger.close()
    if args.command == "register":
        return register_challenger(args.report, args.model, args.registry)
    if args.command == "live-check":
        if args.account:
            if not args.latency:
                raise ValueError("--latency is required for native account verification")
            return await account_gate(config, args)
        return validate_evidence(args.report, args.model, args.integration)
    if args.command == "auth-check":
        from .integration import auth_check

        return await auth_check(config)
    if args.command == "integration-test":
        from .integration import integration_test

        return await integration_test(
            config, args.symbol, args.notional, args.roundtrips, args.latency_out
        )
    if args.command == "recover":
        from .integration import recover

        if args.timeout <= 0:
            raise ValueError("Recovery timeout must be positive")
        return await recover(config, args.timeout)
    if args.command == "live-status":
        from .evidence import forward_metrics
        from .performance import execution_metrics

        store = Store(config.live.ledger)
        try:
            status = store.get("live_status", {})
            models = {e["model_sha256"] for e in store.episodes() if not e["integration"]}
            return {
                "runtime": status,
                "execution": execution_metrics(store),
                "pending_transactions": store.pending_transactions(),
                "orders": store.orders(),
                "actual_episodes": store.episodes(),
                "performance": {
                    m: forward_metrics(store.episodes(), m, config.live.minimum_forward_episodes)
                    for m in sorted(models)
                },
            }
        finally:
            store.close()
    if args.command == "live":
        from .live import run_live

        return await run_live(
            config, args.report, args.model, args.latency, args.stage, args.seconds, args.registry
        )
    if args.command == "promote":
        from pathlib import Path

        result = await account_gate(config, args)
        if not result["allowed"]:
            raise RuntimeError("Promotion gate: " + "; ".join(result["reasons"]))
        report = json.loads(Path(args.report).read_text())
        registry = Path(args.registry)
        old = json.loads(registry.read_text()) if registry.exists() else {}
        candidate = {
            "report": str(Path(args.report).resolve()),
            "model": str(Path(args.model).resolve()),
            "sha256": report["model_sha256"],
            "created_ns": time.time_ns(),
            "champion": report["champion"],
            "validation": result,
        }
        old["champion"] = candidate
        registry.parent.mkdir(parents=True, exist_ok=True)
        temporary = registry.with_suffix(".tmp")
        temporary.write_text(json.dumps(old, indent=2))
        temporary.replace(registry)
        return candidate
    if args.command == "resume":
        from .integration import connected
        from .evidence import runtime_gate
        from pathlib import Path

        report = json.loads(Path(args.report).read_text())
        latency = Latency.load(args.latency)

        async def clear(broker, instruments):
            await broker.reconcile()
            if broker.snapshot.positions or broker.active or broker.store.pending_transactions():
                raise RuntimeError("Resume requires a reconciled flat account")
            gate = runtime_gate(
                report,
                args.model,
                config,
                broker.store,
                broker.snapshot,
                broker.gateway.limits,
                latency,
                args.stage,
            )
            if not gate["allowed"]:
                raise RuntimeError("Resume gate: " + "; ".join(gate["reasons"]))
            from .account import clock_health

            if not clock_health(config.live.max_clock_error_ms)["healthy"]:
                raise RuntimeError("Resume requires verified clock synchronization")
            if broker.store.get("live_suspension") == "live_advantage_disappeared":
                from .evidence import forward_metrics

                rows = [
                    e
                    for e in broker.store.episodes()
                    if not e["integration"] and e["model_sha256"] == report["model_sha256"]
                ][-config.live.retirement_window :]
                if (
                    rows
                    and not forward_metrics(
                        rows, report["model_sha256"], config.live.retirement_window
                    )["allowed"]
                ):
                    raise RuntimeError("Retired model cannot resume without new measured advantage")
            broker.store.set("live_suspension", None)
            broker.store.set("live_peak_equity", broker.snapshot.equity)
            broker.store.set(
                "last_resume",
                {
                    "wall_ns": time.time_ns(),
                    "model_sha256": report["model_sha256"],
                    "stage": args.stage,
                },
            )
            return {"resumed": True, "stage": args.stage, "model_sha256": report["model_sha256"]}

        return await connected(config, clear)
    if args.command == "lab":
        return await laboratory(args.config, args.latency, args.out, args.interval, args.cycles)
    raise ValueError("Unknown command")


async def account_gate(config, args):
    from pathlib import Path
    from .integration import connected
    from .evidence import runtime_gate
    from .account import clock_health

    report = json.loads(Path(args.report).read_text())
    latency = Latency.load(args.latency)

    async def check(broker, instruments):
        await broker.reconcile()
        result = runtime_gate(
            report,
            args.model,
            config,
            broker.store,
            broker.snapshot,
            broker.gateway.limits,
            latency,
            args.stage,
        )
        clock = clock_health(config.live.max_clock_error_ms)
        if not clock["healthy"]:
            result["allowed"] = False
            result["reasons"].append("host clock synchronization is unverified")
        from .broker import margin_reason

        margin = margin_reason(broker.snapshot, config, broker.store.get("live_peak_equity"))
        if margin:
            result["allowed"] = False
            result["reasons"].append(margin)
        if any(not broker.protected(symbol) for symbol in broker.snapshot.positions):
            result["allowed"] = False
            result["reasons"].append("native position protection is incomplete")
        result["clock"] = clock
        return result

    return await connected(config, check)


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = parser().parse_args()
    try:
        with threadpool_limits(limits=2):
            result = asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130
    except aiohttp.ClientError as error:
        status = getattr(error, "status", None)
        print(
            json.dumps(
                {
                    "error": type(error).__name__,
                    "status": status,
                    "action": "Verify allowed exchange domains and regional access in environment settings",
                }
            ),
            file=sys.stderr,
        )
        return 2
    except (ValueError, RuntimeError, FileNotFoundError, KeyError) as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, allow_nan=False))
    if args.command in ("live-check", "integration-test") and not result["allowed"]:
        return 3
    return 0
