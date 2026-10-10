"""Promotion evidence is derived from reconciled records, never toggle flags."""

import hashlib
import json
import math
from pathlib import Path
import time
import numpy as np

from .models import block_mean_ci


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def source_digest():
    return hashlib.sha256(
        b"".join(
            f.name.encode() + b"\0" + f.read_bytes()
            for f in sorted(Path(__file__).parent.glob("*.py"))
        )
    ).hexdigest()


def research_gate(report, model_path, config=None):
    reasons = []
    metadata = report["metadata"]
    if metadata["synthetic"] or metadata["latency"]["synthetic"]:
        reasons.append("synthetic data")
    if digest(model_path) != report["model_sha256"]:
        reasons.append("model artifact hash mismatch")
    if report.get("source_manifest_sha256") != source_digest():
        reasons.append("research must be reproduced with this source version")
    if not report.get("beats_simple_baselines_after_costs"):
        reasons.append("no measured advantage over baselines")
    ci = report.get("champion_expectancy_ci_bps_family_adjusted", [None, None])
    if ci[0] is None or not math.isfinite(ci[0]) or ci[0] <= 0:
        reasons.append("no positive uncertainty-adjusted expectancy")
    if report.get("positive_out_of_sample_periods", 0) < 2:
        reasons.append("insufficient separate positive held-out periods")
    # A sequential selection policy may use several families. The loaded final
    # family must also have its own positive independent held-out observations.
    rows = [r for r in report["results"] if r["model"] == report["champion"]]
    if sum(r["net_pnl"] > 0 for r in rows) < 2:
        reasons.append("loaded model lacks separate positive held-out periods")
    interval = block_mean_ci(
        [t["net_bps"] for r in rows for t in r["trades"]], samples=1000, alpha=0.05 / 6
    )
    if interval[0] is None or interval[0] <= 0:
        reasons.append("loaded model has no positive adjusted expectancy")
    for field, reason in (
        ("clock_step_book_events", "clock discontinuities in research data"),
        ("unverified_sequence_book_events", "unverified book continuity"),
    ):
        if metadata.get(field):
            reasons.append(reason)
    if config:
        old = report["evaluation_config"]
        for name in (
            "reference_venue",
            "reference_rest",
            "reference_ws",
            "lighter_rest",
            "lighter_ws",
        ):
            if getattr(config.feeds, name) != old["feeds"][name]:
                reasons.append(f"evaluated market source changed: {name}")
        if set(config.feeds.symbols) - set(metadata["symbols"]) and config.feeds.symbols != ["ALL"]:
            reasons.append("requested market was not evaluated")
        for name in (
            "max_gross_leverage",
            "max_loss_fraction",
            "max_market_fraction",
            "max_drawdown_fraction",
            "max_slippage_bps",
        ):
            if getattr(config.risk, name) > old["risk"][name]:
                reasons.append(f"risk policy exceeds evaluated {name}")
        if config.risk.margin_reserve_fraction < old["risk"]["margin_reserve_fraction"]:
            reasons.append("margin reserve decreased after evaluation")
        for name in ("taker_fee_bps", "funding_reserve_bps_per_hour", "extra_slippage_bps"):
            if getattr(config.research, name) != old["research"][name]:
                reasons.append(f"cost assumptions changed: {name}")
    return {"allowed": not reasons, "reasons": reasons}


def forward_metrics(episodes, model_hash, minimum=20):
    selected = [
        e
        for e in episodes
        if e["model_sha256"] == model_hash and not e["integration"] and e.get("reconciled")
    ]
    values = [e["net_bps"] for e in selected]
    ci = block_mean_ci(values, samples=2000, alpha=0.01)
    days = {e["end_wall_ns"] // (86400 * 10**9) for e in selected}
    gains = sum(max(0, e["net_pnl"]) for e in selected)
    losses = -sum(min(0, e["net_pnl"]) for e in selected)
    allowed = len(selected) >= minimum and len(days) >= 2 and ci[0] is not None and ci[0] > 0
    return {
        "allowed": allowed,
        "episodes": len(selected),
        "separate_days": len(days),
        "net_pnl": sum(e["net_pnl"] for e in selected),
        "expectancy_ci_bps_99pct": ci,
        "profit_factor": gains / losses if losses else None,
        "expected_shortfall_5pct_bps": float(
            np.mean(sorted(values)[: max(1, math.ceil(len(values) * 0.05))])
        )
        if values
        else None,
        "source": "actual portfolio flat-to-flat cash; includes fees and funding",
        "profitability_proven": allowed,
    }


def integration_summary(store, config):
    records = store.orders()
    episodes = [e for e in store.episodes() if e["integration"] and e.get("reconciled")]
    entries = [
        r
        for r in records
        if r["payload"].get("integration")
        and r["payload"]["role"] == "entry"
        and r["status"] in ("filled", "cancelled")
        and r["payload"].get("filled_quantity", 0) > 0
    ]
    stops = [
        r
        for r in records
        if r["payload"]["role"] == "stop"
        and r["payload"].get("integration")
        and r["payload"].get("filled_quantity", 0) > 0
    ]
    exits = [
        r
        for r in records
        if r["payload"]["role"] == "exit"
        and r["payload"].get("exchange", {}).get("reduce_only")
        and r["payload"].get("filled_quantity", 0) > 0
    ]
    cancelled = [
        r
        for r in records
        if r["payload"].get("probe")
        and r["payload"]["role"] == "entry"
        and r["status"] == "cancelled"
    ]
    delays = {
        role: [
            r["payload"]["observed_fill_delay_ms"]
            for r in records
            if r["payload"]["role"] == role
            and r["payload"].get("integration")
            and not r["payload"].get("probe")
            and "observed_fill_delay_ms" in r["payload"]
        ]
        for role in ("entry", "exit")
    }
    reasons = []
    for present, reason in (
        (entries, "no verified entry fill"),
        (stops, "no native protective stop fill"),
        (exits, "no reduce-only exit fill"),
        (cancelled, "no native cancel lifecycle"),
        (episodes, "no reconciled all-in costs"),
    ):
        if not present:
            reasons.append(reason)
    if min(map(len, delays.values())) < 20:
        reasons.append("at least 20 measured entry and exit delays required")
    if store.pending_transactions():
        reasons.append("transactions await reconciliation")
    proof = store.get("native_integration_proof")
    if not proof or proof.get("source_sha256") != source_digest():
        reasons.append("current-source integration procedure has not completed")
    if proof and proof["verified_wall_ns"] > time.time_ns():
        reasons.append("integration timestamp is in the future")
    # Native lifecycle qualification survives restarts for this source/account.
    # Current authorization, fee schedule, clock, books, margin, and observed
    # performance are rechecked during operation rather than assumed timeless.
    result = {
        "allowed": not reasons,
        "reasons": reasons,
        "entry_orders": [r["client_id"] for r in entries],
        "stop_orders": [r["client_id"] for r in stops],
        "exit_orders": [r["client_id"] for r in exits],
        "cancel_orders": [r["client_id"] for r in cancelled],
        "delays_ms": delays,
        "observed_all_in_cost_bps_per_fill": max(
            (e["all_in_cost_bps_per_fill"] for e in episodes), default=None
        ),
    }
    if proof:
        result["proof"] = proof
    return result


def runtime_gate(report, model_path, config, store, snapshot, limits, latency, stage="production"):
    result = research_gate(report, model_path, config)
    reasons = list(result["reasons"])
    integration = integration_summary(store, config)
    reasons.extend(integration["reasons"])
    if latency.synthetic or latency.account_tier != snapshot.tier:
        reasons.append("latency does not match the actual account tier")
    if report["metadata"]["latency"] != latency.summary():
        reasons.append("research latency differs from loaded observations")
    proof = integration.get("proof", {})
    fee_fingerprint = {
        k: limits[k] for k in ("user_tier", "current_maker_fee_tick", "current_taker_fee_tick")
    }
    if proof.get("fee_fingerprint") != fee_fingerprint:
        reasons.append("account-tier fee schedule changed or is unverified")
    cost = integration["observed_all_in_cost_bps_per_fill"]
    if cost is None or report["evaluation_config"]["research"]["taker_fee_bps"] < cost:
        reasons.append("research fees are below observed all-in execution costs")
    if proof.get("account_index") != snapshot.account_index:
        reasons.append("integration account mismatch")
    forward = forward_metrics(
        store.episodes(), report["model_sha256"], config.live.minimum_forward_episodes
    )
    if stage == "production" and not forward["allowed"]:
        reasons.append("actual forward advantage has not been established")
    if stage not in ("pilot", "production"):
        reasons.append("unknown deployment stage")
    return {
        "allowed": not reasons,
        "reasons": list(dict.fromkeys(reasons)),
        "stage": stage,
        "forward": forward,
        "integration": integration,
    }


def export_latency(store, path, tier):
    from .latency import Latency

    records = store.orders()
    delays = {
        role: tuple(
            r["payload"]["observed_fill_delay_ms"]
            for r in records
            if r["payload"]["role"] == role
            and r["payload"].get("integration")
            and not r["payload"].get("probe")
            and "observed_fill_delay_ms" in r["payload"]
        )
        for role in ("entry", "exit")
    }
    end = max(r["payload"].get("first_fill_wall_ns", 0) for r in records)
    latency = Latency(delays["entry"], delays["exit"], "exchange-reconciled-live-ledger", tier, end)
    payload = {**latency.summary(), "entry_ms": delays["entry"], "exit_ms": delays["exit"]}
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(payload, indent=2))
    return latency.summary()
