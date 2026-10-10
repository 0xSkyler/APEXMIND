"""Descriptive actual execution records; no inference from acknowledgements."""

from collections import Counter
import numpy as np
from .account import number


def quantiles(values):
    return {
        "n": len(values),
        "median": float(np.median(values)) if values else None,
        "p95": float(np.quantile(values, 0.95)) if values else None,
    }


def execution_metrics(store):
    records, fills = store.orders(), store.fills()
    roles = ("entry", "exit", "stop")
    slippage = {role: [] for role in roles}
    delays = {role: [] for role in ("entry", "exit")}
    for r in records:
        p, role = r["payload"], r["payload"]["role"]
        selected = [f for f in fills if f["client_id"] == r["client_id"]]
        if selected and role in roles and p.get("expected_price"):
            quantity = sum(number(f["quantity"]) for f in selected)
            price = sum(number(f["quantity"]) * number(f["price"]) for f in selected) / quantity
            slippage[role].append(p["side"] * (price / p["expected_price"] - 1) * 1e4)
        if role in delays and "observed_fill_delay_ms" in p:
            delays[role].append(p["observed_fill_delay_ms"])
    episodes = store.episodes()
    return {
        "source": "exchange-reconciled records; integration probes included",
        "order_status_counts": dict(Counter(r["status"] for r in records)),
        "fill_count": len(fills),
        "slippage_bps": {k: quantiles(v) for k, v in slippage.items()},
        "submit_to_first_observed_fill_ms": {k: quantiles(v) for k, v in delays.items()},
        "latency_scope": "same-process observations; includes polling/queue uncertainty",
        "maximum_observed_mark_equity_drawdown": store.get("live_maximum_drawdown"),
        "drawdown_scope": "authoritative polling observations; intra-poll extrema are not measured",
        "flat_to_flat_episodes": len(episodes),
        "gross_pnl": sum(e["gross_pnl"] for e in episodes),
        "net_cash_pnl": sum(e["net_pnl"] for e in episodes),
        "combined_cost_and_funding_debit": sum(e["gross_pnl"] - e["net_pnl"] for e in episodes),
        "funding_events": [f for e in episodes for f in e["funding_events"]],
        "external_capital_changes": store.get("external_capital_changes", []),
    }
