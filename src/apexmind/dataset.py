from collections import defaultdict
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import numpy as np
from .book import BookError, vwap
from .config import Config
from .features import FEATURES, FeatureEngine
from .latency import Latency


def build_dataset(store, config: Config, latency: Latency, out: str, after_wall_ns=None):
    rng = np.random.default_rng(config.research.seed)
    engine = FeatureEngine(config.feeds, config.research.notional, np.median(latency.entry_ms))
    books, decisions, rejected = defaultdict(list), [], defaultdict(int)
    delays, clock_steps, unverified = [], 0, 0
    first_wall, last_wall, synthetic_events = None, None, False
    excluded = 0
    for event in store.events(limit=config.research.max_events):
        if event.kind == "raw":
            continue
        if after_wall_ns is not None and event.wall_ns <= after_wall_ns:
            excluded += 1
            continue
        first_wall = event.wall_ns if first_wall is None else min(first_wall, event.wall_ns)
        last_wall = event.wall_ns if last_wall is None else max(last_wall, event.wall_ns)
        synthetic_events |= bool(event.payload.get("synthetic", False))
        if event.kind == "book":
            if event.venue == "lighter":
                books[(event.run, event.symbol)].append(event)
            if event.exchange_ns:
                delays.append((event.wall_ns - event.exchange_ns) / 1e6)
            clock_steps += not event.payload.get("clock_healthy", True)
            unverified += not event.payload.get("sequence_verified", False)
        decision = engine.update(event)
        if decision:
            decisions.append(decision)
    if first_wall is None:
        raise ValueError("No market data; collect or explicitly run the synthetic demo")
    if not latency.synthetic and latency.measured_until_wall_ns >= first_wall:
        raise ValueError("Latency measurements must precede the research data to avoid leakage")
    indices = {
        key: np.array([e.mono_ns for e in events], dtype=np.int64) for key, events in books.items()
    }
    rows = []
    maxgap = config.research.max_execution_gap_ms * 1e6
    for d in decisions:
        key = (d.run, d.symbol)
        events, times = books[key], indices[key]
        entry_delay = float(rng.choice(latency.entry_ms))
        exit_delay = float(rng.choice(latency.exit_ms))
        entry_target = d.mono_ns + int(entry_delay * 1e6)
        entry_index = int(np.searchsorted(times, entry_target))
        if entry_index >= len(events) or times[entry_index] - entry_target > maxgap:
            rejected["entry_feed_gap"] += 2
            continue
        entry = events[entry_index]
        exit_target = entry.mono_ns + int(
            (config.research.holding_seconds * 1000 + exit_delay) * 1e6
        )
        exit_index = int(np.searchsorted(times, exit_target))
        if exit_index >= len(events) or times[exit_index] - exit_target > maxgap:
            rejected["exit_feed_gap"] += 2
            continue
        exit_event = events[exit_index]
        path = events[entry_index : exit_index + 1]
        if any(not e.payload.get("clock_healthy", True) for e in path) or any(
            b.mono_ns - a.mono_ns > maxgap for a, b in zip(path, path[1:], strict=False)
        ):
            rejected["holding_feed_gap_or_clock_step"] += 2
            continue
        for side in (1, -1):
            try:
                top = entry.payload["asks"][0][0] if side == 1 else entry.payload["bids"][0][0]
                quantity = config.research.notional / top
                ep = vwap(entry.payload["asks" if side == 1 else "bids"], quantity)
                xp = vwap(exit_event.payload["bids" if side == 1 else "asks"], quantity)
                paths = [vwap(e.payload["bids" if side == 1 else "asks"], quantity) for e in path]
            except BookError:
                rejected["insufficient_depth"] += 1
                continue
            gross = side * (xp / ep - 1) * 1e4
            hold = (exit_event.mono_ns - entry.mono_ns) / 1e9
            funding = (
                max(abs(d.funding_bps_hour), config.research.funding_reserve_bps_per_hour)
                * hold
                / 3600
            )
            fees = config.research.taker_fee_bps * (1 + xp / ep)
            slip = 2 * config.research.extra_slippage_bps
            net = gross - fees - funding - slip
            mae = max(0.0, -min(side * (price / ep - 1) * 1e4 for price in paths))
            rows.append(
                (
                    d.x + [side],
                    net,
                    gross,
                    funding,
                    fees,
                    slip,
                    mae,
                    (abs(ep / top - 1) * 1e4),
                    d.wall_ns,
                    d.mono_ns,
                    exit_event.mono_ns,
                    d.symbol,
                    d.run,
                    side,
                    entry_delay,
                    hold,
                    ep,
                    xp,
                    d.wall_ns + exit_event.mono_ns - d.mono_ns,
                    (d.book["bids"][0][0] + d.book["asks"][0][0]) / 2,
                    max(0.0, side * (ep / d.book["asks" if side == 1 else "bids"][0][0] - 1) * 1e4),
                )
            )
    if not rows:
        raise ValueError(
            "No execution-conditioned labels; inspect freshness, funding, coverage and delays"
        )
    names = [
        "x",
        "net_bps",
        "gross_bps",
        "funding_bps",
        "fee_bps",
        "slippage_reserve_bps",
        "mae_bps",
        "entry_impact_bps",
        "wall_ns",
        "decision_ns",
        "label_end_ns",
        "symbol",
        "run",
        "side",
        "entry_delay_ms",
        "holding_seconds",
        "entry_price",
        "exit_price",
        "label_end_wall_ns",
        "decision_price",
        "entry_slippage_bps",
    ]
    arrays = {name: np.array([r[i] for r in rows]) for i, name in enumerate(names)}
    metadata = {
        "schema": 1,
        "synthetic": latency.synthetic or synthetic_events,
        "data_period_wall_ns": [first_wall, last_wall],
        "excluded_pre_measurement_events": excluded,
        "symbols": sorted(set(arrays["symbol"])),
        "runs": sorted(set(arrays["run"])),
        "features": FEATURES + ["side"],
        "instruments": store.get("instruments", []),
        "total_decisions": len(decisions),
        "total_tested_sides": len(decisions) * 2,
        "labelled_sides": len(rows),
        "rejected_labels": dict(rejected),
        "latency": latency.summary(),
        "config": asdict(config),
        "clock_step_book_events": int(clock_steps),
        "unverified_sequence_book_events": int(unverified),
        "feed_delay_ms": {
            "n": len(delays),
            "p95": float(np.quantile(delays, 0.95)) if delays else None,
        },
        "assumptions": [
            "Aggressive VWAP against captured future depth; no passive fills",
            "Full fills only; hidden liquidity and queue position unavailable",
            "Explicit account fee assumption and extra slippage reserve",
            "Conservative prorated funding reserve, not settled account funding",
            "USDT/USDC quote basis is unhedged",
            "Exchange clock offset is unknown; feed delay includes clock error",
        ],
    }
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(out, **arrays, metadata=json.dumps(metadata, allow_nan=False))
    return {**metadata, "sha256": hashlib.sha256(Path(out).read_bytes()).hexdigest()}


def load_dataset(path):
    with np.load(path, allow_pickle=False) as data:
        arrays = {k: data[k] for k in data.files if k != "metadata"}
        metadata = json.loads(str(data["metadata"]))
    order = np.argsort(arrays["wall_ns"], kind="stable")
    return {k: v[order] for k, v in arrays.items()}, metadata
