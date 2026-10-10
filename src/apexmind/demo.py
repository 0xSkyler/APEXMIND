"""Deterministic synthetic fixture. Output is NEVER evidence of market alpha."""

from dataclasses import asdict
import math
from pathlib import Path
import numpy as np
from .config import Config
from .dataset import build_dataset
from .evaluation import research
from .feeds import Instrument
from .latency import Latency
from .store import Event, Store


def generate(store, seconds=180, seed=42):
    rng = np.random.default_rng(seed)
    wall = 1_700_000_000_000_000_000
    mono = 10_000_000_000
    symbols = {"BTC": 30000.0, "ETH": 2000.0}
    instruments = [
        Instrument(s, i, s + "USDT", "0.0001", "1.0", 5, 2) for i, s in enumerate(symbols)
    ]
    store.set("instruments", [asdict(i) for i in instruments])
    reference, lighter = dict(symbols), dict(symbols)
    for tick in range(seconds * 10):
        now = mono + tick * 100_000_000
        for symbol_index, symbol in enumerate(symbols):
            innovation = rng.normal(0, 0.00008)
            reference[symbol] *= math.exp(innovation)
            # An artificial lag is deliberately engineered for software testing.
            lighter[symbol] += (reference[symbol] - lighter[symbol]) * 0.35
            for j, (venue, mid) in enumerate(
                (("reference", reference[symbol]), ("lighter", lighter[symbol]))
            ):
                offset = symbol_index * 10_000_000 + j * 1_000_000

                def event(kind, payload, venue=venue, symbol=symbol, now=now, offset=offset):
                    store.append(
                        Event(
                            "synthetic-fixture",
                            venue,
                            symbol,
                            kind,
                            wall + (now - mono) + offset,
                            now + offset,
                            wall + (now - mono) + offset - 5_000_000,
                            {**payload, "synthetic": True, "clock_healthy": True},
                        )
                    )

                if tick % 100 == 0 and venue == "lighter":
                    event("funding", {"rate_bps_hour": 0.5})
                event(
                    "trade",
                    {
                        "size": float(rng.uniform(0.01, 0.1)),
                        "side": 1 if innovation > 0 else -1,
                        "price": mid,
                    },
                )
                spread = mid * 0.00004
                event(
                    "book",
                    {
                        "bids": [
                            (mid - spread - k * spread, float(rng.uniform(2, 4))) for k in range(10)
                        ],
                        "asks": [
                            (mid + spread + k * spread, float(rng.uniform(2, 4))) for k in range(10)
                        ],
                        "sequence": tick + 1,
                        "sequence_verified": True,
                    },
                )
    return store.summary()


def demo(config: Config, out, seconds=180):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    db_path = out / "synthetic.sqlite"
    if db_path.exists():
        raise ValueError("Demo output database already exists; use a new output directory")
    store = Store(db_path)
    try:
        generate(store, seconds, config.research.seed)
        latency = Latency(
            tuple(np.linspace(20, 80, 30)),
            tuple(np.linspace(20, 80, 30)),
            "SYNTHETIC_TEST_FIXTURE",
            "synthetic",
            0,
            True,
        )
        dataset_path = out / "synthetic.npz"
        build_dataset(store, config, latency, str(dataset_path))
    finally:
        store.close()
    result = research(dataset_path, config, out / "evaluation")
    return {
        **result,
        "warning": "Synthetic fixture validates software only; no real trading history",
    }
