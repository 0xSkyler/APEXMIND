from dataclasses import dataclass
import csv
import hashlib
import json
from pathlib import Path
import numpy as np


@dataclass(frozen=True)
class Latency:
    entry_ms: tuple[float, ...]
    exit_ms: tuple[float, ...]
    source: str
    account_tier: str
    measured_until_wall_ns: int
    synthetic: bool = False

    def __post_init__(self):
        for values in (self.entry_ms, self.exit_ms):
            if len(values) < 20 or not np.isfinite(values).all() or min(values) <= 0:
                raise ValueError(
                    "At least 20 positive, finite measured delays per order role required"
                )
        if not self.source or not self.account_tier:
            raise ValueError("Latency provenance and account tier are required")

    def summary(self):
        return {
            "source": self.source,
            "account_tier": self.account_tier,
            "measured_until_wall_ns": self.measured_until_wall_ns,
            "synthetic": self.synthetic,
            "entry_n": len(self.entry_ms),
            "exit_n": len(self.exit_ms),
            "entry_median_ms": float(np.median(self.entry_ms)),
            "entry_p95_ms": float(np.quantile(self.entry_ms, 0.95)),
            "exit_p95_ms": float(np.quantile(self.exit_ms, 0.95)),
        }

    @classmethod
    def load(cls, path):
        raw = json.loads(Path(path).read_text())
        return cls(
            tuple(raw["entry_ms"]),
            tuple(raw["exit_ms"]),
            raw["source"],
            raw["account_tier"],
            int(raw["measured_until_wall_ns"]),
            bool(raw.get("synthetic", False)),
        )


def import_measurements(csv_path, out, tier):
    """Import local submit-to-observed-fill timings, NOT REST acknowledgement RTT."""
    records = list(csv.DictReader(Path(csv_path).open()))
    delays = {"entry": [], "exit": []}
    end = 0
    for r in records:
        if r["role"] not in delays:
            raise ValueError("Latency role must be entry or exit")
        if r["account_tier"] != tier or r["venue"] != "lighter":
            raise ValueError("Mixed account tiers or exchange latency records")
        delay = (int(r["observed_fill_mono_ns"]) - int(r["submitted_mono_ns"])) / 1e6
        delays[r["role"]].append(delay)
        end = max(end, int(r["observed_fill_wall_ns"]))
    digest = hashlib.sha256(Path(csv_path).read_bytes()).hexdigest()
    result = Latency(
        tuple(delays["entry"]), tuple(delays["exit"]), f"csv-sha256:{digest}", tier, end
    )
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    Path(out).write_text(
        json.dumps(
            {**result.summary(), "entry_ms": result.entry_ms, "exit_ms": result.exit_ms}, indent=2
        )
    )
    return result.summary()
