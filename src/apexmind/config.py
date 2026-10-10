from dataclasses import dataclass, field, fields
from pathlib import Path
import tomllib
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Feeds:
    lighter_rest: str = "https://mainnet.zklighter.elliot.ai"
    lighter_ws: str = "wss://mainnet.zklighter.elliot.ai/stream"
    reference_venue: str = "kraken"
    reference_rest: str = "https://futures.kraken.com"
    reference_ws: str = "wss://futures.kraken.com/ws/v1"
    symbols: list[str] = field(default_factory=lambda: ["BTC", "ETH"])
    max_freshness_ms: int = 500
    decision_interval_ms: int = 250
    request_per_second: float = 2
    depth_levels: int = 100
    max_clock_step_ms: float = 50


@dataclass(frozen=True)
class Research:
    notional: float = 12
    holding_seconds: float = 2
    taker_fee_bps: float = 2
    funding_reserve_bps_per_hour: float = 1
    extra_slippage_bps: float = 1
    max_execution_gap_ms: int = 500
    seed: int = 42
    min_train_rows: int = 100
    folds: int = 3
    max_features: int = 12
    bootstrap_samples: int = 500
    max_events: int = 250_000


@dataclass(frozen=True)
class Risk:
    initial_balance: float = 10
    max_gross_leverage: float = 1
    max_market_fraction: float = 0.6
    max_loss_fraction: float = 0.01
    max_drawdown_fraction: float = 0.1
    margin_reserve_fraction: float = 0.3
    minimum_edge_bps: float = 1
    max_slippage_bps: float = 5


@dataclass(frozen=True)
class Storage:
    database: str = "data/apex.sqlite"
    artifacts: str = "artifacts"


@dataclass(frozen=True)
class Live:
    ledger: str = "data/live.sqlite"
    account_poll_seconds: float = 1
    account_max_age_seconds: float = 5
    order_timeout_seconds: float = 10
    feed_outage_seconds: float = 5
    transaction_per_second: float = 1
    request_per_second: float = 1
    margin_buffer_ratio: float = 2
    protective_slippage_bps: float = 100
    min_stop_bps: float = 10
    minimum_forward_episodes: int = 20
    retirement_window: int = 20
    max_clock_error_ms: float = 50
    queue_capacity: int = 10000
    max_episode_seconds: float = 300
    episode_transaction_budget: int = 60


@dataclass(frozen=True)
class Config:
    feeds: Feeds = field(default_factory=Feeds)
    research: Research = field(default_factory=Research)
    risk: Risk = field(default_factory=Risk)
    storage: Storage = field(default_factory=Storage)
    live: Live = field(default_factory=Live)

    def __post_init__(self):
        if self.feeds.reference_venue not in ("kraken", "binance"):
            raise ValueError("Unsupported reference venue")
        for key in ("lighter_rest", "reference_rest", "lighter_ws", "reference_ws"):
            url = urlsplit(getattr(self.feeds, key))
            expected = "wss" if key.endswith("_ws") else "https"
            if (
                url.scheme != expected
                or not url.hostname
                or url.username
                or url.password
                or url.query
                or url.fragment
            ):
                raise ValueError(
                    "Public feed URLs must use verified TLS and contain no credentials"
                )
        if self.feeds.max_freshness_ms <= 0 or self.feeds.decision_interval_ms <= 0:
            raise ValueError("Feed timing limits must be positive")
        if self.feeds.request_per_second <= 0 or not self.feeds.symbols:
            raise ValueError("Request budget and symbols are required")
        if not 1 <= self.research.max_events <= 1_000_000 or not 1 <= self.research.folds <= 10:
            raise ValueError("Research event budget or fold count is outside supported bounds")
        if self.research.notional <= 0 or self.research.holding_seconds <= 0:
            raise ValueError("Research notional and holding period must be positive")
        if (
            min(
                self.research.taker_fee_bps,
                self.research.extra_slippage_bps,
                self.research.funding_reserve_bps_per_hour,
            )
            < 0
        ):
            raise ValueError("Costs cannot be negative")
        if self.risk.initial_balance <= 0 or self.risk.max_gross_leverage <= 0:
            raise ValueError("Balance and leverage must be positive")
        if (
            min(
                self.live.account_poll_seconds,
                self.live.account_max_age_seconds,
                self.live.request_per_second,
                self.live.transaction_per_second,
                self.live.order_timeout_seconds,
                self.live.feed_outage_seconds,
            )
            <= 0
            or self.live.margin_buffer_ratio <= 1
            or self.live.queue_capacity < 100
            or self.live.minimum_forward_episodes < 20
            or self.live.retirement_window < 10
            or not 0 < self.live.protective_slippage_bps < 1000
            or self.live.min_stop_bps <= 0
            or self.live.max_clock_error_ms <= 0
            or not 0 <= self.risk.max_slippage_bps < 1000
            or self.live.max_episode_seconds <= 0
            or not 10 <= self.live.episode_transaction_budget <= 80
        ):
            raise ValueError("Invalid live timing, capacity, margin, or evidence policy")
        for key in (
            "max_market_fraction",
            "max_loss_fraction",
            "max_drawdown_fraction",
            "margin_reserve_fraction",
        ):
            if not 0 < getattr(self.risk, key) < 1:
                raise ValueError(f"{key} must be between zero and one")


def load_config(path: str | Path = "config/default.toml") -> Config:
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    sections = {
        "feeds": Feeds,
        "research": Research,
        "risk": Risk,
        "storage": Storage,
        "live": Live,
    }
    if set(raw) - sections.keys():
        raise ValueError("Unknown configuration section")
    out = {}
    for name, cls in sections.items():
        values = raw.get(name, {})
        if set(values) - {f.name for f in fields(cls)}:
            raise ValueError(f"Unknown settings in {name}")
        out[name] = cls(**values)
    return Config(**out)
