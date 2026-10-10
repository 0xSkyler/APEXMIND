from dataclasses import replace
import pytest
from apexmind.book import Book, BookError, vwap
from apexmind.config import Config
from apexmind.demo import generate
from apexmind.features import FeatureEngine, FEATURES
from apexmind.feeds import map_instruments
from apexmind.store import Event, Store


def test_snapshot_delta_deletion_and_duplicate():
    b = Book()
    b.update([[99, 2], [98, 3]], [[101, 2]], 10, snapshot=True)
    assert b.update([[99, 0], [100, 4]], [], 11, previous=10)
    assert 99 not in b.bids and b.mid == 100.5
    assert not b.update([[97, 1]], [], 11, previous=10)
    assert b.sequence == 11 and b.sequence_verified


def test_book_gap_invalidates_until_new_snapshot():
    b = Book()
    with pytest.raises(BookError, match="before snapshot"):
        b.update([[99, 2]], [[101, 2]], 1)
    b.update([[99, 2]], [[101, 2]], 10, snapshot=True)
    with pytest.raises(BookError, match="gap"):
        b.update([], [], 12, previous=11)
    assert not b.valid
    b.update([[98, 2]], [[101, 2]], 20, snapshot=True)
    assert b.valid


@pytest.mark.parametrize(
    "bids,asks",
    [
        ([[102, 2]], [[101, 2]]),
        ([[99, -1]], [[101, 2]]),
        ([[float("nan"), 2]], [[101, 2]]),
        ([], [[101, 2]]),
    ],
)
def test_reject_corrupt_books(bids, asks):
    with pytest.raises(BookError):
        Book().update(bids, asks, 1, snapshot=True)


def test_vwap_walks_depth_and_never_invents_fills():
    assert vwap([[100, 1], [102, 2]], 2) == 101
    with pytest.raises(BookError, match="Insufficient"):
        vwap([[100, 1]], 2)


def test_unverified_sequence_is_recorded():
    b = Book()
    b.update([[99, 2]], [[101, 2]], 10, snapshot=True)
    b.update([[99, 3]], [], 15)
    assert not b.payload()["sequence_verified"]


def test_mapping_excludes_spot_and_multiplied_assets():
    template = {
        "status": "active",
        "market_type": "perp",
        "symbol": "BTC",
        "market_id": 1,
        "min_base_amount": "0.0001",
        "min_quote_amount": "5",
        "supported_size_decimals": 5,
        "supported_price_decimals": 2,
        "multiplier": "1",
    }
    lighter = {
        "order_books": [
            template,
            {**template, "symbol": "ETH", "market_id": 2, "market_type": "spot"},
            {**template, "symbol": "SHIB", "market_id": 3, "multiplier": "1000"},
        ]
    }
    ref = {
        "symbols": [
            {
                "status": "TRADING",
                "contractType": "PERPETUAL",
                "quoteAsset": "USDT",
                "baseAsset": s,
                "symbol": s + "USDT",
            }
            for s in ("BTC", "ETH", "SHIB")
        ]
    }
    assert [m.symbol for m in map_instruments(lighter, ref, ["ALL"])] == ["BTC"]
    with pytest.raises(ValueError, match="ineligible"):
        map_instruments(lighter, ref, ["BTC", "ETH"])


def test_kraken_mapping_requires_linear_unit_contract():
    lighter = {
        "order_books": [
            {
                "status": "active",
                "market_type": "perp",
                "symbol": "BTC",
                "market_id": 1,
                "min_base_amount": "0.00007",
                "min_quote_amount": "10",
                "supported_size_decimals": 5,
                "supported_price_decimals": 1,
                "multiplier": "1",
            }
        ]
    }
    reference = {
        "instruments": [
            {
                "symbol": "PF_XBTUSD",
                "type": "flexible_futures",
                "tradeable": True,
                "contractSize": 1,
            },
            {
                "symbol": "PI_XBTUSD",
                "type": "futures_inverse",
                "tradeable": True,
                "contractSize": 1,
            },
        ]
    }
    mapped = map_instruments(lighter, reference, ["BTC"], "kraken")
    assert mapped[0].reference_symbol == "PF_XBTUSD"
    assert mapped[0].reference_quote == "USD"


def test_features_are_arrival_causal_and_reset_on_restart(tmp_path):
    store = Store(tmp_path / "data.sqlite")
    generate(store, seconds=8)
    events = list(store.events())
    engine = FeatureEngine(Config().feeds)
    decisions = [d for e in events if (d := engine.update(e))]
    assert decisions and len(decisions[-1].x) == len(FEATURES)
    cut = next(
        i for i, e in enumerate(events) if e.wall_ns == decisions[0].wall_ns and e.kind == "book"
    )
    fresh = FeatureEngine(Config().feeds)
    prefix = [d for e in events[: cut + 1] if (d := fresh.update(e))]
    assert prefix[-1].x == decisions[0].x
    e = events[cut]
    assert fresh.update(replace(e, run="new-run")) is None
    store.close()


def test_stale_reference_and_disconnect_prevent_decisions(tmp_path):
    store = Store(tmp_path / "data.sqlite")
    generate(store, seconds=8)
    events = list(store.events())
    engine = FeatureEngine(Config().feeds)
    for e in events:
        engine.update(e)
    latest = next(e for e in reversed(events) if e.venue == "lighter" and e.kind == "book")
    stale = replace(
        latest, mono_ns=latest.mono_ns + 2_000_000_000, wall_ns=latest.wall_ns + 2_000_000_000
    )
    assert engine.update(stale) is None
    engine.update(
        Event(
            latest.run,
            "reference",
            latest.symbol,
            "disconnect",
            stale.wall_ns,
            stale.mono_ns,
            None,
            {},
        )
    )
    assert engine.update(replace(stale, mono_ns=stale.mono_ns + 1_000_000_000)) is None
    store.close()
