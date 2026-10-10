"""Public feeds; no private keys or exchange trading endpoints are used here."""

import asyncio
from collections import deque
from dataclasses import asdict, dataclass, replace
from decimal import Decimal
import json
import logging
import os
import ssl
import shutil
import time
import aiohttp
from .book import Book, BookError
from .config import Config
from .store import Clock, Store

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class Instrument:
    symbol: str
    market_id: int
    reference_symbol: str
    min_base: str
    min_quote: str
    size_decimals: int
    price_decimals: int
    lighter_quote: str = "USDC"
    reference_quote: str = "USDT"


class RequestBudget:
    def __init__(self, per_second: float):
        self.interval = 1 / per_second
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            await asyncio.sleep(max(0, self.next_at - now))
            self.next_at = time.monotonic() + self.interval


def session():
    context = ssl.create_default_context(cafile=os.environ.get("SSL_CERT_FILE"))
    return aiohttp.ClientSession(
        trust_env=True,
        connector=aiohttp.TCPConnector(ssl=context),
        timeout=aiohttp.ClientTimeout(total=20),
    )


async def get_json(client, url, budget):
    await budget.wait()
    async with client.get(url) as response:
        response.raise_for_status()
        return await response.json()


def websocket_proxy():
    # aiohttp's trust_env does not use HTTPS_PROXY for WSS automatically.
    # Use the provided CONNECT proxy, retaining certificate verification.
    return os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")


def map_instruments(lighter: dict, reference: dict, symbols: list[str], venue="binance"):
    if venue == "binance":
        references = {
            m["baseAsset"]: m
            for m in reference["symbols"]
            if m["status"] == "TRADING"
            and m["contractType"] == "PERPETUAL"
            and m["quoteAsset"] == "USDT"
        }
    elif venue == "kraken":
        references = {}
        for m in reference["instruments"]:
            product = m["symbol"].upper()
            if (
                not product.startswith("PF_")
                or not product.endswith("USD")
                or m.get("type") != "flexible_futures"
                or not m.get("tradeable")
                or Decimal(str(m.get("contractSize", 0))) != 1
            ):
                continue
            base = product[3:-3]
            base = {"XBT": "BTC"}.get(base, base)
            if base in references:
                raise ValueError("Ambiguous reference contract mapping")
            references[base] = {"symbol": m["symbol"]}
    else:
        raise ValueError("Unsupported reference venue")
    result = []
    for m in lighter["order_books"]:
        symbol = m["symbol"]
        if (
            m["status"] != "active"
            or m.get("market_type") != "perp"
            or symbol not in references
            or Decimal(str(m.get("multiplier", "1"))) != 1
        ):
            continue
        if symbols != ["ALL"] and symbol not in symbols:
            continue
        result.append(
            Instrument(
                symbol,
                int(m["market_id"]),
                references[symbol]["symbol"],
                m["min_base_amount"],
                m["min_quote_amount"],
                int(m["supported_size_decimals"]),
                int(m["supported_price_decimals"]),
                reference_quote="USD" if venue == "kraken" else "USDT",
            )
        )
    if not result:
        raise ValueError("No exact active perpetual instrument mappings; refusing guesses")
    if symbols != ["ALL"] and set(symbols) - {m.symbol for m in result}:
        raise ValueError("A requested instrument is ineligible or ambiguously mapped")
    return sorted(result, key=lambda m: m.symbol)


async def discover(config: Config):
    budget = RequestBudget(config.feeds.request_per_second)
    async with session() as client:
        lighter, reference = await asyncio.gather(
            get_json(client, config.feeds.lighter_rest + "/api/v1/orderBooks", budget),
            get_json(
                client,
                config.feeds.reference_rest
                + (
                    "/derivatives/api/v3/instruments"
                    if config.feeds.reference_venue == "kraken"
                    else "/fapi/v1/exchangeInfo"
                ),
                budget,
            ),
        )
    return map_instruments(lighter, reference, config.feeds.symbols, config.feeds.reference_venue)


class Collector:
    def __init__(self, config: Config, store: Store, instruments: list[Instrument], on_event=None):
        self.config, self.store, self.instruments = config, store, instruments
        self.clock = Clock(config.feeds.max_clock_step_ms)
        self.budget = RequestBudget(config.feeds.request_per_second)
        self.seen_trades = set()
        self.trade_ids = deque()
        self.messages = 0
        self.on_event = on_event
        self.coverage = set()

    def emit(self, raw, kind, payload, exchange_ms=None):
        event = replace(
            raw,
            kind=kind,
            payload={**payload, "clock_healthy": raw.payload.get("clock_healthy", True)},
            exchange_ns=int(exchange_ms) * 1_000_000 if exchange_ms else None,
        )
        self.store.append(event)
        self.coverage.add((event.venue, event.symbol, kind))
        if self.on_event:
            self.on_event(event)
        self.messages += 1

    def trade(self, raw, payload, exchange_ms, trade_id):
        key = (raw.venue, raw.symbol, str(trade_id))
        if key in self.seen_trades:
            return
        self.seen_trades.add(key)
        self.trade_ids.append(key)
        if len(self.trade_ids) > 100_000:
            self.seen_trades.remove(self.trade_ids.popleft())
        self.emit(raw, "trade", payload, exchange_ms)

    async def lighter(self, client, instrument):
        book = Book()
        async with client.ws_connect(
            self.config.feeds.lighter_ws,
            heartbeat=20,
            max_msg_size=4 * 1024 * 1024,
            proxy=websocket_proxy(),
        ) as ws:
            for channel in ("order_book", "trade", "market_stats"):
                await ws.send_json(
                    {"type": "subscribe", "channel": f"{channel}/{instrument.market_id}"}
                )
            async for message in ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    if message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        raise ConnectionError("Lighter socket closed")
                    continue
                raw = self.clock.event(
                    "lighter", instrument.symbol, "raw", json.loads(message.data)
                )
                self.store.append(raw)
                data = raw.payload
                if data.get("type") == "ping":
                    await ws.send_json({"type": "pong"})
                    continue
                if data.get("type") == "shutdown":
                    raise ConnectionError("Lighter requests reconnect")
                if data.get("type") == "error":
                    raise ValueError("Lighter subscription rejected")
                if "order_book" in data:
                    ob = data["order_book"]
                    snapshot = data["type"] == "subscribed/order_book"
                    begin = ob.get("begin_offset")
                    previous = int(begin) - 1 if begin is not None else None
                    if book.update(
                        ob["bids"],
                        ob["asks"],
                        int(ob["offset"]),
                        snapshot=snapshot,
                        previous=previous,
                    ):
                        self.emit(
                            raw,
                            "book",
                            book.payload(self.config.feeds.depth_levels),
                            data.get("timestamp"),
                        )
                for trade in data.get("trades", []):
                    # Maker ask implies the aggressor bought; do not invert this.
                    self.trade(
                        raw,
                        {
                            "price": float(trade["price"]),
                            "size": float(trade["size"]),
                            "side": 1 if trade["is_maker_ask"] else -1,
                            "id": str(trade["trade_id"]),
                        },
                        trade.get("timestamp"),
                        trade["trade_id"],
                    )
                stats = data.get("market_stats")
                if isinstance(stats, dict):
                    self.emit(raw, "stats", stats, data.get("timestamp"))
                    if "current_funding_rate" in stats:
                        # Lighter current funding is reported in percent per hour.
                        self.emit(
                            raw,
                            "funding",
                            {
                                "rate_bps_hour": float(stats["current_funding_rate"]) * 100,
                                "source": "market_stats_percent_per_hour",
                            },
                            data.get("timestamp"),
                        )
        raise ConnectionError("Lighter stream ended")

    async def binance(self, client, instrument):
        s = instrument.reference_symbol.lower()
        streams = "/".join(f"{s}@{name}" for name in ("depth@100ms", "aggTrade", "bookTicker"))
        url = self.config.feeds.reference_ws + "?streams=" + streams
        book = Book()
        async with client.ws_connect(url, heartbeat=20, proxy=websocket_proxy()) as ws:
            # Open stream before the REST snapshot. Messages buffered by the
            # websocket transport are bridged using U/u/pu, never time buckets.
            snapshot = await get_json(
                client,
                self.config.feeds.reference_rest
                + f"/fapi/v1/depth?symbol={instrument.reference_symbol}&limit=1000",
                self.budget,
            )
            book.update(
                snapshot["bids"], snapshot["asks"], int(snapshot["lastUpdateId"]), snapshot=True
            )
            bridged = False
            async for message in ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    if message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        raise ConnectionError("Reference socket closed")
                    continue
                data = json.loads(message.data).get("data", {})
                raw = self.clock.event(
                    "reference",
                    instrument.symbol,
                    "raw",
                    data,
                    int(data["E"]) * 1_000_000 if data.get("E") else None,
                )
                self.store.append(raw)
                if data.get("e") == "depthUpdate":
                    last = book.sequence
                    if data["u"] < last or (bridged and data["u"] == last):
                        continue
                    if not bridged:
                        if not data["U"] <= last <= data["u"]:
                            raise BookError("Reference snapshot/stream bridge failed")
                        previous = last
                    else:
                        previous = int(data["pu"])
                    if data["u"] == last and not bridged:
                        bridged = True
                        continue
                    book.update(data["b"], data["a"], int(data["u"]), previous=previous)
                    bridged = True
                    self.emit(raw, "book", book.payload(self.config.feeds.depth_levels), data["E"])
                elif data.get("e") == "aggTrade":
                    self.trade(
                        raw,
                        {
                            "price": float(data["p"]),
                            "size": float(data["q"]),
                            "side": -1 if data["m"] else 1,
                            "id": str(data["a"]),
                        },
                        data["T"],
                        data["a"],
                    )
                elif data.get("e") == "bookTicker":
                    self.emit(raw, "bbo", {k: data[k] for k in ("b", "B", "a", "A")}, data.get("E"))
        raise ConnectionError("Reference stream ended")

    async def kraken(self, client, instrument):
        book = Book()
        async with client.ws_connect(
            self.config.feeds.reference_ws, heartbeat=20, proxy=websocket_proxy()
        ) as ws:
            for feed in ("book", "trade"):
                await ws.send_json(
                    {
                        "event": "subscribe",
                        "feed": feed,
                        "product_ids": [instrument.reference_symbol],
                    }
                )
            async for message in ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    if message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        raise ConnectionError("Kraken stream closed")
                    continue
                data = json.loads(message.data)
                raw = self.clock.event("reference", instrument.symbol, "raw", data)
                self.store.append(raw)
                if data.get("event") in ("error", "subscribed_failed"):
                    raise ValueError("Kraken subscription rejected")
                if "event" in data:
                    continue
                feed = data.get("feed")
                if feed == "book_snapshot":
                    bids = [(x["price"], x["qty"]) for x in data["bids"]]
                    asks = [(x["price"], x["qty"]) for x in data["asks"]]
                    book.update(bids, asks, int(data["seq"]), snapshot=True)
                    self.emit(
                        raw,
                        "book",
                        book.payload(self.config.feeds.depth_levels),
                        data.get("timestamp"),
                    )
                elif feed == "book":
                    if data["side"] not in ("buy", "sell"):
                        raise ValueError("Unknown Kraken book side")
                    level = [(data["price"], data["qty"])]
                    sequence = int(data["seq"])
                    if book.update(
                        level if data["side"] == "buy" else [],
                        level if data["side"] == "sell" else [],
                        sequence,
                        previous=sequence - 1,
                    ):
                        self.emit(
                            raw,
                            "book",
                            book.payload(self.config.feeds.depth_levels),
                            data.get("timestamp"),
                        )
                elif feed == "trade":
                    if data["side"] not in ("buy", "sell"):
                        raise ValueError("Unknown Kraken aggressor side")
                    self.trade(
                        raw,
                        {
                            "size": float(data["qty"]),
                            "price": float(data["price"]),
                            "side": 1 if data["side"] == "buy" else -1,
                            "id": data["uid"],
                        },
                        data.get("time"),
                        data["uid"],
                    )
                # trade_snapshot is historical; preserve it as raw data without
                # introducing old prints into current arrival-time flow features.
        raise ConnectionError("Kraken stream ended")

    async def reference(self, client, instrument):
        await getattr(self, self.config.feeds.reference_venue)(client, instrument)

    async def reconnect(self, client, instrument, venue):
        attempt = 0
        while True:
            if shutil.disk_usage(".").free < 500 * 1024**2:
                raise RuntimeError("Collector stopped: less than 500 MiB disk space remains")
            started = time.monotonic()
            try:
                await getattr(self, venue)(client, instrument)
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, ConnectionError, ValueError, KeyError, TypeError) as error:
                LOG.warning(
                    "%s %s feed failure: %s", venue, instrument.symbol, type(error).__name__
                )
                disconnect = self.clock.event(
                    venue, instrument.symbol, "disconnect", {"error_type": type(error).__name__}
                )
                self.store.append(disconnect)
                if self.on_event:
                    self.on_event(disconnect)
            attempt = 0 if time.monotonic() - started > 60 else attempt + 1
            await asyncio.sleep(min(30, 2 ** min(attempt, 5)))

    async def run(self, seconds: float | None = None):
        self.store.set("instruments", [asdict(i) for i in self.instruments])
        self.store.set(
            "feed_contract",
            {
                "reference": self.config.feeds.reference_venue,
                "quote_currency_basis": "Lighter USDC / reference USD or USDT; no FX hedge",
                "sequence_policy": "Kraken seq or Binance pu; Lighter begin_offset when provided",
                "run": self.clock.run,
            },
        )
        async with session() as client:
            tasks = [
                asyncio.create_task(self.reconnect(client, i, venue))
                for i in self.instruments
                for venue in ("lighter", "reference")
            ]
            try:
                if seconds is None:
                    await asyncio.gather(*tasks)
                else:
                    await asyncio.wait_for(asyncio.gather(*tasks), timeout=seconds)
            except TimeoutError:
                pass
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        if not self.messages:
            raise RuntimeError("No normalized market events captured; collection was unsuccessful")
        required = {
            (venue, i.symbol, kind)
            for i in self.instruments
            for venue in ("lighter", "reference")
            for kind in ("book", "trade")
        }
        required |= {("lighter", i.symbol, "funding") for i in self.instruments}
        missing = required - self.coverage
        if missing:
            raise RuntimeError("Collection lacks required channels: " + str(sorted(missing)))
        return self.store.summary()
