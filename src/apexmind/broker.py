"""Exchange-authoritative order lifecycle, protective orders, and cash accounting."""

from dataclasses import asdict
from decimal import Decimal
import math
import time

from .account import number
from .book import vwap
from .execution import ExecutionBlocked
from .native import units

ACTIVE = {"pending", "open", "in-progress"}
TERMINAL = {"filled", "cancelled", "rejected"}
CASH_FLOWS = (
    "inflow",
    "outflow",
    "pool_inflow",
    "pool_outflow",
    "spot_inflow",
    "spot_outflow",
    "staking_inflow",
    "staking_outflow",
    "staking_pnl",
    "pool_pnl",
    "trade_spot_pnl",
)


def client_id(order):
    return str(order["client_order_index"])


def cash_counters(rows):
    latest = max(rows, key=lambda row: int(row["timestamp"]))
    return {k: number(latest[k]) for k in CASH_FLOWS}


def margin_reason(snapshot, config, peak=None):
    if snapshot.equity <= 0:
        return "insolvent_account"
    if snapshot.equity <= snapshot.maintenance_margin * config.live.margin_buffer_ratio:
        return "maintenance_margin_buffer"
    if snapshot.available < snapshot.equity * config.risk.margin_reserve_fraction:
        return "available_margin_reserve"
    if peak and snapshot.equity <= peak * (1 - config.risk.max_drawdown_fraction):
        return "drawdown_limit"
    if sum(p.notional for p in snapshot.positions.values()) > (
        snapshot.equity * config.risk.max_gross_leverage * 1.001
    ):
        return "gross_exposure_limit"
    for p in snapshot.positions.values():
        if p.margin_mode != 0:
            return "isolated_margin_requires_reduction"
        mark = p.notional / p.quantity
        if p.liquidation_price > 0 and p.side * (mark - p.liquidation_price) / mark * 1e4 <= (
            config.live.min_stop_bps + config.live.protective_slippage_bps
        ):
            return "liquidation_price_buffer"
    return None


class LiveBroker:
    def __init__(self, gateway, config, instruments):
        self.gateway, self.store, self.config = gateway, gateway.store, config
        previous = self.store.get("live_instruments", [])
        self.instruments = {i["symbol"]: i for i in [*previous, *instruments]}
        self.store.set("live_instruments", list(self.instruments.values()))
        self.snapshot = None
        self.last_reconciled_ns = 0
        self.active = []

    def account_fresh(self):
        if self.snapshot is None or time.monotonic_ns() - self.snapshot.observed_mono_ns > (
            self.config.live.account_max_age_seconds * 1e9
        ):
            raise ExecutionBlocked("Authoritative account snapshot is stale")

    def record(self, identifier, symbol, role, side, quantity, **details):
        return {
            "client_id": identifier,
            "symbol": symbol,
            "payload": {
                "role": role,
                "side": side,
                "quantity": quantity,
                **details,
            },
        }

    def order_payload(self, identifier, symbol, side, quantity, price, *, stop=0, reduce=False):
        i = self.instruments[symbol]
        base, limit = (
            units(quantity, i["size_decimals"]),
            units(price, i["price_decimals"], up=side == -1),
        )
        trigger = units(stop, i["price_decimals"], up=side == -1)
        if not 0 < base < 2**63 or not 0 < limit < 2**32 or not 0 <= trigger < 2**32:
            raise ExecutionBlocked("Order precision exceeds native encoding bounds")
        return dict(
            MarketIndex=i["market_id"],
            ClientOrderIndex=int(identifier),
            BaseAmount=base,
            Price=limit,
            IsAsk=int(side == -1),
            Type=2 if stop else 0,
            TimeInForce=0,
            ReduceOnly=int(reduce),
            TriggerPrice=trigger,
            OrderExpiry=-1 if stop else 0,
        )

    async def enter(
        self,
        symbol,
        side,
        quantity,
        book,
        forecast,
        model_hash,
        preflight=None,
        integration=False,
        stop_override=None,
    ):
        self.account_fresh()
        if self.store.pending_transactions():
            raise ExecutionBlocked("Transaction reconciliation is incomplete")
        if symbol in self.snapshot.positions or any(
            o["symbol"] == symbol
            and o["payload"]["role"] == "entry"
            and o["status"] not in TERMINAL
            for o in self.store.orders()
        ):
            raise ExecutionBlocked("An entry or position already exists for this market")
        i = self.instruments[symbol]
        entry = vwap(book["asks"] if side == 1 else book["bids"], quantity)
        if quantity < number(i["min_base"]) or quantity * entry < number(i["min_quote"]):
            raise ExecutionBlocked("Order is below exchange minimums")
        stop_bps = max(self.config.live.min_stop_bps, forecast["adverse"], -forecast["q05"])
        stop = stop_override or entry * (1 - side * stop_bps / 1e4)
        exit_side = -side
        close_limit = stop * (1 + exit_side * self.config.live.protective_slippage_bps / 1e4)
        entry_limit = entry * (1 + side * self.config.risk.max_slippage_bps / 1e4)
        ids = self.store.allocate_ids(2)
        records = [
            self.record(
                ids[0],
                symbol,
                "entry",
                side,
                quantity,
                forecast=forecast,
                model_sha256=model_hash,
                integration=integration,
                expected_price=entry,
                stop_id=ids[1],
                holding_seconds=forecast["holding_seconds"],
            ),
            self.record(
                ids[1],
                symbol,
                "stop",
                exit_side,
                quantity,
                parent_id=ids[0],
                model_sha256=model_hash,
                integration=integration,
                expected_price=stop,
                position_tied=True,
            ),
        ]
        payload = {
            "orders": [
                self.order_payload(ids[0], symbol, side, quantity, entry_limit),
                self.order_payload(
                    ids[1], symbol, exit_side, quantity, close_limit, stop=stop, reduce=True
                ),
            ]
        }
        # Official attached-order semantics: zero child size follows the actual
        # parent fill and remains tied to that position, including partial fills.
        payload["orders"][1]["BaseAmount"] = 0

        # OTO creation is atomic at the exchange. No unprotected entry is sent.
        def check():
            self.account_fresh()
            if preflight:
                preflight()

        return await self.gateway.signed_transaction("group", payload, records, check)

    async def close_position(self, symbol, price=None, reason="protective_exit"):
        self.snapshot = await self.gateway.snapshot()
        position = self.snapshot.positions.get(symbol)
        if position is None:
            return None
        self.verify_ownership()
        if any(
            o["symbol"] == symbol and o["payload"]["role"] == "exit" and o["status"] not in TERMINAL
            for o in self.store.orders()
        ):
            raise ExecutionBlocked("An exit already awaits reconciliation")
        identifier = self.store.allocate_ids()[0]
        side = -position.side
        # A stale quote is unsuitable for entries. A conservative bound around
        # an authoritative account mark is available for risk reduction only.
        anchor = price or position.notional / position.quantity
        limit = anchor * (1 + side * self.config.live.protective_slippage_bps / 1e4)
        native = self.order_payload(identifier, symbol, side, position.quantity, limit, reduce=True)
        payload = dict(
            market_index=native["MarketIndex"],
            client_order_index=int(identifier),
            base_amount=native["BaseAmount"],
            price=native["Price"],
            is_ask=side == -1,
            order_type=0,
            time_in_force=0,
            reduce_only=True,
            trigger_price=0,
            order_expiry=0,
        )
        record = self.record(
            identifier,
            symbol,
            "exit",
            side,
            position.quantity,
            expected_price=anchor,
            reason=reason,
        )
        entries = [
            r
            for r in self.store.orders()
            if r["symbol"] == symbol
            and r["payload"]["role"] == "entry"
            and r["payload"].get("filled_quantity", 0)
        ]
        if entries:
            record["payload"].update(
                {k: entries[-1]["payload"][k] for k in ("integration", "model_sha256")}
            )
        return await self.gateway.signed_transaction(
            "order", payload, [record], self.account_fresh, risk_reducing=True
        )

    async def cancel(self, record):
        raw = record["payload"].get("exchange")
        if raw is None or record["status"] in TERMINAL:
            return None
        # Never remove the last protective order while a position remains.
        if record["payload"]["role"] == "stop" and record["symbol"] in self.snapshot.positions:
            raise ExecutionBlocked("Cannot cancel protection for an open position")
        for tx in self.store.pending_transactions():
            p = tx["payload"]
            if p["operation"] == "cancel" and p["request"]["order_index"] == int(
                raw["order_index"]
            ):
                raise ExecutionBlocked("Cancellation already awaits reconciliation")
        return await self.gateway.signed_transaction(
            "cancel",
            {
                "market_index": int(raw["market_index"]),
                "order_index": int(raw["order_index"]),
            },
            risk_reducing=True,
        )

    async def reconcile(self):
        records = self.store.orders()
        # Final fills are durable. Old inactive orders disappear from the native
        # lookup window; do not pretend absence invalidates an archived fill.
        selected = [r for r in records if r["status"] not in TERMINAL]
        found = await self.gateway.orders([r["client_id"] for r in selected]) if selected else []
        by_id = {client_id(o): o for o in found}
        for record in selected:
            raw = by_id.get(record["client_id"])
            if raw is None:
                if record["status"] == "rejected":
                    continue  # Locally rejected before POST, never sent.
                raise ExecutionBlocked(
                    "Submitted client ID is absent; absence does not prove rejection"
                )
            self._identity(record, raw)
            trades = (
                await self.gateway.trades(str(raw["order_index"]))
                if number(raw["filled_base_amount"])
                else []
            )
            fills = [self._fill(record, raw, t) for t in trades]
            total = sum(Decimal(str(f["quantity"])) for f in fills)
            expected = Decimal(raw["filled_base_amount"])
            if total != expected:
                raise ExecutionBlocked("Trade pagination does not match cumulative filled quantity")
            state = raw["status"]
            if state == "filled":
                status = "filled"
            elif state.startswith("canceled"):
                status = "cancelled"
            elif state in ACTIVE:
                status = "partial" if total else "open"
            else:
                raise ExecutionBlocked("Unknown exchange order status")
            payload = {
                **record["payload"],
                "exchange": raw,
                "filled_quantity": float(total),
                "reconciled_run": self.gateway.run,
            }
            if total and not payload.get("first_fill_wall_ns"):
                payload["first_fill_wall_ns"] = time.time_ns()
                exchange_wall_ns = min(int(t["timestamp"]) for t in trades) * 1_000_000
                if (
                    payload["submitted_wall_ns"] - self.config.feeds.max_freshness_ms * 1e6
                    <= exchange_wall_ns
                    <= payload["first_fill_wall_ns"]
                ):
                    payload["first_fill_exchange_wall_ns"] = exchange_wall_ns
                if payload.get("run") == self.gateway.run:
                    payload["observed_fill_mono_ns"] = time.monotonic_ns()
                    payload["observed_fill_delay_ms"] = (
                        payload["observed_fill_mono_ns"] - payload["submitted_mono_ns"]
                    ) / 1e6
            self.store.sync_live_order({**record, "status": status, "payload": payload}, fills)
        self.active = await self.gateway.active_orders()
        known = {r["client_id"] for r in self.store.orders()}
        if any(client_id(o) not in known for o in self.active):
            raise ExecutionBlocked("Unmanaged active orders found; use a dedicated account")
        current = {r["client_id"]: r for r in self.store.orders()}
        for tx in self.store.pending_transactions():
            p = tx["payload"]
            ids = p["client_ids"]
            resolved = bool(ids) and all(current[i]["payload"].get("exchange") for i in ids)
            if p["operation"] == "cancel":
                targets = [
                    r
                    for r in current.values()
                    if r["payload"].get("exchange", {}).get("order_index")
                    == p["request"]["order_index"]
                ]
                resolved = bool(targets) and all(r["status"] in TERMINAL for r in targets)
            if resolved:
                # Effects alone are insufficient if a different process submitted
                # a conflicting transaction. Verify exact transaction ownership.
                native = await self.gateway.transaction_status(tx["hash"])
                if (
                    native["hash"] != tx["hash"]
                    or native["account_index"] != self.gateway.account_index
                    or native["api_key_index"] != self.gateway.key_index
                    or native["nonce"] != tx["nonce"]
                ):
                    raise ExecutionBlocked("Transaction identity mismatch")
                if native.get("executed_at", 0) > 0:
                    self.store.transaction(
                        tx["hash"],
                        tx["nonce"],
                        "resolved",
                        {
                            **p,
                            "exchange": {
                                k: native[k]
                                for k in (
                                    "hash",
                                    "nonce",
                                    "account_index",
                                    "api_key_index",
                                    "executed_at",
                                    "status",
                                )
                            },
                        },
                    )
        self.snapshot = await self.gateway.snapshot()
        self.verify_ownership()
        self.last_reconciled_ns = time.monotonic_ns()
        return self.snapshot

    def _identity(self, record, raw):
        i, p = self.instruments[record["symbol"]], record["payload"]
        if (
            raw["owner_account_index"] != self.gateway.account_index
            or raw["market_index"] != i["market_id"]
            or client_id(raw) != record["client_id"]
            or bool(raw["is_ask"]) != (p["side"] == -1)
        ):
            raise ExecutionBlocked("Order account, instrument, side, or client ID mismatch")
        if p["role"] in ("stop", "exit") and not raw["reduce_only"]:
            raise ExecutionBlocked("Protective order is not reduce-only")
        if p["role"] == "stop" and raw["type"] != "stop-loss":
            raise ExecutionBlocked("Protection is not a native stop loss")
        actual = units(raw["initial_base_amount"], i["size_decimals"])
        requested = units(p["quantity"], i["size_decimals"])
        tied_zero = p["role"] == "stop" and p.get("position_tied") and actual == 0
        if (
            actual != requested
            and not tied_zero
            and not (p["role"] in ("stop", "exit") and 0 < actual <= requested)
        ):
            raise ExecutionBlocked("Exchange order quantity mismatch")

    def _fill(self, record, order, trade):
        side = "ask" if record["payload"]["side"] == -1 else "bid"
        identifier = str(trade.get(f"{side}_client_id_str", trade[f"{side}_client_id"]))
        if (
            trade[f"{side}_account_id"] != self.gateway.account_index
            or identifier != record["client_id"]
            or trade["market_id"] != order["market_index"]
            or str(trade[f"{side}_id"]) != str(order["order_index"])
        ):
            raise ExecutionBlocked("Trade identity mismatch")
        if number(trade["size"]) <= 0 or number(trade["price"]) <= 0:
            raise ExecutionBlocked("Invalid trade size or price")
        return {
            "id": f"{trade.get('trade_id_str', trade['trade_id'])}:{self.gateway.account_index}",
            "client_id": record["client_id"],
            "symbol": record["symbol"],
            "quantity": trade["size"],
            "price": trade["price"],
            "side": record["payload"]["side"],
            "exchange_timestamp": trade["timestamp"],
            "raw": trade,
        }

    def verify_ownership(self):
        net = {}
        for f in self.store.fills():
            net[f["symbol"]] = net.get(f["symbol"], Decimal(0)) + Decimal(f["quantity"]) * f["side"]
        for symbol in set(net) | set(self.snapshot.positions):
            p = self.snapshot.positions.get(symbol)
            if p and (
                symbol not in self.instruments
                or p.market_id != self.instruments[symbol]["market_id"]
            ):
                raise ExecutionBlocked("Position market identity is outside the native catalog")
            actual = Decimal(str(p.quantity)) * p.side if p else Decimal(0)
            if actual != net.get(symbol, Decimal(0)):
                raise ExecutionBlocked(
                    "Account position differs from owned fills; recovery requires reconciliation"
                )

    def protected(self, symbol):
        position = self.snapshot.positions[symbol]
        records = {r["client_id"]: r for r in self.store.orders()}
        for r in self.store.orders():
            raw = r["payload"].get("exchange", {})
            if (
                r["symbol"] == symbol
                and r["payload"]["role"] == "stop"
                and r["status"] not in TERMINAL
                and raw.get("reduce_only")
                and raw.get("trigger_status") in ("ready", "mark-price")
                and bool(raw.get("is_ask")) == (position.side == 1)
                and (
                    number(raw.get("remaining_base_amount", 0)) >= position.quantity
                    or (
                        r["payload"].get("position_tied")
                        and number(raw.get("initial_base_amount", -1)) == 0
                        and raw.get("parent_order_index")
                        == records[r["payload"]["parent_id"]]["payload"]
                        .get("exchange", {})
                        .get("order_index")
                    )
                )
            ):
                return True
        return False

    async def settle_episode(self, model_hash, integration=False):
        """Net cash is measured only flat-to-flat in a dedicated classic account."""
        s = self.snapshot
        open_entries = any(
            r["payload"]["role"] == "entry" and r["status"] not in TERMINAL
            for r in self.store.orders()
        )
        state = self.store.get("live_episode")
        if state is None:
            if s.positions or open_entries:
                raise ExecutionBlocked("No flat baseline exists for recovered exposure")
            state = {
                "id": str(time.time_ns()),
                "start_wall_ns": s.observed_wall_ns,
                "collateral": s.collateral,
                "model_sha256": model_hash,
                "fill_ids": [f["id"] for f in self.store.fills()],
                "integration": integration,
            }
            baseline = await self.gateway.cash_history(
                state["start_wall_ns"] // 1_000_000 - 120_000, state["start_wall_ns"] // 1_000_000
            )
            if not baseline:
                raise ExecutionBlocked("Transfer-inclusive cash baseline is unavailable")
            # These chart fields are cumulative account cash counters. Existing
            # deposits do not contaminate new flat-to-flat episodes.
            state["cash_counters"] = cash_counters(baseline)
            state["cash_baseline"] = baseline
            self.store.set("live_episode", state)
            return None
        if s.positions or open_entries or self.store.pending_transactions():
            return None
        fills = [f for f in self.store.fills() if f["id"] not in set(state["fill_ids"])]
        if not fills:
            if not math.isclose(s.collateral, state["collateral"], abs_tol=1e-6):
                rows = await self.gateway.cash_history(
                    state["start_wall_ns"] // 1_000_000, s.observed_wall_ns // 1_000_000
                )
                counters = cash_counters(rows) if rows else {}
                change = s.collateral - state["collateral"]
                verified = (
                    counters.get("inflow", 0)
                    - state["cash_counters"]["inflow"]
                    - counters.get("outflow", 0)
                    + state["cash_counters"]["outflow"]
                )
                other = any(
                    abs(counters.get(k, 0) - state["cash_counters"][k]) > 1e-8
                    for k in CASH_FLOWS
                    if k not in ("inflow", "outflow")
                )
                if other or not math.isclose(change, verified, abs_tol=1e-6):
                    raise ExecutionBlocked("Unexplained cash change on a flat account")
                changes = self.store.get("external_capital_changes", [])
                self.store.set(
                    "external_capital_changes",
                    changes
                    + [
                        {
                            "wall_ns": s.observed_wall_ns,
                            "cash_change": change,
                            "cash_counters": counters,
                        }
                    ],
                )
                peak = self.store.get("live_peak_equity", state["collateral"])
                self.store.set("live_peak_equity", max(s.equity, peak + change))
                self.store.set("live_episode", None)
            return None
        start, end = state["start_wall_ns"] // 1_000_000, s.observed_wall_ns // 1_000_000
        transactions = await self.gateway.account_transactions()
        hashes = {r[0] for r in self.store.db.execute("SELECT hash FROM live_transactions")}
        # Native triggered-order execution may create a system transaction.
        # Its trade is still independently verified by owned client/order IDs.
        hashes.update(f["raw"]["tx_hash"] for f in fills)
        fill_clients = {f["client_id"] for f in fills}
        required_hashes = {
            r["payload"]["tx_hash"] for r in self.store.orders() if r["client_id"] in fill_clients
        }
        relevant = [t for t in transactions if int(t["transaction_time"]) >= start]
        # If the rolling response is full, completeness cannot be assumed.
        if len(transactions) >= 100 and min(t["transaction_time"] for t in transactions) >= start:
            raise ExecutionBlocked("Episode transaction history is incomplete")
        if any(t["hash"] not in hashes for t in relevant):
            raise ExecutionBlocked("External account activity contaminates cash evidence")
        if required_hashes - {t["hash"] for t in relevant}:
            raise ExecutionBlocked("Filled orders lack complete native transaction history")
        if any(not t.get("committed_at") for t in relevant):
            return None
        history = await self.gateway.cash_history(start - 120_000, end + 120_000)
        if not history:
            raise ExecutionBlocked("Transfer-inclusive cash history is unavailable")
        if "cash_counters" not in state:
            raise ExecutionBlocked("Recovered episode has no transfer-inclusive baseline")
        final_counters = cash_counters(history)
        if any(abs(final_counters[k] - state["cash_counters"][k]) > 1e-8 for k in CASH_FLOWS):
            raise ExecutionBlocked(
                "Transfers or unrelated cash activity contaminate episode evidence"
            )
        funding = await self.gateway.funding(start, end)
        # All signed quantities return to zero. Cash includes actual fees/funding;
        # native fee ticks remain raw rather than an invented conversion.
        gross = -sum(number(f["quantity"]) * number(f["price"]) * f["side"] for f in fills)
        net = s.collateral - state["collateral"]
        entry_volume = sum(
            number(f["quantity"]) * number(f["price"])
            for f in fills
            if next(r for r in self.store.orders() if r["client_id"] == f["client_id"])["payload"][
                "role"
            ]
            == "entry"
        )
        volume = sum(number(f["quantity"]) * number(f["price"]) for f in fills)
        if entry_volume <= 0:
            raise ExecutionBlocked("Cash episode contains no owned entry notional")
        episode = {
            **state,
            "end_wall_ns": s.observed_wall_ns,
            "ending_collateral": s.collateral,
            "gross_pnl": gross,
            "net_pnl": net,
            "net_bps": net / entry_volume * 1e4,
            "entry_notional": entry_volume,
            "traded_notional": volume,
            "all_in_cost_bps_per_fill": max(0, gross - net) / volume * 1e4,
            "funding_events": funding,
            "fills": fills,
            "transactions": [
                {
                    k: t[k]
                    for k in (
                        "hash",
                        "nonce",
                        "account_index",
                        "api_key_index",
                        "transaction_time",
                        "executed_at",
                        "committed_at",
                    )
                    if k in t
                }
                for t in relevant
            ],
            "cash_history": history,
            "account_tier": s.tier,
            "reconciled": True,
            "account_index": s.account_index,
            "source": "exchange_reconciled_flat_to_flat_cash",
        }
        self.store.episode(state["id"], episode)
        self.store.set("live_episode", None)
        return episode

    def status(self):
        return {
            "account": asdict(self.snapshot) if self.snapshot else None,
            "orders": self.store.orders(),
            "pending_transactions": self.store.pending_transactions(),
            "episodes": len(self.store.episodes()),
        }
