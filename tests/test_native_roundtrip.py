"""Scripted HTTPS exchange fixture, not an actual exchange integration result."""

import asyncio
import json
import time

from apexmind.broker import LiveBroker
from apexmind.config import Config
from apexmind.native import NativeGateway
from apexmind.store import Store
from test_live_system import BOOK, FORECAST, INSTRUMENT, Response, raw_order, trade


class LocalSigner:
    def create_auth_token_with_expiry(self, **kwargs):
        return "fixture-authentication", None

    def sign_create_grouped_orders(self, kind, orders, **kwargs):
        assert kind == 1
        payload = [{name: getattr(o, name) for name, _ in o._fields_} for o in orders]
        return (
            28,
            json.dumps({"orders": payload, "nonce": kwargs["nonce"]}),
            "tx" + str(kwargs["nonce"]),
            None,
        )

    def sign_create_order(self, **kwargs):
        return 14, json.dumps(kwargs), "tx" + str(kwargs["nonce"]), None

    async def close(self):
        pass


class ScriptedHTTPS:
    def __init__(self, store):
        self.store, self.nonce = store, 1
        self.orders, self.trades, self.transactions = [], {}, []
        self.quantity, self.collateral = 0.0, 1000.0
        self.authenticated_reads = 0

    def get(self, url, params, headers):
        path = url.rsplit("/", 1)[-1]
        if headers:
            assert headers["Authorization"] == "fixture-authentication"
            self.authenticated_reads += 1
        if path == "nextNonce":
            return Response({"nonce": self.nonce})
        if path == "accountLimits":
            return Response(
                dict(user_tier="standard", current_maker_fee_tick=0, current_taker_fee_tick=0)
            )
        if path == "account":
            positions = (
                [
                    dict(
                        symbol="BTC",
                        market_id=1,
                        sign=1,
                        position=str(self.quantity),
                        avg_entry_price="100",
                        position_value=str(self.quantity * 100),
                        unrealized_pnl="0",
                        liquidation_price="50",
                        margin_mode=0,
                        initial_margin_fraction="10000",
                    )
                ]
                if self.quantity
                else []
            )
            return Response(
                {
                    "accounts": [
                        dict(
                            account_index=123,
                            index=123,
                            account_trading_mode=0,
                            collateral=str(self.collateral),
                            available_balance=str(self.collateral - self.quantity * 100),
                            cross_initial_margin_requirement=str(self.quantity * 100),
                            cross_maintenance_margin_requirement=str(self.quantity * 5),
                            positions=positions,
                            transaction_time=time.time_ns() // 1_000_000,
                        )
                    ]
                }
            )
        if path == "accountOrders":
            identifiers = params["client_order_indexes"].split(",")
            return Response(
                {"orders": [o for o in self.orders if str(o["client_order_index"]) in identifiers]}
            )
        if path == "accountActiveOrders":
            return Response({"orders": [o for o in self.orders if o["status"] == "open"]})
        if path == "trades":
            return Response({"trades": self.trades.get(str(params["order_index"]), [])})
        if path == "tx":
            return Response(next(t for t in self.transactions if t["hash"] == params["value"]))
        if path == "accountTxs":
            return Response({"txs": self.transactions})
        if path == "pnl":
            assert params["count_back"] >= 3
            return Response(
                {
                    "pnl": [
                        dict(
                            timestamp=time.time_ns() // 1_000_000,
                            inflow=1000,
                            outflow=0,
                            pool_inflow=0,
                            pool_outflow=0,
                            spot_inflow=0,
                            spot_outflow=0,
                            staking_inflow=0,
                            staking_outflow=0,
                            staking_pnl=0,
                            pool_pnl=0,
                            trade_spot_pnl=0,
                        )
                    ]
                }
            )
        if path == "positionFunding":
            return Response({"position_fundings": []})
        raise AssertionError("Unexpected native GET: " + path)

    def post(self, url, data, headers):
        assert url.endswith("/sendTx") and self.store.pending_transactions()
        body = json.loads(data["tx_info"])
        assert body["nonce"] == self.nonce
        tx_hash = "tx" + str(self.nonce)
        self.transactions.append(
            dict(
                hash=tx_hash,
                account_index=123,
                api_key_index=2,
                nonce=self.nonce,
                executed_at=1,
                committed_at=1,
                status=1,
                transaction_time=time.time_ns() // 1_000_000,
            )
        )
        self.nonce += 1
        if "orders" in body:
            parent, stop = body["orders"]
            quantity = parent["BaseAmount"] / 100
            self.quantity = quantity
            identifier = str(parent["ClientOrderIndex"])
            self.orders.append(
                raw_order(identifier, quantity=str(quantity), filled=str(quantity), status="filled")
            )
            self.orders.append(
                raw_order(
                    str(stop["ClientOrderIndex"]), side=-1, quantity=str(quantity), role="stop"
                )
            )
            fill = trade(identifier, size=str(quantity), price="100", tid="1")
            fill["tx_hash"] = tx_hash
            self.trades[str(int(identifier) + 100)] = [fill]
        else:
            assert body["reduce_only"] and body["is_ask"]
            quantity = body["base_amount"] / 100
            assert quantity == self.quantity
            self.quantity = 0.0
            self.collateral += quantity * (101 - 100) - 0.02
            identifier = str(body["client_order_index"])
            self.orders.append(
                raw_order(
                    identifier,
                    side=-1,
                    quantity=str(quantity),
                    filled=str(quantity),
                    status="filled",
                    role="exit",
                )
            )
            fill = trade(identifier, size=str(quantity), price="101", side=-1, tid="2")
            fill["tx_hash"] = tx_hash
            self.trades[str(int(identifier) + 100)] = [fill]
            self.orders[1]["status"] = "canceled-reduce-only"
        return Response({"code": 200, "tx_hash": tx_hash})


def test_signed_native_roundtrip_reconciles_cash_and_survives_restart(tmp_path):
    async def exercise():
        store = Store(tmp_path / "native.sqlite")
        http = ScriptedHTTPS(store)
        gateway = NativeGateway(
            "https://mainnet.zklighter.elliot.ai",
            LocalSigner(),
            store,
            123,
            2,
            request_rate=10000,
            transaction_rate=10000,
            http=http,
        )
        broker = LiveBroker(gateway, Config(), [INSTRUMENT])
        await broker.reconcile()
        await broker.settle_episode("fixture-model")
        await broker.enter("BTC", 1, 0.1, BOOK, FORECAST, "fixture-model")
        assert not store.fills()  # Acknowledged, not executed locally yet.
        await broker.reconcile()
        assert broker.protected("BTC") and not store.pending_transactions()
        assert len(store.fills()) == 1
        await broker.close_position("BTC", 101)
        await broker.reconcile()
        assert not broker.snapshot.positions and not store.pending_transactions()
        episode = await broker.settle_episode("fixture-model")
        assert abs(episode["net_pnl"] - 0.08) < 1e-8 and len(episode["fills"]) == 2
        assert http.authenticated_reads >= 6
        gateway.run = "restart"
        await broker.reconcile()
        assert len(store.fills()) == 2 and len(store.episodes()) == 1
        assert "fixture-authentication" not in json.dumps(broker.status())
        store.close()

    asyncio.run(exercise())
