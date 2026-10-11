"""Official local signing plus verified HTTPS transport; POST is never retried."""

import asyncio
from dataclasses import asdict, replace
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import hashlib
import json
import os
import time
import uuid
import aiohttp
from .account import AccountError, AccountSnapshot
from .execution import ExecutionBlocked, credential_presence, read_signing_key
from .feeds import RequestBudget, session


def units(value, decimals, up=False):
    return int(
        (Decimal(str(value)) * Decimal(10) ** decimals).to_integral_value(
            rounding=ROUND_UP if up else ROUND_DOWN
        )
    )


class NativeGateway:
    def __init__(
        self, url, signer, store, index, key_index, request_rate=1, transaction_rate=1, http=None
    ):
        self.url, self.signer, self.store = url.rstrip("/"), signer, store
        self.account_index, self.key_index = index, key_index
        self.http = http
        self.owns_http = http is None
        self.requests = RequestBudget(request_rate)
        self.transactions = RequestBudget(transaction_rate)
        self.lock = asyncio.Lock()
        self.run = uuid.uuid4().hex
        self.limits = None
        self.last_limits_ns = 0
        binding = {"account_index": index, "key_index": key_index, "url": self.url}
        existing = store.get("live_binding")
        if existing and existing != binding:
            raise ExecutionBlocked("Ledger is bound to another account, key, or network")
        store.set("live_binding", binding)

    @classmethod
    async def from_environment(cls, url, store, config):
        if not all(credential_presence().values()):
            raise ExecutionBlocked(
                "Configure LIGHTER_ACCOUNT_INDEX, LIGHTER_API_KEY_INDEX, and LIGHTER_SIGNING_KEY_FILE"
            )
        import lighter

        index = int(os.environ["LIGHTER_ACCOUNT_INDEX"])
        key_index = int(os.environ["LIGHTER_API_KEY_INDEX"])
        if index < 0 or not 0 <= key_index < 255:
            raise ExecutionBlocked("Invalid account or API key index")
        key = read_signing_key()
        try:
            signer = lighter.SignerClient(
                url=url, account_index=index, api_private_keys={key_index: key}
            )
        except Exception:
            raise ExecutionBlocked("Local signer initialization failed") from None
        try:
            return cls(
                url,
                signer,
                store,
                index,
                key_index,
                config.live.request_per_second,
                config.live.transaction_per_second,
            )
        except Exception:
            await signer.close()
            raise

    def auth(self):
        token, error = self.signer.create_auth_token_with_expiry(api_key_index=self.key_index)
        if error or not token:
            raise ExecutionBlocked("Local authentication signing failed")
        return token

    async def request(self, path, params=None, private=False, form=None, preflight=None):
        if self.http is None:
            self.http = session()
        await self.requests.wait()
        if preflight:
            preflight()
        headers = {"Authorization": self.auth()} if private else {}
        try:
            request = (
                self.http.post(self.url + path, data=form, headers=headers)
                if form is not None
                else self.http.get(self.url + path, params=params, headers=headers)
            )
            async with request as response:
                if response.status == 429:
                    self.requests.interval = min(60, self.requests.interval * 2)
                    raise ExecutionBlocked(
                        "Exchange throttled request; rate reduced and POST not retried"
                    )
                if response.status != 200:
                    raise ExecutionBlocked(f"Exchange request failed (HTTP {response.status})")
                data = await response.json()
        except (aiohttp.ClientError, TimeoutError):
            raise ExecutionBlocked("Exchange transport failed; no submission retry") from None
        if data.get("code", 200) != 200:
            raise ExecutionBlocked("Exchange returned an unsuccessful result code")
        return data

    async def snapshot(self):
        if self.limits is None or time.monotonic_ns() - self.last_limits_ns > 60e9:
            self.limits = await self.request(
                "/api/v1/accountLimits", {"account_index": self.account_index}, True
            )
            self.last_limits_ns = time.monotonic_ns()
        cursor = None
        combined = None
        seen = set()
        observed_mono, observed_wall = time.monotonic_ns(), time.time_ns()
        for _ in range(100):
            params = {"by": "index", "value": str(self.account_index), "active_only": "false"}
            if cursor:
                params["cursor"] = cursor
            data = await self.request("/api/v1/account", params)
            accounts = [
                a for a in data["accounts"] if int(a["account_index"]) == self.account_index
            ]
            if len(accounts) != 1:
                raise AccountError("Expected one selected account per response page")
            page = accounts[0]
            if combined is None:
                combined = {**page, "positions": list(page["positions"])}
            else:
                if page["transaction_time"] != combined["transaction_time"]:
                    raise AccountError("Account changed during pagination; retry a fresh read")
                combined["positions"].extend(page["positions"])
            cursor = data.get("next_cursor")
            if not cursor:
                break
            if cursor in seen:
                raise AccountError("Account pagination repeated a cursor")
            seen.add(cursor)
        else:
            raise AccountError("Account pagination exceeded budget")
        snapshot = AccountSnapshot.parse(
            combined, self.account_index, str(self.limits["user_tier"])
        )
        snapshot = replace(snapshot, observed_mono_ns=observed_mono, observed_wall_ns=observed_wall)
        self.store.set("last_account_snapshot", asdict(snapshot))
        return snapshot

    async def active_orders(self):
        data = await self.request(
            "/api/v1/accountActiveOrders",
            {"account_index": self.account_index, "market_type": "perp"},
            True,
        )
        if data.get("next_cursor"):
            raise AccountError("Incomplete active order response")
        return data["orders"]

    async def orders(self, client_ids):
        results = []
        for start in range(0, len(client_ids), 20):
            data = await self.request(
                "/api/v1/accountOrders",
                {
                    "account_index": self.account_index,
                    "client_order_indexes": ",".join(client_ids[start : start + 20]),
                },
                True,
            )
            if data.get("next_cursor"):
                raise AccountError("Incomplete client order lookup")
            results.extend(data["orders"])
        return results

    async def trades(self, order_index):
        cursor = None
        results = []
        seen = set()
        for _ in range(100):
            params = {
                "account_index": self.account_index,
                "order_index": order_index,
                "sort_by": "timestamp",
                "sort_dir": "asc",
                "limit": 100,
                "aggregate": "false",
            }
            if cursor:
                params["cursor"] = cursor
            data = await self.request("/api/v1/trades", params, True)
            results.extend(data["trades"])
            cursor = data.get("next_cursor")
            if not cursor:
                return results
            if cursor in seen:
                raise AccountError("Trade pagination repeated a cursor")
            seen.add(cursor)
        raise AccountError("Trade pagination exceeded budget")

    async def account_transactions(self):
        return (
            await self.request(
                "/api/v1/accountTxs",
                {"by": "account_index", "value": str(self.account_index), "limit": 100},
                True,
            )
        )["txs"]

    async def transaction_status(self, tx_hash):
        return await self.request("/api/v1/tx", {"by": "hash", "value": tx_hash})

    async def cash_history(self, start_ms, end_ms):
        # Mainnet currently accepts hourly PnL buckets. Audit episodes are much
        # shorter, so widen their read window to include complete chart rows;
        # the cumulative cash counters still provide the requested baseline.
        resolution_ms = 3_600_000
        start_ms = min(start_ms, end_ms - 2 * resolution_ms)
        data = await self.request(
            "/api/v1/pnl",
            {
                "by": "index",
                "value": str(self.account_index),
                "resolution": "1h",
                "start_timestamp": start_ms,
                "end_timestamp": end_ms,
                "count_back": min(
                    10000, max(3, int((end_ms - start_ms) / resolution_ms) + 3)
                ),
                "ignore_transfers": "false",
            },
            True,
        )
        return data["pnl"]

    async def funding(self, start_ms, end_ms):
        results = []
        cursor = None
        seen = set()
        for _ in range(100):
            params = {
                "account_index": self.account_index,
                "limit": 100,
                "start_timestamp": start_ms,
                "end_timestamp": end_ms,
            }
            if cursor:
                params["cursor"] = cursor
            data = await self.request("/api/v1/positionFunding", params, True)
            results.extend(data["position_fundings"])
            cursor = data.get("next_cursor")
            if not cursor:
                return results
            if cursor in seen:
                raise AccountError("Funding pagination repeated a cursor")
            seen.add(cursor)
        raise AccountError("Funding pagination exceeded budget")

    async def signed_transaction(
        self, operation, payload, order_records=(), preflight=None, risk_reducing=False
    ):
        # Only the four trading operations implemented below can be signed.
        async with self.lock:
            await self.transactions.wait()
            nonce = int(
                (
                    await self.request(
                        "/api/v1/nextNonce",
                        {"account_index": self.account_index, "api_key_index": self.key_index},
                    )
                )["nonce"]
            )
            pending = self.store.pending_transactions()
            if pending and (not risk_reducing or nonce <= max(p["nonce"] for p in pending)):
                raise ExecutionBlocked("Uncertain transaction outcome; fresh exposure blocked")
            if preflight:
                preflight()
            kwargs = {"nonce": nonce, "api_key_index": self.key_index}
            if operation == "group":
                import lighter.signer_client

                orders = [lighter.signer_client.CreateOrderTxReq(**o) for o in payload["orders"]]
                signed = self.signer.sign_create_grouped_orders(1, orders, **kwargs)
            elif operation == "order":
                signed = self.signer.sign_create_order(**payload, **kwargs)
            elif operation == "cancel":
                signed = self.signer.sign_cancel_order(**payload, **kwargs)
            elif operation == "leverage":
                signed = self.signer.sign_update_leverage(**payload, **kwargs)
            else:
                raise ExecutionBlocked("Unsupported signing operation")
            tx_type, tx_info, tx_hash, error = signed
            if error or not tx_info or not tx_hash:
                raise ExecutionBlocked("Local transaction signing failed")
            now_mono, now_wall = time.monotonic_ns(), time.time_ns()
            journal = {
                "tx_hash": tx_hash,
                "operation": operation,
                "request": payload,
                "client_ids": [r["client_id"] for r in order_records],
                "submitted_mono_ns": now_mono,
                "submitted_wall_ns": now_wall,
                "run": self.run,
                "signed_payload_sha256": hashlib.sha256(tx_info.encode()).hexdigest(),
            }
            # Record identifiers before POST, atomically with the nonce journal.
            with self.store.db:
                for record in order_records:
                    self.store.db.execute(
                        "INSERT INTO orders VALUES(?,?,?,?,?)",
                        (
                            record["client_id"],
                            record["symbol"],
                            "submitting",
                            json.dumps({**record["payload"], **journal}),
                            now_wall,
                        ),
                    )
                self.store.db.execute(
                    "INSERT INTO live_transactions VALUES(?,?,?,?,?)",
                    (tx_hash, nonce, "submitting", json.dumps(journal), now_wall),
                )
            posted = False

            def final_preflight():
                nonlocal posted
                if preflight:
                    preflight()
                posted = True

            try:
                response = await self.request(
                    "/api/v1/sendTx",
                    form={"tx_type": str(tx_type), "tx_info": tx_info},
                    preflight=final_preflight,
                )
            except BaseException:
                status = "unknown" if posted else "rejected"
                self.store.transaction(tx_hash, nonce, status, journal)
                if not posted:
                    for record in order_records:
                        self.store.order(
                            record["client_id"],
                            record["symbol"],
                            "rejected",
                            {**record["payload"], **journal},
                        )
                    raise
                raise ExecutionBlocked(
                    "Submission outcome unknown; reconcile by client IDs before another entry"
                ) from None
            if response.get("tx_hash") != tx_hash:
                self.store.transaction(tx_hash, nonce, "unknown", journal)
                raise ExecutionBlocked("Transaction response identity mismatch")
            self.store.transaction(
                tx_hash, nonce, "acknowledged", {**journal, "ack_mono_ns": time.monotonic_ns()}
            )
            return {
                "tx_hash": tx_hash,
                "client_ids": journal["client_ids"],
                "status": "acknowledged",
            }

    async def close(self):
        if self.owns_http and self.http is not None:
            await self.http.close()
        await self.signer.close()
