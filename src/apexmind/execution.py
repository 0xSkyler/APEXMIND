"""Narrow SDK adapter. No transfers, approvals, or withdrawal methods exposed.

Live submission is disabled until native integration and evidence gates pass.
"""

import asyncio
import os
from pathlib import Path
import stat
import time
from .governance import validate_evidence


class ExecutionBlocked(RuntimeError):
    pass


def execution_choice(forecast, spread_bps, signal_lifetime_ms, passive_evidence=None):
    aggressive = forecast["lower_mean"] - spread_bps / 2
    if forecast["execution_delay_ms"] >= signal_lifetime_ms:
        return {"mode": "reject", "reason": "signal_expires_before_execution"}
    if passive_evidence is not None:
        required = ("fill_probability", "delay_ms", "adverse_selection_bps", "measured_samples")
        if any(k not in passive_evidence for k in required):
            raise ValueError("Incomplete passive execution evidence")
        probability = passive_evidence["fill_probability"]
        if not 0 <= probability <= 1 or passive_evidence["measured_samples"] < 100:
            return {
                "mode": "aggressive" if aggressive > 0 else "reject",
                "reason": "insufficient_passive_evidence",
            }
        passive = probability * (
            forecast["lower_mean"] + spread_bps / 2 - passive_evidence["adverse_selection_bps"]
        )
        if passive_evidence["delay_ms"] < signal_lifetime_ms and passive > max(0, aggressive):
            return {"mode": "passive", "reason": "measured_conditional_fill_advantage"}
    return {
        "mode": "aggressive" if aggressive > 0 else "reject",
        "reason": "conservative_aggressive_only_without_passive_measurements",
    }


def credential_presence():
    return {
        name: bool(os.environ.get(name))
        for name in ("LIGHTER_ACCOUNT_INDEX", "LIGHTER_API_KEY_INDEX", "LIGHTER_SIGNING_KEY_FILE")
    }


def read_signing_key():
    path = Path(os.environ["LIGHTER_SIGNING_KEY_FILE"])
    # Open without following a final symlink, then check the opened inode.
    # Checking the pathname before reading creates a substitution race.
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise ExecutionBlocked("Signing key must be a protected regular local file") from None
    with os.fdopen(descriptor) as file:
        info = os.fstat(file.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise ExecutionBlocked("Signing key must be a protected regular local file")
        if stat.S_IMODE(info.st_mode) & 0o077 or info.st_uid != os.getuid():
            raise ExecutionBlocked(
                "Signing key file must have mode 0600 or stricter and belong to the service user"
            )
        key = file.read(256).strip()
    # Proxy placeholders are not local signing keys. Values are never logged.
    material = key.removeprefix("0x")
    if len(material) != 64 or any(c not in "0123456789abcdefABCDEF" for c in material):
        raise ExecutionBlocked("Invalid local signing key encoding")
    return key


class LighterBroker:
    def __init__(self, client, store, instruments, account_index, requests_per_second=1):
        self.client, self.store, self.instruments = client, store, instruments
        self.account_index = account_index
        self.lock = asyncio.Lock()
        self.interval = 1 / requests_per_second
        self.last_submission = 0.0

    @classmethod
    def from_environment(cls, url, store, instruments):
        if not all(credential_presence().values()):
            raise ExecutionBlocked(
                "Account index, API key index and secure local signer file required"
            )
        import lighter

        client = lighter.SignerClient(
            url=url,
            account_index=int(os.environ["LIGHTER_ACCOUNT_INDEX"]),
            api_private_keys={int(os.environ["LIGHTER_API_KEY_INDEX"]): read_signing_key()},
        )
        return cls(client, store, instruments, int(os.environ["LIGHTER_ACCOUNT_INDEX"]))

    async def submit(self, intent, report_path, model_path, integration_path=None):
        evidence = validate_evidence(report_path, model_path, integration_path)
        if not evidence["allowed"]:
            raise ExecutionBlocked("Evidence gate: " + "; ".join(evidence["reasons"]))
        if self.store.unresolved():
            raise ExecutionBlocked("Unreconciled orders; no retries or new exposure allowed")
        if intent.get("mode") != "aggressive" or not intent.get("risk_approved"):
            raise ExecutionBlocked("Native passive execution and unapproved sizing are unavailable")
        instrument = self.instruments[intent["symbol"]]
        async with self.lock:
            await asyncio.sleep(max(0, self.last_submission + self.interval - time.monotonic()))
            client_id = str(intent["client_id"])
            self.store.order(client_id, intent["symbol"], "submitting", intent)
            started_mono, started_wall = time.monotonic_ns(), time.time_ns()
            try:
                _, response, error = await self.client.create_market_order_if_slippage(
                    market_index=instrument["market_id"],
                    client_order_index=int(client_id),
                    base_amount=int(intent["base_units"]),
                    max_slippage=intent["max_slippage_bps"] / 1e4,
                    is_ask=intent["side"] == -1,
                    reduce_only=bool(intent.get("reduce_only", False)),
                )
            except Exception:
                self.store.order(client_id, intent["symbol"], "unknown", intent)
                # A network exception might follow acceptance; never blindly resubmit.
                raise ExecutionBlocked(
                    "Submission outcome unknown; reconciliation required"
                ) from None
            finally:
                self.last_submission = time.monotonic()
            if error or response is None or response.code != 200:
                self.store.order(client_id, intent["symbol"], "unknown", intent)
                raise ExecutionBlocked("Exchange response requires reconciliation")
            self.store.order(
                client_id,
                intent["symbol"],
                "acknowledged",
                {
                    **intent,
                    "submitted_mono_ns": started_mono,
                    "submitted_wall_ns": started_wall,
                    "ack_mono_ns": time.monotonic_ns(),
                    "tx_hash": response.tx_hash,
                },
            )
            # An acknowledgement is never recorded as an executed fill.
            return {"client_id": client_id, "status": "acknowledged"}

    def reconcile_fill(self, client_id, exchange_trade):
        if exchange_trade["account_index"] != self.account_index:
            raise ExecutionBlocked("Execution belongs to another account")
        pending = next(
            (r for r in self.store.unresolved() if r["client_id"] == str(client_id)), None
        )
        if pending is None:
            return False
        payload = pending["payload"]
        key = f"lighter:{exchange_trade['trade_id']}:{self.account_index}"
        payload = {
            **payload,
            "trade": exchange_trade,
            "observed_fill_mono_ns": time.monotonic_ns(),
            "observed_fill_wall_ns": time.time_ns(),
        }
        if not self.store.execution(key, payload):
            return False
        # Caller must supply exchange-verified cumulative fill, not one partial
        # trade presented as an entire order. Partial orders remain unresolved.
        complete = (
            exchange_trade.get("cumulative_base_units", 0) >= pending["payload"]["base_units"]
        )
        self.store.order(
            str(client_id), pending["symbol"], "filled" if complete else "partial", payload
        )
        return True

    async def close(self):
        await self.client.close()
