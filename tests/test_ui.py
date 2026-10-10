"""Real local HTTP/process checks; signing material is a nonfunctional fixture."""

import asyncio
import json
import os
from pathlib import Path
import re
import stat
import sys
import time

import pytest
from aiohttp.test_utils import TestClient, TestServer, unused_port

from apexmind.store import Store
from apexmind.ui import Controller, Credentials, Settings, UIError, create_app

FAKE_KEY = "0x" + "ab" * 32


@pytest.fixture
def settings(tmp_path):
    # Copy the actual TOML with isolated ledger/data paths; no account is contacted.
    source = Path("config/default.toml").read_text()
    source = source.replace('ledger = "data/live.sqlite"', f'ledger = "{tmp_path}/live.sqlite"')
    source = source.replace(
        'database = "data/apex.sqlite"', f'database = "{tmp_path}/market.sqlite"'
    )
    config = tmp_path / "config.toml"
    config.write_text(source)
    return Settings(str(config), str(tmp_path / "private"), report=str(tmp_path / "missing.json"))


def test_api_save_is_private_persistent_and_rotates_without_returning_key(tmp_path):
    store = Credentials(tmp_path / "private")
    try:
        store.save("001", "2", FAKE_KEY)
        old = store.load()
        assert old["account_index"] == "1"
        assert "ab" * 32 not in json.dumps(old)
        assert stat.S_IMODE(store.path.stat().st_mode) == 0o700
        for name in ("account.json", old["key_file"]):
            assert stat.S_IMODE((store.path / name).stat().st_mode) == 0o600
        store.save("1", "2", "")
        assert store.load() == old
        with pytest.raises(UIError, match="changing account"):
            store.save("9", "2", "")
        with pytest.raises(UIError, match="64 hexadecimal"):
            store.save("1", "2", "invalid")
        assert store.load() == old
        store.save("1", "2", "cd" * 32)
        current = store.load()
        assert current["key_file"] != old["key_file"]
        assert not (store.path / old["key_file"]).exists()
        assert FAKE_KEY not in json.dumps(store.environment())
    finally:
        store.close()
    restarted = Credentials(tmp_path / "private")
    try:
        assert restarted.load() == current
    finally:
        restarted.close()


def test_failed_save_preserves_prior_identity_and_key(tmp_path, monkeypatch):
    store = Credentials(tmp_path / "private")
    try:
        store.save("1", "2", FAKE_KEY)
        prior = store.load()

        def fail(*args, **kwargs):
            raise OSError("simulated atomic replacement failure")

        monkeypatch.setattr(os, "replace", fail)
        with pytest.raises(OSError):
            store.save("3", "4", "cd" * 32)
        assert store.load() == prior
        assert len(list(store.path.glob("*.key"))) == 1
        assert not list(store.path.glob("*.tmp"))
    finally:
        store.close()


def test_symlinks_unsafe_permissions_and_duplicate_ui_are_rejected(tmp_path):
    root = tmp_path / "private"
    store = Credentials(root)
    try:
        with pytest.raises(UIError, match="another UI"):
            Credentials(root)
        store.save("1", "2", FAKE_KEY)
        key = root / store.load()["key_file"]
        elsewhere = tmp_path / "outside.key"
        key.replace(elsewhere)
        key.symlink_to(elsewhere)
        with pytest.raises(UIError):
            store.load()
        key.unlink()
        elsewhere.replace(key)
        key.chmod(0o644)
        with pytest.raises(UIError):
            store.load()
    finally:
        store.close()
    link = tmp_path / "link"
    link.symlink_to(root, target_is_directory=True)
    with pytest.raises(UIError):
        Credentials(link)


@pytest.mark.parametrize(
    "account,api", [("-1", "2"), ("1", "255"), ("1", "2.5"), (None, "2"), (str(2**63), "2")]
)
def test_invalid_indices_never_create_key_files(tmp_path, account, api):
    store = Credentials(tmp_path / "private")
    try:
        with pytest.raises(UIError):
            store.save(account, api, FAKE_KEY)
        assert not list(store.path.glob("*.key"))
    finally:
        store.close()


def test_http_save_requires_local_host_origin_token_and_never_echoes_key(settings):
    async def check():
        controller = Controller(settings)
        port = unused_port()
        client = TestClient(TestServer(create_app(controller, port), host="127.0.0.1", port=port))
        await client.start_server()
        try:
            response = await client.get("/")
            html = await response.text()
            token = re.search(r'name="apex-token" content="([0-9a-f]+)"', html)[1]
            assert "frame-ancestors 'none'" in response.headers["Content-Security-Policy"]
            assert response.headers["Cache-Control"] == "no-store"
            body = {"account_index": "1", "api_key_index": "2", "private_key": FAKE_KEY}
            origin = f"http://127.0.0.1:{port}"
            headers = {"Origin": origin, "X-APEX-Token": token}
            assert (await client.post("/api/save", json=body)).status == 403
            assert (
                await client.post("/api/save", json=body, headers={**headers, "X-APEX-Token": "é"})
            ).status == 403
            assert (
                await client.post(
                    "/api/save", json=body, headers={**headers, "Origin": "https://foreign.example"}
                )
            ).status == 403
            assert (
                await client.post(
                    "/api/save", json=body, headers={**headers, "Host": "foreign.example"}
                )
            ).status == 403
            assert (
                await client.post(
                    "/api/save", json=body, headers={**headers, "Sec-Fetch-Site": "cross-site"}
                )
            ).status == 403
            assert controller.credentials.load() is None
            response = await client.post("/api/save", json=body, headers=headers)
            assert response.status == 200
            text = await response.text()
            assert FAKE_KEY not in text and "ab" * 32 not in text
            assert json.loads(text)["key_saved"]
            assert (await client.get("/api/status")).status == 403
            response = await client.get("/api/status", headers={"X-APEX-Token": token})
            assert FAKE_KEY not in await response.text()
            response = await client.post("/api/start", json={}, headers=headers)
            assert response.status == 409
            assert "qualified strategy" in (await response.json())["message"]
            assert controller.task is None
            assert controller.state == "Blocked"
        finally:
            await client.close()

    asyncio.run(check())


def test_start_is_single_and_stop_signals_real_child_then_recovers(settings, monkeypatch):
    async def check():
        controller = Controller(settings)
        controller.credentials.save("1", "2", FAKE_KEY)
        monkeypatch.setattr(controller, "prerequisites", lambda: None)
        original = asyncio.create_subprocess_exec
        commands = []

        async def fixture_child(*args, **kwargs):
            # Execute an actual child with the manager's pipes/signals. No exchange calls.
            command = args[5]
            commands.append(command)
            assert all(FAKE_KEY not in argument for argument in args)
            assert FAKE_KEY not in json.dumps(kwargs["env"])
            script = (
                "import signal; signal.signal(signal.SIGTERM, lambda *_: exit(0)); signal.pause()"
                if command == "live"
                else "print('scripted native check')"
            )
            return await original(sys.executable, "-c", script, **kwargs)

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fixture_child)
        try:
            await controller.start()
            first = controller.task
            await controller.start()
            assert controller.task is first
            with pytest.raises(UIError, match="Stop the runner"):
                await controller.save(
                    {"account_index": "1", "api_key_index": "2", "private_key": ""}
                )
            async with asyncio.timeout(5):
                while controller.phase != "live" or controller.process is None:
                    await asyncio.sleep(0.01)
            assert controller.status()["state"] != "Running"  # no real heartbeat
            await controller.stop()
            await asyncio.wait_for(first, 5)
            assert commands == ["auth-check", "recover", "live-check", "live", "recover"]
            assert controller.state == "Stopped"
            assert not controller.busy
            assert "recovery completed" in controller.message
        finally:
            await controller.close()

    asyncio.run(check())


def test_failed_native_gate_never_launches_live_and_preserves_recovery(settings, monkeypatch):
    async def check():
        controller = Controller(settings)
        controller.credentials.save("1", "2", FAKE_KEY)
        monkeypatch.setattr(controller, "prerequisites", lambda: None)
        commands = []

        async def command(args, env, timeout=None):
            commands.append(args[0])
            return (
                (3, "", "unknown SDK output: " + FAKE_KEY)
                if args[0] == "live-check"
                else (0, "", "")
            )

        monkeypatch.setattr(controller, "command", command)
        try:
            await controller.start()
            await controller.task
            assert commands == ["auth-check", "recover", "live-check", "recover"]
            assert controller.state == "Blocked"
            assert FAKE_KEY not in json.dumps(controller.status())
        finally:
            await controller.close()

    asyncio.run(check())


def test_status_needs_current_healthy_heartbeat(settings):
    async def check():
        controller = Controller(settings)
        # An unrelated old heartbeat cannot turn a starting runner into Running.
        wait = asyncio.Event()
        controller.task = asyncio.create_task(wait.wait())
        controller.phase = "live"
        controller.started_ns = time.time_ns()
        store = Store(controller.config.live.ledger)
        try:
            status = {
                "gate": {"allowed": True},
                "clock": {"healthy": True},
                "heartbeat_wall_ns": controller.started_ns - 1,
            }
            store.set("live_status", status)
            assert controller.status()["state"] != "Running"
            store.set("live_status", {**status, "heartbeat_wall_ns": time.time_ns()})
            assert controller.status()["state"] == "Running"
            store.set(
                "live_status",
                {**status, "heartbeat_wall_ns": time.time_ns(), "clock": {"healthy": False}},
            )
            assert controller.status()["state"] == "Suspended"
        finally:
            store.close()
            wait.set()
            await controller.close()

    asyncio.run(check())
