"""Local API setup and guarded live-runner controls. No credentials are returned."""

import asyncio
from dataclasses import dataclass
import fcntl
from importlib.resources import files
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import signal
import sqlite3
import stat
import sys
import time

from aiohttp import web

from .config import load_config
from .execution import ExecutionBlocked
from .live import LedgerLock


class UIError(ValueError):
    pass


def index(value, maximum):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,20}", value.strip()):
        raise UIError("Enter a valid account index and API key index.")
    number = int(value)
    if number > maximum:
        raise UIError("Enter a valid account index and API key index.")
    return str(number)


class Credentials:
    def __init__(self, directory):
        self.path = Path(directory).absolute()
        self.path.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            self.fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            info = os.fstat(self.fd)
            if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
                raise UIError("The UI storage directory must belong to you and have mode 0700.")
            self.lease = self.open_file("ui.lock", os.O_RDWR | os.O_CREAT)
            fcntl.flock(self.lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, UIError):
            self.close()
            raise UIError("UI storage is unsafe or another UI is already using it.") from None

    def open_file(self, name, flags):
        fd = os.open(name, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=self.fd)
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o077
        ):
            os.close(fd)
            raise UIError("Saved API info must be in protected regular files.")
        return fd

    def read(self, name):
        with os.fdopen(self.open_file(name, os.O_RDONLY)) as source:
            data = source.read(4097)
        if len(data) > 4096:
            raise UIError("Saved API info is invalid. Save it again.")
        return data

    def load(self):
        try:
            raw = json.loads(self.read("account.json"))
            account = index(raw["account_index"], 2**63 - 1)
            api = index(raw["api_key_index"], 254)
            name = raw["key_file"]
            if not isinstance(name, str) or not re.fullmatch(r"signing-[0-9a-f]{32}\.key", name):
                raise UIError("Saved API info is invalid. Save it again.")
            if not re.fullmatch(r"(?:0x)?[0-9a-fA-F]{64}", self.read(name).strip()):
                raise UIError("Saved API key is invalid. Save it again.")
            return {"account_index": account, "api_key_index": api, "key_file": name}
        except FileNotFoundError:
            return None
        except (KeyError, TypeError, UnicodeError, json.JSONDecodeError, OSError):
            raise UIError("Saved API info could not be read. Save it again.") from None

    def write(self, name, content):
        with os.fdopen(self.open_file(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL), "w") as out:
            out.write(content)
            out.flush()
            os.fsync(out.fileno())

    def save(self, account, api, key):
        account, api = index(account, 2**63 - 1), index(api, 254)
        if not isinstance(key, str):
            raise UIError("Enter your API private key.")
        key = key.strip()
        if key and not re.fullmatch(r"(?:0x)?[0-9a-fA-F]{64}", key):
            raise UIError("The API private key must contain 64 hexadecimal characters.")
        try:
            old = self.load()
        except UIError:
            if not key:
                raise
            old = None
        if not key:
            if not old:
                raise UIError("Enter your API private key before saving.")
            if (account, api) != (old["account_index"], old["api_key_index"]):
                raise UIError("Enter the private key when changing account or API key index.")
        name = "signing-" + secrets.token_hex(16) + ".key" if key else old["key_file"]
        temporary = "account-" + secrets.token_hex(16) + ".tmp"
        saved = {"account_index": account, "api_key_index": api, "key_file": name}
        committed = False
        try:
            if key:
                self.write(name, key + "\n")
            self.write(temporary, json.dumps(saved))
            os.replace(temporary, "account.json", src_dir_fd=self.fd, dst_dir_fd=self.fd)
            committed = True
            os.fsync(self.fd)
        finally:
            for orphan in (temporary, name if key and not committed else None):
                if orphan:
                    try:
                        os.unlink(orphan, dir_fd=self.fd)
                    except FileNotFoundError:
                        pass
        if key and old and old["key_file"] != name:
            os.unlink(old["key_file"], dir_fd=self.fd)
            os.fsync(self.fd)

    def environment(self):
        saved = self.load()
        if not saved:
            raise UIError("Save your API info before starting.")
        return {
            **os.environ,
            "LIGHTER_ACCOUNT_INDEX": saved["account_index"],
            "LIGHTER_API_KEY_INDEX": saved["api_key_index"],
            "LIGHTER_SIGNING_KEY_FILE": str(self.path / saved["key_file"]),
            "OMP_NUM_THREADS": "2",
            "OPENBLAS_NUM_THREADS": "2",
            "MKL_NUM_THREADS": "2",
        }

    def close(self):
        for name in ("lease", "fd"):
            fd = getattr(self, name, None)
            if fd is not None:
                os.close(fd)
                setattr(self, name, None)


@dataclass(frozen=True)
class Settings:
    config_path: str
    directory: str = "secrets/ui"
    report: str = "reports/cvifa-001/report.json"
    model: str = "reports/cvifa-001/champion.joblib"
    latency: str = "data/latency.json"
    stage: str = "production"
    registry: str = "artifacts/registry.json"


class Controller:
    def __init__(self, settings):
        self.settings = settings
        if settings.stage not in ("pilot", "production"):
            raise UIError("Select pilot or production on the host.")
        self.config = load_config(settings.config_path)
        self.credentials = Credentials(settings.directory)
        self.lock = asyncio.Lock()
        self.task = self.process = None
        self.stopping = False
        self.phase = ""
        self.state, self.message = "Stopped", "Save your API info, then start."
        self.started_ns = 0

    @property
    def busy(self):
        return self.task is not None and not self.task.done()

    def status(self):
        state, message = self.state, self.message
        try:
            saved = self.credentials.load()
        except UIError:
            saved = None
            state, message = "Blocked", "Saved API info could not be read. Save it again."
        if self.busy and self.phase == "live" and not self.stopping:
            try:
                path = Path(self.config.live.ledger).absolute()
                with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.1) as db:
                    row = db.execute("SELECT value FROM state WHERE key='live_status'").fetchone()
                live = json.loads(row[0]) if row else {}
                stamp = live.get("heartbeat_wall_ns", 0)
                fresh = (
                    stamp >= self.started_ns
                    and 0
                    <= time.time_ns() - stamp
                    < max(10, self.config.live.account_poll_seconds * 4) * 1e9
                )
                if fresh:
                    if (
                        live.get("suspended")
                        or not live.get("gate", {}).get("allowed")
                        or not live.get("clock", {}).get("healthy")
                    ):
                        state, message = (
                            "Suspended",
                            "Trading is paused. Protection and recovery remain active.",
                        )
                    else:
                        state, message = (
                            "Running",
                            "Live runner active. Qualification and protection checks continue.",
                        )
                else:
                    state, message = "Checking", "Waiting for a fresh account and protection check."
            except (OSError, sqlite3.Error, ValueError, TypeError):
                pass
        return {
            "state": state,
            "message": message,
            "busy": self.busy,
            "key_saved": saved is not None,
            "account_index": saved["account_index"] if saved else "",
            "api_key_index": saved["api_key_index"] if saved else "",
        }

    async def save(self, data):
        async with self.lock:
            if self.busy:
                raise UIError("Stop the runner before changing API info.")
            lease = LedgerLock(self.config.live.ledger)
            try:
                self.credentials.save(
                    data.get("account_index"),
                    data.get("api_key_index"),
                    data.get("private_key", ""),
                )
            finally:
                lease.close()
            self.state, self.message = "Stopped", "API info saved securely on this host."
            return self.status()

    def prerequisites(self):
        from .account import clock_health
        from .evidence import research_gate
        from .latency import Latency

        s = self.settings
        if not all(Path(path).is_file() for path in (s.report, s.model, s.latency)):
            raise UIError(
                "Trading is blocked: a qualified strategy and measured latency must be prepared first."
            )
        try:
            report = json.loads(Path(s.report).read_text())
            gate = research_gate(report, s.model, self.config)
            if not gate["allowed"]:
                raise UIError("Strategy qualification failed: " + "; ".join(gate["reasons"]))
            if Latency.load(s.latency).synthetic:
                raise UIError("Trading requires real measured execution latency.")
        except (KeyError, TypeError, json.JSONDecodeError):
            raise UIError("Strategy or latency evidence is invalid. Prepare it again.") from None
        if not clock_health(self.config.live.max_clock_error_ms)["healthy"]:
            raise UIError("Clock synchronization must be verified before starting.")

    async def start(self):
        async with self.lock:
            if self.busy:
                return self.status()
            env = self.credentials.environment()
            try:
                await asyncio.to_thread(self.prerequisites)
            except (ValueError, OSError) as error:
                self.state, self.message = "Blocked", str(error)
                raise UIError(self.message) from None
            self.stopping = False
            self.state, self.message = "Starting", "Checking account authorization."
            self.task = asyncio.create_task(self.workflow(env))
            return self.status()

    @staticmethod
    async def consume(stream):
        tail = b""
        while data := await stream.read(4096):
            tail = (tail + data)[-16384:]
        return tail.decode(errors="replace")

    async def command(self, command, env, timeout=None):
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "apexmind",
            "--config",
            self.settings.config_path,
            *command,
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        readers = [
            asyncio.create_task(self.consume(stream))
            for stream in (self.process.stdout, self.process.stderr)
        ]
        try:
            if self.stopping and self.phase != "cleanup":
                self.process.send_signal(signal.SIGTERM)
            if timeout:
                try:
                    await asyncio.wait_for(self.process.wait(), timeout)
                except TimeoutError:
                    self.process.send_signal(signal.SIGTERM)
                    # Never kill a native submission or invent a rejected outcome.
                    await self.process.wait()
                    raise UIError(
                        "The account operation timed out. Verify recovery before restarting."
                    ) from None
            else:
                await self.process.wait()
            return self.process.returncode, *(await asyncio.gather(*readers))
        finally:
            await asyncio.gather(*readers, return_exceptions=True)
            self.process = None

    @staticmethod
    def failure(output, label):
        code, stdout, stderr = output
        for text in (stdout, stderr):
            for line in text.splitlines():
                if "Exchange request failed (HTTP" in line:
                    match = re.search(r"HTTP ([0-9]{3})", line)
                    if match:
                        return f"Exchange access failed (HTTP {match[1]}). Check authorization and connectivity."
                if "Another process owns" in line:
                    return "Another trading runner is already active."
                if "Ledger is bound to another" in line:
                    return (
                        "API info does not match the account bound to the existing trading state."
                    )
        return f"{label} failed. Check account access, qualification, and host prerequisites."

    async def workflow(self, env):
        s = self.settings
        paths = [
            "--report",
            s.report,
            "--model",
            s.model,
            "--latency",
            s.latency,
            "--stage",
            s.stage,
        ]
        authenticated = False
        problem = None
        try:
            stages = [
                ("auth", "Checking account authorization.", ["auth-check"], 90),
                (
                    "recover",
                    "Recovering owned exposure before startup.",
                    ["recover", "--timeout", "120"],
                    150,
                ),
                (
                    "qualify",
                    "Verifying trading qualification.",
                    ["live-check", "--account", *paths],
                    180,
                ),
                (
                    "live",
                    "Starting the live runner.",
                    ["live", *paths, "--registry", s.registry],
                    None,
                ),
            ]
            for phase, message, command, timeout in stages:
                if self.stopping:
                    break
                self.phase, self.message = phase, message
                if phase == "live":
                    self.started_ns = time.time_ns()
                output = await self.command(command, env, timeout)
                if output[0] and not self.stopping:
                    raise UIError(self.failure(output, message.rstrip(".")))
                if phase == "auth" and output[0] == 0:
                    authenticated = True
        except (OSError, ValueError, RuntimeError):
            # Do not echo arbitrary SDK output, credentials, or tracebacks into the page.
            problem = "Startup or trading stopped. Check account access, qualification, and host prerequisites."
            error = sys.exception()
            if isinstance(error, UIError):
                problem = str(error)
        finally:
            self.phase = "cleanup"
            if authenticated:
                self.state, self.message = "Stopping", "Verifying recovery of owned exposure."
                try:
                    output = await self.command(["recover", "--timeout", "120"], env, 150)
                    if output[0]:
                        problem = "Recovery is incomplete. Verify the actual account before restarting; native stops are retained."
                except (OSError, ValueError, RuntimeError):
                    problem = "Recovery could not be verified. Check the actual account before restarting."
            self.state = "Blocked" if problem else "Stopped"
            self.message = problem or (
                "Runner stopped. Owned exposure recovery completed."
                if authenticated
                else "Runner stopped. No trading was started."
            )
            self.phase = ""

    async def stop(self):
        async with self.lock:
            if self.busy:
                self.stopping = True
                self.state, self.message = (
                    "Stopping",
                    "Stopping the runner and verifying owned exposure.",
                )
                if self.process and self.process.returncode is None and self.phase != "cleanup":
                    try:
                        self.process.send_signal(signal.SIGTERM)
                    except ProcessLookupError:
                        pass
            return self.status()

    async def close(self):
        await self.stop()
        if self.task:
            await self.task
        self.credentials.close()


CONTROLLER = web.AppKey("controller", Controller)


def create_app(controller, port=8765):
    token = secrets.token_hex(32)
    allowed = {f"127.0.0.1:{port}", f"localhost:{port}"}

    @web.middleware
    async def security(request, handler):
        try:
            local = ipaddress.ip_address(request.remote).is_loopback
        except ValueError:
            local = False
        if (
            not local
            or request.host not in allowed
            or request.headers.get("Sec-Fetch-Site") == "cross-site"
        ):
            return web.json_response({"message": "Local access only."}, status=403)
        if request.path.startswith("/api/"):
            if not secrets.compare_digest(
                request.headers.get("X-APEX-Token", "").encode(), token.encode()
            ):
                return web.json_response({"message": "Reload this page to continue."}, status=403)
            if request.method == "POST" and (
                request.headers.get("Origin") != f"http://{request.host}"
                or request.content_type != "application/json"
            ):
                return web.json_response({"message": "Reload this page to continue."}, status=403)
        try:
            response = await handler(request)
        except (UIError, ExecutionBlocked) as error:
            response = web.json_response({"message": str(error)}, status=409)
        except (OSError, ValueError, TypeError):
            response = web.json_response(
                {"message": "The request could not be completed. Check the host setup."}, status=400
            )
        response.headers.update(
            {
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
                "Referrer-Policy": "no-referrer",
                "Content-Security-Policy": f"default-src 'none'; script-src 'nonce-{token}'; style-src 'nonce-{token}'; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
            }
        )
        return response

    app = web.Application(middlewares=[security], client_max_size=4096)
    app[CONTROLLER] = controller

    async def page(request):
        html = files("apexmind").joinpath("web/ui.html").read_text()
        return web.Response(text=html.replace("__TOKEN__", token), content_type="text/html")

    async def status(request):
        return web.json_response(controller.status())

    async def save(request):
        try:
            data = await request.json()
        except (ValueError, UnicodeError):
            raise UIError("Enter valid API info.") from None
        if not isinstance(data, dict) or set(data) - {
            "account_index",
            "api_key_index",
            "private_key",
        }:
            raise UIError("Enter valid API info.")
        return web.json_response(await controller.save(data))

    async def start(request):
        return web.json_response(await controller.start(), status=202)

    async def stop(request):
        return web.json_response(await controller.stop(), status=202)

    async def cleanup(app):
        await app[CONTROLLER].close()

    app.add_routes(
        [
            web.get("/", page),
            web.get("/api/status", status),
            web.post("/api/save", save),
            web.post("/api/start", start),
            web.post("/api/stop", stop),
        ]
    )
    app.on_cleanup.append(cleanup)
    return app


async def serve(settings, port=8765):
    if not 1024 <= port <= 65535:
        raise UIError("Use a local port from 1024 to 65535.")
    controller = Controller(settings)
    runner = web.AppRunner(create_app(controller, port), access_log=None)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    try:
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", port).start()
        print(f"APEX MIND UI: http://127.0.0.1:{port} (local access only)", flush=True)
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stopped.set)
        await stopped.wait()
    finally:
        await runner.cleanup()
        await controller.close()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
