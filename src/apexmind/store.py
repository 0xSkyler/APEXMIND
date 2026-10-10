from dataclasses import asdict, dataclass
import json
from pathlib import Path
import sqlite3
import time
import uuid


@dataclass(frozen=True)
class Event:
    run: str
    venue: str
    symbol: str
    kind: str
    wall_ns: int
    mono_ns: int
    exchange_ns: int | None
    payload: dict


class Clock:
    def __init__(self, max_step_ms: float = 50):
        self.run = uuid.uuid4().hex
        self.offset = time.time_ns() - time.monotonic_ns()
        self.max_step_ns = max_step_ms * 1e6
        self.healthy = True

    def event(
        self, venue: str, symbol: str, kind: str, payload: dict, exchange_ns: int | None = None
    ) -> Event:
        mono, wall = time.monotonic_ns(), time.time_ns()
        # A wall-clock step invalidates this run until the collector restarts. This does
        # not establish exchange clock synchronization; external NTP is required.
        if abs(wall - mono - self.offset) > self.max_step_ns:
            self.healthy = False
        return Event(
            self.run,
            venue,
            symbol,
            kind,
            wall,
            mono,
            exchange_ns,
            {**payload, "clock_healthy": self.healthy},
        )


class Store:
    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS events(
          id INTEGER PRIMARY KEY, run TEXT NOT NULL, venue TEXT NOT NULL,
          symbol TEXT NOT NULL, kind TEXT NOT NULL, wall_ns INTEGER NOT NULL,
          mono_ns INTEGER NOT NULL, exchange_ns INTEGER, payload TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS event_run ON events(run, mono_ns, id);
        CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS orders(
          client_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, status TEXT NOT NULL,
          payload TEXT NOT NULL, updated_ns INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS executions(
          id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS live_transactions(
          hash TEXT PRIMARY KEY, nonce INTEGER NOT NULL, status TEXT NOT NULL,
          payload TEXT NOT NULL, updated_ns INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS live_fills(
          id TEXT PRIMARY KEY, client_id TEXT NOT NULL, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS live_episodes(
          id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        """)

    def append(self, event: Event):
        row = asdict(event)
        row["payload"] = json.dumps(row["payload"], allow_nan=False)
        with self.db:
            self.db.execute(
                "INSERT INTO events(run,venue,symbol,kind,wall_ns,mono_ns,"
                "exchange_ns,payload) VALUES(?,?,?,?,?,?,?,?)",
                tuple(row.values()),
            )

    def events(self, limit: int | None = None):
        # Arrival order, never exchange time; mono clocks are only comparable
        # inside one collection run. The feature engine resets between runs.
        query = (
            "SELECT run,venue,symbol,kind,wall_ns,mono_ns,exchange_ns,payload "
            "FROM events WHERE kind!='raw' ORDER BY id"
        )
        if limit is not None:
            query = (
                "SELECT run,venue,symbol,kind,wall_ns,mono_ns,exchange_ns,payload FROM "
                "(SELECT * FROM events WHERE kind!='raw' ORDER BY id DESC LIMIT ?) ORDER BY id"
            )
        for row in self.db.execute(query, (limit,) if limit is not None else ()):
            yield Event(*row[:-1], json.loads(row[-1]))

    def paper_entry(self, record, portfolio_state):
        with self.db:
            inserted = self.db.execute(
                "INSERT OR IGNORE INTO executions VALUES(?,?)",
                ("intent:" + record["client_id"], json.dumps({"mode": "paper_intent"})),
            ).rowcount
            if not inserted:
                return False
            self.db.execute(
                "INSERT INTO orders VALUES(?,?,?,?,?)",
                (
                    record["client_id"],
                    record["symbol"],
                    "pending",
                    json.dumps(record),
                    time.time_ns(),
                ),
            )
            self.db.execute(
                "INSERT INTO state VALUES('paper_portfolio',?) ON CONFLICT(key) "
                "DO UPDATE SET value=excluded.value",
                (json.dumps(portfolio_state),),
            )
        return True

    def stream_commit(self, state, order=None, execution=None):
        with self.db:
            self.db.execute(
                "INSERT INTO state VALUES('stream_paper',?) ON CONFLICT(key) "
                "DO UPDATE SET value=excluded.value",
                (json.dumps(state, allow_nan=False),),
            )
            if order:
                client_id, symbol, status, payload = order
                self.db.execute(
                    "INSERT INTO orders VALUES(?,?,?,?,?) ON CONFLICT(client_id) "
                    "DO UPDATE SET status=excluded.status,payload=excluded.payload,updated_ns=excluded.updated_ns",
                    (
                        client_id,
                        symbol,
                        status,
                        json.dumps(payload, allow_nan=False),
                        time.time_ns(),
                    ),
                )
            if execution:
                self.db.execute(
                    "INSERT OR IGNORE INTO executions VALUES(?,?)",
                    (execution[0], json.dumps(execution[1], allow_nan=False)),
                )

    def paper_close(self, record, portfolio_state):
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO executions VALUES(?,?)",
                (record["client_id"], json.dumps(record)),
            )
            self.db.execute(
                "UPDATE orders SET status='filled',payload=?,updated_ns=? WHERE client_id=?",
                (json.dumps(record), time.time_ns(), record["client_id"]),
            )
            self.db.execute(
                "INSERT INTO state VALUES('paper_portfolio',?) ON CONFLICT(key) "
                "DO UPDATE SET value=excluded.value",
                (json.dumps(portfolio_state),),
            )

    def get(self, key: str, default=None):
        row = self.db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key: str, value):
        with self.db:
            self.db.execute(
                "INSERT INTO state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, json.dumps(value, allow_nan=False)),
            )

    def order(self, client_id: str, symbol: str, status: str, payload: dict):
        with self.db:
            self.db.execute(
                "INSERT INTO orders VALUES(?,?,?,?,?) ON CONFLICT(client_id) "
                "DO UPDATE SET status=excluded.status,payload=excluded.payload,"
                "updated_ns=excluded.updated_ns",
                (client_id, symbol, status, json.dumps(payload, allow_nan=False), time.time_ns()),
            )

    def unresolved(self):
        return [
            dict(client_id=r[0], symbol=r[1], status=r[2], payload=json.loads(r[3]))
            for r in self.db.execute(
                "SELECT client_id,symbol,status,payload FROM orders "
                "WHERE status NOT IN ('filled','cancelled','rejected')"
            )
        ]

    def execution(self, execution_id: str, payload: dict) -> bool:
        with self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO executions VALUES(?,?)",
                (execution_id, json.dumps(payload, allow_nan=False)),
            )
        return cursor.rowcount == 1

    def summary(self):
        return {
            "events": self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            "kinds": dict(self.db.execute("SELECT kind,COUNT(*) FROM events GROUP BY kind")),
            "unresolved_orders": len(self.unresolved()),
        }

    def close(self):
        self.db.close()

    def allocate_ids(self, count=1):
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            row = self.db.execute(
                "SELECT value FROM state WHERE key='live_client_counter'"
            ).fetchone()
            first = int(json.loads(row[0])) + 1 if row else time.time_ns() // 1_000_000
            if first + count >= 2**48:
                raise RuntimeError("Client order identifier space exhausted")
            self.db.execute(
                "INSERT INTO state VALUES('live_client_counter',?) ON CONFLICT(key) "
                "DO UPDATE SET value=excluded.value",
                (json.dumps(first + count - 1),),
            )
        return [str(i) for i in range(first, first + count)]

    def transaction(self, tx_hash, nonce, status, payload):
        with self.db:
            self.db.execute(
                "INSERT INTO live_transactions VALUES(?,?,?,?,?) ON CONFLICT(hash) "
                "DO UPDATE SET status=excluded.status,payload=excluded.payload,updated_ns=excluded.updated_ns",
                (tx_hash, nonce, status, json.dumps(payload, allow_nan=False), time.time_ns()),
            )

    def pending_transactions(self):
        return [
            {"hash": r[0], "nonce": r[1], "status": r[2], "payload": json.loads(r[3])}
            for r in self.db.execute(
                "SELECT hash,nonce,status,payload FROM live_transactions "
                "WHERE status NOT IN ('resolved','rejected')"
            )
        ]

    def orders(self):
        return [
            {"client_id": r[0], "symbol": r[1], "status": r[2], "payload": json.loads(r[3])}
            for r in self.db.execute("SELECT client_id,symbol,status,payload FROM orders")
        ]

    def sync_live_order(self, record, fills):
        with self.db:
            for fill in fills:
                self.db.execute(
                    "INSERT OR IGNORE INTO live_fills VALUES(?,?,?)",
                    (fill["id"], record["client_id"], json.dumps(fill, allow_nan=False)),
                )
            self.db.execute(
                "UPDATE orders SET status=?,payload=?,updated_ns=? WHERE client_id=?",
                (
                    record["status"],
                    json.dumps(record["payload"], allow_nan=False),
                    time.time_ns(),
                    record["client_id"],
                ),
            )

    def fills(self):
        return [
            json.loads(r[0])
            for r in self.db.execute("SELECT payload FROM live_fills ORDER BY rowid")
        ]

    def episode(self, episode_id, payload):
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO live_episodes VALUES(?,?)",
                (episode_id, json.dumps(payload, allow_nan=False)),
            )

    def episodes(self):
        return [
            json.loads(r[0])
            for r in self.db.execute("SELECT payload FROM live_episodes ORDER BY rowid")
        ]
