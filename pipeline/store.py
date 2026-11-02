"""Event stores. Idempotency lives here: event_id is the PRIMARY KEY and every write is
INSERT ... ON CONFLICT (event_id) DO NOTHING, so replaying a message is a no-op.

SqliteStore and PostgresStore use the same SQL shape, so the logic exercised locally on
SQLite is the logic that runs on Postgres.
"""
import json
import sqlite3
import threading
from typing import Iterable, List, Set

from .models import Event


class TransientStoreError(Exception):
    """Write failed in a way that is safe to retry (lock timeout, dropped connection...)."""


class SqliteStore:
    DDL = """
    CREATE TABLE IF NOT EXISTS device_events (
        event_id    TEXT PRIMARY KEY,
        device_id   TEXT NOT NULL,
        event_type  TEXT NOT NULL,
        occurred_at REAL NOT NULL,
        payload     TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_events_device_time ON device_events (device_id, occurred_at);
    """

    def __init__(self, path: str, synchronous: str = "FULL"):
        self.path = path
        self.synchronous = synchronous
        self._local = threading.local()
        self._conn().executescript(self.DDL)

    def _conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "c", None)
        if c is None:
            c = sqlite3.connect(self.path, timeout=10, isolation_level=None)  # explicit txns
            c.execute("PRAGMA journal_mode=WAL")
            c.execute(f"PRAGMA synchronous={self.synchronous}")
            self._local.c = c
        return c

    def insert_batch(self, events: Iterable[Event]) -> int:
        """Insert in ONE transaction; returns the number of rows actually inserted."""
        rows = [(e.event_id, e.device_id, e.event_type, e.occurred_at, json.dumps(e.payload))
                for e in events]
        c = self._conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            before = c.total_changes
            c.executemany(
                "INSERT INTO device_events (event_id, device_id, event_type, occurred_at, payload) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING", rows)
            inserted = c.total_changes - before
            c.execute("COMMIT")
            return inserted
        except sqlite3.OperationalError as e:
            self._rollback(c)
            raise TransientStoreError(str(e)) from e
        except BaseException:
            self._rollback(c)
            raise

    @staticmethod
    def _rollback(c) -> None:
        try:
            c.execute("ROLLBACK")
        except sqlite3.Error:
            pass

    def count(self) -> int:
        return self._conn().execute("SELECT COUNT(*) FROM device_events").fetchone()[0]

    def all_ids(self) -> Set[str]:
        return {r[0] for r in self._conn().execute("SELECT event_id FROM device_events")}


class PostgresStore:
    """psycopg2 adapter (NOT exercised in the sandbox this was written in -- see README)."""

    DDL = """
    CREATE TABLE IF NOT EXISTS device_events (
        event_id    TEXT PRIMARY KEY,
        device_id   TEXT NOT NULL,
        event_type  TEXT NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL,
        payload     JSONB NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_events_device_time ON device_events (device_id, occurred_at);
    """

    def __init__(self, dsn: str):
        import psycopg2  # lazy
        import psycopg2.extras
        self._pg = psycopg2
        self._extras = psycopg2.extras
        self.dsn = dsn
        self._local = threading.local()
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(self.DDL)

    def _conn(self):
        c = getattr(self._local, "c", None)
        if c is None or c.closed:
            c = self._pg.connect(self.dsn)
            self._local.c = c
        return c

    def insert_batch(self, events: Iterable[Event]) -> int:
        rows = [(e.event_id, e.device_id, e.event_type, e.occurred_at, json.dumps(e.payload))
                for e in events]
        if not rows:
            return 0
        conn = self._conn()
        try:
            with conn:  # one transaction: commit on success, rollback on error
                with conn.cursor() as cur:
                    out = self._extras.execute_values(
                        cur,
                        "INSERT INTO device_events (event_id, device_id, event_type, occurred_at, payload) "
                        "VALUES %s ON CONFLICT (event_id) DO NOTHING RETURNING event_id",
                        rows, template="(%s, %s, %s, to_timestamp(%s), %s::jsonb)",
                        page_size=len(rows), fetch=True)
            return len(out)
        except (self._pg.OperationalError, self._pg.InterfaceError) as e:
            try:
                conn.close()
            except Exception:
                pass
            raise TransientStoreError(str(e)) from e

    def reset(self) -> None:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE device_events")

    def count(self) -> int:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM device_events")
            return cur.fetchone()[0]

    def all_ids(self) -> Set[str]:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT event_id FROM device_events")
            return {r[0] for r in cur.fetchall()}
