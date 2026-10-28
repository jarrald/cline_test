"""Storage node: one SQLite database per shard, exposed over HTTP.

This is the "database tier". A shard is a single-writer SQLite database with
WAL enabled; replicas receive asynchronous writes from the service tier (see
mesh.storage) which mirrors how read replicas are fed from a primary.

The node is deliberately dumb: it stores JSON documents, does optimistic
concurrency (``expected_version``) and a small transactional counter operation
that is idempotent per ``idempotency_key``. Keeping the node generic means the
order and inventory domains own their schemas, not the database.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid

from libs.mesh import config, httpkit, logging as mesh_log, metrics
from libs.mesh.httpkit import Problem

SERVICE = "storage-node"


class SqliteStore:
    def __init__(self, data_dir: str, node_id: str) -> None:
        os.makedirs(data_dir, exist_ok=True)
        self.node_id = node_id
        self.path = os.path.join(data_dir, "shard.db")
        self._local = threading.local()
        self._write_lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=10.0, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=10000")
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        conn.execute(
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, doc TEXT NOT NULL, "
            "version INTEGER NOT NULL, updated_at REAL NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS applied (idem_key TEXT PRIMARY KEY, response TEXT NOT NULL, "
            "created_at REAL NOT NULL)"
        )

    def get(self, key: str) -> dict | None:
        row = self._connect().execute("SELECT doc, version FROM kv WHERE key = ?", (key,)).fetchone()
        if row is None:
            return None
        return {"key": key, "value": json.loads(row[0]), "version": row[1], "node": self.node_id}

    def put(
        self, key: str, value, expected_version: int | None, version: int | None, monotonic: bool
    ) -> dict:
        conn = self._connect()
        with self._write_lock:
            current = conn.execute("SELECT version FROM kv WHERE key = ?", (key,)).fetchone()
            current_version = current[0] if current else 0
            if expected_version is not None and expected_version != current_version:
                raise Problem(409, "version_conflict", f"expected version {expected_version}, found {current_version}")
            if monotonic and version is not None and version <= current_version:
                return {"key": key, "version": current_version, "skipped": True, "node": self.node_id}
            new_version = version if version is not None else current_version + 1
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO kv (key, doc, version, updated_at) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET doc=excluded.doc, version=excluded.version, updated_at=excluded.updated_at",
                    (key, json.dumps(value), new_version, time.time()),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return {"key": key, "version": new_version, "node": self.node_id}

    def mutate(self, key: str, op: str, amount: int, idem_key: str) -> dict:
        conn = self._connect()
        with self._write_lock:
            cached = conn.execute("SELECT response FROM applied WHERE idem_key = ?", (idem_key,)).fetchone()
            if cached is not None:
                response = json.loads(cached[0])
                response["cached"] = True
                return response
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute("SELECT doc, version FROM kv WHERE key = ?", (key,)).fetchone()
                if row is None:
                    raise Problem(404, "not_found", f"document {key!r} does not exist")
                doc = json.loads(row[0])
                available = int(doc.get("available", 0))
                delta = -amount if op == "decrement" else amount
                if op == "decrement" and available < amount:
                    new_available = available
                    outcome = "insufficient"
                else:
                    new_available = available + delta
                    outcome = "applied"
                doc["available"] = new_available
                new_version = row[1] + 1
                conn.execute(
                    "UPDATE kv SET doc = ?, version = ?, updated_at = ? WHERE key = ?",
                    (json.dumps(doc), new_version, time.time(), key),
                )
                response = {
                    "key": key,
                    "value": doc,
                    "version": new_version,
                    "outcome": outcome,
                    "node": self.node_id,
                    "cached": False,
                }
                conn.execute(
                    "INSERT INTO applied (idem_key, response, created_at) VALUES (?, ?, ?)",
                    (idem_key, json.dumps(response), time.time()),
                )
                conn.execute("COMMIT")
                return response
            except Problem:
                conn.execute("ROLLBACK")
                raise
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def delete(self, key: str) -> bool:
        with self._write_lock:
            cursor = self._connect().execute("DELETE FROM kv WHERE key = ?", (key,))
        return cursor.rowcount > 0

    def stats(self) -> dict:
        conn = self._connect()
        rows = conn.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
        applied = conn.execute("SELECT COUNT(*) FROM applied").fetchone()[0]
        size = os.path.getsize(self.path) if os.path.exists(self.path) else 0
        return {
            "node": self.node_id,
            "keys": rows,
            "applied_idempotency_keys": applied,
            "size_bytes": size,
        }


def build_app(store: SqliteStore, max_concurrency: int) -> httpkit.App:
    ready = {"ok": True, "reason": "db open"}

    def readiness() -> tuple[bool, str]:
        return ready["ok"], ready["reason"]

    app = httpkit.App(SERVICE, max_concurrency=max_concurrency, readiness=readiness)

    @app.route("GET", "/kv/{key}", name="kv.get")
    def get_key(req: httpkit.Request):
        result = store.get(req.params["key"])
        if result is None:
            raise Problem(404, "not_found", "document not found")
        result["consistent"] = True
        return result

    @app.route("PUT", "/kv/{key}", name="kv.put")
    def put_key(req: httpkit.Request):
        payload = req.json()
        if "value" not in payload:
            raise Problem(400, "invalid_body", "'value' is required")
        result = store.put(
            req.params["key"],
            payload["value"],
            expected_version=payload.get("expected_version"),
            version=payload.get("version"),
            monotonic=bool(payload.get("monotonic", False)),
        )
        app.metrics.inc("storage_writes_total", result=str(result.get("skipped", False)))
        return result

    @app.route("POST", "/kv/{key}/mutate", name="kv.mutate")
    def mutate_key(req: httpkit.Request):
        payload = req.json()
        op = payload.get("op", "decrement")
        if op not in {"increment", "decrement"}:
            raise Problem(400, "invalid_op", "op must be increment or decrement")
        idem = str(payload.get("idempotency_key") or uuid.uuid4().hex)
        result = store.mutate(req.params["key"], op, int(payload.get("amount", 1)), idem)
        app.metrics.inc("storage_mutations_total", op=op, outcome=result.get("outcome", "cached"))
        return result

    @app.route("DELETE", "/kv/{key}", name="kv.delete")
    def delete_key(req: httpkit.Request):
        removed = store.delete(req.params["key"])
        if not removed:
            raise Problem(404, "not_found", "document not found")
        return {"deleted": req.params["key"]}

    @app.route("GET", "/stats", name="stats")
    def stats(_: httpkit.Request):
        payload = store.stats()
        payload["role"] = config.env("SHARD_ROLE", "primary")
        payload["shard"] = config.env("SHARD_NAME", "shard-0")
        return payload

    return app


def main() -> None:
    mesh_log.configure(SERVICE, config.env("POD_NAME", "storage-0"))
    metrics.configure(SERVICE, config.env("POD_NAME", "storage-0"))
    data_dir = config.env("DATA_DIR", os.path.join("runtime", "data", "storage-0"))
    port = config.env_int("PORT", 11000)
    node_id = config.env("NODE_ID", config.env("POD_NAME", "storage-0"))
    max_concurrency = config.env_int("MAX_CONCURRENCY", 64)
    store = SqliteStore(data_dir, node_id)
    app = build_app(store, max_concurrency)
    mesh_log.get_logger("storage-node").info(
        "storage node ready", extra=mesh_log.fields(shard=config.env("SHARD_NAME", "shard-0"), data_dir=data_dir)
    )
    app.serve(port)


if __name__ == "__main__":
    main()
