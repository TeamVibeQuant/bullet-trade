"""External-process persistence and authentication for the QMT shared-memory RPC."""
import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from uuid import uuid4

from .shared_memory_rpc import ShmError, ShmRpcClient


MUTATING_PATHS = frozenset(("/place_order", "/cancel_order", "/api/order/buy",
                            "/api/order/sell", "/api/order/cancel_all", "/api/order/cancel_order"))


class ShmJournal:
    """Write intent BEFORE IPC; pending intents survive crashes and block new trades.

Only the external server opens this SQLite file. No credentials are recorded.
An operator must reconcile and resolve pending intents after an uncertain outcome.
"""
    def __init__(self, path, channel):
        self.path, self.channel = Path(path).expanduser().resolve(), channel
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = self.connect()
        try:
            with db:
                db.execute("CREATE TABLE IF NOT EXISTS intents (channel TEXT, key TEXT, fingerprint TEXT, request_id TEXT, created REAL, response TEXT, details TEXT, PRIMARY KEY(channel,key))")
                db.execute("CREATE TABLE IF NOT EXISTS state (channel TEXT PRIMARY KEY, value TEXT)")
        finally:
            db.close()

    def connect(self):
        # Short transactions, FULL sync; do not share sqlite connections across threads.
        db = sqlite3.connect(str(self.path), timeout=10)
        db.execute("PRAGMA synchronous=FULL")
        return db

    def begin(self, key, fingerprint, request_id, details=None):
        db = self.connect()
        try:
            with db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("SELECT fingerprint,response FROM intents WHERE channel=? AND key=?", (self.channel, key)).fetchone()
                if row:
                    if row[0] != fingerprint:
                        raise ShmError("IPC_IDEMPOTENCY_CONFLICT", "Idempotency key reused with a different order")
                    if row[1] is not None:
                        return json.loads(row[1])
                pending = db.execute("SELECT request_id FROM intents WHERE channel=? AND response IS NULL LIMIT 1", (self.channel,)).fetchone()
                if pending:
                    raise ShmError("IPC_OUTCOME_UNKNOWN", "Unresolved trading request %s in %s; reconcile orders and resolve journal intent before new trades" % (pending[0], self.path), True)
                db.execute("INSERT INTO intents VALUES (?,?,?,?,?,NULL,?)", (self.channel, key, fingerprint, request_id, time.time(), json.dumps(details or {}, ensure_ascii=False)))
        finally:
            db.close()
        return None

    def finish(self, key, response):
        db = self.connect()
        try:
            with db:
                db.execute("UPDATE intents SET response=? WHERE channel=? AND key=?", (json.dumps(response, ensure_ascii=False), self.channel, key))
        finally:
            db.close()

    def load_tags(self):
        db = self.connect()
        try:
            row = db.execute("SELECT value FROM state WHERE channel=?", (self.channel,)).fetchone()
            return json.loads(row[0]) if row else {"by_id": {}, "pending": []}
        finally:
            db.close()

    def save_tags(self, tags):
        db = self.connect()
        try:
            with db:
                db.execute("INSERT OR REPLACE INTO state VALUES (?,?)", (self.channel, json.dumps(tags, ensure_ascii=False)))
        finally:
            db.close()


class BigQmtShmTransport:
    def __init__(self, config):
        self.config = config
        self.rpc = ShmRpcClient(config.shm_name, config.shm_size, config.shm_poll_seconds)
        self.lock = threading.Lock()
        self.journal = None

    def request(self, path, payload, method):
        # Queueing time consumes the request budget too: a stale order must not
        # get a fresh timeout after waiting behind a long history query.
        deadline = time.monotonic() + self.config.timeout_seconds
        if not self.lock.acquire(timeout=self.config.timeout_seconds):
            raise ShmError("IPC_BUSY", "Local IPC queue timed out; nothing submitted")
        try:
            return self._request_locked(path, payload, method, deadline)
        finally:
            self.lock.release()

    def _request_locked(self, path, payload, method, deadline):
        if self.journal is None:
            self.journal = ShmJournal(self.config.shm_journal, self.config.shm_name)
        payload = dict(payload or {})
        payload.setdefault("request_id", "bt-" + uuid4().hex)
        envelope = {"path": path, "method": method, "payload": payload,
                    "headers": {"X-BulletTrade-Password": self.config.password or "",
                                "X-BulletTrade-Secret": self.config.secret or ""}}
        key = None
        if path in MUTATING_PATHS:
            identifier = str(payload.get("idempotency_key") or payload["request_id"])
            scope = [path, payload.get("account_id"), payload.get("account_key"),
                     payload.get("sub_account_id"), payload.get("virtual_account_id"), identifier]
            key = hashlib.sha256(json.dumps(scope, sort_keys=True).encode()).hexdigest()
            if path in ("/place_order", "/api/order/buy", "/api/order/sell"):
                payload.setdefault("qmt_user_order_id", "BT" + key[:21])
            content = {k: v for k, v in payload.items() if k not in ("request_id", "wait_timeout")}
            fingerprint = hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()
            cached = self.journal.begin(key, fingerprint, payload["request_id"], {"path": path, "payload": payload})
            if cached is not None:
                return cached
        try:
            # Helper restores only once, after authentication. Sending the last
            # checkpoint also supports fresh clients after server/QMT restarts.
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ShmError("IPC_EXPIRED", "Request expired before IPC publication")
            result = self.rpc.request(envelope, remaining,
                                      prepare=lambda session: {"order_tags": self.journal.load_tags()})
            if not isinstance(result.get("response"), dict):
                raise ShmError("IPC_PROTOCOL", "Missing helper response envelope", True)
            if "order_tags" in result:
                self.journal.save_tags(result["order_tags"])
            response = result["response"]
            if key is not None:
                if response.get("code") in {"PLACE_ORDER_FAILED", "PLACE_ORDER_REJECTED", "CANCEL_ORDER_FAILED", "QMT_ACTION_FAILED"}:
                    raise ShmError("IPC_OUTCOME_UNKNOWN", response.get("message", "QMT action failed after dispatch"), True)
                self.journal.finish(key, response)
            return response
        except ShmError as exc:
            if key is not None:
                if exc.uncertain:
                    raise ShmError("IPC_OUTCOME_UNKNOWN", "Trading outcome unknown: request_id=%s; %s. Do not resubmit; reconcile the journal." % (payload["request_id"], exc), True) from exc
                self.journal.finish(key, {"ok": False, "code": exc.code, "message": str(exc)})
            raise
        except Exception as exc:
            if key is not None:
                raise ShmError("IPC_OUTCOME_UNKNOWN", "Trading intent %s remains pending after %s; reconcile journal before retry" % (payload["request_id"], type(exc).__name__), True) from exc
            raise
