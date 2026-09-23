"""Durable local jobs and append-only progress, with a single service owner."""
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from .rollout import stable_hash


class JobStore:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        for name in ("runs", "bundles", "calibration"):
            (self.root / name).mkdir(exist_ok=True)
        self.path = self.root / "jobs.db"
        self.lock_file = None
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, idem TEXT UNIQUE, digest TEXT, spec TEXT, state TEXT, error TEXT, created REAL)")
            db.execute("CREATE TABLE IF NOT EXISTS progress(seq INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, body TEXT)")

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            # sqlite3's own context manager commits/rolls back but does not close.
            # Release on the owning thread, not later in the sender's cyclic GC.
            db.close()

    def acquire_owner(self):
        self.lock_file = (self.root / "service.lock").open("a+b")
        self.lock_file.seek(0)
        if os.name == "nt":
            import msvcrt
            if not self.lock_file.read(1):
                self.lock_file.write(b"0")
                self.lock_file.flush()
            self.lock_file.seek(0)
            msvcrt.locking(self.lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with self.connect() as db:
            interrupted = db.execute("SELECT id FROM jobs WHERE state IN ('QUEUED','RUNNING','CANCELLING')").fetchall()
            db.execute("UPDATE jobs SET state='INTERRUPTED',error='Service stopped; mutations were not replayed' WHERE state IN ('QUEUED','RUNNING','CANCELLING')")
        for row in interrupted:
            self.progress(row[0], {"state": "INTERRUPTED"})

    def release_owner(self):
        if self.lock_file:
            self.lock_file.close()
            self.lock_file = None

    def create(self, spec, key):
        body = spec.model_dump(mode="json")
        digest = stable_hash(body)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute("SELECT id,digest FROM jobs WHERE idem=?", (key,)).fetchone()
            if existing:
                if existing["digest"] != digest:
                    raise ValueError("Idempotency key conflicts with different experiment")
                return existing["id"], False
            run_id = uuid.uuid4().hex
            db.execute("INSERT INTO jobs VALUES(?,?,?,?,?,?,?)", (run_id, key, digest, json.dumps(body), "QUEUED", None, time.time()))
        (self.root / "runs" / run_id).mkdir()
        self.progress(run_id, {"state": "QUEUED"})
        return run_id, True

    def get(self, run_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (run_id,)).fetchone()
        if row is None:
            raise KeyError(run_id)
        return {"id": row["id"], "state": row["state"], "error": row["error"],
                "spec": json.loads(row["spec"]), "created_at_unix_s": row["created"]}

    def list(self):
        with self.connect() as db:
            ids = [row[0] for row in db.execute("SELECT id FROM jobs ORDER BY created DESC LIMIT 200")]
        return [self.get(run_id) for run_id in ids]

    def update(self, run_id, state, error=None):
        with self.connect() as db:
            db.execute("UPDATE jobs SET state=?,error=? WHERE id=?", (state, error, run_id))
        self.progress(run_id, {"state": state, "error": error})

    def progress(self, run_id, value):
        with self.connect() as db:
            db.execute("INSERT INTO progress(run_id,body) VALUES(?,?)", (run_id, json.dumps(value)))

    def events(self, run_id, after=0):
        with self.connect() as db:
            return [{"seq": row[0], **json.loads(row[1])} for row in db.execute("SELECT seq,body FROM progress WHERE run_id=? AND seq>? ORDER BY seq LIMIT 1000", (run_id, after))]
