"""Operator-owned HTTP hook. Run separately from the public analysis API."""
import asyncio
import hmac
import json
import sqlite3
import time
from contextlib import asynccontextmanager
from typing import Literal
from fastapi import FastAPI, Header, HTTPException
from pydantic import Field
from .schemas import Record, WarmupSpec
from .rollout import stable_hash
from .warmup import WarmupFailure, WARMUP_FAILURE_REASONS


class OperationPayload(Record):
    config_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,80}$")
    expected_generation: int = Field(ge=0)
    plan_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    plan_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    expires_at_unix_s: float
    drain_timeout_s: float = Field(gt=0, le=60)
    max_tokens: int = Field(ge=1, le=32)
    warmup: WarmupSpec = Field(default_factory=WarmupSpec)
    operation_timeout_s: float = Field(default=60, gt=0, le=90)


class HookOperation(Record):
    operation: Literal["prepare", "drain", "apply", "readiness", "warmup", "observe", "rollback"]
    worker_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    payload: OperationPayload


def create_hook_app(adapter, token: str, database):
    if len(token) < 24:
        raise ValueError("Hook token must contain at least 24 characters")
    def connect():
        return sqlite3.connect(database, timeout=10)
    with connect() as db:
        db.execute("CREATE TABLE IF NOT EXISTS operations (key TEXT PRIMARY KEY, digest TEXT NOT NULL, status TEXT NOT NULL, result TEXT)")
        db.execute("UPDATE operations SET status='interrupted' WHERE status='running'")
    lock = asyncio.Lock()
    app = FastAPI(title="TransitionBench operator hook", version="1.0")

    def authorize(authorization):
        if not hmac.compare_digest(authorization or "", "Bearer " + token):
            raise HTTPException(401, detail={"code": "unauthorized"})

    @app.get("/v1/snapshot")
    async def snapshot(authorization: str | None = Header(default=None)):
        authorize(authorization)
        return [s.model_dump(mode="json") for s in await adapter.snapshot()]

    @app.post("/v1/operations")
    async def operation(body: HookOperation, authorization: str | None = Header(default=None), idempotency_key: str = Header(min_length=1, max_length=160)):
        authorize(authorization)
        digest = stable_hash(body.model_dump(mode="json"))
        async with lock:
            with connect() as db:
                row = db.execute("SELECT digest,status,result FROM operations WHERE key=?", (idempotency_key,)).fetchone()
                if row:
                    if row[0] != digest:
                        raise HTTPException(409, detail={"code": "idempotency_conflict"})
                    if row[1] != "complete":
                        raise HTTPException(409, detail={"code": "operation_requires_reconciliation"})
                    return json.loads(row[2])
                if time.time() >= body.payload.expires_at_unix_s:
                    raise HTTPException(409, detail={"code": "expired_plan"})
                db.execute("INSERT INTO operations VALUES(?,?,?,NULL)", (idempotency_key, digest, "running"))
            try:
                async with asyncio.timeout(min(body.payload.operation_timeout_s, max(.001,body.payload.expires_at_unix_s-time.time()))):
                    result = await adapter.operation(body.operation, body.worker_id, body.payload.model_dump(mode="json"), idempotency_key)
                with connect() as db:
                    db.execute("UPDATE operations SET status='complete',result=? WHERE key=?", (json.dumps(result), idempotency_key))
                return result
            except WarmupFailure as exc:
                reason = exc.report.get('reason')
                if not isinstance(reason, str) or reason not in WARMUP_FAILURE_REASONS:
                    reason = 'unclassified'
                with connect() as db:
                    db.execute("UPDATE operations SET status='failed' WHERE key=?", (idempotency_key,))
                # Never reflect model output, arbitrary exception text, or operator paths.
                raise HTTPException(409, detail={'code': 'warmup_failed', 'reason': reason})
            except TimeoutError:
                with connect() as db:
                    db.execute("UPDATE operations SET status='uncertain' WHERE key=?", (idempotency_key,))
                raise HTTPException(409, detail={'code':'operation_outcome_uncertain'})
            except Exception:
                with connect() as db:
                    db.execute("UPDATE operations SET status='failed' WHERE key=?", (idempotency_key,))
                raise HTTPException(409, detail={"code": "operation_failed", "message": "Inspect operator logs and reconcile before retry"})

    return app
