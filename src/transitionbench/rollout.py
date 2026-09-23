"""One recoverable rolling executor for every policy and deployment adapter."""
import asyncio
import hashlib
import json
import sqlite3
import time
import uuid
from typing import Protocol
import httpx
from .endpoint import NetworkPolicy, Refusal
from .schemas import ResourceBudget, WorkerSnapshot, WarmupSpec
from .warmup import warmup_reservation


class DeploymentAdapter(Protocol):
    async def snapshot(self) -> list[WorkerSnapshot]: ...
    async def operation(self, operation: str, worker_id: str, payload: dict, key: str) -> dict: ...


OPERATIONS = {"prepare", "drain", "apply", "readiness", "warmup", "observe", "rollback"}


def stable_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


class PlanStore:
    def __init__(self, path):
        self.path = str(path)
        with self.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS plans (id TEXT PRIMARY KEY, body TEXT NOT NULL, hash TEXT NOT NULL, approved INTEGER DEFAULT 0, state TEXT NOT NULL, events TEXT NOT NULL, elapsed REAL DEFAULT 0)")
            db.execute("CREATE TABLE IF NOT EXISTS leases (target TEXT PRIMARY KEY, plan_id TEXT NOT NULL, expires REAL NOT NULL)")

    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    def create(self, target, config_id, snapshots, budget: ResourceBudget, ttl_s=300, warmup=None, initial_condition=None):
        warmup = warmup or WarmupSpec()
        warm_requests, warm_tokens = warmup_reservation(warmup, budget.max_output_tokens)
        if len(snapshots) != 2 or len({s.worker_id for s in snapshots}) != 2:
            raise ValueError("Exactly two workers required")
        if budget.reserved_gpus != 2 or budget.max_requests < warm_requests or budget.max_output_tokens < 32:
            raise ValueError("Plan needs two reserved devices and warmup/rollback request allowance")
        if budget.max_total_tokens < warm_tokens:
            raise ValueError("Plan lacks warmup/recovery token reservation")
        if len({s.device_uuid for s in snapshots}) != 2 or any(not s.device_uuid for s in snapshots):
            raise ValueError("Two distinct device identities required")
        if any(not s.ready or not s.accepting for s in snapshots):
            raise ValueError("Initial workers must be serving")
        if ttl_s <= 0 or ttl_s > 600:
            raise ValueError("Plan expiry must be within 600 seconds")
        plan_id = uuid.uuid4().hex
        body = {"id": plan_id, "target": target, "config_id": config_id,
                "snapshots": [s.model_dump(mode="json") for s in snapshots],
                "budget": budget.model_dump(mode="json"), "expires_at_unix_s": time.time() + ttl_s,
                "drain_timeout_s": 10, "operation_timeout_s": min(60, budget.max_duration_s),
                "readiness_timeout_s": min(90, budget.max_duration_s),
                "approval_scope": "experimental-two-worker-rollout", "warmup": warmup.model_dump(mode='json'),
                "warmup_reservation": {"max_requests": warm_requests, "max_total_tokens": warm_tokens}}
        if initial_condition is not None:
            body['initial_condition'] = initial_condition
        digest = stable_hash(body)
        with self.connect() as db:
            db.execute("INSERT INTO plans(id,body,hash,state,events) VALUES(?,?,?,?,?)", (plan_id, json.dumps(body), digest, "PLANNED", "[]"))
        return self.get(plan_id)

    def get(self, plan_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM plans WHERE id=?", (plan_id,)).fetchone()
        if row is None:
            raise KeyError(plan_id)
        return {**json.loads(row["body"]), "hash": row["hash"], "approved": bool(row["approved"]),
                "state": row["state"], "events": json.loads(row["events"]), "elapsed_s": row["elapsed"]}

    def approve(self, plan_id, digest):
        plan = self.get(plan_id)
        if digest != plan["hash"] or time.time() >= plan["expires_at_unix_s"] or plan["state"] != "PLANNED":
            raise ValueError("Exact unexpired plan hash required for approval")
        with self.connect() as db:
            db.execute("UPDATE plans SET approved=1 WHERE id=?", (plan_id,))
        return self.get(plan_id)

    def acquire(self, plan_id):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM plans WHERE id=?", (plan_id,)).fetchone()
            body = json.loads(row["body"])
            if not row["approved"] or stable_hash(body) != row["hash"]:
                raise ValueError("Exact plan approval is required")
            if row["state"] != "PLANNED":
                raise ValueError("Interrupted or active plan needs explicit recovery; mutations will not be replayed")
            if time.time() >= body["expires_at_unix_s"]:
                raise ValueError("Plan expired")
            # Even expired leases remain held until reconciliation: expiry does
            # not prove a stalled external mutation stopped.
            if db.execute("SELECT 1 FROM leases WHERE target=?", (body["target"],)).fetchone():
                raise ValueError("Target has an active or unreconciled experiment lease")
            db.execute("INSERT INTO leases VALUES(?,?,?)", (body["target"], plan_id, body["expires_at_unix_s"]))
            db.execute("UPDATE plans SET state='PREPARING' WHERE id=?", (plan_id,))

    def event(self, plan_id, state, worker_id=None, detail="", elapsed_s=0):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute("SELECT events FROM plans WHERE id=?", (plan_id,)).fetchone()
            events = json.loads(previous[0])
            events.append({"state": state, "worker_id": worker_id, "at_unix_s": time.time(), "elapsed_s": elapsed_s, "detail": detail})
            db.execute("UPDATE plans SET state=?,events=?,elapsed=? WHERE id=?", (state, json.dumps(events), elapsed_s, plan_id))
            if state in ("COMPLETE", "ABORTED", "STAYED"):
                db.execute("DELETE FROM leases WHERE plan_id=?", (plan_id,))

    def recover_interrupted(self):
        with self.connect() as db:
            db.execute("UPDATE plans SET state='ROLLBACK_PENDING' WHERE state NOT IN ('PLANNED','COMPLETE','STAYED','ABORTED','FAILED','ROLLBACK_PENDING')")


async def execute_plan(store: PlanStore, plan_id: str, adapter: DeploymentAdapter, cancel=None, lease_held=False, started_at=None):
    plan = store.get(plan_id)
    if plan["state"] == "COMPLETE":
        return plan
    if "warmup" not in plan:
        raise ValueError("Recreate and approve legacy plan with explicit warmup contract")
    if lease_held:
        if plan["state"] != "WAITING_POLICY":
            raise ValueError("Expected an experiment-held policy lease")
    else:
        store.acquire(plan_id)
    started, changed = started_at if started_at is not None else time.monotonic(), []
    last_operation = None
    cancel = cancel or asyncio.Event()
    originals = {s["worker_id"]: s for s in plan["snapshots"]}

    def event(state, worker=None, detail=""):
        store.event(plan_id, state, worker, detail, time.monotonic() - started)

    async def invoke(operation, worker, generation, config, recovery=False):
        nonlocal last_operation
        last_operation = operation
        elapsed = time.monotonic() - started
        if not recovery:
            if cancel.is_set():
                raise ValueError("Cancellation requested")
            if elapsed > plan["budget"]["max_duration_s"] / 2 or elapsed * 2 > plan["budget"]["max_reserved_gpu_seconds"] / 2:
                raise ValueError("Remaining half-budget reserved for recovery")
        remaining = min(plan["budget"]["max_duration_s"] - elapsed,
                        plan["budget"]["max_reserved_gpu_seconds"] / 2 - elapsed)
        if remaining <= 0:
            raise ValueError("Hard resource/duration budget exhausted; operator recovery required")
        payload = {"config_id": config, "expected_generation": generation, "plan_id": plan_id,
                   "plan_hash": plan["hash"], "expires_at_unix_s": plan["expires_at_unix_s"],
                   "drain_timeout_s": plan["drain_timeout_s"], "max_tokens": min(32, plan["budget"]["max_output_tokens"]),
                   "warmup": plan['warmup']}
        operation_timeout = (plan.get('readiness_timeout_s', plan['operation_timeout_s'])
                             if operation == 'readiness' else plan['operation_timeout_s'])
        payload['operation_timeout_s'] = min(operation_timeout, remaining)
        async with asyncio.timeout(payload['operation_timeout_s']):
            return await adapter.operation(operation, worker, payload, f"{plan_id}:{worker}:{operation}:{generation}")

    async def snapshot():
        remaining = min(plan['budget']['max_duration_s'], plan['budget']['max_reserved_gpu_seconds']/2) - (time.monotonic()-started)
        if remaining <= 0:
            raise TimeoutError('Observation budget exhausted')
        async with asyncio.timeout(min(plan['operation_timeout_s'], remaining)):
            return await adapter.snapshot()

    async def observed(worker_id, config, generation, require_accepting=False):
        snapshots = await snapshot()
        if len(snapshots) != 2 or len({s.device_uuid for s in snapshots}) != 2:
            raise ValueError("Two-device invariant lost")
        if any(s.worker_id not in originals or s.device_uuid != originals[s.worker_id]["device_uuid"] for s in snapshots):
            raise ValueError("Approved physical device assignment changed")
        worker_snapshot = next(s for s in snapshots if s.worker_id == worker_id)
        if worker_snapshot.config_id != config or worker_snapshot.generation != generation or not worker_snapshot.ready or (require_accepting and not worker_snapshot.accepting):
            raise ValueError("Observed generation/readiness does not confirm application")
        if time.time() - worker_snapshot.observed_at_unix_s > 10:
            raise ValueError("Worker observation stale")
        return worker_snapshot

    try:
        snapshots = await snapshot()
        if {s.worker_id for s in snapshots} != set(originals):
            raise ValueError("Worker set changed since plan")
        for s in snapshots:
            previous = originals[s.worker_id]
            if (s.generation, s.config_id, s.device_uuid) != (previous["generation"], previous["config_id"], previous["device_uuid"]) or not s.ready or not s.accepting or time.time() - s.observed_at_unix_s > 10:
                raise ValueError("State changed or stale since approval")
            await invoke("prepare", s.worker_id, s.generation, plan["config_id"])
        for worker, original in originals.items():
            generation = original["generation"]
            event("DRAINING_ONE_WORKER", worker)
            changed.append(worker)  # Include drain-only failures in recovery.
            drained = await invoke("drain", worker, generation, plan["config_id"])
            if drained.get("in_flight") != 0 or drained.get("accepting") is not False:
                raise ValueError("Drain not confirmed; forced termination refused")
            event("RECONFIGURING", worker)
            await invoke("apply", worker, generation, plan["config_id"])
            generation += 1
            event("READINESS_CHECK", worker)
            await invoke("readiness", worker, generation, plan["config_id"])
            await observed(worker, plan["config_id"], generation)
            event("WARMING", worker)
            await invoke("warmup", worker, generation, plan["config_id"])
            event("OBSERVING", worker)
            await invoke("observe", worker, generation, plan["config_id"])
            await observed(worker, plan["config_id"], generation, True)
            event("NEXT_WORKER_OR_COMPLETE", worker)
        event("COMPLETE")
    except (Exception, asyncio.CancelledError) as exc:
        event("ABORTING", detail=type(exc).__name__ + ": " + str(exc)[:200])
        if changed and (last_operation in ('apply','rollback') or isinstance(exc, (TimeoutError, asyncio.CancelledError, httpx.TransportError))):
            event("ROLLBACK_PENDING", detail="Operation outcome uncertain; inspect actual state before recovery")
            return store.get(plan_id)
        for worker in reversed(changed):
            try:
                event("ROLLING_BACK", worker)
                current = next(s for s in await snapshot() if s.worker_id == worker)
                original = originals[worker]
                if current.config_id != original["config_id"]:
                    await invoke("drain", worker, current.generation, original["config_id"], True)
                    await invoke("rollback", worker, current.generation, original["config_id"], True)
                    current.generation += 1
                await invoke("readiness", worker, current.generation, original["config_id"], True)
                await invoke("warmup", worker, current.generation, original["config_id"], True)
                await invoke("observe", worker, current.generation, original["config_id"], True)
                await observed(worker, original["config_id"], current.generation, True)
            except (Exception, asyncio.CancelledError) as recovery_error:
                event("ROLLBACK_PENDING", worker, type(recovery_error).__name__ + ": operator reconciliation required")
                return store.get(plan_id)
        event("ABORTED")
    return store.get(plan_id)


class HTTPHookAdapter:
    def __init__(self, base_url, token, network: NetworkPolicy):
        self.base_url, self.token, self.network = base_url.rstrip("/"), token, network

    async def call(self, method, path, body=None, key=None):
        url, host, sni = self.network.resolve(self.base_url + path)
        timeout = max(65, body['payload'].get('operation_timeout_s', 60) + 5) if body else 65
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=False, trust_env=False) as client:
            response = await client.request(method, url, headers={"Host": host, "Authorization": "Bearer " + self.token,
                                            "Idempotency-Key": key or "read"}, json=body, extensions={"sni_hostname": sni})
            if response.status_code != 200:
                from .warmup import WARMUP_FAILURE_REASONS
                detail = {}
                try:
                    detail = response.json().get('detail',{})
                except (ValueError, AttributeError):
                    pass
                if not isinstance(detail, dict):
                    detail = {}
                code = detail.get('code')
                if code == 'operation_outcome_uncertain':
                    raise TimeoutError('Remote operation outcome uncertain')
                known_codes = ('warmup_failed', 'idempotency_conflict', 'operation_requires_reconciliation',
                    'expired_plan', 'operation_failed', 'unauthorized')
                suffix = f" ({code}" if code in known_codes else ''
                reason = detail.get('reason')
                if code == 'warmup_failed' and isinstance(reason, str) and reason in WARMUP_FAILURE_REASONS:
                    suffix += ':' + reason
                if suffix:
                    suffix += ')'
                raise Refusal("hook_error", f"Deployment hook returned HTTP {response.status_code}{suffix}")
            return response.json()

    async def snapshot(self):
        return [WorkerSnapshot.model_validate(s) for s in await self.call("GET", "/v1/snapshot")]

    async def operation(self, operation, worker_id, payload, key):
        if operation not in OPERATIONS:
            raise Refusal("unsupported_operation", "Unknown deployment operation")
        return await self.call("POST", "/v1/operations", {"operation": operation, "worker_id": worker_id, "payload": payload}, key)
