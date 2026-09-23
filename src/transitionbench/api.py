"""Versioned loopback HTTP API. No Docker authority is held in this process."""
import asyncio
import hmac
import json
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware
from . import __version__
from .endpoint import Refusal
from .evidence import import_bundle
from .policy import evaluate_decision
from .proposals import AutoReviewInput, ImportedEvidence, OutcomeInput, ProposalInput, ProposalStore, automatic_request, pair_imported, profile
from .schemas import DecisionInput, EndpointSpec, ExperimentSpec, Record, ResourceBudget, WorkloadSpec, WarmupSpec
from pydantic import Field
from .service import Service, paired_comparison
from .workloads import generate


class PlanRequest(Record):
    config_id: str
    budget: ResourceBudget
    warmup: WarmupSpec = Field(default_factory=WarmupSpec)


class ApprovalRequest(Record):
    plan_hash: str


class ComparisonRequest(Record):
    run_ids: list[str]


def create_app(root=None, config=None):
    service = Service(root or os.environ.get("TRANSITIONBENCH_HOME", ".transitionbench"), config)
    proposals = ProposalStore(service.store)

    @asynccontextmanager
    async def lifespan(app):
        service.store.acquire_owner()
        service.plans.recover_interrupted()
        yield
        await service.close()

    app = FastAPI(title="TransitionBench", version="1.0", lifespan=lifespan)
    app.state.service = service
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=["localhost", "127.0.0.1", "[::1]", "testserver"])

    @app.middleware("http")
    async def boundary(request: Request, call_next):
        if request.method in ("POST", "PUT", "DELETE", "PATCH"):
            if request.headers.get("x-transitionbench") != "1":
                return JSONResponse({"error": {"code": "local_client_required", "message": "X-TransitionBench: 1 required"}}, status_code=403)
            origin = request.headers.get("origin")
            if origin and origin != str(request.base_url).rstrip("/"):
                return JSONResponse({"error": {"code": "origin_refused"}}, status_code=403)
            limit = 33 * 1024 * 1024 if request.url.path == "/api/v1/bundles/import" else 128 * 1024
            size = request.headers.get("content-length")
            if size is None or not size.isdigit() or int(size) > limit:
                return JSONResponse({"error": {"code": "payload_limit"}}, status_code=413)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'"
        return response

    @app.exception_handler(Refusal)
    async def refusal(request, exc):
        return JSONResponse({"error": {"code": exc.code, "message": str(exc)}}, status_code=422)

    @app.exception_handler(ValueError)
    async def invalid(request, exc):
        return JSONResponse({"error": {"code": "invalid_operation", "message": str(exc)}}, status_code=422)

    @app.exception_handler(KeyError)
    async def missing(request, exc):
        return JSONResponse({"error": {"code": "not_found"}}, status_code=404)

    def operator(token):
        expected = os.environ.get(service.config.get("approval_token_env", "TRANSITIONBENCH_OPERATOR_TOKEN"))
        if not expected or not hmac.compare_digest(token or "", expected):
            raise HTTPException(403, detail={"code": "operator_approval_required"})

    @app.get("/healthz")
    def health():
        return {"status": "ok", "version": __version__}

    @app.get("/api/v1/capabilities")
    def capabilities():
        return service.capabilities()

    @app.get("/api/v1/bundles/{bundle_id}/profile")
    def bundle_profile(bundle_id: str):
        return profile(service.store, bundle_id)

    @app.post("/api/v1/proposals")
    def propose(body: ProposalInput, idempotency_key: str = Header(min_length=1, max_length=128)):
        return proposals.create(body, idempotency_key)

    @app.post("/api/v1/proposals/auto")
    def auto_propose(body: AutoReviewInput, idempotency_key: str = Header(min_length=1, max_length=128)):
        return proposals.create(automatic_request(service.store, body), idempotency_key)

    @app.get("/api/v1/proposals/{proposal_id}")
    def get_proposal(proposal_id: str):
        return proposals.get(proposal_id)

    @app.post("/api/v1/proposals/{proposal_id}/outcomes")
    def record_outcome(proposal_id: str, body: OutcomeInput, idempotency_key: str = Header(min_length=1, max_length=128)):
        return proposals.outcome(proposal_id, body, idempotency_key)

    @app.post("/api/v1/proposals/{proposal_id}/outcomes/imported")
    def imported_outcome(proposal_id: str, body: ImportedEvidence, idempotency_key: str = Header(min_length=1, max_length=128)):
        original = proposals.get(proposal_id)["request"]
        pairs, _, _, _ = pair_imported(service.store, body.bundle_ids, original["current_config"], original["candidate_config"])
        return proposals.outcome(proposal_id, OutcomeInput(pairs=pairs), idempotency_key)

    @app.get("/api/v1/endpoints")
    def endpoints():
        return [{"id": k, "spec": {name: value for name, value in v["spec"].items() if name != "key_env"}, "budget": v["budget"]} for k, v in service.endpoints.items()]

    @app.post("/api/v1/endpoints")
    def register_endpoint(body: EndpointSpec, x_operator_token: str | None = Header(default=None)):
        operator(x_operator_token)
        entry = service.endpoints.get(body.id)
        if not entry or EndpointSpec.model_validate(entry["spec"]) != body:
            raise Refusal("operator_configuration_required", "Add the exact endpoint and budget to the local operator configuration before registration")
        service.network.resolve(body.base_url)
        return {"id": body.id, "registered": True, "live_verified": False}

    @app.post("/api/v1/workloads")
    def workload(body: WorkloadSpec):
        rows = generate(body)
        return {"spec": body.model_dump(mode="json"), "offered": len(rows),
                "requests": [{"request_id": r.request_id, "scheduled_s": r.scheduled_s, "workload_class": r.workload_class, "prefix_group": r.prefix_group} for r in rows]}

    @app.post("/api/v1/experiments/validate")
    def validate(body: ExperimentSpec):
        return service.validate(body)

    @app.post("/api/v1/runs", status_code=202)
    async def run(body: ExperimentSpec, idempotency_key: str = Header(min_length=1, max_length=128)):
        return await service.submit(body, idempotency_key)

    @app.get("/api/v1/runs")
    def runs():
        return [service.get(r["id"]) for r in service.store.list()]

    @app.get("/api/v1/runs/{run_id}")
    def get_run(run_id: str):
        return service.get(run_id)

    @app.get("/api/v1/runs/{run_id}/records")
    def records(run_id: str):
        return service.details(run_id)

    @app.get("/api/v1/runs/{run_id}/events")
    async def events(run_id: str, request: Request, after: int = 0, last_event_id: str | None = Header(default=None)):
        service.store.get(run_id)
        cursor = max(after, int(last_event_id or 0))
        async def stream():
            nonlocal cursor
            while not await request.is_disconnected():
                rows = service.store.events(run_id, cursor)
                for row in rows:
                    cursor = row["seq"]
                    yield f"id: {cursor}\ndata: {json.dumps(row)}\n\n"
                if service.store.get(run_id)["state"] not in ("QUEUED", "RUNNING", "CANCELLING"):
                    break
                yield ": heartbeat\n\n"
                await asyncio.sleep(.25)
        return StreamingResponse(stream(), media_type="text/event-stream")

    @app.post("/api/v1/runs/{run_id}/cancel")
    async def cancel(run_id: str):
        return await service.cancel(run_id)

    @app.get("/api/v1/runs/{run_id}/artifacts")
    def artifacts(run_id: str):
        run = service.get(run_id)
        return {"run_id": run_id, "mode": run["mode"], "origin": run["origin"],
                "files": ["report.html", "evidence.zip", "verification.json"] if run["summary"] else []}

    @app.get("/api/v1/runs/{run_id}/artifacts/{name}")
    def artifact(run_id: str, name: str):
        service.store.get(run_id)
        if name not in ("report.html", "evidence.zip", "verification.json"):
            raise KeyError(name)
        root = service.store.root / "runs" / run_id
        path = root / "bundle" / name if name == "report.html" else root / name
        if not path.is_file():
            raise KeyError(name)
        return FileResponse(path, filename=name)

    @app.post("/api/v1/bundles/import")
    async def upload_bundle(file: UploadFile):
        bundle_id = uuid.uuid4().hex
        root = service.store.root / "bundles"
        archive, total = root / (bundle_id + ".zip"), 0
        try:
            with archive.open("wb") as target:
                while part := await file.read(65536):
                    total += len(part)
                    if total > 32 * 1024 * 1024:
                        raise ValueError("Bundle exceeds 32 MiB")
                    target.write(part)
            verification = import_bundle(archive, root / bundle_id)
        finally:
            archive.unlink(missing_ok=True)
        return {"bundle_id": bundle_id, "verification": verification}

    @app.post("/api/v1/decisions/evaluate")
    def decide(body: DecisionInput):
        return evaluate_decision(body)

    @app.post("/api/v1/runs/compare")
    def compare(body: ComparisonRequest):
        if len(body.run_ids) > 200:
            raise ValueError("At most 200 runs per comparison")
        return paired_comparison([service.get(r) for r in body.run_ids])

    @app.post("/api/v1/transition-plans")
    async def plan(body: PlanRequest):
        adapter = service.hook_adapter()
        return service.plans.create(service.config["hook"]["base_url"], body.config_id, await adapter.snapshot(), body.budget, warmup=body.warmup)

    @app.get("/api/v1/transition-plans/{plan_id}")
    def get_plan(plan_id: str):
        return service.plans.get(plan_id)

    @app.post("/api/v1/transition-plans/{plan_id}/approve")
    def approve(plan_id: str, body: ApprovalRequest, x_operator_token: str | None = Header(default=None)):
        operator(x_operator_token)
        return service.plans.approve(plan_id, body.plan_hash)

    @app.post("/api/v1/transition-plans/{plan_id}/execute", status_code=202)
    async def execute(plan_id: str, x_operator_token: str | None = Header(default=None)):
        operator(x_operator_token)
        plan = service.plans.get(plan_id)
        if not plan["approved"]:
            raise ValueError("Exact plan approval required")
        # Durable plan ID returned immediately; executor journals every stage.
        task_key = "plan-" + plan_id
        if task_key not in service.tasks:
            service.tasks[task_key] = asyncio.create_task(execute_plan(service.plans, plan_id, service.hook_adapter()))
        return {"plan_id": plan_id, "state": plan["state"]}

    static = Path(__file__).parent / "static"
    if static.exists():
        app.mount("/", StaticFiles(directory=static, html=True), name="demo")
    return app
