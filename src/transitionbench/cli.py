"""Local CLI. All remote operations are thin HTTP clients."""
import argparse
import asyncio
import json
import os
import sys
import httpx
from pathlib import Path
from .evidence import import_bundle
from .lab import doctor
from .schemas import ExperimentSpec, ResourceBudget, WorkloadSpec
from .sdk import Client
from .verifier import verify_bundle


def emit(value):
    print(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False))


def load(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main(argv=None):
    parser = argparse.ArgumentParser(prog="transitionbench", description="Transition-aware experiments with truthful evidence modes")
    parser.add_argument("--api", default="http://127.0.0.1:8765")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("doctor")
    p = commands.add_parser("convert-logs", help="Convert explicit reference request logs into native evidence; CPU only")
    p.add_argument("source", help="Directory containing run.json, requests.jsonl, transitions.jsonl")
    p.add_argument("--output", required=True, help="New output directory; never overwritten")
    for name in ('calibration-plan', 'calibration-collect'):
        p = commands.add_parser(name, help='Plan or acquire bounded calibration with an operator-owned hook')
        p.add_argument('source')
        p.add_argument('--operator', required=True)
        p.add_argument('--data-dir', required=True, help='Same owned data directory as the analysis API; stop that API first')
        p.add_argument('--output', required=True)
        if name == 'calibration-collect':
            p.add_argument('--approve-plan-hash', required=True)
    p = commands.add_parser('calibration-check', help='Recompute and qualify local measured calibration; no GPU calls')
    p.add_argument('source')
    p.add_argument('--output')
    p = commands.add_parser('prepare-study', help='Freeze trial order and estimate a bounded rental envelope without starting anything')
    p.add_argument('source')
    p.add_argument('--output')
    for name in ("demo", "serve"):
        p = commands.add_parser(name)
        p.add_argument("--port", type=int, default=8765)
        p.add_argument("--data-dir", default=".transitionbench")
        p.add_argument("--config")
    p = commands.add_parser("verify")
    p.add_argument("bundle")
    p = commands.add_parser("import")
    p.add_argument("archive")
    p = commands.add_parser("proposal-import", help="Upload native evidence paths from a review file; emit a proposal request without evaluating")
    p.add_argument("source")
    p = commands.add_parser("proposal", help="Evaluate imported evidence; does not deploy")
    p.add_argument("spec")
    p.add_argument("--key")
    p = commands.add_parser("review", help="Import native bundles, pair them and evaluate automatically; no deployment")
    p.add_argument("bundles", nargs="+")
    p.add_argument("--horizon", type=float, default=120)
    p.add_argument("--window", type=float, default=30)
    p.add_argument("--loss-allowance", type=float, default=0)
    p.add_argument("--key")
    p = commands.add_parser("proposal-get")
    p.add_argument("proposal_id")
    p = commands.add_parser("proposal-outcome", help="Append independent matched observations to a frozen proposal")
    p.add_argument("proposal_id")
    p.add_argument("spec")
    p.add_argument("--key")
    p = commands.add_parser("evidence-profile")
    p.add_argument("bundle_id")
    p = commands.add_parser("smoke", help="Send one bounded synthetic arithmetic request to an operator-approved endpoint")
    p.add_argument("endpoint_id")
    for name in ("validate", "run", "plan", "decision"):
        p = commands.add_parser(name)
        p.add_argument("spec")
        if name == "run":
            p.add_argument("--wait", action="store_true")
    for name in ("inspect", "cancel", "export"):
        p = commands.add_parser(name)
        p.add_argument("run_id")
        if name == "export":
            p.add_argument("output")
    p = commands.add_parser("approve")
    p.add_argument("plan_id")
    p.add_argument("plan_hash")
    p = commands.add_parser("execute")
    p.add_argument("plan_id")
    p = commands.add_parser("lab-hook")
    p.add_argument("config")
    p.add_argument("--port", type=int, default=8770)
    p.add_argument("--data-dir", default=".transitionbench-hook")
    p = commands.add_parser("lab-start")
    p.add_argument("config")
    p = commands.add_parser('lab-warmup', help='Bounded initial warmup on the explicitly authorized local two-GPU lab')
    p.add_argument('config')
    p.add_argument('--policy')
    commands.add_parser("mcp")
    args = parser.parse_args(argv)
    try:
        if args.command == "convert-logs":
            from .external import convert_logs
            emit(convert_logs(args.source, args.output))
            return
        if args.command in ('calibration-plan', 'calibration-collect'):
            from .collection import CollectionSpec, collection_plan, CalibrationCollector
            from .service import Service
            value = CollectionSpec.model_validate(load(args.source))
            service = Service(args.data_dir, load(args.operator))
            async def acquire():
                service.store.acquire_owner()
                try:
                    plan = collection_plan(value, service.config, service.code_revision)
                    if args.command == 'calibration-plan':
                        with Path(args.output).open('x', encoding='utf-8') as file:
                            file.write(json.dumps(plan, indent=2, allow_nan=False))
                        return plan
                    collector = CalibrationCollector(service, service.hook_adapter(), value, args.output, plan)
                    return await collector.run(args.approve_plan_hash)
                finally:
                    await service.close()
            result = asyncio.run(acquire())
            emit(result)
            if result.get('status') == 'BUDGET_REFUSED':
                raise SystemExit(1)
            return
        if args.command == 'prepare-study':
            from .preparation import StudyPreparation, prepare_study
            result = prepare_study(StudyPreparation.model_validate(load(args.source)))
            if args.output:
                with Path(args.output).open('x', encoding='utf-8') as file:
                    file.write(json.dumps(result, indent=2, allow_nan=False))
            emit(result)
            if result['status']=='BUDGET_REFUSED':
                raise SystemExit(1)
            return
        if args.command == 'calibration-check':
            from .calibration import qualify_calibration
            result = qualify_calibration(args.source)
            if args.output:
                with Path(args.output).open('x', encoding='utf-8') as file:
                    file.write(json.dumps(result, indent=2, allow_nan=False))
            emit(result)
            return
        if args.command == "doctor":
            emit(doctor())
            return
        if args.command in ("serve", "demo"):
            import uvicorn
            from .api import create_app
            app = create_app(args.data_dir, load(args.config) if args.config else {})
            print(f"TransitionBench: http://127.0.0.1:{args.port} — no external calls in the default demo", flush=True)
            uvicorn.run(app, host="127.0.0.1", port=args.port, access_log=False)
            return
        if args.command == "verify":
            result = verify_bundle(args.bundle)
            emit(result)
            if not result["integrity_valid"] or not result["experiment_valid"]:
                raise SystemExit(1)
            return
        if args.command == "mcp":
            from .mcp_server import main as serve_mcp
            serve_mcp()
            return
        if args.command in ("lab-hook", "lab-start", 'lab-warmup'):
            from .lab import DockerLabAdapter, LabRouter
            config = load(args.config)
            if not doctor()["controlled_ready"]:
                raise ValueError("Linux, Docker and two matching physical GPUs required")
            router = LabRouter([w["port"] for w in config["workers"]])
            adapter = DockerLabAdapter(config, router)
            if args.command == "lab-start":
                async def start():
                    for worker in adapter.workers:
                        await adapter.start_worker(worker, "A", 0)
                asyncio.run(start())
                emit({"started": True, "readiness": "must be verified before experimentation"})
            elif args.command == 'lab-warmup':
                from .schemas import WarmupSpec
                from .warmup import warm_worker
                from .lab import MODEL
                from .evidence import write_json
                import uuid
                policy = WarmupSpec.model_validate(load(args.policy) if args.policy else {})
                async def warm():
                    snapshots = await adapter.snapshot()
                    if any(not s.ready or s.resource_evidence!='independently-observed' for s in snapshots):
                        raise ValueError('Initial warmup requires ready, independently observed workers')
                    session = uuid.uuid4().hex
                    output = adapter.cache / ('transitionbench-'+adapter.owner) / 'warmup' / session
                    output.mkdir(parents=True, exist_ok=False)
                    reports = []
                    async with httpx.AsyncClient(timeout=policy.max_duration_s,trust_env=False) as client:
                        for worker, settings in adapter.workers.items():
                            report = await warm_worker(client, f"http://127.0.0.1:{settings['port']}/v1/chat/completions",
                                MODEL,32,policy,record=lambda record: write_json(output/(worker+'.json'),record))
                            reports.append(report)
                    return {'session':session,'reports':reports,'evidence_directory':str(output),
                        'hardware_effectiveness_proven':False,'exclusive_use_verified':False,
                        'limitation':'Run before starting the lab hook/router; external traffic is not observed here'}
                emit(asyncio.run(warm()))
            else:
                import uvicorn
                from .hook_server import create_hook_app
                root = Path(args.data_dir)
                root.mkdir(parents=True, exist_ok=True)
                token = os.environ.get("TRANSITIONBENCH_HOOK_TOKEN", "")
                app = create_hook_app(adapter, token, root / "hook.db")
                router.install(app, token)
                uvicorn.run(app, host="127.0.0.1", port=args.port, access_log=False)
            return
        with Client(args.api, os.environ.get("TRANSITIONBENCH_OPERATOR_TOKEN")) as client:
            if args.command == "proposal-import":
                result = client.import_review_file(args.source)
            elif args.command == "proposal":
                result = client.propose(load(args.spec), args.key)
            elif args.command == "review":
                ids = [client.import_evidence(path)["bundle_id"] for path in args.bundles]
                result = client.review(ids, args.horizon, args.key, steady_window_s=args.window,
                    uncertainty_requests=args.loss_allowance)
            elif args.command == "proposal-get":
                result = client.get_proposal(args.proposal_id)
            elif args.command == "proposal-outcome":
                result = client.record_outcome(args.proposal_id, load(args.spec), args.key)
            elif args.command == "evidence-profile":
                result = client.evidence_profile(args.bundle_id)
            elif args.command == "validate":
                result = client.validate(load(args.spec))
            elif args.command == "run":
                result = client.run(load(args.spec))
                if args.wait:
                    result = client.wait(result["id"], 3700)
            elif args.command == "smoke":
                spec = ExperimentSpec(mode="LIVE_ENDPOINT", endpoint_id=args.endpoint_id,
                        workload=WorkloadSpec(kind="short", rate_rps=5, injection_s=.2),
                        observation_s=10.2, drain_s=10,
                        budget=ResourceBudget(max_requests=1, max_total_tokens=512, max_output_tokens=32,
                                              max_concurrency=1, max_duration_s=11, reserved_gpus=0, max_reserved_gpu_seconds=0))
                run = client.run(spec.model_dump(mode="json"))
                result = client.wait(run["id"], 30)
            elif args.command == "inspect":
                result = client.get_run(args.run_id)
            elif args.command == "cancel":
                result = client.cancel(args.run_id)
            elif args.command == "export":
                result = client.export(args.run_id, args.output)
            elif args.command == "plan":
                result = client.request("POST", "/api/v1/transition-plans", load(args.spec))
            elif args.command == "decision":
                result = client.evaluate(load(args.spec))
            elif args.command == "approve":
                result = client.request("POST", f"/api/v1/transition-plans/{args.plan_id}/approve", {"plan_hash": args.plan_hash})
            elif args.command == "execute":
                result = client.request("POST", f"/api/v1/transition-plans/{args.plan_id}/execute")
            elif args.command == "import":
                with Path(args.archive).open("rb") as file:
                    response = client.http.post("/api/v1/bundles/import", files={"file": ("bundle.zip", file, "application/zip")})
                    response.raise_for_status()
                    result = response.json()
            emit(result)
            if result.get("state") in ("FAILED", "INVALID", "INTERRUPTED"):
                raise SystemExit(1)
    except httpx.HTTPStatusError as exc:
        try:
            detail = exc.response.json()
        except ValueError:
            detail = {"message": "API returned a non-JSON error"}
        emit({"error": {"code": "api_error", "status": exc.response.status_code, "details": detail}})
        raise SystemExit(1)
    except httpx.HTTPError:
        emit({"error": {"code": "connection_failed", "message": "Cannot reach the configured API; start transitionbench serve"}})
        raise SystemExit(1)
    except (ValueError, OSError, TimeoutError) as exc:
        emit({"error": {"code": "command_failed", "message": str(exc)}})
        raise SystemExit(1)


if __name__ == "__main__":
    main()
