"""Canonical application service, shared by HTTP, CLI, SDK and MCP."""
import asyncio
import json
import os
import platform
import random
import statistics
import time
import uuid
import hashlib
import importlib.metadata
from pathlib import Path
from . import __version__
from .endpoint import EndpointClient, NetworkPolicy, Refusal, run_endpoint
from .evidence import append_jsonl, export_bundle, import_bundle, write_json, zip_bundle
from .metrics import summarize
from .policy import OnlinePolicy, evaluate_decision
from .rollout import HTTPHookAdapter, PlanStore, execute_plan
from .schemas import EndpointSpec, ExperimentSpec, Mode, RequestEvent, ResourceBudget, RunManifest, ValidationReport, WarmupSpec
from .warmup import warmup_reservation
from .calibration import qualify_calibration
from .simulation import CONFIGS, calibrate, simulate
from .store import JobStore
from .verifier import verify_bundle, initial_condition_errors
from .workloads import generate


class Service:
    def __init__(self, root, operator_config=None):
        self.store = JobStore(root)
        self.config = operator_config or {}
        self.network = NetworkPolicy(self.config.get("allowed_origins", []), self.config.get("private_hosts", []))
        self.endpoints = {entry["spec"]["id"]: entry for entry in self.config.get("endpoints", [])}
        self.tasks, self.cancels, self.calibrations = {}, {}, {}
        self.plans = PlanStore(self.store.root / "plans.db")
        digest = hashlib.sha256()
        for source in sorted(Path(__file__).parent.glob("*.py")):
            digest.update(source.name.encode()); digest.update(source.read_bytes())
        self.code_revision = "sha256:" + digest.hexdigest()

    def capabilities(self):
        def cap(state, evidence, reason):
            return {"schema_version": "1.0", "state": state, "evidence": evidence, "reason": reason}
        return {"schema_version": "1.0", "integration_level": "C" if self.config.get("hook") else "A" if self.endpoints else "B",
                "features": {
                    "simulation": cap("supported", "contract-tested", "Synthetic CPU model; no external calls"),
                    "recorded_replay": cap("supported", "contract-tested", "Preserves original provenance; no observed counterfactual"),
                    "live_endpoint": cap("supported" if self.endpoints else "unsupported", "contract-tested", "Requires an operator-configured endpoint and hard budget"),
                    "controlled_rollout": cap("supported" if self.config.get("hook") else "unsupported", "contract-tested" if self.config.get("hook") else "none", "Requires separately authorized hook and independently observed two-device evidence"),
                    "wafer_chat": cap("unknown", "documented", "Public API documented; no live compatibility inferred"),
                    "wafer_deployment_control": cap("unsupported", "none", "No public lifecycle control contract established"),
                    "exact_token_timing": cap("unknown", "none", "Client SSE chunks are not exact token timestamps"),
                    "active_gpu_seconds": cap("unknown", "none", "No active utilization measurement available"),
                    "cache_hit_tokens": cap("unknown", "none", "No verified cache token counter reader is configured"),
                    "cache_hit_requests": cap("unknown", "none", "Token cache hits cannot be relabeled as request cache hits"),
                    "dollar_budget": cap("unsupported", "none", "No dated price contract supplied")}}

    def validate(self, spec: ExperimentSpec):
        errors, limits = [], []
        items = generate(spec.workload)
        if len(items) > spec.budget.max_requests:
            errors.append("Workload exceeds request budget")
        reservation = sum(i.input_token_upper_bound + spec.budget.max_output_tokens for i in items)
        if reservation > spec.budget.max_total_tokens:
            errors.append("Worst-case input/output tokens exceed budget")
        if spec.mode == Mode.LIVE_ENDPOINT:
            entry = self.endpoints.get(spec.endpoint_id)
            if not entry:
                errors.append("Endpoint is not operator-approved")
            else:
                cap = ResourceBudget.model_validate(entry["budget"])
                for field in ("max_requests", "max_total_tokens", "max_output_tokens", "max_concurrency", "max_duration_s"):
                    if getattr(spec.budget, field) > getattr(cap, field):
                        errors.append("Requested " + field + " exceeds authorized envelope")
                if spec.budget.reserved_gpus != 0:
                    errors.append("Endpoint-only experiments cannot assert reserved physical GPUs")
            limits += ["Physical GPU fairness, cache mechanism, and dollar ceiling are unknown", "Task validity uses exact synthetic request markers, not broad natural-language quality"]
        if spec.mode == Mode.CONTROLLED_ROLLOUT:
            if not self.config.get("hook"):
                errors.append("Managed deployment hook is not configured")
            if not spec.plan_id:
                errors.append("Controlled rollout requires an approved exact plan")
            if not spec.endpoint_id or spec.endpoint_id not in self.endpoints:
                errors.append("Controlled lab router endpoint is not approved")
            if spec.budget.reserved_gpus != 2:
                errors.append("Controlled experiment requires exactly two reserved GPUs")
            if spec.budget.max_concurrency < 2:
                errors.append('Controlled traffic needs a separate concurrent warmup slot')
            if spec.budget.max_reserved_gpu_seconds < spec.observation_s * 2:
                errors.append("Reserved resource budget is too small")
            if spec.calibration_id not in self.config.get("calibrations", {}):
                errors.append("An operator-imported measured calibration is required for online policies")
            else:
                try:
                    self.measured_calibration(spec)
                except (ValueError, KeyError, OSError) as exc:
                    errors.append('Measured calibration refused: ' + str(exc)[:200])
            warmup = WarmupSpec()
            if spec.plan_id:
                try:
                    plan = self.plans.get(spec.plan_id)
                    warmup = WarmupSpec.model_validate(plan['warmup'])
                    if plan['budget'] != spec.budget.model_dump(mode='json'):
                        errors.append('Run budget must equal the exact approved plan budget')
                except (KeyError, ValueError):
                    errors.append('Missing plan or explicit warmup contract; recreate and approve')
            warm_requests, warm_tokens = warmup_reservation(warmup, spec.budget.max_output_tokens)
            if len(items) + warm_requests > spec.budget.max_requests or reservation + warm_tokens > spec.budget.max_total_tokens:
                errors.append("Budget must reserve forward/recovery warmup requests and their worst-case tokens separately from traffic")
            entry = self.endpoints.get(spec.endpoint_id)
            if entry:
                envelope = ResourceBudget.model_validate(entry["budget"])
                for field in ("max_requests", "max_total_tokens", "max_output_tokens", "max_concurrency", "max_duration_s"):
                    if getattr(spec.budget, field) > getattr(envelope, field):
                        errors.append("Controlled traffic exceeds the endpoint's " + field + " envelope")
            limits += ["A single controlled run is not a repeated matched policy evaluation", "Calibration, output repeat variability, and router overhead are separate empirical gates"]
        if spec.mode == Mode.RECORDED_REPLAY and not spec.replay_bundle_id:
            errors.append("Replay requires an imported bundle ID")
        if spec.mode == Mode.SIMULATION:
            limits += ["Synthetic event model; not measured GPU performance", "Simulated two-worker budget is not a physical allocation"]
        return ValidationReport(valid=not errors, errors=errors, limitations=limits)

    async def submit(self, spec, key=None):
        report = self.validate(spec)
        if not report.valid:
            raise Refusal("invalid_experiment", "; ".join(report.errors))
        # One active live/managed run per service prevents accidental budget overlap.
        active = [r for r in self.store.list() if r["state"] in ("QUEUED", "RUNNING", "CANCELLING") and r["spec"]["mode"] in ("LIVE_ENDPOINT", "CONTROLLED_ROLLOUT")]
        if spec.mode in (Mode.LIVE_ENDPOINT, Mode.CONTROLLED_ROLLOUT) and active:
            if key:
                with self.store.connect() as db:
                    old = db.execute("SELECT id FROM jobs WHERE idem=?", (key,)).fetchone()
                if old:
                    run_id, _ = self.store.create(spec, key)
                    return self.get(run_id)
            raise Refusal("budget_refusal", "Another live experiment holds the endpoint budget")
        run_id, fresh = self.store.create(spec, key or uuid.uuid4().hex)
        if fresh:
            self.cancels[run_id] = asyncio.Event()
            self.tasks[run_id] = asyncio.create_task(self._run(run_id, spec))
        return self.get(run_id)

    async def _run(self, run_id, spec):
        root = self.store.root / "runs" / run_id
        self.store.update(run_id, "RUNNING")
        cancel = self.cancels[run_id]
        try:
            transitions, decisions, hardware, resources, params = [], [], [], [], {}
            validity = {"valid": True, "errors": []}
            items = generate(spec.workload)
            original_id, original_mode = None, None
            if spec.mode == Mode.SIMULATION:
                if spec.transition_s not in self.calibrations:
                    self.calibrations[spec.transition_s] = await asyncio.to_thread(calibrate, spec.transition_s)
                    write_json(self.store.root / "calibration" / f"{spec.transition_s:g}.json", self.calibrations[spec.transition_s])
                calibration = self.calibrations[spec.transition_s]
                rows, transitions, decisions = await asyncio.to_thread(simulate, spec, calibration, cancel=cancel)
                origin, params = "synthetic", calibration
                resources = [{"start_s": 0, "end_s": spec.observation_s, "reserved_gpus": 2, "origin": "synthetic", "active_gpu_seconds": None}]
            elif spec.mode == Mode.RECORDED_REPLAY:
                if not spec.replay_bundle_id or not spec.replay_bundle_id.isalnum():
                    raise ValueError("Invalid bundle ID")
                bundle = self.store.root / "bundles" / spec.replay_bundle_id
                verified = verify_bundle(bundle)
                if not verified["integrity_valid"]:
                    raise ValueError("Replay evidence integrity failure")
                original = RunManifest.model_validate_json((bundle / "manifest.json").read_text(encoding="utf-8"))
                # Only SLO changes are meaningful recomputation. Preserve all original
                # physical/workload/time assumptions instead of relabeling the run.
                spec = original.experiment.model_copy(update={"mode": Mode.RECORDED_REPLAY, "slo": spec.slo, "replay_bundle_id": spec.replay_bundle_id})
                rows = [RequestEvent.model_validate_json(s) for s in (bundle / "requests.jsonl").read_text(encoding="utf-8").splitlines()]
                transitions = [json.loads(s) for s in (bundle / "transitions.jsonl").read_text(encoding="utf-8").splitlines()]
                decisions = [json.loads(s) for s in (bundle / "decisions.jsonl").read_text(encoding="utf-8").splitlines()]
                origin, hardware, resources, params = original.origin, original.hardware, original.resource_intervals, original.policy_parameters
                original_id, original_mode = original.run_id, original.original_mode or original.mode
                validity = {"valid": verified["experiment_valid"], "errors": verified["experiment_errors"]}
            elif spec.mode == Mode.LIVE_ENDPOINT:
                endpoint = EndpointSpec.model_validate(self.endpoints[spec.endpoint_id]["spec"])
                client = EndpointClient(endpoint, self.network)
                rows, validity = await self.measured_traffic(run_id, spec, client, cancel)
                origin = "measured-black-box"
            else:
                rows, transitions, decisions, hardware, resources, validity = await self._controlled(run_id, spec, cancel)
                origin = "measured-controlled"
                calibration = self.measured_calibration(spec)
                params = {**calibration, 'endpoint_contract': calibration['scope']['endpoint_contract'],
                    'initial_cache_policy': calibration['scope'].get('initial_cache_policy', 'legacy'),
                    'initial_condition': self.plans.get(spec.plan_id).get('initial_condition'),
                    'traffic_concurrency_limit': spec.budget.max_concurrency-1,
                    'warmup': self.plans.get(spec.plan_id)['warmup'],
                    'warmup_reservation': self.plans.get(spec.plan_id)['warmup_reservation']}
            if cancel.is_set():
                validity = {"valid": False, "errors": ["Run cancelled; offered denominator preserved"]}
            versions = {"transitionbench": __version__, "python": platform.python_version(), "code_revision": self.code_revision,
                        **{name: importlib.metadata.version(name) for name in ("fastapi","pydantic","httpx","httpcore","anyio","sniffio","mcp")}}
            if spec.mode == Mode.CONTROLLED_ROLLOUT:
                versions.update(self.config["hook"].get("versions", {}))
            if spec.mode == Mode.RECORDED_REPLAY:
                versions = {**original.versions, "replayed_by": __version__}
            manifest = RunManifest(run_id=run_id, mode=spec.mode, origin=origin, experiment=spec,
                offered_ids=[r.request_id for r in rows], created_at_unix_s=time.time(), versions=versions,
                hardware=hardware, resource_intervals=resources, configurations=CONFIGS if origin == "synthetic" else self.config.get("configurations", {}),
                policy_parameters=params, limitations=self.validate(spec).limitations,
                original_run_id=original_id, original_mode=original_mode)
            if spec.mode == Mode.RECORDED_REPLAY:
                manifest.configurations = original.configurations
                manifest.limitations = original.limitations + ["Recorded replay; alternative policies are not observed counterfactuals"]
            export_bundle(root / "bundle", manifest, rows, transitions, decisions, validity)
            zip_bundle(root / "bundle", root / "evidence.zip")
            checked = verify_bundle(root / "bundle")
            write_json(root / "verification.json", checked)
            if not checked["integrity_valid"]:
                raise ValueError("Produced bundle failed independent integrity verification")
            self.store.update(run_id, "CANCELLED" if cancel.is_set() else "SUCCEEDED" if checked["experiment_valid"] else "INVALID")
        except Exception as exc:
            self.store.update(run_id, "FAILED", type(exc).__name__ + ": " + str(exc)[:300])

    async def measured_traffic(self, run_id, spec, client, cancel, **kwargs):
        root = self.store.root / "runs" / run_id
        journal = asyncio.Queue()
        def record(row):
            journal.put_nowait(row)
        def persist(batch):
            with (root / "request-journal.jsonl").open("a", encoding="utf-8") as stream:
                for row in batch:
                    stream.write(row.model_dump_json() + "\n")
                stream.flush()
            with self.store.connect() as db:
                db.executemany("INSERT INTO progress(run_id,body) VALUES(?,?)", [
                    (run_id,json.dumps({"request_id": row.request_id,"termination":row.termination})) for row in batch])
        async def writer():
            while True:
                row = await journal.get()
                if row is None:
                    return
                batch = [row]
                while not journal.empty() and len(batch) < 50:
                    following = journal.get_nowait()
                    if following is None:
                        await asyncio.to_thread(persist, batch)
                        return
                    batch.append(following)
                await asyncio.to_thread(persist, batch)
        writing = asyncio.create_task(writer())
        try:
            return await run_endpoint(spec, client, cancel, record, **kwargs)
        finally:
            journal.put_nowait(None)
            await writing

    def hook_adapter(self):
        hook = self.config.get("hook")
        if not hook:
            raise Refusal("unsupported_capability", "No deployment hook configured")
        token = os.environ.get(hook["token_env"])
        if not token:
            raise Refusal("credentials_missing", "Operator hook token is unset")
        return HTTPHookAdapter(hook["base_url"], token, self.network)

    def measured_calibration(self, spec):
        entry = self.config['calibrations'][spec.calibration_id]
        if not entry.get('source'):
            raise ValueError('Raw calibration source index required; self-labeled rates are not accepted')
        result = qualify_calibration(entry['source'])
        if result['id'] != spec.calibration_id or result['source_sha256'] != entry.get('source_sha256') or result['qualification_hash'] != entry.get('qualification_hash'):
            raise ValueError('Pinned calibration identity/index/evidence changed')
        if spec.workload.split != 'test' or spec.workload.seed in result['calibration_seeds'] + result['tuning_seeds']:
            raise ValueError('Controlled tests must use held-out test seeds')
        scope = result['scope']
        if scope.get('warmup') is not None:
            if not spec.plan_id or self.plans.get(spec.plan_id).get('warmup') != scope['warmup']:
                raise ValueError('Test warmup differs from the acquired calibration contract')
        if scope.get('traffic_concurrency_limit') not in (None, spec.budget.max_concurrency-1):
            raise ValueError('Test traffic concurrency differs from calibration')
        endpoint = EndpointSpec.model_validate(self.endpoints[spec.endpoint_id]['spec'])
        if (result['workload_kind'] != spec.workload.kind or
            scope.get('long_prefix_mode', 'legacy') != spec.workload.long_prefix_mode or
            scope['slo'] != spec.slo.model_dump(mode='json') or
            scope['endpoint_id'] != spec.endpoint_id or scope['output_tokens'] != spec.budget.max_output_tokens or
            scope['concurrency'] != spec.budget.max_concurrency or scope['configurations'] != self.config.get('configurations') or
            scope['endpoint_contract'] != {k:getattr(endpoint,k) for k in ('model','temperature','seed','streaming')}):
            raise ValueError('Test workload/model/sampling/resource contract differs from calibration')
        return result

    async def _controlled(self, run_id, spec, cancel):
        adapter = self.hook_adapter()
        snapshots = await adapter.snapshot()
        if len(snapshots) != 2 or len({s.device_uuid for s in snapshots}) != 2 or len({s.device_model for s in snapshots}) != 1 or any(s.resource_evidence != "independently-observed" or not s.process_id for s in snapshots):
            raise Refusal("unsupported_capability", "Controlled mode requires independent two-GPU/process evidence")
        plan = self.plans.get(spec.plan_id)
        if not plan["approved"] or plan["state"] != "PLANNED":
            raise Refusal("approval_required", "An approved unexecuted plan is required")
        if plan["budget"] != spec.budget.model_dump(mode="json"):
            raise Refusal("budget_refusal", "Run budget must equal the exact approved plan budget")
        endpoint = EndpointSpec.model_validate(self.endpoints[spec.endpoint_id]["spec"])
        calibration = self.measured_calibration(spec)
        if calibration['scope'].get('initial_cache_policy') == 'fresh-workers':
            errors = initial_condition_errors(plan.get('initial_condition'),
                [s.model_dump(mode='json') for s in snapshots], plan['warmup'])
            if errors:
                raise Refusal('initial_condition', '; '.join(errors))
        if sorted((s.device_uuid,s.device_model) for s in snapshots) != calibration['scope']['devices']:
            raise Refusal('calibration_scope', 'Observed devices differ from calibration')
        for key in ('model_revision','tokenizer_revision','engine','driver','code_revision'):
            actual = self.code_revision if key=='code_revision' else self.config['hook'].get('versions',{}).get(key)
            if calibration['scope']['versions'].get(key) != actual:
                raise Refusal('calibration_scope', 'Runtime version differs from calibration: '+key)
        if any(s.config_id != calibration["static_best"] for s in snapshots):
            raise Refusal("initial_condition", "Every matched policy must start from the calibrated best fixed configuration")
        client = EndpointClient(endpoint, self.network)
        await client.discover()
        self.plans.acquire(spec.plan_id)
        self.plans.event(spec.plan_id, "WAITING_POLICY")
        queue = asyncio.Queue()
        policy = OnlinePolicy(spec.policy, calibration, spec.horizon_s, spec.min_practical_gain_requests)
        started, decisions = time.monotonic(), []
        def on_arrival(kind, prefix, at, depth):
            queue.put_nowait((kind, prefix, at, depth))
        reserved = plan['warmup_reservation']
        traffic_budget = spec.budget.model_copy(update={
            'max_requests':spec.budget.max_requests-reserved['max_requests'],
            'max_total_tokens':spec.budget.max_total_tokens-reserved['max_total_tokens'],
            'max_concurrency':spec.budget.max_concurrency-1})
        traffic_spec = spec.model_copy(update={'budget':traffic_budget})
        traffic = asyncio.create_task(self.measured_traffic(run_id, traffic_spec, client, cancel, on_arrival=on_arrival, epoch=started, skip_discovery=True))
        rollout = None
        try:
            while not traffic.done() and not cancel.is_set():
                try:
                    kind, prefix, at, depth = await asyncio.wait_for(queue.get(), .1)
                except TimeoutError:
                    continue
                policy.observe_arrival(kind, prefix, at)
                target, decision = policy.choose(at, calibration["static_best"], depth)
                if decision:
                    decisions.append({**decision.model_dump(mode="json"), "at_s": at})
                if target != calibration["static_best"]:
                    if target != plan["config_id"]:
                        raise Refusal("approval_scope", "Policy target differs from exact approved configuration")
                    rollout = await execute_plan(self.plans, spec.plan_id, adapter, cancel, lease_held=True, started_at=started)
                    break
            rows, validity = await traffic
            remaining = max(0, spec.observation_s - (time.monotonic() - started))
            if remaining:
                try:
                    await asyncio.wait_for(cancel.wait(), remaining)
                except TimeoutError:
                    pass
            if rollout is None:
                self.plans.event(spec.plan_id, "STAYED", detail="Policy did not execute a transition", elapsed_s=time.monotonic()-started)
                rollout = self.plans.get(spec.plan_id)
        except BaseException:
            cancel.set()
            await traffic
            self.plans.event(spec.plan_id, "ROLLBACK_PENDING", detail="Controlled trial interrupted; reconcile before releasing lease")
            raise
        for row in rows:
            row.origin = "measured-controlled"
        if rollout["state"] not in ("COMPLETE", "STAYED"):
            validity = {"valid": False, "errors": ["Rollout " + rollout["state"]]}
        transitions = [{"schema_version": "1.0", "operation_id": spec.plan_id,
                        "worker_id": e["worker_id"] or "executor", "state": e["state"],
                        "at_s": e["elapsed_s"], "from_config": "initial", "to_config": plan["config_id"],
                        "generation": None, "origin": "measured-controlled", "detail": e["detail"]} for e in rollout["events"]]
        elapsed = max(spec.observation_s, time.monotonic() - started)
        return rows, transitions, decisions, [s.model_dump(mode="json") for s in snapshots], [
            {"start_s": 0, "end_s": elapsed, "reserved_gpus": 2, "origin": "independently-observed", "active_gpu_seconds": None}], validity

    def get(self, run_id):
        job = self.store.get(run_id)
        root = self.store.root / "runs" / run_id
        summary = root / "bundle" / "summary.json"
        job["summary"] = json.loads(summary.read_text(encoding="utf-8")) if summary.exists() else None
        if job["summary"] is not None and job["spec"]["mode"] == "RECORDED_REPLAY":
            # Replay resolves the original experiment, while the submitted request
            # stays unchanged in storage for idempotency. Present the actual evidence.
            job["spec"] = json.loads((root / "bundle" / "manifest.json").read_text(encoding="utf-8"))["experiment"]
        job["mode"] = job["spec"]["mode"]
        job["origin"] = job["summary"]["origin"] if job["summary"] else "synthetic" if job["mode"] == "SIMULATION" else "unknown"
        return job

    def details(self, run_id):
        self.store.get(run_id)
        root = self.store.root / "runs" / run_id / "bundle"
        if not root.exists():
            return {"requests": [], "transitions": [], "decisions": [], "manifest": None}
        return {**{name: [json.loads(s) for s in (root / (name + ".jsonl")).read_text(encoding="utf-8").splitlines()] for name in ("requests", "transitions", "decisions")},
                "manifest": json.loads((root / "manifest.json").read_text(encoding="utf-8"))}

    async def cancel(self, run_id):
        job = self.store.get(run_id)
        if job["state"] in ("QUEUED", "RUNNING", "CANCELLING"):
            self.cancels[run_id].set()
            self.store.update(run_id, "CANCELLING")
        return self.get(run_id)

    async def close(self):
        for cancel in self.cancels.values():
            cancel.set()
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        self.store.release_owner()


def paired_comparison(runs):
    """Run-level paired bootstrap; never resample correlated individual requests."""
    groups = {}
    for run in runs:
        if not run.get("summary") or run.get("state") != "SUCCEEDED":
            raise ValueError("Only completed, valid runs can enter policy comparisons")
        spec = run["spec"]
        key = json.dumps({k: v for k, v in spec.items() if k not in ("policy", "plan_id")}, sort_keys=True)
        group = groups.setdefault(key, {})
        if spec["policy"] in group:
            raise ValueError("Duplicate policy within matched workload seed")
        group[spec["policy"]] = run
    output = []
    for baseline in ("StaticBest", "SteadyStateFirst", "FixedHysteresis"):
        pairs = [(g["StateAware"], g[baseline]) for g in groups.values() if "StateAware" in g and baseline in g]
        differences = [a["summary"]["qualified"] - b["summary"]["qualified"] for a, b in pairs]
        ci = None
        if len(differences) >= 3:
            rng = random.Random(713)
            means = sorted(statistics.mean(rng.choices(differences, k=len(differences))) for _ in range(2000))
            ci = [means[49], means[1949]]
        output.append({"baseline": baseline, "matched_trials": len(pairs), "difference_requests": statistics.mean(differences) if differences else None,
                       "paired_bootstrap_95_interval": ci, "run_pairs": [[a["id"], b["id"]] for a, b in pairs],
                       "inference_unit": "matched runs", "limitations": "Small run counts do not certify p99 or production benefit"})
    return output
