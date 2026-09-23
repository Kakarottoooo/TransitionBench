"""Read-only deployment reviews over imported, independently checked evidence.

No deployment authority, provider calls, or invented counterfactual observations.
"""
import hashlib
import json
import math
import time
import uuid
from bisect import bisect_left, bisect_right
from typing import Annotated

from pydantic import Field, model_validator
from .metrics import qualifies
from .policy import evaluate_decision
from .schemas import DecisionInput, Record, RequestEvent, RunManifest
from .verifier import verify_bundle

BundleID = Annotated[str, Field(pattern=r"^[a-f0-9]{32}$")]


class EvidencePair(Record):
    current: BundleID
    candidate: BundleID
    transition: BundleID


class ProposalInput(Record):
    pairs: list[EvidencePair] = Field(min_length=1, max_length=12)
    current_config: str = Field(min_length=1, max_length=128)
    candidate_config: str = Field(min_length=1, max_length=128)
    expected_context: str = Field(pattern=r"^[a-f0-9]{64}$")
    horizon_s: float = Field(gt=0, le=3600)
    steady_window_s: float = Field(gt=0, le=3600)
    uncertainty_requests: float = Field(ge=0)
    min_gain_requests: float = Field(default=2, ge=0)
    max_evidence_age_s: float = Field(default=86400, gt=0, le=2592000)
    advice_ttl_s: float = Field(default=300, gt=0, le=3600)

    @model_validator(mode="after")
    def distinct(self):
        if self.current_config == self.candidate_config:
            raise ValueError("Current and candidate configurations must differ")
        return self


class OutcomeInput(Record):
    pairs: list[EvidencePair] = Field(min_length=1, max_length=12)


class ImportedEvidence(Record):
    bundle_ids: list[BundleID] = Field(min_length=3, max_length=36)


class AutoReviewInput(ImportedEvidence):
    horizon_s: float = Field(default=120, gt=0, le=3600)
    steady_window_s: float = Field(default=30, gt=0, le=3600)
    uncertainty_requests: float = Field(default=0, ge=0)
    max_evidence_age_s: float = Field(default=86400, gt=0, le=2592000)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def cumulative_curve(current, transition, start, times):
    """Inclusive completion counts since the actual switch; no steady-state extrapolation."""
    a, b = sorted(current), sorted(transition)
    a0, b0 = bisect_left(a, start), bisect_left(b, start)
    return [{"after_switch_s": t, "net_requests": bisect_right(b, start+t)-b0-bisect_right(a, start+t)+a0}
            for t in times]


def repayment_bound(curve, allowance=0):
    """First positive cumulative balance with no later negative balance in this window."""
    suffix_min = math.inf
    answer = None
    for point in reversed(curve):
        net = point["net_requests"]-allowance
        suffix_min = min(suffix_min, net)
        if net > 0 and suffix_min >= 0:
            answer = point["after_switch_s"]
    return answer


def load_bundle(store, bundle_id):
    # Also used by GET routes: never trust a URL segment as a filesystem path.
    import re
    if not re.fullmatch(r"[a-f0-9]{32}", bundle_id):
        raise ValueError("Invalid bundle ID")
    root = store.root / "bundles" / bundle_id
    if not root.is_dir():
        raise KeyError(bundle_id)
    check = verify_bundle(root)
    if not check["integrity_valid"] or not check["experiment_valid"]:
        raise ValueError("Evidence integrity or experiment validity failed")
    manifest = RunManifest.model_validate_json((root / "manifest.json").read_text(encoding="utf-8"))
    rows = [RequestEvent.model_validate_json(line) for line in (root / "requests.jsonl").read_text(encoding="utf-8").splitlines() if line]
    events = [json.loads(line) for line in (root / "transitions.jsonl").read_text(encoding="utf-8").splitlines() if line]
    return manifest, rows, events, digest(json.loads((root / "checksums.json").read_text(encoding="utf-8")))


def context(manifest, rows):
    p, e = manifest.policy_parameters, manifest.experiment
    endpoint = dict(p.get("endpoint_contract", {}))
    # Location/secret binding are not model identity. Never export credentials.
    for key in ("key_env", "base_url", "id"):
        endpoint.pop(key, None)
    return {"versions": manifest.versions, "configurations": manifest.configurations,
        "hardware": manifest.hardware, "reserved_gpus": e.budget.reserved_gpus,
        "max_output_tokens": e.budget.max_output_tokens, "max_concurrency": e.budget.max_concurrency,
        "slo": e.slo.model_dump(mode="json"), "endpoint_contract": endpoint,
        # Unassessed failed/dropped requests remain zero in metrics. Their absence
        # of an assessment is an outcome, not a different checker contract.
        "quality_checks": sorted({r.quality_check for r in rows if r.quality_check != "not-assessed"}),
        "phases": p.get("trial", {}).get("phases"),
        "injection_s": e.workload.injection_s, "observation_s": e.observation_s,
        "scope": p.get("local_restart_scope", "operator-declared experiment")}


def profile(store, bundle_id):
    m, rows, events, checksum = load_bundle(store, bundle_id)
    scope = context(m, rows)
    return {"bundle_id": bundle_id, "context": scope, "context_sha256": digest(scope),
        "evidence_sha256": checksum, "origin": m.origin, "run_id": m.run_id,
        "seed": m.experiment.workload.seed, "initial_config": m.policy_parameters.get("initial_config"),
        "transition_count": len([e for e in events if e.get("state") == "STOPPING"]),
        "metadata_authority": "producer-declared; not independent hardware or task-quality certification"}


def pair_imported(store, bundle_ids, current=None, candidate=None):
    """Unambiguous mechanical pairing, never choose the most favorable repeat."""
    if len(set(bundle_ids)) != len(bundle_ids):
        raise ValueError("Duplicate bundle IDs: provide one current, candidate and transition per seed")
    loaded = [(bid, *load_bundle(store, bid)[:3]) for bid in bundle_ids]
    moving = [(bid,m,rows,events) for bid,m,rows,events in loaded if events]
    if not moving:
        raise ValueError("Actual transition evidence is missing")
    manifest = moving[0][1]
    current = current or manifest.policy_parameters.get("initial_config")
    if candidate is None:
        others = set(manifest.configurations)-{current}
        if current not in manifest.configurations or len(others) != 1:
            raise ValueError("Automatic review needs exactly two declared configurations; use the explicit API for other cases")
        candidate = others.pop()
    groups = {}
    for bid,m,rows,events in loaded:
        role = "transition" if events else "current" if m.policy_parameters.get("initial_config") == current else "candidate" if m.policy_parameters.get("initial_config") == candidate else None
        if role is None:
            raise ValueError("Unrecognized starting configuration")
        pair = groups.setdefault(m.experiment.workload.seed, {})
        if role in pair:
            raise ValueError("Ambiguous repeated role for a seed; do not select favorable repeats")
        pair[role] = bid
    if any(set(pair) != {"current","candidate","transition"} for pair in groups.values()):
        raise ValueError("Each seed needs current, candidate and actual-transition evidence")
    pairs = [EvidencePair(**groups[seed]) for seed in sorted(groups)]
    return pairs, current, candidate, digest(context(manifest, moving[0][2]))


def automatic_request(store, value):
    pairs, current, candidate, scope = pair_imported(store, value.bundle_ids)
    return ProposalInput(pairs=pairs, current_config=current, candidate_config=candidate,
        expected_context=scope, horizon_s=value.horizon_s, steady_window_s=value.steady_window_s,
        uncertainty_requests=value.uncertainty_requests, max_evidence_age_s=value.max_evidence_age_s)


def inspect_pairs(store, body, now):
    results, issues, seen, seeds = [], [], set(), set()
    for pair in body.pairs:
        data = {role: load_bundle(store, getattr(pair, role)) for role in ("current", "candidate", "transition")}
        local_issues = []
        traces = []
        for role, (m, rows, events, checksum) in data.items():
            if checksum in seen or m.run_id in seen:
                local_issues.append("duplicate_evidence")
            seen.update((checksum, m.run_id))
            if m.origin == "synthetic" or any(r.origin != m.origin for r in rows):
                local_issues.append("measured_provenance_required")
            if m.mode not in ("LIVE_ENDPOINT", "CONTROLLED_ROLLOUT"):
                local_issues.append("original_measurement_required")
            if now - m.created_at_unix_s > body.max_evidence_age_s or m.created_at_unix_s > now + 60:
                local_issues.append("evidence_stale_or_future_dated")
            if digest(context(m, rows)) != body.expected_context:
                local_issues.append("context_mismatch")
            p = m.policy_parameters
            if not p.get("trial", {}).get("phases") or not p.get("offered_sha256") or not m.versions or not m.configurations:
                local_issues.append("missing_workload_or_version_contract")
            if body.current_config not in m.configurations or body.candidate_config not in m.configurations:
                local_issues.append("unknown_configuration")
            elif m.configurations[body.current_config] == m.configurations[body.candidate_config]:
                local_issues.append("configurations_are_identical")
            expected = body.candidate_config if role == "candidate" else body.current_config
            if p.get("initial_config") != expected:
                local_issues.append("initial_configuration_mismatch")
            if role != "transition" and events:
                local_issues.append("fixed_reference_contains_transition")
            traces.append(digest([p.get("offered_sha256"), m.experiment.workload.seed,
                sorted((r.request_id, r.scheduled_s, r.workload_class, r.prefix_group) for r in rows)]))
        if len(set(traces)) != 1:
            local_issues.append("offered_trace_not_paired")
        m, rows, events, _ = data["transition"]
        seed = m.experiment.workload.seed
        if seed in seeds:
            local_issues.append("independent_seeds_required")
        seeds.add(seed)
        starts = [e["at_s"] for e in events if e.get("state") == "STOPPING"]
        ready = [e["at_s"] for e in events if e.get("state") == "COMPLETE"]
        if len(starts) != 1 or len(ready) != 1 or ready[0] <= starts[0]:
            local_issues.append("one_completed_transition_required")
        end = m.experiment.workload.injection_s
        begin = end - body.steady_window_s
        if begin < 0 or (ready and ready[0] > begin):
            local_issues.append("steady_window_not_after_readiness")
        if starts and body.horizon_s > end - starts[0] + 1e-6:
            local_issues.append("horizon_exceeds_observed_support")
        # This small evaluator deliberately requires a homogeneous calibration phase.
        if len(m.policy_parameters.get("trial", {}).get("phases", [])) != 1:
            local_issues.append("homogeneous_calibration_required")
        issues.extend(local_issues)
        if local_issues:
            continue
        qualified = {role: [r for r in rs if qualifies(r, mm.experiment.slo, mm.experiment.observation_s)]
            for role, (mm, rs, _, _) in data.items()}
        rates = {role: sum(begin <= r.scheduled_s < end for r in rs) / body.steady_window_s
            for role, rs in qualified.items()}
        start = starts[0]
        deficit = max(0, sum(r.completed_s >= start for r in qualified["candidate"])
            - sum(r.completed_s >= start for r in qualified["transition"]))
        times = sorted({float(t) for t in range(math.floor(end-start)+1)} |
                       {h for h in (body.horizon_s, body.horizon_s/2, body.horizon_s*2) if h <= end-start})
        curve = cumulative_curve([r.completed_s for r in qualified["current"]],
                                 [r.completed_s for r in qualified["transition"]], start, times)
        first = min((r.completed_s-start for r in qualified["transition"] if r.scheduled_s >= start), default=None)
        results.append({"seed": seed, "bundle_ids": pair.model_dump(exclude={"schema_version"}),
            "run_ids": [v[0].run_id for v in data.values()],
            "evidence_sha256": {k: v[3] for k, v in data.items()},
            "current_goodput_rps": rates["current"], "candidate_goodput_rps": rates["candidate"],
            "transition_tail_goodput_rps": rates["transition"],
            "delta_rps": min(rates["candidate"], rates["transition"]) - rates["current"], "candidate_relative_deficit_requests": deficit,
            "transition_start_s": start, "ready_s": ready[0],
            "declared_transition_complete_after_switch_s": ready[0]-start,
            "first_qualified_completion_after_switch_s": first,
            "service_observation": "First qualified completion for a request scheduled after switch start; an observed serving bound, not exact availability or candidate-worker attribution.",
            "cumulative_net_curve": curve,
            "offered": len(rows), "qualified": {k: len(v) for k, v in qualified.items()},
            "steady_window": [begin, end], "supported_horizon_s": end-start})
    return results, sorted(set(issues))


def evaluate(store, body):
    now = time.time()
    pairs, issues = inspect_pairs(store, body, now)
    if len(body.pairs) < 3:
        issues.append("at_least_three_paired_seeds_required")
    decision = {"action": "INSUFFICIENT_EVIDENCE", "gain_requests": None, "break_even_s": None,
        "reasons": issues, "expires_at_unix_s": now + body.advice_ttl_s}
    if not issues:
        # Observed extrema are a cautious point estimate, NOT a confidence bound.
        delta = min(p["delta_rps"] for p in pairs)
        cost = max(p["candidate_relative_deficit_requests"] for p in pairs)
        decision = evaluate_decision(DecisionInput(current_goodput_rps=max(0, -delta),
            candidate_goodput_rps=max(0, delta), transition_deficit_requests=cost,
            horizon_s=body.horizon_s, uncertainty_requests=body.uncertainty_requests,
            min_gain_requests=body.min_gain_requests, origin="measured-black-box",
            evidence_ids=[getattr(p, r) for p in body.pairs for r in ("current", "candidate", "transition")])).model_dump(mode="json")
        decision["expires_at_unix_s"] = now + body.advice_ttl_s
        decision["steady_rate_model"] = {k: decision[k] for k in ("gain_requests", "break_even_s")}
        # Keep the full transition deficit diagnostic, but do not subtract it again:
        # the paired cumulative curve already includes downtime, failures and recovery.
        mappings = [{p["after_switch_s"]: p["net_requests"] for p in pair["cumulative_net_curve"]} for pair in pairs]
        common = sorted(set.intersection(*(set(m) for m in mappings)))
        envelope = [{"after_switch_s": t, "net_requests": min(m[t] for m in mappings)} for t in common]
        gain = next(p["net_requests"] for p in envelope if p["after_switch_s"] == body.horizon_s)
        adjusted = gain-body.uncertainty_requests
        action = "SWITCH" if adjusted > 0 and adjusted >= body.min_gain_requests else "KEEP" if delta <= 0 and adjusted <= 0 else "WAIT"
        decision.update(action=action, gain_requests=gain,
            break_even_s=repayment_bound([p for p in envelope if p["after_switch_s"] <= body.horizon_s], body.uncertainty_requests),
            observed_transition_complete_after_switch_s=max(p["ready_s"]-p["transition_start_s"] for p in pairs),
            net_gain_lower_envelope=envelope, curve_resolution_s=1,
            sensitivity=[{"horizon_s": h, "gain_requests": min(m[h] for m in mappings)}
                for h in (body.horizon_s/2, body.horizon_s, body.horizon_s*2) if h in common],
            reasons=[{"SWITCH": "Every paired transition curve covers the extra allowance and practical gain threshold within the assumed horizon",
                "WAIT": "The measured cumulative gain does not yet cover the extra allowance and practical threshold",
                "KEEP": "Neither cumulative gain nor steady advantage supports a change at this horizon"}[action]])
        decision["delta_rps"] = delta
    from pathlib import Path
    source_hash = digest({name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
        for name in ("proposals.py", "metrics.py", "policy.py", "verifier.py")})
    return {"created_at_unix_s": now, "evaluator_sha256": source_hash,
        "request": body.model_dump(mode="json"), "decision": decision,
        "pairs": pairs, "forecast_kind": "paired-transition-curve-v1", "execution_authorized": False,
        "limitations": ["Horizon and uncertainty allowance are operator assumptions, not observations.",
            "Minimum paired gain and maximum observed deficit are not statistical confidence bounds or worst-case guarantees.",
            "The rate difference uses the lower of fixed-candidate and post-transition tail goodput in each pair.",
            "Advice uses the minimum paired cumulative net completions, including the transition, within measured support. It is a calibration-based forecast, not a held-out observation or confidence bound.",
            "Declared transition completion may include warmup; requests may already be served before it. No automatic routing permission is granted.",
            "Context metadata, configuration attribution and task-quality checks are producer declarations; checksums do not certify them.",
            "Advice applies only to the declared workload and environment; no capacity, future demand or production benefit is inferred.",
            "No deployment is executed. A candidate already deployed avoids the measured transition cost."],
        "next_step": "Collect fresh matched current, candidate and actual-transition runs for each reported evidence gap." if issues
            else "Submit independent matched outcome evidence; compare actual net qualified requests with this frozen forecast."}


class ProposalStore:
    def __init__(self, store):
        self.store = store
        with store.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS proposals(id TEXT PRIMARY KEY, idem TEXT UNIQUE, digest TEXT, body TEXT)")
            db.execute("CREATE TABLE IF NOT EXISTS proposal_outcomes(id TEXT PRIMARY KEY, proposal_id TEXT, idem TEXT, digest TEXT, body TEXT, UNIQUE(proposal_id, idem))")

    def create(self, body, key):
        fingerprint = digest(body.model_dump(mode="json"))
        # Idempotent retries keep the original forecast even if evidence has aged.
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT digest,body FROM proposals WHERE idem=?", (key,)).fetchone()
            if old:
                if old["digest"] != fingerprint:
                    raise ValueError("Idempotency key conflicts with different proposal")
                return json.loads(old["body"])
            result = evaluate(self.store, body)
            result["id"] = uuid.uuid4().hex
            db.execute("INSERT INTO proposals VALUES(?,?,?,?)", (result["id"], key, fingerprint, json.dumps(result)))
        return result

    def get(self, proposal_id):
        with self.store.connect() as db:
            row = db.execute("SELECT body FROM proposals WHERE id=?", (proposal_id,)).fetchone()
            if not row:
                raise KeyError(proposal_id)
            result = json.loads(row["body"])
            result["expired"] = time.time() > result["decision"]["expires_at_unix_s"]
            result["outcomes"] = [json.loads(r[0]) for r in db.execute("SELECT body FROM proposal_outcomes WHERE proposal_id=? ORDER BY rowid", (proposal_id,))]
        return result

    def outcome(self, proposal_id, body, key):
        frozen = self.get(proposal_id)
        fingerprint = digest(body.model_dump(mode="json"))
        with self.store.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT digest,body FROM proposal_outcomes WHERE proposal_id=? AND idem=?", (proposal_id, key)).fetchone()
            if old:
                if old["digest"] != fingerprint:
                    raise ValueError("Idempotency key conflicts with different outcome")
                return json.loads(old["body"])
            request = ProposalInput.model_validate({**frozen["request"], "pairs": [p.model_dump() for p in body.pairs]})
            pairs, issues = inspect_pairs(self.store, request, time.time())
            calibration_runs = {rid for p in frozen["pairs"] for rid in p["run_ids"]}
            calibration_seeds = {p["seed"] for p in frozen["pairs"]}
            if any(calibration_runs.intersection(p["run_ids"]) or p["seed"] in calibration_seeds for p in pairs):
                issues.append("calibration_evidence_reused")
            if frozen["decision"]["action"] == "INSUFFICIENT_EVIDENCE":
                issues.append("original_forecast_not_qualified")
            observed = []
            if not issues:
                for p in pairs:
                    qualified = {}
                    for role in ("current", "transition"):
                        m, rows, _, _ = load_bundle(self.store, p["bundle_ids"][role])
                        qualified[role] = sorted(r.completed_s for r in rows if qualifies(r, m.experiment.slo, m.experiment.observation_s))
                    start, horizon = p["transition_start_s"], request.horizon_s
                    times = sorted(set([0.0, horizon] + [float(i) for i in range(1, math.ceil(horizon))]))
                    curve = cumulative_curve(qualified["current"], qualified["transition"], start, times)
                    # Existing forecasts retain their original, declared post-readiness definition.
                    legacy = frozen["forecast_kind"] != "paired-transition-curve-v1"
                    repayment = repayment_bound([x for x in curve if not legacy or x["after_switch_s"] >= p["ready_s"]-start])
                    actual = curve[-1]["net_requests"]
                    observed.append({"seed": p["seed"], "bundle_ids": p["bundle_ids"],
                        "observed_net_requests": actual,
                        "predicted_net_requests": frozen["decision"]["gain_requests"],
                        "prediction_error_requests": actual-frozen["decision"]["gain_requests"],
                        "observed_payback_after_switch_s": repayment,
                        "payback_definition": "post-declared-readiness-positive-tail" if legacy else "cumulative-positive-nonnegative-tail",
                        "declared_transition_complete_after_switch_s": p["ready_s"]-start,
                        "first_qualified_completion_after_switch_s": p["first_qualified_completion_after_switch_s"],
                        "observed_tail_after_payback_s": None if repayment is None else horizon-repayment,
                        "curve": curve})
            result = {"id": uuid.uuid4().hex, "created_at_unix_s": time.time(), "proposal_id": proposal_id,
                "status": "NOT_COMPARABLE" if issues else "COMPARABLE", "issues": sorted(set(issues)),
                "request": body.model_dump(mode="json"), "pairs": observed,
                "submitted_after_advice_expired": frozen["expired"],
                "interpretation": "Separate matched trials; observed cumulative difference, not an observed production counterfactual. Payback uses 1-second resolution and only the remaining observed tail."}
            db.execute("INSERT INTO proposal_outcomes VALUES(?,?,?,?,?)", (result["id"], proposal_id, key, fingerprint, json.dumps(result)))
        return result
