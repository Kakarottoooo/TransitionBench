"""Event-driven two-server model. Service dynamics never inspect policy names.

Workers use FCFS; one request per worker, cached-prefix service acceleration,
and a seeded lognormal service perturbation. Configuration B favors long inputs,
A favors short inputs. Reconfiguration drains one worker, clears its simulated
cache, waits startup and warmup, then repeats on the other. This is explanatory,
not a vLLM performance model. Every compared policy uses these same mechanics.
"""
import hashlib
import heapq
import json
import random
import statistics
import threading
from dataclasses import dataclass, field
from .metrics import summarize
from .policy import OnlinePolicy
from .schemas import ExperimentSpec, RequestEvent, TransitionEvent, WorkloadSpec
from .workloads import generate

CONFIGS = {"A": {"short_s": .16, "long_s": .43}, "B": {"short_s": .23, "long_s": .29}}


@dataclass
class Worker:
    config: str
    busy_until: float = 0
    ready_at: float = 0
    generation: int = 0
    accepting: bool = True
    cache: set = field(default_factory=set)


def service_seconds(config, item, cached, seed):
    digest = hashlib.sha256(f"{seed}:{item.request_id}".encode()).digest()
    rng = random.Random(int.from_bytes(digest[:8]))
    base = CONFIGS[config][item.workload_class + "_s"]
    return base * (.65 if cached and item.workload_class == "long" else 1) * rng.lognormvariate(0, .10)


def simulate(spec: ExperimentSpec, calibration=None, fixed=None, switch_at=None, cancel=None):
    cancel = cancel or threading.Event()
    items = generate(spec.workload)
    if len(items) > spec.budget.max_requests:
        raise ValueError("Offered workload exceeds request budget")
    if sum(i.input_token_upper_bound + spec.budget.max_output_tokens for i in items) > spec.budget.max_total_tokens:
        raise ValueError("Worst-case token budget exceeded")
    start = fixed or calibration.get("static_best_by_workload", {}).get(spec.workload.kind, calibration["static_best"])
    workers = [Worker(start), Worker(start)]
    policy = None if fixed else OnlinePolicy(spec.policy, calibration, spec.horizon_s, spec.min_practical_gain_requests)
    rows, transitions, decisions = [], [], []
    pending_completions = []
    current, rollout_until, operation = start, 0.0, 0

    def rollout(at, target):
        nonlocal current, rollout_until, operation
        operation += 1
        heapq.heappush(config_events, (at, "drain", 0, target))
        current, rollout_until = target, float("inf")

    def advance(until):
        nonlocal rollout_until
        while config_events and config_events[0][0] <= until:
            stage, action, index, target = heapq.heappop(config_events)
            w = workers[index]
            def emit(state, at):
                transitions.append(TransitionEvent(operation_id=f"sim-{operation}", worker_id=str(index), state=state,
                    at_s=at, from_config=w.config, to_config=target, generation=w.generation, origin="synthetic"))
            if action == "drain":
                w.accepting = False
                emit("DRAINING_ONE_WORKER", stage)
                # Requests already assigned to this worker finish before mutation.
                heapq.heappush(config_events, (max(stage, w.busy_until), "apply", index, target))
            elif action == "apply":
                emit("RECONFIGURING", stage)
                w.generation += 1
                w.config = target
                w.cache.clear()
                w.ready_at = stage + spec.transition_s + .2
                heapq.heappush(config_events, (stage + spec.transition_s, "ready", index, target))
            elif action == "ready":
                emit("READINESS_CHECK", stage)
                emit("WARMING", stage)
                heapq.heappush(config_events, (stage + .2, "observe", index, target))
            else:
                emit("OBSERVING", stage)
                w.accepting = True
                if index == 0:
                    heapq.heappush(config_events, (stage, "drain", 1, target))
                else:
                    rollout_until = stage

    config_events = []
    for item in items:
        at = item.scheduled_s
        while pending_completions and pending_completions[0] <= at:
            heapq.heappop(pending_completions)
        advance(at)
        if switch_at is not None and at >= switch_at and operation == 0:
            rollout(at, "B" if current == "A" else "A")
        if policy:
            policy.observe_arrival(item.workload_class, item.prefix_group, at)
            if at >= rollout_until:
                target, decision = policy.choose(at, current, len(pending_completions))
                if decision:
                    decisions.append({**decision.model_dump(mode="json"), "at_s": at, "from_config": current, "to_config": target})
                if target != current:
                    rollout(at, target)
        # Apply stages due now before dispatching; the second worker remains available.
        advance(at)
        row = RequestEvent(request_id=item.request_id, scheduled_s=at, workload_class=item.workload_class,
                           prefix_group=item.prefix_group, origin="synthetic", quality_check="synthetic-validity-assumption")
        if cancel.is_set():
            row.termination = "cancelled"
        elif len(pending_completions) >= spec.budget.max_concurrency:
            row.termination = "client_drop"
        else:
            index = min((i for i in range(2) if workers[i].accepting), key=lambda i: max(at, workers[i].busy_until, workers[i].ready_at))
            worker = workers[index]
            begin = max(at, worker.busy_until, worker.ready_at)
            duration = service_seconds(worker.config, item, item.prefix_group in worker.cache, spec.workload.seed)
            end = begin + duration
            worker.busy_until = end
            worker.cache.add(item.prefix_group)
            heapq.heappush(pending_completions, end)
            row.dispatch_s = at
            row.first_content_s = begin + duration * .35
            row.final_content_s = row.completed_s = end
            row.termination, row.finish_reason = "complete", "stop"
            row.output_chars, row.output_tokens = len(item.expected), 12
            row.token_origin, row.quality_valid = "synthetic", True
            row.worker_id, row.config_id, row.scheduling_lag_s = str(index), worker.config, 0
        rows.append(row)
    advance(spec.observation_s)
    transitions.sort(key=lambda e: (e.at_s, e.worker_id))
    return rows, transitions, decisions


def calibrate(transition_s=3):
    """Run fixed configurations on calibration seeds; tune only on tuning seeds."""
    rates, totals = {}, {}
    records = []
    for config in CONFIGS:
        rates[config], totals[config] = {}, 0
        for kind in ("short", "long-prefix"):
            observed = []
            for seed in (11, 12, 13):
                spec = ExperimentSpec(workload=WorkloadSpec(kind=kind, split="calibration", seed=seed, injection_s=10, rate_rps=12),
                                      observation_s=20, drain_s=10, transition_s=transition_s)
                rows, _, _ = simulate(spec, fixed=config)
                valid = [r for r in rows if r.completed_s and r.first_content_s]
                # Service is observed through known two-server fixture timestamps;
                # this is not an estimator for opaque serverless endpoints.
                durations = [(r.completed_s - r.first_content_s) / .65 for r in valid]
                observed.append(2 / statistics.mean(durations))
                totals[config] += summarize(rows, spec.slo, spec.observation_s)["qualified"]
            rates[config]["long" if kind == "long-prefix" else "short"] = statistics.mean(observed)
            records.append({"configuration": config, "workload": kind, "capacity_rps_samples": observed})
    costs = {}
    for source, target in (("A", "B"), ("B", "A")):
        for kind in ("short", "long-prefix"):
            groups = {}
            for switch_time in (0, 2):
                for seed in (11, 12, 13):
                    spec = ExperimentSpec(workload=WorkloadSpec(kind=kind, split="calibration", seed=seed, injection_s=20, rate_rps=12),
                                          observation_s=30, drain_s=10, transition_s=transition_s)
                    steady, _, _ = simulate(spec, fixed=target)
                    moving, _, _ = simulate(spec, fixed=source, switch_at=switch_time)
                    # Deficit counts completions in the transition window relative
                    # to B already steady; pre-window completions are excluded.
                    from .metrics import qualifies
                    a = sum(qualifies(r, spec.slo, spec.observation_s) and r.completed_s >= switch_time for r in steady)
                    b = sum(qualifies(r, spec.slo, spec.observation_s) and r.completed_s >= switch_time for r in moving)
                    queued = sum(r.dispatch_s is not None and r.dispatch_s < switch_time and r.completed_s > switch_time for r in moving)
                    bucket = "backlogged" if queued > 2 else "idle"
                    groups.setdefault(bucket, []).append(max(0, a-b))
            costs[f"{source}>{target}:{'long' if kind == 'long-prefix' else 'short'}"] = {
                "by_queue": {bucket: {"mean": statistics.mean(samples), "spread": max(samples)-min(samples),
                                      "samples": samples} for bucket,samples in groups.items() if len(samples)>=3},
                "reference": "candidate-steady", "uncertainty_kind": "observed range, not confidence interval"}
    fixed_scores, fixed_winners = {}, {}
    # A strong fixed baseline is selected separately for each predeclared workload
    # family from held-out calibration seeds, never from test outcomes.
    for kind in ("short", "long-prefix", "prefix-shift", "mixed-burst"):
        fixed_scores[kind] = {}
        for config in CONFIGS:
            score = 0
            for seed in (11, 12, 13):
                spec = ExperimentSpec(workload=WorkloadSpec(kind=kind, split="calibration", seed=seed, rate_rps=9), transition_s=transition_s)
                rows, _, _ = simulate(spec, fixed=config)
                score += summarize(rows, spec.slo, spec.observation_s)["qualified"]
            fixed_scores[kind][config] = score
        fixed_winners[kind] = max(fixed_scores[kind], key=fixed_scores[kind].get)
    result = {"id": f"synthetic-calibration-v4-{transition_s:g}", "origin": "synthetic", "rates": rates,
              "costs": costs, "static_best": max(totals, key=totals.get), "static_qualified": totals,
              "calibration_seeds": [11, 12, 13], "tuning_seeds": [21, 22], "records": records,
              "static_best_by_workload": fixed_winners, "fixed_scores_by_workload": fixed_scores,
              "hysteresis": {}, "tuning_trials": []}
    best_score = -1
    for advantage, persistence, dwell in ((.05, 1, 5), (.1, 2, 10), (.2, 3, 15), (.3, 4, 20)):
        params = {"advantage_fraction": advantage, "persistence_s": persistence, "dwell_s": dwell}
        result["hysteresis"] = params
        score = 0
        for seed in (21, 22):
            for kind in ("mixed-burst", "prefix-shift", "short"):
                spec = ExperimentSpec(policy="FixedHysteresis", transition_s=transition_s,
                                      workload=WorkloadSpec(seed=seed, split="tuning", kind=kind, rate_rps=9))
                rows, _, _ = simulate(spec, result)
                score += summarize(rows, spec.slo, spec.observation_s)["qualified"]
        result["tuning_trials"].append({**params, "qualified": score})
        if score > best_score:
            best_score, best = score, params
    result["hysteresis"] = best
    return result
