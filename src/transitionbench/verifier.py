"""Independent, standard-library-only verifier. Does NOT import producer metrics.

Copy this file anywhere and run: python verifier.py BUNDLE_DIRECTORY
Integrity is relative to the supplied manifest, not authenticity or certification.
"""
import hashlib
import json
import math
import sys
import statistics
from pathlib import Path

REQUIRED = {"manifest.json", "requests.jsonl", "transitions.jsonl", "decisions.jsonl",
            "summary.json", "quality.json", "validity.json", "resources.json", "report.html"}
MAX_BYTES = 32 * 1024 * 1024


def initial_condition_errors(condition, hardware, warmup):
    """Check the reset receipt against the observed starting workers, not a claim label."""
    try:
        if condition['policy'] != 'fresh-workers' or condition['warmup'] != warmup or not warmup['complete_probe_sequence']:
            raise ValueError('Fresh initial condition requires the same fixed warmup sequence')
        before = {s['worker_id']: s for s in condition['before']}
        after = {s['worker_id']: s for s in condition['after']}
        observed = {s['worker_id']: s for s in hardware}
        if any(len(rows) != 2 for rows in (condition['before'], condition['after'], hardware)) or len(before) != 2 or set(before) != set(after) or set(after) != set(observed):
            raise ValueError('Fresh initial condition requires the same two workers')
        for worker, new in after.items():
            old = before[worker]
            if (new['generation'] != old['generation'] + 1 or
                    type(new['process_id']) is not int or new['process_id'] <= 0 or
                    new['process_id'] == old['process_id'] or new['device_uuid'] != old['device_uuid']):
                raise ValueError('Fresh initial condition lacks a new process/generation on the same GPU')
            if not new['ready'] or not new['accepting'] or new['in_flight']:
                raise ValueError('Fresh initial condition is not ready and idle')
            if any(new[k] != observed[worker][k] for k in ('config_id','generation','process_id','device_uuid','device_model')):
                raise ValueError('Observed initial worker differs from reset receipt')
        return []
    except (KeyError, TypeError, ValueError) as exc:
        return ['Initial cache contract invalid: ' + str(exc)]


def verify_bundle(directory) -> dict:
    root = Path(directory)
    integrity_errors, experiment_errors = [], []
    result = {"integrity_valid": False, "experiment_valid": False,
              "integrity_errors": integrity_errors, "experiment_errors": experiment_errors,
              "recomputed": {}, "certified": False}
    try:
        if root.is_symlink() or not root.is_dir():
            raise ValueError("Bundle must be an ordinary directory")
        files = list(root.iterdir())
        if any(p.is_symlink() or not p.is_file() for p in files):
            raise ValueError("Links and nested paths are forbidden")
        if sum(p.stat().st_size for p in files) > MAX_BYTES:
            raise ValueError("Bundle exceeds 32 MiB")
        checks = json.loads((root / "checksums.json").read_text(encoding="utf-8"))
        if set(checks) != REQUIRED or {p.name for p in files} != REQUIRED | {"checksums.json"}:
            raise ValueError("Missing or unexpected bundle files")
        for name, expected in checks.items():
            if Path(name).name != name or ":" in name or "\\" in name:
                raise ValueError("Unsafe path")
            if hashlib.sha256((root / name).read_bytes()).hexdigest() != expected:
                integrity_errors.append("Checksum mismatch: " + name)
        if integrity_errors:
            return result
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        rows = [json.loads(s) for s in (root / "requests.jsonl").read_text(encoding="utf-8").splitlines() if s]
        transitions = [json.loads(s) for s in (root / "transitions.jsonl").read_text(encoding="utf-8").splitlines() if s]
        if manifest["schema_version"] != "1.0" or any(r["schema_version"] != "1.0" for r in rows):
            raise ValueError("Incompatible schema")
        spec = manifest["experiment"]
        if manifest['policy_parameters'].get('initial_cache_policy') == 'fresh-workers':
            experiment_errors.extend(initial_condition_errors(manifest['policy_parameters'].get('initial_condition'),
                manifest['hardware'], manifest['policy_parameters'].get('warmup')))
        horizon = spec["observation_s"]
        if not math.isfinite(horizon) or horizon <= 0:
            raise ValueError("Invalid horizon")
        ids = [r["request_id"] for r in rows]
        offered = manifest["offered_ids"]
        if len(set(ids)) != len(ids) or len(set(offered)) != len(offered):
            raise ValueError("Duplicate requests")
        if set(ids) != set(offered):
            raise ValueError("Missing or unoffered request records")
        qualified = 0
        by_class, latencies = {}, []
        for r in rows:
            if r["origin"] != manifest["origin"]:
                raise ValueError("Request origin conflicts with manifest")
            if r.get("clock_domain") != manifest["clock_domain"]:
                raise ValueError("Mixed clock domains")
            times = [r.get(k) for k in ("scheduled_s", "dispatch_s", "first_content_s", "final_content_s", "completed_s")]
            present = [v for v in times if v is not None]
            if any(not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0 for v in present) or present != sorted(present):
                raise ValueError("Impossible request lifecycle order")
            s, d, f, last, c = times
            if s >= spec["workload"]["injection_s"] and spec["workload"]["arrival_model"] == "open-loop":
                raise ValueError("Request offered outside injection interval")
            if r["termination"] == "complete" and (d is None or c is None):
                raise ValueError("Completion lacks lifecycle")
            if f is not None and d is None:
                raise ValueError("Content lacks dispatch")
            if c is not None and d is not None:
                latencies.append({"request_id": r["request_id"], "scheduled_e2e_s": c - s,
                                  "api_e2e_s": c - d, "client_queue_s": d - s,
                                  "first_content_s": None if f is None else f - s})
            ok = (r["termination"] == "complete" and r["quality_valid"] is True and r["output_chars"] > 0
                  and c is not None and c <= horizon and f is not None
                  and c - s <= spec["slo"]["e2e_s"] and f - s <= spec["slo"]["first_content_s"])
            qualified += int(ok)
            group = by_class.setdefault(r["workload_class"], {"offered": 0, "qualified": 0})
            group["offered"] += 1
            group["qualified"] += int(ok)
            if (r.get("scheduling_lag_s") or 0) > spec["max_dispatch_lag_s"]:
                experiment_errors.append("Client scheduling tolerance exceeded: " + r["request_id"])
        for group in by_class.values():
            group["attainment"] = group["qualified"] / group["offered"]
        resources = json.loads((root / "resources.json").read_text(encoding="utf-8"))
        if resources != manifest["resource_intervals"]:
            raise ValueError("Resource records disagree with manifest")
        reserved = None if not resources else 0
        for entry in resources:
            if entry["end_s"] < entry["start_s"] or entry["reserved_gpus"] not in (0, 1, 2):
                raise ValueError("Invalid resource interval")
            reserved += (entry["end_s"] - entry["start_s"]) * entry["reserved_gpus"]
        points = sorted({x[k] for x in resources for k in ("start_s", "end_s")})
        for start, end in zip(points, points[1:]):
            active = sum(x["reserved_gpus"] for x in resources if x["start_s"] <= start and x["end_s"] >= end)
            if active > spec["budget"]["reserved_gpus"]:
                experiment_errors.append("Concurrent reserved GPU budget exceeded")
        if reserved is not None and reserved > spec["budget"]["max_reserved_gpu_seconds"] + 1e-8:
            experiment_errors.append("Reserved GPU-seconds budget exceeded")
        mode, origin = manifest["mode"], manifest["origin"]
        if mode not in ("SIMULATION", "RECORDED_REPLAY", "LIVE_ENDPOINT", "CONTROLLED_ROLLOUT"):
            raise ValueError("Unsupported mode")
        effective = manifest.get("original_mode") if mode == "RECORDED_REPLAY" else mode
        if {"SIMULATION": "synthetic", "LIVE_ENDPOINT": "measured-black-box", "CONTROLLED_ROLLOUT": "measured-controlled"}.get(effective) != origin:
            raise ValueError("Unsupported evidence claim level")
        if origin == "measured-controlled":
            hw = manifest["hardware"]
            if len(hw) != 2 or len({h.get("device_uuid") for h in hw}) != 2 or any(not h.get("device_uuid") or not h.get("process_id") or h.get("resource_evidence") != "independently-observed" for h in hw):
                experiment_errors.append("Missing independent two-device/process evidence")
            if not resources or len({h.get("device_model") for h in hw}) != 1:
                experiment_errors.append("Missing matched hardware/resource accounting")
            if not manifest["configurations"] or not all(k in manifest["versions"] for k in ("model_revision", "tokenizer_revision", "engine", "driver", "code_revision")):
                experiment_errors.append("Incomplete controlled configuration provenance")
        prev = {}
        for event in transitions:
            if event["at_s"] < prev.get(event["worker_id"], 0):
                raise ValueError("Transition time moves backward")
            prev[event["worker_id"]] = event["at_s"]
        result["recomputed"] = {"offered": len(rows), "qualified": qualified,
                                "attainment": qualified / len(rows) if rows else 0,
                                "goodput_rps": qualified / horizon, "by_class": by_class,
                                "latencies": latencies, "reserved_gpu_seconds": reserved}
        summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
        for key in ("offered", "qualified", "attainment", "goodput_rps"):
            if not math.isclose(summary[key], result["recomputed"][key], rel_tol=1e-9, abs_tol=1e-9):
                raise ValueError("Summary mismatch: " + key)
        if summary["latencies"] != latencies or summary["by_class"] != by_class:
            raise ValueError("Latency or class summary mismatch")
        validity = json.loads((root / "validity.json").read_text(encoding="utf-8"))
        if not validity["valid"]:
            experiment_errors.extend(validity["errors"])
        result["integrity_valid"] = True
        result["experiment_valid"] = not experiment_errors
    except (OSError, ValueError, KeyError, TypeError, OverflowError) as exc:
        integrity_errors.append(str(exc))
    return result


def compare_bundles(directories):
    """Independent paired count differences, without importing producer logic."""
    groups, errors = {}, []
    for directory in directories:
        checked = verify_bundle(directory)
        if not checked["integrity_valid"] or not checked["experiment_valid"]:
            errors.append({"bundle": str(directory), "verification": checked})
            continue
        manifest = json.loads((Path(directory) / "manifest.json").read_text(encoding="utf-8"))
        spec = manifest["experiment"]
        contract = {k: v for k, v in spec.items() if k not in ("policy", "plan_id")}
        key = json.dumps(contract, sort_keys=True)
        group = groups.setdefault(key, {})
        if spec["policy"] in group:
            errors.append({"bundle": str(directory), "error": "Duplicate policy within paired contract"})
        group[spec["policy"]] = (manifest["run_id"], checked["recomputed"]["qualified"])
    comparisons = []
    for baseline in ("StaticBest", "SteadyStateFirst", "FixedHysteresis"):
        pairs = [(g["StateAware"], g[baseline]) for g in groups.values() if "StateAware" in g and baseline in g]
        differences = [a[1] - b[1] for a, b in pairs]
        comparisons.append({"baseline": baseline, "matched_trials": len(pairs),
                            "mean_difference_requests": statistics.mean(differences) if differences else None,
                            "run_pairs": [[a[0], b[0]] for a, b in pairs]})
    return {"integrity_valid": not errors, "experiment_valid": not errors,
            "errors": errors, "comparisons": comparisons, "certified": False}


if __name__ == "__main__":
    verification = verify_bundle(sys.argv[1]) if len(sys.argv) == 2 else compare_bundles(sys.argv[1:])
    print(json.dumps(verification, indent=2))
    sys.exit(0 if verification["integrity_valid"] and verification["experiment_valid"] else 1)
