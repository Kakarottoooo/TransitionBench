"""Stdlib-only arithmetic check of a qualified exported review and its raw bundles.

Usage: python scripts/verify_proposal_arithmetic.py review.json bundles_directory
Does not import the producer, infer task correctness, or certify metadata.
"""
import hashlib
import json
import math
from pathlib import Path
import re
import sys


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def verify(review, bundles):
    spec = review["request"]
    values = []
    curves = []
    for pair in spec["pairs"]:
        good, manifests, events = {}, {}, {}
        for role in ("current", "candidate", "transition"):
            if not re.fullmatch("[a-f0-9]{32}", pair[role]):
                raise ValueError("Unsafe bundle ID")
            root = bundles / pair[role]
            checksums = load(root / "checksums.json")
            for name, checksum in checksums.items():
                if Path(name).name != name or "\\" in name:
                    raise ValueError("Unsafe member name")
                if hashlib.sha256((root/name).read_bytes()).hexdigest() != checksum:
                    raise ValueError("Changed raw evidence: " + name)
            manifest = load(root/"manifest.json")
            manifests[role] = manifest
            exp, rows = manifest["experiment"], [json.loads(x) for x in (root/"requests.jsonl").read_text().splitlines() if x]
            if len(rows) != len(manifest["offered_ids"]) or {r["request_id"] for r in rows} != set(manifest["offered_ids"]):
                raise ValueError("Incomplete offered denominator")
            slo = exp["slo"]
            good[role] = [r for r in rows if r["termination"] == "complete" and r["quality_valid"] and r["output_chars"] > 0
                and r["completed_s"] is not None and r["first_content_s"] is not None
                and r["completed_s"] <= exp["observation_s"]
                and r["completed_s"]-r["scheduled_s"] <= slo["e2e_s"]
                and r["first_content_s"]-r["scheduled_s"] <= slo["first_content_s"]]
            events[role] = [json.loads(x) for x in (root/"transitions.jsonl").read_text().splitlines() if x]
        start = next(e["at_s"] for e in events["transition"] if e["state"] == "STOPPING")
        ready = next(e["at_s"] for e in events["transition"] if e["state"] == "COMPLETE")
        end = manifests["current"]["experiment"]["workload"]["injection_s"]
        rates = {role: sum(end-spec["steady_window_s"] <= r["scheduled_s"] < end for r in rows)/spec["steady_window_s"] for role, rows in good.items()}
        loss = max(0, sum(r["completed_s"] >= start for r in good["candidate"])-sum(r["completed_s"] >= start for r in good["transition"]))
        values.append((min(rates["candidate"],rates["transition"])-rates["current"], loss, ready-start))
        times = sorted(set(range(math.floor(end-start)+1)) | {spec['horizon_s'], spec['horizon_s']/2})
        curves.append({t: sum(start <= r['completed_s'] <= start+t for r in good['transition'])
                         -sum(start <= r['completed_s'] <= start+t for r in good['current']) for t in times})
    delta, deficit, readiness = min(x[0] for x in values), max(x[1] for x in values), max(x[2] for x in values)
    gain = delta*spec["horizon_s"]-deficit
    payback = deficit/delta if delta > 0 else None
    action = "KEEP" if delta <= 0 else "SWITCH" if gain-spec["uncertainty_requests"] >= spec["min_gain_requests"] and spec["horizon_s"] >= readiness else "WAIT"
    expected = dict(gain_requests=gain, break_even_s=payback, transition_deficit_requests=deficit,
        observed_ready_after_switch_s=readiness, delta_rps=delta)
    if review['forecast_kind'] == 'paired-transition-curve-v1':
        gain = min(c[spec['horizon_s']] for c in curves)
        times = sorted(t for t in set.intersection(*(set(c) for c in curves)) if t <= spec['horizon_s'])
        # Grid plus requested/sensitivity horizons can contain fractional checkpoints.
        balance = [min(c[t] for c in curves)-spec['uncertainty_requests'] for t in times]
        payback = next((t for i,t in enumerate(times) if balance[i] > 0 and min(balance[i:]) >= 0),None)
        adjusted = gain-spec['uncertainty_requests']
        action = 'SWITCH' if adjusted > 0 and adjusted >= spec['min_gain_requests'] else 'KEEP' if delta <= 0 and adjusted <= 0 else 'WAIT'
        expected = dict(gain_requests=gain,break_even_s=payback,transition_deficit_requests=deficit,
            observed_transition_complete_after_switch_s=readiness,delta_rps=delta)
    for key, value in expected.items():
        actual = review["decision"][key]
        if value is None:
            assert actual is None, key
        else:
            assert math.isclose(actual,value,rel_tol=1e-10,abs_tol=1e-8), key
    assert review["decision"]["action"] == action
    return {"status":"PASS", "scope":"independent raw-event arithmetic, not metadata authenticity or forecast validation",
        "action":action, "pairs":len(values), **expected}


if __name__ == "__main__":
    print(json.dumps(verify(load(Path(sys.argv[1])),Path(sys.argv[2])),indent=2))
