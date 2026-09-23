"""Replay the explicitly projected reference logs through public conversion and SDK.

Run the installed analysis server first. This sends evidence to that server only;
it does not collect measurements, call model providers, or deploy anything.
"""
import argparse
import hashlib
import json
from pathlib import Path
import time

from transitionbench.external import convert_logs
from transitionbench.sdk import Client
from transitionbench.verifier import verify_bundle


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Extracted reference directory containing index.json")
    parser.add_argument("--output", required=True, type=Path, help="New directory for converted evidence and results")
    parser.add_argument("--api", default="http://127.0.0.1:8765")
    args = parser.parse_args()
    index = json.loads((args.source / "index.json").read_text(encoding="utf-8"))
    args.output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    result = {"data_origin": index["data_origin"], "runs": [], "reviews": []}
    with Client(args.api) as client:
        ids = {"calibration": [], "heldout": []}
        for run in index["runs"]:
            name = run["directory"]
            if Path(name).name != name or "\\" in name or name in (".", ".."):
                raise ValueError("Reference run directory must be a single name")
            converted = convert_logs(args.source / name, args.output / name)
            root = Path(converted["bundle"])
            check = verify_bundle(root)
            if not check["integrity_valid"] or not check["experiment_valid"]:
                raise ValueError(check)
            summary = json.loads((root / "summary.json").read_text())
            digest = hashlib.sha256(json.dumps(summary, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
            if digest != run["original_summary_sha256"]:
                raise ValueError(f"Native summary parity failed: {name}")
            imported = client.import_evidence(converted["archive"])["bundle_id"]
            ids[run["split"]].append(imported)
            result["runs"].append({"run": name, "bundle_id": imported, "summary_parity": True, "offered": summary["offered"], "qualified": summary["qualified"], "source_sha256": converted["source_sha256"]})
        for horizon in (20, 40, 120):
            # Historical timestamps remain intact; this is a replay, not fresh evidence.
            review = client.review(ids["calibration"], horizon_s=horizon, max_evidence_age_s=2592000)
            if review["decision"]["action"] == "INSUFFICIENT_EVIDENCE":
                raise ValueError({"message": "Historical replay declined; do not refresh measurement timestamps", "decision": review["decision"]})
            outcome = client.observe(review["id"], ids["heldout"])
            if outcome["status"] != "COMPARABLE":
                raise ValueError({"message": "Outcome evidence is not comparable", "issues": outcome["issues"]})
            if client.get_proposal(review["id"])["decision"] != review["decision"]:
                raise ValueError("Saved forecast changed after observation")
            for kind, value in (("review", review), ("outcome", outcome)):
                (args.output / f"{kind}-{horizon}.json").write_text(json.dumps(value, indent=2), encoding="utf-8")
            result["reviews"].append({"horizon_s": horizon, "action": review["decision"]["action"], "gain_requests": review["decision"]["gain_requests"], "outcome_status": outcome["status"], "observed_net_requests": [p["observed_net_requests"] for p in outcome["pairs"]], "forecast_preserved": True})
    result["elapsed_s"] = time.monotonic() - started
    (args.output / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({"result": str(args.output / "result.json"), "runs": len(result["runs"]), "reviews": result["reviews"], "elapsed_s": result["elapsed_s"]}, indent=2))


if __name__ == "__main__":
    main()
