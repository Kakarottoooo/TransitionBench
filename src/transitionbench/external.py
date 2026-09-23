"""One explicit request-log format into the canonical evidence pipeline.

No collection, scoring, inference, or deployment implementation lives here.
"""
import hashlib
import json
import math
from pathlib import Path
import tempfile

from .evidence import export_bundle, zip_bundle
from .schemas import RequestEvent, RunManifest
from .verifier import MAX_BYTES, verify_bundle


# All other RequestEvent fields retain their documented names.
NAMES = {"request_id": "id", "scheduled_s": "arrival", "dispatch_s": "sent",
         "first_content_s": "first_content", "first_reasoning_s": "first_reasoning",
         "final_content_s": "final_content", "completed_s": "end",
         "chunk_times_s": "chunks", "scheduling_lag_s": "dispatch_lag",
         "termination": "status", "quality_valid": "quality_pass"}
TIMES = {"arrival", "sent", "first_content", "first_reasoning", "final_content", "end", "dispatch_lag"}
REQUIRED = {"id", "arrival", "sent", "first_content", "end", "status",
            "quality_pass", "quality_check", "output_chars", "workload_class", "prefix_group"}


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _json(text):
    return json.loads(text, object_pairs_hook=_object)


def _number(value, name):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{name}: expected a finite nonnegative number")
    return value


def _lines(text, name):
    for line, text_line in enumerate(text.splitlines(), 1):
        if not text_line.strip():
            continue
        try:
            value = _json(text_line)
            if not isinstance(value, dict):
                raise ValueError("expected an object")
        except ValueError as exc:
            raise ValueError(f"{name}:{line}: {exc}") from exc
        yield line, value


def _require(value, fields, name):
    if not isinstance(value, dict):
        raise ValueError(f"{name}: expected an object")
    missing = set(fields.split()) - value.keys()
    if missing:
        raise ValueError(f"{name}: missing explicit fields {sorted(missing)}")


def convert_logs(source, output):
    """Convert a directory of three reference files to a NEW native bundle folder.

    Validation happens in a temporary sibling directory; rejected input creates
    no bundle. Metadata, quality checks and offered IDs are operator declarations,
    not facts this converter can certify. Output also contains evidence.zip.
    """
    source, output = Path(source), Path(output)
    if output.exists():
        raise ValueError("output already exists; choose a new directory")
    files = ("run.json", "requests.jsonl", "transitions.jsonl")
    # Bound allocation before reading; hash exactly the bytes actually parsed.
    raw = {}
    for name in files:
        with (source / name).open("rb") as handle:
            data = handle.read(MAX_BYTES + 1)
        raw[name] = data
        if sum(map(len, raw.values())) > MAX_BYTES:
            raise ValueError("reference input exceeds 32 MiB")
    meta = _json(raw["run.json"].decode("utf-8-sig"))
    expected = {"format", "time_unit", "time_basis", "capture_end", "manifest", "validity"}
    if not isinstance(meta, dict) or set(meta) != expected:
        raise ValueError("run.json requires exactly: " + ", ".join(sorted(expected)))
    if meta["format"] != "transitionbench-request-log-v1":
        raise ValueError("unsupported reference format")
    if meta["time_unit"] not in ("s", "ms") or meta["time_basis"] != "run-relative-monotonic":
        raise ValueError("declare time_unit s or ms and time_basis run-relative-monotonic")
    scale = 1 if meta["time_unit"] == "s" else 0.001
    capture = _number(meta["capture_end"], "capture_end") * scale
    value = meta["manifest"]
    _require(value, "run_id mode origin experiment offered_ids created_at_unix_s versions hardware resource_intervals configurations policy_parameters limitations clock_domain", "manifest")
    experiment = value.get("experiment", {})
    _require(experiment, "mode slo workload observation_s drain_s budget max_dispatch_lag_s", "manifest.experiment")
    _require(experiment["budget"], "max_requests max_total_tokens max_output_tokens max_concurrency max_duration_s reserved_gpus max_reserved_gpu_seconds", "manifest.experiment.budget")
    _require(experiment["workload"], "kind seed split rate_rps injection_s arrival_model long_prefix_mode", "manifest.experiment.workload")
    if not isinstance(experiment, dict) or not isinstance(experiment.get("slo"), dict) or not {"e2e_s", "first_content_s"} <= experiment["slo"].keys():
        raise ValueError("manifest.experiment.slo requires explicit e2e_s and first_content_s")
    for field in ("e2e_s", "first_content_s"):
        _number(experiment["slo"].get(field), f"slo.{field}")
    if not isinstance(value.get("offered_ids"), list) or not value["offered_ids"]:
        raise ValueError("manifest.offered_ids must declare the full offered request roster")
    if any(not isinstance(x, str) or not x for x in value["offered_ids"]):
        raise ValueError("manifest.offered_ids requires nonempty string IDs")
    if len(value["offered_ids"]) != len(set(value["offered_ids"])):
        raise ValueError("manifest.offered_ids contains duplicates")
    manifest = RunManifest.model_validate(value)
    if manifest.clock_domain != "client-monotonic-relative":
        raise ValueError("manifest.clock_domain must be client-monotonic-relative; synchronize clocks before import")
    if capture < manifest.experiment.observation_s:
        raise ValueError("capture_end precedes observation window; check time units")
    validity = meta["validity"]
    if not isinstance(validity, dict) or validity.get("valid") is not True or validity.get("errors") != []:
        raise ValueError("source validity must explicitly be valid=true, errors=[]; retain invalid source for diagnosis")
    if "external_import" in manifest.policy_parameters:
        raise ValueError("already converted metadata; use the original source to retain provenance")
    manifest.policy_parameters["external_import"] = {
        "format": meta["format"], "time_unit": meta["time_unit"], "time_basis": meta["time_basis"],
        "capture_end_s": capture,
        "source_sha256": {name: hashlib.sha256(data).hexdigest() for name, data in raw.items()},
        "certified": False,
    }
    reverse = {NAMES.get(k, k): k for k in RequestEvent.model_fields}
    rows = []
    for line, row in _lines(raw["requests.jsonl"].decode("utf-8-sig"), "requests.jsonl"):
        try:
            missing, unknown = REQUIRED - row.keys(), row.keys() - reverse.keys()
            if missing or unknown:
                raise ValueError(f"missing fields {sorted(missing)}; unknown fields {sorted(unknown)}")
            if type(row["quality_pass"]) is not bool:
                raise ValueError("quality_pass must be an explicit boolean, not inferred from HTTP status")
            if not isinstance(row["quality_check"], str) or not row["quality_check"].strip() or (row["quality_pass"] and row["quality_check"] == "not-assessed"):
                raise ValueError("quality_check must identify the check; not-assessed cannot pass")
            for key in TIMES & row.keys():
                if row[key] is not None:
                    row[key] = _number(row[key], key) * scale
                    if row[key] > capture:
                        raise ValueError(f"{key} exceeds capture_end; check time units and clock basis")
            if "chunks" in row:
                if not isinstance(row["chunks"], list):
                    raise ValueError("chunks must be a list")
                row["chunks"] = [_number(x, "chunks") * scale for x in row["chunks"]]
                if any(x > capture for x in row["chunks"]):
                    raise ValueError("chunks exceed capture_end")
            for key in ("output_chars", "output_tokens", "input_tokens", "attempt", "status_code"):
                if row.get(key) is not None and type(row[key]) is not int:
                    raise ValueError(f"{key} must be an integer")
            native = {reverse[k]: v for k, v in row.items()}
            native.setdefault("origin", manifest.origin)
            native.setdefault("clock_domain", manifest.clock_domain)
            rows.append(RequestEvent.model_validate(native))
        except ValueError as exc:
            raise ValueError(f"requests.jsonl:{line}: {exc}") from exc
    events = []
    for line, event in _lines(raw["transitions.jsonl"].decode("utf-8-sig"), "transitions.jsonl"):
        if set(event) != {"time", "worker_id", "state"}:
            raise ValueError(f"transitions.jsonl:{line}: expected time, worker_id, state")
        if event["state"] not in {"STOPPING", "STARTING", "COMPLETE"} or not isinstance(event["worker_id"], str) or not event["worker_id"]:
            raise ValueError(f"transitions.jsonl:{line}: invalid worker or state")
        at = _number(event.pop("time"), f"transitions.jsonl:{line}:time") * scale
        if at > capture or (events and at < events[-1]["at_s"]):
            raise ValueError(f"transitions.jsonl:{line}: time outside capture or not ordered")
        events.append({**event, "at_s": at})
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".log-import-", dir=output.parent) as scratch:
        staged = Path(scratch) / "bundle"
        export_bundle(staged, manifest, rows, events, [], validity=validity)
        check = verify_bundle(staged)
        if not check["integrity_valid"] or not check["experiment_valid"]:
            raise ValueError("evidence rejected: " + "; ".join(check["integrity_errors"] + check["experiment_errors"]))
        # Keep the archive outside the native folder's exact member contract.
        zip_bundle(staged, Path(scratch) / "evidence.zip")
        if (Path(scratch) / "evidence.zip").stat().st_size > MAX_BYTES:
            raise ValueError("converted archive exceeds 32 MiB")
        Path(scratch).rename(output)
    return {"bundle": str(output / "bundle"), "archive": str(output / "evidence.zip"),
            "offered": len(rows), "experiment_valid": True, "certified": False,
            "source_sha256": manifest.policy_parameters["external_import"]["source_sha256"]}
