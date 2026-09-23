"""Reference-format fixtures validate integration, not measured performance."""
import json
from pathlib import Path
import pytest

from fastapi.testclient import TestClient
from transitionbench.api import create_app
from transitionbench.cli import main
from transitionbench.evidence import zip_bundle

from test_proposals import bundle
from transitionbench.external import convert_logs
from transitionbench.verifier import verify_bundle


def source_logs(tmp_path, seed=1, role="transition"):
    bundle(tmp_path, seed, role)
    native = tmp_path / f"{seed}-{role}"
    source = tmp_path / f"source-{seed}-{role}"
    source.mkdir()
    manifest = json.loads((native / "manifest.json").read_text())
    (source / "run.json").write_text(json.dumps({
        "format": "transitionbench-request-log-v1", "time_unit": "s",
        "time_basis": "run-relative-monotonic", "capture_end": 50,
        "manifest": manifest,
        "validity": json.loads((native / "validity.json").read_text()),
    }))
    names = {"request_id": "id", "scheduled_s": "arrival", "dispatch_s": "sent",
             "first_content_s": "first_content", "first_reasoning_s": "first_reasoning",
             "final_content_s": "final_content", "completed_s": "end",
             "chunk_times_s": "chunks", "scheduling_lag_s": "dispatch_lag",
             "termination": "status", "quality_valid": "quality_pass"}
    rows = [json.loads(line) for line in (native / "requests.jsonl").read_text().splitlines()]
    (source / "requests.jsonl").write_text("".join(json.dumps({names.get(k,k): v for k,v in r.items()})+"\n" for r in rows))
    events = [json.loads(line) for line in (native / "transitions.jsonl").read_text().splitlines()]
    (source / "transitions.jsonl").write_text("".join(json.dumps({("time" if k == "at_s" else k): v for k,v in r.items()})+"\n" for r in events))
    return source, native


def test_external_records_preserve_native_summary_and_independent_verification(tmp_path):
    source, native = source_logs(tmp_path)
    output = tmp_path / "converted"
    result = convert_logs(source, output)
    assert result["experiment_valid"] is True
    output = output / "bundle"
    assert json.loads((output / "summary.json").read_text()) == json.loads((native / "summary.json").read_text())
    check = verify_bundle(output)
    assert check["integrity_valid"] and check["experiment_valid"], check
    converted = json.loads((output / "manifest.json").read_text())
    original = json.loads((native / "manifest.json").read_text())
    assert converted["created_at_unix_s"] == original["created_at_unix_s"]
    assert len(converted["policy_parameters"]["external_import"]["source_sha256"]) == 3


@pytest.mark.parametrize("field", ["hardware", "resource_intervals", "clock_domain"])
def test_missing_operator_context_is_not_silently_defaulted(tmp_path, field):
    source, _ = source_logs(tmp_path)
    meta = json.loads((source / "run.json").read_text())
    del meta["manifest"][field]
    (source / "run.json").write_text(json.dumps(meta))
    with pytest.raises(ValueError, match=field):
        convert_logs(source, tmp_path / "converted")
    assert not (tmp_path / "converted").exists()


@pytest.mark.parametrize("case,diagnostic", [
    ("missing_quality", "quality_pass"), ("string_quality", "boolean"),
    ("unassessed_pass", "not-assessed"), ("missing_slo", "slo"),
    ("missing_request", "Missing or unoffered"), ("wrong_unit", "capture_end"),
    ("unknown_unit", "time_unit"), ("epoch_clock", "time_basis"),
    ("invalid_source", "source validity"), ("reverse_lifecycle", "lifecycle"),
    ("duplicate_key", "duplicate JSON key"), ("event_outside", "outside capture"),
])
def test_bad_input_is_diagnostic_and_leaves_no_output(tmp_path, case, diagnostic):
    source, _ = source_logs(tmp_path)
    meta = json.loads((source / "run.json").read_text())
    rows = [json.loads(s) for s in (source / "requests.jsonl").read_text().splitlines()]
    if case == "missing_quality": del rows[0]["quality_pass"]
    if case == "string_quality": rows[0]["quality_pass"] = "false"
    if case == "unassessed_pass": rows[0].update(quality_pass=True, quality_check="not-assessed")
    if case == "missing_slo": del meta["manifest"]["experiment"]["slo"]["e2e_s"]
    if case == "missing_request": rows.pop()
    if case == "wrong_unit": rows[1]["arrival"] = 500
    if case == "unknown_unit": meta["time_unit"] = "us"
    if case == "epoch_clock": meta["time_basis"] = "unix"
    if case == "invalid_source": meta["validity"]["valid"] = False
    if case == "reverse_lifecycle": rows[1]["sent"] = 0
    if case == "event_outside": (source / "transitions.jsonl").write_text('{"time":1000,"state":"COMPLETE","worker_id":"fixture"}\n')
    (source / "run.json").write_text(json.dumps(meta))
    (source / "requests.jsonl").write_text("".join(json.dumps(r)+"\n" for r in rows))
    if case == "duplicate_key":
        (source / "requests.jsonl").write_text('{"id":"a","id":"b"}\n')
    with pytest.raises(ValueError, match=diagnostic):
        convert_logs(source, tmp_path / "converted")
    assert not (tmp_path / "converted").exists()


def test_cli_explicit_milliseconds_and_no_overwrite(tmp_path, capsys):
    source, native = source_logs(tmp_path)
    meta = json.loads((source / "run.json").read_text())
    meta.update(time_unit="ms", capture_end=50000)
    (source / "run.json").write_text(json.dumps(meta))
    rows = [json.loads(s) for s in (source / "requests.jsonl").read_text().splitlines()]
    for row in rows:
        for key in ("arrival", "sent", "first_content", "first_reasoning", "final_content", "end", "dispatch_lag"):
            if row.get(key) is not None: row[key] *= 1000
        row["chunks"] = [v * 1000 for v in row["chunks"]]
    (source / "requests.jsonl").write_text("".join(json.dumps(r)+"\n" for r in rows))
    (source / "transitions.jsonl").write_text('{"time":5000,"state":"STOPPING","worker_id":"fixture"}\n{"time":15000,"state":"COMPLETE","worker_id":"fixture"}\n')
    output = tmp_path / "converted"
    main(["convert-logs", str(source), "--output", str(output)])
    assert json.loads(capsys.readouterr().out)["offered"] == 80
    summary = json.loads((output / "bundle/summary.json").read_text())
    original = json.loads((native / "summary.json").read_text())
    for key in ("offered", "qualified", "attainment", "goodput_rps"):
        assert summary[key] == original[key]
    assert verify_bundle(output / "bundle")["experiment_valid"]
    before = (output / "evidence.zip").read_bytes()
    with pytest.raises(ValueError, match="already exists"):
        convert_logs(source, output)
    assert (output / "evidence.zip").read_bytes() == before


def test_review_and_feedback_are_identical_across_ingress_paths(tmp_path):
    imports = {"native": [[], []], "converted": [[], []]}
    with TestClient(create_app(tmp_path / "service")) as client:
        for seed in range(1, 7):
            for role in ("current", "candidate", "transition"):
                source, native = source_logs(tmp_path, seed, role)
                converted = convert_logs(source, tmp_path / f"converted-{seed}-{role}")
                native_zip = tmp_path / f"native-{seed}-{role}.zip"
                zip_bundle(native, native_zip)
                for lane, archive in (("native", native_zip), ("converted", converted["archive"])):
                    response = client.post("/api/v1/bundles/import", files={"file": ("evidence.zip", Path(archive).read_bytes())}, headers={"X-TransitionBench": "1"})
                    assert response.status_code == 200, response.text
                    imports[lane][seed > 3].append(response.json()["bundle_id"])
        reviews, outcomes = {}, {}
        for lane, (calibration, heldout) in imports.items():
            headers = {"X-TransitionBench": "1", "Idempotency-Key": lane}
            response = client.post("/api/v1/proposals/auto", json={"bundle_ids": calibration, "horizon_s": 30, "steady_window_s": 10}, headers=headers)
            assert response.status_code == 200, response.text
            reviews[lane] = response.json()
            response = client.post(f"/api/v1/proposals/{reviews[lane]['id']}/outcomes/imported", json={"bundle_ids": heldout}, headers=headers)
            assert response.status_code == 200, response.text
            outcomes[lane] = response.json()
            frozen = client.get(f"/api/v1/proposals/{reviews[lane]['id']}").json()
            assert frozen["decision"] == reviews[lane]["decision"]
        def numerical_decision(review):
            return {k: v for k, v in review["decision"].items() if k not in ("evidence_ids", "expires_at_unix_s")}
        assert numerical_decision(reviews["native"]) == numerical_decision(reviews["converted"])
        assert reviews["converted"]["decision"]["action"] == "SWITCH"
        for lane in outcomes:
            assert outcomes[lane]["status"] == "COMPARABLE"
        fields = ("observed_net_requests", "prediction_error_requests", "observed_payback_after_switch_s", "curve")
        for a, b in zip(outcomes["native"]["pairs"], outcomes["converted"]["pairs"]):
            for field in fields:
                assert a[field] == b[field]
        # A valid conversion is not permission to recommend beyond measured support.
        refused = client.post("/api/v1/proposals/auto", json={"bundle_ids": imports["converted"][0], "horizon_s": 100, "steady_window_s": 10}, headers={"X-TransitionBench": "1", "Idempotency-Key": "outside"}).json()
        assert refused["decision"]["action"] == "INSUFFICIENT_EVIDENCE"
        bad = imports["converted"][0][:-1] + [imports["converted"][1][-1]]
        mismatch = client.post("/api/v1/proposals/auto", json={"bundle_ids": bad, "horizon_s": 30}, headers={"X-TransitionBench": "1", "Idempotency-Key": "mismatch"})
        assert mismatch.status_code == 422, mismatch.text
