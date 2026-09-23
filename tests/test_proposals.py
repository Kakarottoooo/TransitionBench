"""Public deployment review contract. Generated events are test fixtures, not results."""
import io
import json
import time
import zipfile
import pytest

from fastapi.testclient import TestClient
from transitionbench.api import create_app
from transitionbench.evidence import export_bundle
from transitionbench.schemas import ExperimentSpec, RequestEvent, RunManifest

HEADERS = {"X-TransitionBench": "1", "Idempotency-Key": "proposal-test"}


def bundle(tmp_path, seed, role, *, origin="measured-black-box", age=0, ready_s=15, transition_intervals=None):
    name = f"{seed}-{role}"
    mode = "SIMULATION" if origin == "synthetic" else "LIVE_ENDPOINT"
    spec = ExperimentSpec(mode=mode, workload={"seed": seed, "injection_s": 40, "rate_rps": 2}, observation_s=50, drain_s=10)
    rows = []
    for i in range(80):
        t = i / 2
        good = role == "candidate" or (role == "current" and i % 2 == 0) or (role == "transition" and (t < 5 and i % 2 == 0 or t >= 15))
        if role == "transition" and transition_intervals is not None:
            good = (t < 5 and i % 2 == 0) or any(lo <= t < hi for lo,hi in transition_intervals)
        rows.append(RequestEvent(request_id=str(i), scheduled_s=t, dispatch_s=t, first_content_s=t+.05,
            completed_s=t+.1, output_chars=1, finish_reason="stop", termination="complete",
            quality_valid=good, quality_check="not-assessed" if role == "current" and i == 1 else "fixture-marker", origin=origin))
    manifest = RunManifest(run_id=name, mode=mode, origin=origin, experiment=spec,
        offered_ids=[r.request_id for r in rows], created_at_unix_s=time.time()-age,
        versions={"engine": "fixture-1", "model_revision": "fixture-1"},
        configurations={"A": {"cache": False}, "B": {"cache": True}},
        policy_parameters={"initial_config": "B" if role == "candidate" else "A",
            "offered_sha256": str(seed), "trial": {"seed": seed, "phases": [{"kind": "short", "rate": 2, "duration_s": 40}]}})
    events = [{"worker_id": "fixture", "state": "STOPPING", "at_s": 5}, {"worker_id": "fixture", "state": "COMPLETE", "at_s": ready_s}] if role == "transition" else []
    folder = tmp_path / name
    export_bundle(folder, manifest, rows, events, [])
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as z:
        for path in folder.iterdir():
            z.write(path, path.name)
    return archive.getvalue()


def import_pairs(client, tmp_path, seeds=(1, 2, 3), **kw):
    pairs = []
    for seed in seeds:
        pair = {}
        for role in ("current", "candidate", "transition"):
            response = client.post("/api/v1/bundles/import", files={"file": ("evidence.zip", bundle(tmp_path, seed, role, **kw))}, headers=HEADERS)
            assert response.status_code == 200, response.text
            pair[role] = response.json()["bundle_id"]
        pairs.append(pair)
    return pairs


def proposal(client, pairs):
    profile = client.get(f"/api/v1/bundles/{pairs[0]['current']}/profile")
    assert profile.status_code == 200, profile.text
    return dict(pairs=pairs, current_config="A", candidate_config="B",
        expected_context=profile.json()["context_sha256"], horizon_s=30,
        steady_window_s=10, uncertainty_requests=0, min_gain_requests=2)


def test_import_evaluate_and_persist_read_only_recommendation(tmp_path):
    root = tmp_path / "service"
    with TestClient(create_app(root)) as client:
        pairs = import_pairs(client, tmp_path)
        body = proposal(client, pairs)
        response = client.post("/api/v1/proposals", json=body, headers=HEADERS)
        assert response.status_code == 200, response.text
        review = response.json()
        assert review["decision"]["action"] == "SWITCH", review
        assert review["decision"]["break_even_s"] == 21
        assert review["decision"]["steady_rate_model"]["break_even_s"] == 20
        assert review["decision"]["gain_requests"] == 10
        assert review["execution_authorized"] is False
        assert review["forecast_kind"] == "paired-transition-curve-v1"
        assert len(review["pairs"]) == 3
        assert client.post("/api/v1/proposals", json=body, headers=HEADERS).json()["id"] == review["id"]
    with TestClient(create_app(root)) as client:
        saved = client.get(f"/api/v1/proposals/{review['id']}").json()
        assert saved["decision"] == review["decision"]


@pytest.mark.parametrize("change,issue", [
    ({"expected_context": "0"*64}, "context_mismatch"),
    ({"horizon_s": 100}, "horizon_exceeds_observed_support"),
    ({"steady_window_s": 39}, "steady_window_not_after_readiness"),
    ({"max_evidence_age_s": .000001}, "evidence_stale_or_future_dated"),
])
def test_unqualified_evidence_never_produces_deployment_advice(tmp_path, change, issue):
    with TestClient(create_app(tmp_path / "service")) as client:
        body = proposal(client, import_pairs(client, tmp_path))
        body.update(change)
        review = client.post("/api/v1/proposals", json=body, headers=HEADERS).json()
        assert review["decision"]["action"] == "INSUFFICIENT_EVIDENCE"
        assert issue in review["decision"]["reasons"]
        assert review["decision"]["gain_requests"] is None


def test_duplicate_and_synthetic_evidence_are_not_replicated_measurements(tmp_path):
    with TestClient(create_app(tmp_path / "service")) as client:
        pairs = import_pairs(client, tmp_path, seeds=(1,), origin="synthetic")
        body = proposal(client, pairs*3)
        result = client.post("/api/v1/proposals", json=body, headers=HEADERS).json()
        assert result["decision"]["action"] == "INSUFFICIENT_EVIDENCE"
        assert {"duplicate_evidence", "independent_seeds_required", "measured_provenance_required"} <= set(result["decision"]["reasons"])


def test_short_horizon_wait_and_idempotency_conflict(tmp_path):
    with TestClient(create_app(tmp_path / "service")) as client:
        body = proposal(client, import_pairs(client, tmp_path))
        body["horizon_s"] = 10
        result = client.post("/api/v1/proposals", json=body, headers=HEADERS).json()
        assert result["decision"]["action"] == "WAIT"
        body["horizon_s"] = 30
        assert client.post("/api/v1/proposals", json=body, headers=HEADERS).status_code == 422


def test_independent_outcome_compares_frozen_forecast_without_rewriting_it(tmp_path):
    with TestClient(create_app(tmp_path / "service")) as client:
        body = proposal(client, import_pairs(client, tmp_path))
        review = client.post("/api/v1/proposals", json=body, headers=HEADERS).json()
        pairs = import_pairs(client, tmp_path, seeds=(4,))
        route = f"/api/v1/proposals/{review['id']}/outcomes"
        response = client.post(route, json={"pairs": pairs}, headers=HEADERS)
        assert response.status_code == 200, response.text
        outcome = response.json()
        assert outcome["status"] == "COMPARABLE"
        assert outcome["pairs"][0]["observed_net_requests"] == 10
        assert outcome["pairs"][0]["prediction_error_requests"] == 0
        assert 20 < outcome["pairs"][0]["observed_payback_after_switch_s"] <= 21
        assert client.post(route, json={"pairs": pairs}, headers=HEADERS).json()["id"] == outcome["id"]
        saved = client.get(f"/api/v1/proposals/{review['id']}").json()
        assert saved["decision"] == review["decision"]
        assert len(saved["outcomes"]) == 1
        reused = client.post(route, json={"pairs": body["pairs"]}, headers={**HEADERS, "Idempotency-Key": "reused"}).json()
        assert reused["status"] == "NOT_COMPARABLE"
        assert "calibration_evidence_reused" in reused["issues"]


def test_serving_before_warmup_complete_can_already_repay_transition(tmp_path):
    with TestClient(create_app(tmp_path / "service")) as client:
        body = proposal(client, import_pairs(client, tmp_path, ready_s=30))
        body["horizon_s"] = 23
        result = client.post("/api/v1/proposals", json=body, headers=HEADERS).json()
        assert result["decision"]["gain_requests"] > 2
        assert result["decision"]["action"] == "SWITCH"
        assert result["decision"]["gain_requests"] == 3
        assert result["decision"]["observed_transition_complete_after_switch_s"] == 25
        assert result["pairs"][0]["first_qualified_completion_after_switch_s"] == pytest.approx(10.1)
        assert result["decision"]["break_even_s"] == 21
        assert all(x["horizon_s"] <= 35 for x in result["decision"]["sensitivity"])


def test_early_service_does_not_erase_later_transition_losses(tmp_path):
    with TestClient(create_app(tmp_path / "service")) as client:
        body = proposal(client, import_pairs(client, tmp_path, transition_intervals=[(5,8),(20,40)]))
        body["horizon_s"] = 10
        early = client.post("/api/v1/proposals", json=body, headers=HEADERS).json()
        assert early["pairs"][0]["first_qualified_completion_after_switch_s"] == pytest.approx(.1)
        assert early["decision"]["action"] == "WAIT"
        assert early["decision"]["gain_requests"] < 0
        assert early["decision"]["break_even_s"] is None
        body["horizon_s"] = 30
        later = client.post("/api/v1/proposals", json=body, headers={**HEADERS,"Idempotency-Key":"later"}).json()
        assert later["decision"]["action"] == "SWITCH"
        assert later["decision"]["break_even_s"] == 25


def test_new_outcome_payback_can_precede_declared_warmup_completion(tmp_path):
    with TestClient(create_app(tmp_path / "service")) as client:
        body = proposal(client, import_pairs(client, tmp_path, ready_s=30))
        body["horizon_s"] = 23
        review = client.post("/api/v1/proposals", json=body, headers=HEADERS).json()
        fresh = import_pairs(client,tmp_path,seeds=(4,),ready_s=30)
        outcome = client.post(f"/api/v1/proposals/{review['id']}/outcomes",json={"pairs":fresh},headers=HEADERS).json()
        assert outcome["status"] == "COMPARABLE"
        assert outcome["pairs"][0]["observed_payback_after_switch_s"] == 21
        assert outcome["pairs"][0]["declared_transition_complete_after_switch_s"] == 25


def test_cumulative_advice_keeps_explicit_allowance_and_no_gain_wait(tmp_path):
    with TestClient(create_app(tmp_path / "service")) as client:
        body = proposal(client, import_pairs(client,tmp_path,ready_s=30))
        body.update(horizon_s=23,uncertainty_requests=2)
        result = client.post("/api/v1/proposals",json=body,headers=HEADERS).json()
        assert result["decision"]["gain_requests"] == 3
        assert result["decision"]["action"] == "WAIT"  # 3 minus allowance 2 is below practical gain 2.


def test_expiry_is_reported_without_mutating_original_decision(tmp_path, monkeypatch):
    with TestClient(create_app(tmp_path / "service")) as client:
        body = proposal(client, import_pairs(client, tmp_path))
        review = client.post("/api/v1/proposals", json=body, headers=HEADERS).json()
        monkeypatch.setattr("transitionbench.proposals.time.time", lambda: review["decision"]["expires_at_unix_s"]+1)
        saved = client.get(f"/api/v1/proposals/{review['id']}").json()
        assert saved["expired"] is True
        assert saved["decision"] == review["decision"]


def test_imported_evidence_is_reverified_and_trace_swaps_are_refused(tmp_path):
    root = tmp_path / "service"
    with TestClient(create_app(root)) as client:
        body = proposal(client, import_pairs(client, tmp_path))
        body["pairs"][0]["candidate"], body["pairs"][1]["candidate"] = body["pairs"][1]["candidate"], body["pairs"][0]["candidate"]
        review = client.post("/api/v1/proposals", json=body, headers=HEADERS).json()
        assert "offered_trace_not_paired" in review["decision"]["reasons"]
        evidence = root / "bundles" / body["pairs"][0]["current"] / "requests.jsonl"
        evidence.write_text(evidence.read_text()+"{}\n")
        response = client.post("/api/v1/proposals", json=body, headers={**HEADERS,"Idempotency-Key":"tampered"})
        assert response.status_code == 422


def test_configuration_swap_is_not_a_valid_switch(tmp_path):
    with TestClient(create_app(tmp_path / "service")) as client:
        body = proposal(client, import_pairs(client, tmp_path))
        body.update(current_config="B", candidate_config="A")
        review = client.post("/api/v1/proposals", json=body, headers=HEADERS).json()
        assert review["decision"]["action"] == "INSUFFICIENT_EVIDENCE"
        assert "initial_configuration_mismatch" in review["decision"]["reasons"]


def test_auto_review_pairs_evidence_and_attaches_fresh_outcome(tmp_path):
    with TestClient(create_app(tmp_path / "service")) as client:
        pairs = import_pairs(client, tmp_path)
        bundle_ids = [bid for p in reversed(pairs) for bid in reversed(list(p.values()))]
        response = client.post("/api/v1/proposals/auto", json={"bundle_ids":bundle_ids,"horizon_s":30,"steady_window_s":10}, headers=HEADERS)
        assert response.status_code == 200, response.text
        review = response.json()
        assert review["decision"]["action"] == "SWITCH"
        assert review["request"]["current_config"] == "A"
        assert review["request"]["candidate_config"] == "B"
        fresh = import_pairs(client, tmp_path, seeds=(4,))
        result = client.post(f"/api/v1/proposals/{review['id']}/outcomes/imported", json={"bundle_ids":list(fresh[0].values())}, headers=HEADERS)
        assert result.status_code == 200, result.text
        assert result.json()["status"] == "COMPARABLE"
        duplicate = client.post("/api/v1/proposals/auto", json={"bundle_ids":bundle_ids+[bundle_ids[0]],"horizon_s":30,"steady_window_s":10}, headers={**HEADERS,"Idempotency-Key":"ambiguous"})
        assert duplicate.status_code == 422

