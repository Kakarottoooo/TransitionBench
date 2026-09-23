import json
import pytest
from transitionbench.schemas import ExperimentSpec, RunManifest, RequestEvent
from transitionbench.evidence import export_bundle
from transitionbench.verifier import verify_bundle


def test_independent_recompute_and_tamper(tmp_path):
    manifest = RunManifest(run_id="test", mode="SIMULATION", origin="synthetic",
                           experiment=ExperimentSpec(), offered_ids=["one"],
                           created_at_unix_s=0, versions={"transitionbench": "0.1.0"})
    rows = [RequestEvent(request_id="one", scheduled_s=0, dispatch_s=0,
                         first_content_s=.1, final_content_s=.2, completed_s=.2,
                         termination="complete", quality_valid=True, output_chars=3)]
    export_bundle(tmp_path / "bundle", manifest, rows, [], [])
    verified = verify_bundle(tmp_path / "bundle")
    assert verified["integrity_valid"]
    assert verified["recomputed"]["qualified"] == 1
    assert verified["experiment_valid"]
    (tmp_path / "bundle" / "requests.jsonl").write_text("{}\n")
    assert not verify_bundle(tmp_path / "bundle")["integrity_valid"]
