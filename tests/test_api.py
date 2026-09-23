import time
from fastapi.testclient import TestClient
from transitionbench.api import create_app

HEADERS = {"X-TransitionBench": "1", "Idempotency-Key": "cpu-test"}


def test_job_api_raw_evidence_and_idempotency(tmp_path):
    with TestClient(create_app(tmp_path)) as client:
        assert client.get("/healthz").json()["status"] == "ok"
        response = client.post("/api/v1/runs", json={}, headers=HEADERS)
        assert response.status_code == 202, response.text
        run_id = response.json()["id"]
        assert client.post("/api/v1/runs", json={}, headers=HEADERS).json()["id"] == run_id
        for _ in range(200):
            run = client.get("/api/v1/runs/" + run_id).json()
            if run["state"] not in ("QUEUED", "RUNNING"):
                break
            time.sleep(.02)
        assert run["state"] == "SUCCEEDED", run
        assert run["origin"] == "synthetic"
        records = client.get(f"/api/v1/runs/{run_id}/records").json()
        assert len(records["requests"]) == run["summary"]["offered"]
        check = client.get(f"/api/v1/runs/{run_id}/artifacts/verification.json").json()
        assert check["integrity_valid"] and check["experiment_valid"]
        conflict = client.post("/api/v1/runs", json={"policy": "StaticBest"}, headers=HEADERS)
        assert conflict.status_code == 422
        assert client.post("/api/v1/runs", json={}).status_code == 403


def test_replay_api_preserves_original_policy_and_workload(tmp_path):
    def wait(client, run_id):
        for _ in range(200):
            run = client.get('/api/v1/runs/' + run_id).json()
            if run['state'] not in ('QUEUED', 'RUNNING'):
                assert run['state'] == 'SUCCEEDED', run
                return run
            time.sleep(.02)
        raise AssertionError('Run did not finish')

    with TestClient(create_app(tmp_path)) as client:
        original = wait(client, client.post('/api/v1/runs', json={
            'policy': 'StaticBest', 'workload': {'kind': 'short', 'seed': 701}},
            headers={**HEADERS, 'Idempotency-Key': 'source'}).json()['id'])
        archive = client.get(f"/api/v1/runs/{original['id']}/artifacts/evidence.zip").content
        imported = client.post('/api/v1/bundles/import', files={'file': ('evidence.zip', archive, 'application/zip')}, headers=HEADERS)
        assert imported.status_code == 200, imported.text
        request = {'mode': 'RECORDED_REPLAY', 'replay_bundle_id': imported.json()['bundle_id'], 'slo': {'e2e_s': 3, 'first_content_s': 1.5}}
        headers = {**HEADERS, 'Idempotency-Key': 'replay'}
        replay = wait(client, client.post('/api/v1/runs', json=request, headers=headers).json()['id'])
        assert replay['spec']['policy'] == 'StaticBest'
        assert replay['spec']['workload'] == original['spec']['workload']
        assert replay['spec']['slo']['e2e_s'] == 3
        assert replay['mode'] == 'RECORDED_REPLAY'
        # The stored request digest remains the idempotency contract, not the resolved spec.
        assert client.post('/api/v1/runs', json=request, headers=headers).json()['id'] == replay['id']
        manifest = client.get(f"/api/v1/runs/{replay['id']}/records").json()['manifest']
        assert replay['spec'] == manifest['experiment']
        assert manifest['original_run_id'] == original['id']
