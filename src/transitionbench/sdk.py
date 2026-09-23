"""Thin Python client for the versioned HTTP boundary."""
import time
import uuid
import httpx


class Client:
    def __init__(self, base_url="http://127.0.0.1:8765", operator_token=None):
        headers = {"X-TransitionBench": "1"}
        if operator_token:
            headers["X-Operator-Token"] = operator_token
        self.http = httpx.Client(base_url=base_url.rstrip("/"), headers=headers, timeout=30, trust_env=False, follow_redirects=False)

    def request(self, method, path, body=None, key=None):
        response = self.http.request(method, path, json=body, headers={"Idempotency-Key": key or uuid.uuid4().hex})
        response.raise_for_status()
        return response.json()

    def capabilities(self):
        return self.request("GET", "/api/v1/capabilities")

    def validate(self, spec):
        return self.request("POST", "/api/v1/experiments/validate", spec)

    def run(self, spec, idempotency_key=None):
        return self.request("POST", "/api/v1/runs", spec, idempotency_key)

    def get_run(self, run_id):
        return self.request("GET", "/api/v1/runs/" + run_id)

    def wait(self, run_id, timeout_s=120):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            run = self.get_run(run_id)
            if run["state"] not in ("QUEUED", "RUNNING", "CANCELLING"):
                return run
            time.sleep(.1)
        raise TimeoutError("Run continues on server; poll its durable ID")

    def cancel(self, run_id):
        return self.request("POST", f"/api/v1/runs/{run_id}/cancel")

    def evaluate(self, decision):
        return self.request("POST", "/api/v1/decisions/evaluate", decision)

    def import_evidence(self, path):
        """Import an existing native bundle directory or ZIP; no provider calls."""
        import io
        import zipfile
        from pathlib import Path
        source = Path(path)
        if source.is_dir():
            from .verifier import REQUIRED
            stream = io.BytesIO()
            with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
                for name in sorted(REQUIRED | {"checksums.json"}):
                    archive.write(source / name, name)
            content = stream.getvalue()
        else:
            content = source.read_bytes()
        response = self.http.post("/api/v1/bundles/import", files={"file": ("evidence.zip", content, "application/zip")})
        response.raise_for_status()
        return response.json()

    def evidence_profile(self, bundle_id):
        return self.request("GET", f"/api/v1/bundles/{bundle_id}/profile")

    def propose(self, proposal, idempotency_key=None):
        return self.request("POST", "/api/v1/proposals", proposal, idempotency_key)

    def review(self, bundle_ids, horizon_s=120, idempotency_key=None, **assumptions):
        """Automatically pair imported measurements; evaluate within their scope."""
        return self.request("POST", "/api/v1/proposals/auto",
            {"bundle_ids": bundle_ids, "horizon_s": horizon_s, **assumptions}, idempotency_key)

    def observe(self, proposal_id, bundle_ids, idempotency_key=None):
        """Pair and attach imported outcome evidence to an existing review."""
        return self.request("POST", f"/api/v1/proposals/{proposal_id}/outcomes/imported",
            {"bundle_ids": bundle_ids}, idempotency_key)

    def get_proposal(self, proposal_id):
        return self.request("GET", f"/api/v1/proposals/{proposal_id}")

    def record_outcome(self, proposal_id, outcome, idempotency_key=None):
        return self.request("POST", f"/api/v1/proposals/{proposal_id}/outcomes", outcome, idempotency_key)

    def import_review_file(self, path):
        """Local adapter: replace the three bundle paths per pair with registry IDs.

        Keep settings (including target context) explicit. This does not certify
        the caller's environment or infer deployment permissions.
        """
        import json
        from pathlib import Path
        source = Path(path).resolve()
        document = json.loads(source.read_text(encoding="utf-8"))
        pairs = []
        for pair in document["pairs"]:
            pairs.append({role: self.import_evidence(source.parent / pair[role])["bundle_id"]
                for role in ("current", "candidate", "transition")})
        return {**document["settings"], "pairs": pairs}

    def records(self, run_id):
        return self.request("GET", f"/api/v1/runs/{run_id}/records")

    def compare(self, run_ids):
        return self.request("POST", "/api/v1/runs/compare", {"run_ids": run_ids})

    def export(self, run_id, path):
        response = self.http.get(f"/api/v1/runs/{run_id}/artifacts/evidence.zip")
        response.raise_for_status()
        from pathlib import Path
        Path(path).write_bytes(response.content)
        return {"path": str(path), "bytes": len(response.content)}

    def close(self):
        self.http.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
