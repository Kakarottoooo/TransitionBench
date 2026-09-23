# HTTP, CLI, Python, TypeScript and MCP

The HTTP API is the language-neutral boundary. Python models in `schemas.py`
generate [OpenAPI](openapi.json), [JSON schemas](schemas), and TypeScript schema
types via `python scripts/export_contract.py`. Clients contain no policy math.

## Local HTTP

```text
transitionbench serve --port 8765 --config examples/operator.json
```

Omit `--config` for analysis/simulation only. `/healthz` and `/docs` describe the
running service. All mutation requests require `X-TransitionBench: 1` and JSON
content type (multipart for import). Browser requests must be same-origin; this
is not a public multi-tenant service. No permissive CORS policy is installed.

| Operation | Route |
|---|---|
| Capability manifest | `GET /api/v1/capabilities` |
| Operator-approved endpoint registration | `POST /api/v1/endpoints` |
| Generate workload metadata | `POST /api/v1/workloads` |
| Validate without traffic | `POST /api/v1/experiments/validate` |
| Start durable job | `POST /api/v1/runs` |
| Status / summary | `GET /api/v1/runs/{id}` |
| Original evidence records | `GET /api/v1/runs/{id}/records` |
| Progress SSE | `GET /api/v1/runs/{id}/events` |
| Stop new work / cancel in-flight | `POST /api/v1/runs/{id}/cancel` |
| Compare matched valid runs | `POST /api/v1/runs/compare` |
| Import verified evidence ZIP | `POST /api/v1/bundles/import` |
| Evaluate assumptions | `POST /api/v1/decisions/evaluate` |
| Create immutable plan | `POST /api/v1/transition-plans` |
| Approve exact hash | `POST /api/v1/transition-plans/{id}/approve` |
| Start rollout / poll plan | `POST .../{id}/execute`, `GET .../{id}` |
| Export artifact list / download | `GET /api/v1/runs/{id}/artifacts[/{name}]` |

`POST /runs` requires `Idempotency-Key`. The same key+body returns the existing
job; changed content under the same key is refused. Runs return 202 immediately.
States are `QUEUED`, `RUNNING`, `CANCELLING`, `SUCCEEDED`, `INVALID`, `CANCELLED`,
`FAILED`, `INTERRUPTED`. A measured negative result is still `SUCCEEDED` when
the experiment is valid. Unfavorable results are not errors.

The SSE `id` is a durable SQLite sequence. Reconnect with `Last-Event-ID` or
`?after=N`; read events strictly greater than that sequence. Heartbeats contain
no experimental event. Reconnects may be handled at least once by callers;
deduplicate on sequence. An interrupted process never replays deployment writes.

Structured refusal codes distinguish `invalid_experiment`, `budget_refusal`,
`unsupported_capability`, `credentials_missing`, `approval_required`, and
`insufficient_evidence`. Validation errors use FastAPI's field-path details.
Payload and import limits are enforced. No arbitrary URLs or shell strings are
accepted in run specifications.

## CLI

The [README](../README.md) shows validate/run/inspect/export/verify. Additional
commands include `doctor`, `demo`, `serve`, `smoke ENDPOINT_ID`, `decision FILE`,
`plan FILE`, `approve PLAN_ID HASH`, `execute PLAN_ID`, `cancel RUN_ID`, `import ZIP`,
`lab-start CONFIG`, `lab-hook CONFIG`, and `mcp`. Global `--api URL` precedes the
subcommand. The demo is one command after wheel installation.

Approval/execute commands read the **local operator authorization token** from
`TRANSITIONBENCH_OPERATOR_TOKEN`. Provider keys are separate and never passed in
HTTP bodies. The demo can only use endpoints already granted a bounded envelope
in the startup configuration; it cannot register an arbitrary destination.

## Clients

```python
from transitionbench.sdk import Client
with Client() as tb:
    job = tb.run({"mode": "SIMULATION"})
    result = tb.wait(job["id"])
    print(result["origin"], result["summary"]["qualified"])
```

Install the supplied npm tarball into an owned Node project:

```text
npm install /absolute/path/dist/transitionbench-local-client-0.1.0.tgz
```

```javascript
import {TransitionBench} from '@transitionbench/local-client';
const tb = new TransitionBench('http://127.0.0.1:8765');
const job = await tb.run({mode: 'SIMULATION'});
console.log(await tb.wait(job.id));
```

Working files: `examples/python_client.py`, `examples/typescript_client.mjs`,
`examples/curl.ps1`. On Unix use curl with the same JSON body and two headers;
do not paste provider credentials into these examples.

## MCP

Start `transitionbench-mcp` or `transitionbench mcp` with the local API running.
The maintained official Python SDK is pinned to 1.26.0 and negotiates protocol
2025-11-25 in the tested stdio handshake. `TRANSITIONBENCH_API` can select a
different loopback port. There is no Streamable HTTP MCP transport in this
release; the REST routes are not called MCP.

Tools: `list_capabilities`, `validate_experiment`, `analyze_bundle`, `get_run`,
`compare_runs`, `explain_decision`, `plan_transition`. The server has **no** live
run, approval, execute, arbitrary-fetch or shell tool. Plan creation does not
grant execution authority. Annotations describe tools; enforced API/hook
permissions provide the actual boundary. Test a real handshake with
`python examples/mcp_client.py`.

