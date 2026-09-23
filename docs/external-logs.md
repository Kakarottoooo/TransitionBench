# Bring request logs into a deployment review

TransitionBench includes a reference input format introduced in 0.4.3. Export your load generator's
per-request observations and deployment events into the fields below, provide
explicit run metadata, then call `convert-logs`. It produces a verified native
bundle accepted by the existing HTTP API, CLI and Python SDK. No core-code edits,
GPU, provider key or rollout permissions are required for conversion or review.

This is a **reference adapter**, not automatic discovery of arbitrary logs. Your
collector still owns request coverage, clock synchronization, quality evaluation
and truthful metadata. An HTTP 200 is not a quality result. A converter cannot
discover requests missing from both your declared roster and your logs, certify
operator declarations, or detect every consistently mislabeled clock/unit.

## Try the complete recorded example

Use Python 3.11–3.13. From the extracted delivery/repository:

```sh
python -m pip install https://github.com/Kakarottoooo/TransitionBench/releases/download/v0.4.4/transitionbench-0.4.4-py3-none-any.whl
python -m zipfile -e examples/external-logs-v1.zip work/reference-logs
transitionbench serve --port 8765 --data-dir work/log-review-service
```

In a second terminal, using the same Python environment:

```sh
python examples/review_external_logs.py work/reference-logs --output work/log-review-result
```

The script converts 18 runs, checks every native summary against its original
canonical hash, independently recomputes each converted bundle, imports it through
the SDK and preserves three reviews before attaching independent outcomes. Inspect
`work/log-review-result/result.json`, `review-40.json` and `outcome-40.json`.
Output directories must be new; the tool never overwrites earlier evidence.

| Window | Saved advice | Predicted net completions | Observed net, seeds 7931/7932/7933 |
|---|---|---:|---|
| 20 s | WAIT | 0 | 0 / 0 / 0 |
| 40 s | SWITCH | 60 | 84 / 86 / 62 |
| 120 s | SWITCH | 700 | 724 / 727 / 703 |

These are the existing APC measurements in a different file format, **not new
experiments or evidence of a third-party collector/customer integration**. The
archive contains nine calibration runs and nine held-out runs, 21,600 offered
records in total. The three windows are prefixes of one homogeneous workload,
not three traffic scenarios. Keeping A had zero qualified completions; immediate
switching tied these recommendations. No new policy advantage is claimed.

The sample keeps its September 22, 2026 measurement timestamps. The replay uses
an explicit 30-day age allowance; after that it correctly refuses new advice.
Do not refresh timestamps to make old evidence pass. Conversion and independent
bundle verification remain available after expiry. Own-data reviews default to
the existing shorter freshness checks.

## Your collector's three-file contract

Each run directory must contain:

1. `run.json`: format/version, clock/unit, capture boundary, explicit run manifest
   and the original acquisition validity result.
2. `requests.jsonl`: **one record for every offered request**, including timeouts,
   failures and dropped/unfinished requests. IDs must exactly match the roster.
3. `transitions.jsonl`: transition events on the same relative clock. An empty
   file is required for a fixed configuration; do not invent a transition.

There are no raw prompts, response bodies or credentials in this format. Remove
secrets from arbitrary metadata before sharing it. Unknown request fields are
rejected; they are not silently retained or interpreted.

`run.json` has exactly these top-level keys:

```json
{
  "format": "transitionbench-request-log-v1",
  "time_unit": "ms",
  "time_basis": "run-relative-monotonic",
  "capture_end": 160000,
  "manifest": {},
  "validity": {"valid": true, "errors": []}
}
```

The empty `manifest` above is a placeholder and is **rejected**. Fill it from
your experiment plan and acquisition record using [RunManifest](schemas/RunManifest.json);
the extracted sample includes complete concrete examples. Reusing the existing
manifest avoids a second, incompatible pairing or resource contract.

Required manifest declarations include `run_id`, `mode`, `origin`, `experiment`,
`offered_ids`, `created_at_unix_s`, `versions`, `hardware`, `resource_intervals`,
`configurations`, `policy_parameters`, `limitations` and
`clock_domain: "client-monotonic-relative"`. Empty hardware/resource lists remain
explicit unknowns where the existing mode permits them; they are not verified
hardware. Preserve actual measurement time, model/engine revision and resource
scope. Never mark generated fixtures as measured.

Within `experiment`, explicitly provide `mode`, `slo` (both `e2e_s` and
`first_content_s`), `workload`, `observation_s`, `drain_s`, `max_dispatch_lag_s` and
all `budget` fields. The workload explicitly declares `kind`, `seed`, `split`,
`rate_rps`, `injection_s`, `arrival_model` and `long_prefix_mode`. For matching,
`policy_parameters` retains the existing `initial_config`, `trial.phases`,
`offered_sha256`, endpoint and scope declarations, where applicable; see
[deployment review](deployment-review.md). Build the offered roster from the
planned/offered trace, not by taking the IDs left in the response log.

**Manifest fields always use canonical seconds** (and Unix seconds for creation
time). `time_unit` applies only to request timestamps, request dispatch lag,
transition `time` and `capture_end`. Supported units are `s` and `ms`. All times
are relative to the *same run origin*, never to each request, restart or worker.
Epoch or unsynchronized host clocks must be aligned by your exporter first.
Capture must include the declared observation window and all recorded events.

Example request in milliseconds (one record on one line):

```json
{"id":"r-1","arrival":1000,"sent":1002,"first_content":1100,"end":1300,"status":"complete","quality_pass":true,"quality_check":"exact-answer-v1","output_chars":2,"workload_class":"short","prefix_group":null}
```

Every field shown is required. Use explicit null timestamps when an event never
occurred. `quality_pass` must be a boolean; it is never inferred from status,
nonempty output or a model answer. Failures remain present with false quality
and their actual terminal status. `not-assessed` is allowed only with false
quality. The adapter does not run or certify the named quality checker.

| External field | Canonical meaning |
|---|---|
| `id` | `request_id` |
| `arrival`, `sent` | scheduled arrival, dispatch |
| `first_content`, `end` | first content, completion time |
| `status` | `complete`, `client_drop`, `timeout`, `error`, `cancelled`, `unfinished`, `partial_stream`, `budget_refusal` |
| `quality_pass` | existing `quality_valid`, explicitly supplied |
| Optional `first_reasoning`, `final_content`, `chunks`, `dispatch_lag` | corresponding canonical timing fields, in declared units |
| Other optional fields | [RequestEvent](schemas/RequestEvent.json) names and semantics: e.g. token counts, finish reason, config/worker IDs |

Do not use the native `*_s` timing names alongside these aliases. Optional origin
and clock overrides must match the manifest; otherwise native verification fails.
Omitted token counts are unknown, not estimated. Dispatch lag is the producer's
scheduler diagnostic, not necessarily API queueing time; provide it when measured.

Example event in milliseconds:

```json
{"time":10000,"worker_id":"worker-0","state":"STOPPING"}
```

Events accept exactly `time`, `worker_id`, `state`; states are `STOPPING`,
`STARTING`, `COMPLETE`. `COMPLETE` is a diagnostic milestone, **not the first
possible service time**. Request completions throughout the transition count.
This reference format supports the existing single-restart review scope.

## Convert, inspect, review and observe

```sh
transitionbench convert-logs your-run --output work/converted-run
transitionbench verify work/converted-run/bundle
```

The output is a directory containing `bundle/` and `evidence.zip`. The original
three files are not modified. Source byte hashes, input unit and capture bound
are retained in `manifest.policy_parameters.external_import`. No timestamps are
refreshed. Native scoring and the independent bundle verifier are reused.
Incomplete/invalid conversion creates no published output directory.

For each of at least three calibration seeds, import a fixed A run, a fixed B
run and an A-to-B transition run with matched traffic and environment contracts.
Subsequent outcome seeds must be independent. Roles and pairs are inferred by
the existing review API from configuration, events and seed, not filenames:

```python
from transitionbench.sdk import Client

with Client("http://127.0.0.1:8765") as client:
    ids = [client.import_evidence(path)["bundle_id"] for path in calibration_zips]
    review = client.review(ids, horizon_s=40)
    new_ids = [client.import_evidence(path)["bundle_id"] for path in heldout_zips]
    outcome = client.observe(review["id"], new_ids)
```

Review still rejects missing/mismatched pairs, incompatible contexts, stale
evidence and horizons beyond observed support. Successful conversion verifies
structure and accounting; it is neither deployment authorization nor proof that
a recommendation is correct. Keep the complete original logs alongside the
converted artifact so provenance hashes can be checked.

## Specific rejection examples

| Input problem | Behavior |
|---|---|
| Missing quality result or SLO | Field-specific conversion error; no guessing |
| String `"false"` instead of boolean | Rejected, not coerced to a pass/fail result |
| Missing offered row / duplicate ID | Native coverage verification rejects it |
| Unsupported unit, wrong clock basis, timestamps outside capture | Conversion error with file/line where applicable |
| Source acquisition marked invalid | Retained source, no valid converted bundle |
| Unknown or duplicate JSON fields | Rejected rather than selecting one value |
| Existing output directory | Rejected; previous evidence retained |
| Pair mismatch or unsupported horizon | Existing review rejects/refuses advice after import |

This release reduces evidence-format integration work. It does not eliminate
the cost of acquiring comparable measurements, prove Wafer lacks this capability,
or establish customer adoption, production benefit or superiority over simple
switching. No frontend or deployment-control changes are needed.
