# Deployment adapter and recoverable execution

`DeploymentAdapter` has two operations: `snapshot()` returns typed worker
observations; `operation(name, worker_id, payload, idempotency_key)` performs one
configured lifecycle step. The allowed operation names are prepare, drain, apply,
readiness, warmup, observe and rollback. Payloads are typed; shell strings and
caller-selected executables are never accepted.

`HTTPHookAdapter` implements this over `GET /v1/snapshot` and
`POST /v1/operations`. `create_hook_app(adapter, token, database)` supplies a
working, authenticated operator-side server. Its separate credential is read
from an environment variable. The analysis API holds only that narrow hook
credential, never a Docker socket. The local example is:

```text
transitionbench lab-hook examples/lab.json --port 8770
```

This command refuses missing experimental authority, unreviewed model license,
an unpinned image, missing Docker, or an unsuitable two-device Linux host.
`tests/test_protocols.py` runs a real HTTP hook conformance test with a CPU
fixture. That test does not claim Docker/GPU compatibility.

## State and ownership

The common executor journals planned → draining one worker → reconfiguring →
readiness → warming → observing → next worker/complete. The other worker remains
available. Draining disables routing, waits for all existing requests, and fails
on a deadline instead of killing in-flight work. Configuration A/B IDs map to
fixed startup arguments; observed generation labels increment on reconstruction.
Immutable plan hashes cover target, snapshots, configuration, budgets and expiry.
Execution rechecks generation, device assignment, freshness and readiness.

SQLite serializes plan acquisition and target leases. A lease is not reclaimed
merely because time expired: an external mutation may still be running. Duplicate
successful operations return their stored result; changed payloads under a key
conflict. Failed/in-progress/interrupted operations require reconciliation.
The hook does not retry uncertain writes.

The first updated worker is observed ready before the second is drained. A 200
acknowledgment alone is insufficient. Failures enter abort/rollback states and
attempt restoration through the same drain/apply/readiness/warmup path. Rollback
time counts. Half the declared runtime/resource allowance is reserved for
recovery. Exhaustion leaves `ROLLBACK_PENDING` with the lease retained.

On API restart, active jobs become `INTERRUPTED`, active plans become
`ROLLBACK_PENDING`, and no write is automatically replayed. Inspect the actual
orchestrator/container state and persisted operation IDs before recovery. There
is deliberately no unauthenticated “force unlock” endpoint.

## Local Docker implementation

The separate `DockerLabAdapter` invokes fixed argument arrays with `shell=False`.
It only stops/removes containers whose exact experiment ownership label matches.
It verifies assigned GPU UUIDs with Docker inspection and joins actual GPU
process IDs from `nvidia-smi` with container host PIDs. Two logical workers on one
device fail the invariant. Image/model/tokenizer revisions are pinned and the
normal compilation cache directory survives replacement.

The lab varies `max_num_batched_tokens` (2048/4096) with `max_num_seqs=32`, fixed
model length, precision and prefix-caching policy. These are pinned engine
startup settings in this adapter. A future supported hot-update adapter must
declare different semantics and run a separate low-cost-control experiment.

The router uses the same least-in-flight rule for every policy and disables a
draining worker. Direct worker endpoints must not receive concurrent unmanaged
traffic. Hook and worker ports bind to loopback. One short warmup operation per
worker was used in 0.1. Version 0.2 uses bounded short/long probes, three valid
windows and two consecutive stable median comparisons; receipts survive failure.
The exact plan binds the warmup policy and reserves forward/recovery requests.
See the typed warmup and resource-budget definitions in [the schema reference](schemas/). **Empirically adequate
GPU workload warmup remains unestablished**. G5 cannot pass until warmup stability, observed configuration
semantics, rollback, quality and full resource accounting are measured on GPUs.

## Adopting an existing orchestrator

Implement the protocol without forking the policy core. Keep orchestration
authorization, ownership and leases in your controller as well as in the caller.
Return actual observed generation/readiness; provide physical resource evidence
only if your instrumentation can substantiate it. Unsupported state stays unknown.
Run the conformance tests for duplicate writes, changed generations, timeouts,
failed readiness, crash recovery and rollback before controlled experiments.
