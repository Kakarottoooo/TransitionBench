# Experiment contract and interpretation

## Evidence modes

`SIMULATION` produces synthetic records. `LIVE_ENDPOINT` measures an actual HTTP
endpoint without claiming deployment control. `CONTROLLED_ROLLOUT` additionally
requires operator approval and observed physical device/process assignments.
`RECORDED_REPLAY` preserves original origin and mode; it never becomes a new live
measurement. Imported resource assertions are not automatically observations.

The checked-in final protocol is
`reports/release-study/predeclared-protocol.json`. It specifies seven
conditions, five matched test seeds, four policies, randomized sequential order,
SLOs, observation boundaries and the stopping rule. Calibration seeds 11–13,
tuning seeds 21–22 and test seeds 101–105 have disjoint prefix namespaces. There
is no favorable-result stopping rule. Earlier implementation experiments are
retained and identified in the journal.

## Primary outcome and clocks

A request qualifies iff it completes with nonempty, validity-approved output,
within the inclusive common observation boundary, and satisfies both scheduled
arrival → completion and scheduled arrival → first-visible-content objectives.
Attainment divides by **all offered requests**. Goodput divides qualified
completions by the **declared observation duration**, even when the last request
finishes early. Injection ends before the common bounded drain allowance.
Timeouts, drops, cancellations, invalid answers and unfinished requests remain.

API latency uses dispatch → completion; client queueing is shown separately.
Durations use a process monotonic clock. Wall-clock timestamps describe record
creation and freshness only. Controlled traffic and rollout share an explicit
monotonic epoch. Server timestamps must carry a clock domain; unsynchronized
host clocks cannot be subtracted.

SSE frames are parsed after arbitrary transport fragmentation. Empty role chunks
do not start visible-content timing. Reasoning content has its own field. Chunk
timestamps are **not** exact inter-token timestamps. Missing usage is null.
For nonstreaming requests, first visible content is observed at the full response;
it does not reveal server TTFT. No automatic generation retry is made, including
after a partial stream, 429, authentication error or privacy rejection.

## Arrival and validity rules

Open-loop scheduled arrivals do not wait for a concurrency slot. A full client
causes a recorded drop; the offered denominator and arrival schedule stay fixed.
Trials exceeding the declared scheduling-lag tolerance are invalid, rather than
credited as a lower-load experiment. TLS/client construction occurs before the
injection clock. Evidence I/O uses a background writer so disk latency does not
silently throttle dispatch. Crash recovery preserves journaled rows and marks
the job interrupted; it does not invent missing completions.

`scripted-session` is a separate causal arrival model: each successive request
waits for the prior response and think time. It is a fixed scripted transcript,
not a natural generated-history conversation. Natural conversational divergence
is not evaluated by this release. JSONL import is available through the Python
workload contract; imported order is preserved and unsafe reservations rejected.

Synthetic prompts ask for a request-specific marker and arithmetic answer.
The live validity gate requires that exact marker, a nonempty answer and a `stop`
finish reason. This detects some omissions, mismatches and truncation. It is not
a comprehensive model task-quality assessment. Deterministic-decoding repeat
variability, task-level quality and instrumentation-independent quality costs
remain required for G5. HTTP 200 alone does not pass validity.

## Policies and calibration

The final StaticBest is selected **per predeclared workload family** using only
calibration seeds. All compared policies start from that same calibrated fixed
configuration. This is stronger than choosing one pooled configuration that is
bad for a particular family. Test results never select its configuration.

SteadyStateFirst uses the same observed past workload mix and steady capacities.
FixedHysteresis adds a threshold, persistence and minimum dwell, chosen from four
predeclared parameter sets using tuning seeds and three workload families.
StateAware adds a transition-deficit lookup conditioned on configuration pair,
observed workload class mix and current outstanding-request bucket (idle ≤ 2,
backlogged > 2). Missing buckets return insufficient evidence. Queue depth is
observable client state, **not a claim about internal KV occupancy**. Actual
cache state and cache-hit metrics are unknown for a black-box endpoint.

Rates are capped by demand estimated only from prior arrivals; spare serving
capacity must not be counted as additional offered requests. Controllers never
receive future arrivals, phase boundaries, target answers or realized output
lengths. Horizon is an operator assumption, not hidden time to the next phase.
Changing it is a sensitivity analysis, not an observed counterfactual.

For approximately stable demand, `gain = (g_B - g_A) × horizon - L` and
`break_even = L / (g_B - g_A)` when the advantage is positive. Rates are in
requests/second; L is requests lost relative to the candidate reference path.
The simplified interpretation calls that reference B steady. The synthetic
calibration compares B's fixed path under its documented initial cache condition
against the transition path; it is not a hardware observation of ideal hot B.
This approximation is a limitation, especially during prefix changes. An L
measured relative to A must never be subtracted as though it were relative to B.
Tests explicitly enforce the reference and units.

The lookup uses repeated calibration deficits and their observed range. That
range is **not a confidence interval**. The decision threshold subtracts it as a
conservative sensitivity margin. It is not a calibrated probabilistic guarantee.
Out-of-distribution or unknown-state direct inputs return insufficient evidence.
The simple online classifier only covers its declared queue/class buckets; it
cannot detect every distribution shift.

## Synthetic environment

Two FCFS single-request servers, seeded lognormal service noise, prefix reuse
speedup and two configuration-specific service-time tables generate every event.
The environment does not inspect policy names. A favors short inputs; B favors
long inputs. A transition drains one worker, reconstructs it, clears its simulated
cache and warms it for 0.2 seconds, before proceeding to the other worker. Already
assigned requests finish before mutation. No third worker is created. This is an
explanation model, not a simulator validated against vLLM or Wafer.

All policies get the same initial state, router, queue cap and rollout mechanics.
Synthetic GPU-seconds are declared model resource accounting, not actual GPU
usage. Active GPU utilization stays unknown. Shared calibration/tuning is separate
from each policy trial. The current calibrator executes 108 synthetic trials per
transition-cost setting (12 capacity, 48 cost-reference/transition, 24 fixed
selection, 24 tuning), totaling 7,200 modeled reserved GPU-seconds, and zero
physical GPU-seconds. Two settings are calibrated for the final study.

## Managed experiment scope

The local reference uses two distinct same-model physical GPUs, one worker each,
the same pinned model/tokenizer, precision and sampling settings. The Docker
adapter reconstructs the worker for the pinned startup batch-token parameter.
It does not claim a restart is necessary for every possible future engine API.
The low-transition-cost synthetic control is a different assumed action cost;
it does not prove hot mutation support in vLLM. Compilation caches persist.

The current managed policy trial permits **at most one exact approved candidate
rollout**. If the controller selects another target, it refuses. Every policy has
the same restriction. Repeated oscillating GPU-policy campaigns and automated
calibration of a new GPU host are not validated here. Operator-imported measured
calibration is required; synthetic calibration cannot authorize a controlled
effectiveness claim. The runbook makes these remaining empirical prerequisites
explicit. A successful hook acknowledgment cannot replace generation/readiness.

## Statistics and confounders

Policy differences are paired by run seed/contract. Bootstrap resampling uses
whole paired runs (2,000 resamples), not correlated individual requests. Fewer
than three pairs yields no interval. Five pairs are exploratory, not a production
p99 certification. Report each class and negative cases. No causal cache claim
is made without targeted ablations. Calibration, prewarming, idle reservation,
overlap, validation and rollback belong in the total resource accounting.
Public endpoint latency cannot establish physical resource fairness or savings.
