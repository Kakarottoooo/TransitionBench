# Plug a deployment review into your evaluation pipeline

For the complete measured case, independent results, limitations, and a CPU-only verification walkthrough, start with the [English delivery report](results.md).

TransitionBench 0.4.0 adds a **read-only deployment review**. Your optimizer proposes a configuration; your serving stack owns rollout. TransitionBench consumes matched measurements, computes a conditional recommendation and checks independent outcomes against the frozen forecast. It does not sit on the inference hot path.

This first integration supports one actual restart, a homogeneous workload, and at least three matched calibration seeds. It is not a general production controller or a Wafer-specific deployment adapter. The useful integration boundary is the evidence export, not access to a provider's chat endpoint.

## Current method: 0.4.1

New proposals use `paired-transition-curve-v1`: reconstruct cumulative qualified
completions of the actual transition minus fixed current configuration, from the
restart start through the requested lifetime. The minimum paired curve includes
downtime, failed requests and recovery; do not subtract the transition deficit a
second time. A positive gain must cover the operator's additional allowance and
minimum practical gain. Horizons beyond measured support are rejected.

Declared `COMPLETE` can include warmup after the endpoint already serves traffic.
It is retained as a diagnostic milestone, not a hard gate on observed service.
The first qualified completion is only an observed serving bound, not exact
availability or attribution to a particular worker. Cumulative repayment is the
first positive checkpoint whose remaining observed curve stays nonnegative;
checkpoints are one second apart with extra requested/sensitivity horizons.
Subtract the allowance for forecast repayment; observed outcomes report raw net
completions. These sample extrema are not statistical confidence bounds.

With the original three calibration seeds, 20 / 40 / 120 second horizons now
produce WAIT / SWITCH / SWITCH and gains 0 / 60 / 700. The calibration repayment
checkpoint is 34 seconds for the latter two; declared completion is 53.21 seconds.
These are development estimates, not fresh independent validation. Replaying the
already-seen v2 outcomes gives 31-second repayment and gains 78–83 at 40 seconds;
the available v1 pair gives zero at 40 seconds, so prediction error remains.

Saved legacy forecasts retain their original numbers and post-COMPLETE repayment
definition. The historical model below describes 0.4.0, not new advice.

## Independent validation and 0.4.2 delivery

The unchanged 0.4.1 evaluator was frozen before three new seeds (7931–7933): all nine GPU trials completed. Observed net gain was 0/0/0 at 20 seconds, 84/86/62 at 40 seconds, and 724/727/703 at 120 seconds, against frozen predictions 0/60/700. First positive cumulative gain occurred at 30/30/33 seconds versus calibrated 34 seconds. Keeping A yielded zero qualified completions; this does not demonstrate recovery from an actual negative balance. Advice tied immediate switching. Version 0.4.2 packages the existing method with a restrained deployment view and downloadable independent results; it changes no scoring logic. See [the final delivery](results.md). No customer cooperation is needed to run the included case or independently recompute evidence.

## Try the actual product

Install the [v0.4.4 wheel](https://github.com/Kakarottoooo/TransitionBench/releases/download/v0.4.4/transitionbench-0.4.4-py3-none-any.whl) with Python 3.11–3.13, then:

```sh
transitionbench serve --port 8765 --data-dir .transitionbench
```

Open `http://127.0.0.1:8765/?view=deployment`. Select **Try the measured GPU case**. Import, verification, pairing and evaluation run automatically. The nine included bundles are the original local GPU calibration observations, not generated metrics. No GPU or provider key is required to review them. Change the expected useful lifetime to challenge the recommendation; detailed evidence and evaluation settings stay collapsed. **Check new outcome evidence** automatically imports, pairs and links subsequent measurements to the frozen forecast.

At the tested 8 requests/s shared-prefix workload, A disables APC and B enables vLLM's existing APC with otherwise identical declared settings. The final 30-second window gives a minimum paired advantage of 8 qualified requests/s. The maximum observed candidate-relative transition deficit is 261 requests.

The previous rate model incorrectly treated declared warm-up completion as a service gate. Current proposals use the cumulative transition curve described above; the historical model is not the interactive result. See [results and boundaries](results.md).

The bundled example is historical. Its declared 30-day age allowance does not refresh timestamps; after it expires the system correctly declines to recommend. Own-data reviews default to a one-day evidence age allowance and five-minute advice TTL.

## Connect existing native measurements

For per-request logs rather than native bundles, start with the
[reference log adapter](external-logs.md), introduced in 0.4.3. It handles explicit units,
quality results and run metadata, then feeds this unchanged review/observe path.

The shortest Python integration uses automatic pairing:

```python
from transitionbench.sdk import Client

with Client("http://127.0.0.1:8765") as client:
    ids = [client.import_evidence(path)["bundle_id"] for path in your_bundle_paths]
    review = client.review(ids, horizon_s=120)
    # Later, when your evaluation pipeline produces fresh matched runs:
    new_ids = [client.import_evidence(path)["bundle_id"] for path in fresh_bundle_paths]
    outcome = client.observe(review["id"], new_ids)
```

Or run `transitionbench review CURRENT.zip CANDIDATE.zip TRANSITION.zip ... --horizon 120` with all paired seeds. The HTTP equivalents are `POST /api/v1/proposals/auto` and `POST /api/v1/proposals/{id}/outcomes/imported`. Automatic reviews are explicitly scoped to imported evidence; they do **not** assert that a live target environment was checked. Ambiguous or repeated roles produce an error rather than selecting favorable evidence. The explicit API below lets an integrator additionally supply a target context fingerprint.

For each seed, supply three native TransitionBench evidence bundles: `current` (fixed A), `candidate` (fixed B from the start), and `transition` (A to B while traffic continues). Use the existing [RunManifest](schemas/RunManifest.json) and [RequestEvent](schemas/RequestEvent.json) contracts. Existing TransitionBench exporters already produce these files.

```python
from transitionbench.sdk import Client

with Client("http://127.0.0.1:8765") as client:
    ids = {
        role: client.import_evidence(path)["bundle_id"]
        for role, path in {
            "current": "seed1-current.zip",
            "candidate": "seed1-candidate.zip",
            "transition": "seed1-transition.zip",
        }.items()
    }
    profile = client.evidence_profile(ids["current"])
    # Inspect profile['context']; confirm it matches the proposed target.
    # Repeat imports for at least two additional paired seeds.
```

Submit `Client.propose(request, idempotency_key="your-evaluation-id")`. Request shape is documented in [ProposalInput](schemas/ProposalInput.json) and [OpenAPI](openapi.json). It contains bundle ID triples, current/candidate configuration IDs, the target context fingerprint, an assumed remaining lifetime, a final steady measurement window and an explicit extra loss allowance. `Client.get_proposal(id)` retrieves the original decision, expiry and outcome history.

The local file adapter [examples/deployment-review.json](../examples/deployment-review.json) replaces filesystem paths with imported IDs. Paths are resolved by the CLI/SDK on your machine, never by the HTTP server on behalf of remote input:

```sh
transitionbench proposal-import examples/deployment-review.json > request.json
transitionbench proposal request.json --key review-001 > review.json
transitionbench proposal-get PROPOSAL_ID
transitionbench proposal-outcome PROPOSAL_ID outcome.json --key outcome-001
```

`outcome.json` contains `{"pairs": [{"current": "BUNDLE_ID", "candidate": "BUNDLE_ID", "transition": "BUNDLE_ID"}]}`. IDs must come from newly imported independent matched runs; reusing calibration run IDs or seeds is rejected as a comparable outcome. Outcomes append without rewriting the forecast. A changed model, workload, quality checker, SLO, configuration or declared resource context produces `NOT_COMPARABLE`, with the gaps retained for inspection.

## Exporting from a different evaluator

Map each **offered** request, including every timeout, drop and failed answer, to a `RequestEvent`. Use one monotonic clock relative to trial start. Record the scheduled arrival, first visible content, completion, finish reason and your task-quality result. Never drop failed rows or substitute dispatch time for scheduled arrival. `transitionbench.evidence.export_bundle` produces the native files; `zip_bundle` packages them.

The manifest must include actual model/engine revisions, both configuration dictionaries, declared hardware/resource budget, SLO and output limits. `policy_parameters` must contain:

- `initial_config`: the starting configuration ID for that run;
- `trial.phases`: the measured homogeneous workload kind, rate, prefix behavior and duration;
- `offered_sha256`: digest of the complete offered trace, including payload identity; paired trials must have the same digest and request schedule;
- optionally `endpoint_contract` and `local_restart_scope` to make model invocation and deployment scope explicit.

The actual transition trace must contain one `STOPPING` and one later `COMPLETE`, with worker IDs and relative `at_s` timestamps. Fixed references must have no transitions. The diagnostic final steady window must be after declared COMPLETE; this does not gate earlier measured service. Preserve the original raw logs alongside exported bundles: metadata and quality labels are producer declarations, not independent hardware attestation or semantic correctness certification.

## What the review verifies

The release includes a sealed independent study archive. Recompute it without installing the application:

```sh
python scripts/verify_prospective_v4.py TransitionBench-prospective-v4-evidence.zip verified-v4
```

Download the archive from the [release](https://github.com/Kakarottoooo/TransitionBench/releases/tag/v0.4.4). The standard-library script verifies archive and member hashes, raw trial arithmetic, frozen forecasts and nine matched outcome windows. This is a consistency check, not metadata authentication or an independent GPU rerun.

Imported checksums and original summaries are independently reverified on every new evaluation. The evaluator then checks pairing, separate seeds, freshness, configuration roles and context. It reconstructs qualified rates from raw events with the full offered denominator. For the retained legacy rate-model diagnostic, the modeled rate difference uses the smaller of fixed-candidate and transitioned tail rates, minus current rate. It uses the smallest paired difference and largest observed deficit, then subtracts the operator's extra loss allowance for admission. These extrema are not confidence bounds or a worst-case guarantee.

An independent outcome reports cumulative transitioned-minus-current qualified completions over the assumed horizon, forecast error, and an observed cumulative repayment bound at one-second resolution (legacy forecasts retain their original post-COMPLETE restriction). The nonnegative tail is limited to the observed remainder; no inference beyond it is made. Separate trials are not an observed production counterfactual.

All write requests require `X-TransitionBench: 1`; proposals/outcomes additionally require `Idempotency-Key`. The loopback service keeps existing execution approval boundaries unchanged. Recommendations confer no rollout authority. The current product does not automatically observe distribution drift or a running production service: integration code must present the current context and submit fresh matched outcome evidence.

## The acceptance that matters next

Have an engineer export one new A/B/transition evaluation through this contract, without modifying TransitionBench. Judge whether the resulting advice changes or validates a real rollout choice and whether the engineer can independently challenge the numbers. That is the next product validation; another favorable benchmark alone would not establish adoption or Wafer production benefit.
