# Results and claim boundaries

This page separates software delivery, scoped experiments, and unproven claims. It is a curated summary, not a merger of experiments from different hardware or protocols.

## Independently checked accounting correction

The packaged September 22, 2026 study has nine complete runs: three new seeds, each with keep, actual restart, and already-on-candidate reference. It contains 10,800 offered requests on one RTX 3080 Ti. APC is a vLLM feature; both configurations use a 2,048-token scheduling budget. The change enables prefix caching.

| Horizon | Forecast net | Observed net: 7931 / 7932 / 7933 |
|---|---:|---:|
| 20 s | 0 | 0 / 0 / 0 |
| 40 s | 60 | 84 / 86 / 62 |
| 120 s | 700 | 724 / 727 / 703 |

The calibrated crossing was 34 seconds; measured first positive cumulative checkpoints were 30, 30 and 33 seconds. Keeping A produced zero qualified completions, so these are not repayment of an observed preceding negative balance. The 20-second WAIT tied immediate switching. The three horizons are nested prefixes of the same homogeneous future traffic, not three independent scenarios.

Declared warm-up completion occurred roughly 49–52 seconds after the trigger. Qualified service before that marker establishes why the old marker-based gate was wrong. Failed startup campaigns and development observations were not pooled into this complete held-out campaign. See the [release's raw evidence](https://github.com/Kakarottoooo/TransitionBench/releases/download/v0.4.4/TransitionBench-prospective-v4-evidence.zip) and [independent verifier](../scripts/verify_prospective_v4.py).

## The simpler policy won in an earlier, separate study

A 36-trial local study covered stable, transient and sustained traffic with three payload seeds and four policies. Under sustained traffic:

| Seed | Keep | Immediate switch | Cost-aware rule | Already on candidate |
|---|---:|---:|---:|---:|
| 7801 | 290 | 1441 | 1188 | 1708 |
| 7802 | 292 | 1452 | 1210 | 1708 |
| 7803 | 296 | 1457 | 1218 | 1715 |

Each run offered 1,760 requests. Waiting for confirmation cost the complicated rule 239–253 qualified completions relative to immediate switching. Already-on-candidate is a different starting condition, not a free migration. Transient-traffic advantages were inconsistent. These data do not support a generally superior state-aware policy.

## Latest state-aware research: no positive result

Execution-path diagnostics found a large improvement from CUDA Graph to eager on one fixed trace, but that is not a deployment-policy comparison. An eight-run eager capacity screen then retained these complete results:

| Trace | RPS | A512 qualified | B4096 qualified | Offered each |
|---|---:|---:|---:|---:|
| 8501 | 8 | 96 | 116 | 480 |
| 8501 | 12 | 16 | 3 | 720 |
| 8502 | 8 | 332 | 285 | 480 |
| 8502 | 12 | 4 | 0 | 720 |

Both configurations used eager, APC and the same 768 KV blocks. No candidate passed the frozen cross-trace screening rule; subsequent state and policy stages were not executed. This is not a proof that all state mechanisms are useless. Prior cloud runs with CUDA crashes were excluded from healthy-performance claims, not treated as policy wins.

A CPU-only mechanism review subsequently found unequal legacy warm/cold history lengths (797 vs 896 tokens per request). It checked an equal-token control and an approximately 3,024-token initial differential prefix inventory. Neither inventory nor an unloaded reconstruction-time proxy establishes qualified-service gain under queueing. State-dependent KEEP/SWITCH crossover and incremental benefit over tuned simple baselines remain unproven. No further GPU campaign was launched by that review.

## What can be claimed

- Installable review/observe software with a local web UI and explicit log input contract.
- A measured accounting correction and raw-data recomputation path.
- Preserved counterexamples and failed hypotheses, including a simpler policy performing better.

Not claimed: a new caching algorithm; generally superior StateAware decisions; a fully validated broad multi-GPU deployment policy; Wafer integration, adoption, production savings or hiring outcome. The [public demo](https://transitionbench-demo.ziweiguo.chatgpt.site) displays the recorded case only. The full evaluation application runs locally; no production evaluation service is hosted.
