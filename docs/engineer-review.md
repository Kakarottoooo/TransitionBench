# A five-minute engineering review

For a no-install first look, open the [public recorded-case demo](https://transitionbench-demo.ziweiguo.chatgpt.site). It shows the frozen predictions, independent outcomes and evidence downloads. Follow the local workflow below to import evidence and compute a review yourself.

## 1. Start with the decision

“I built TransitionBench to evaluate whether a proposed inference deployment adds useful service within the time available. It includes the restart and recovery period, not only the candidate's steady-state throughput.”

Install the [release](https://github.com/Kakarottoooo/TransitionBench/releases/tag/v0.4.4), start `transitionbench serve`, and open `http://127.0.0.1:8765/?view=deployment`. This walkthrough uses recorded evidence only.

## 2. Import the measured case

Select **Try the measured GPU case**. The tool imports nine calibration bundles, pairs current/candidate/transition runs and validates the contract. Show the lifetime assumption: 120 seconds. The frozen calibration predicts 700 additional qualified completions. Change it to 40 seconds: 60 additional completions. At 20 seconds the calibration predicts zero and recommends waiting.

These are conditional estimates from calibration, not new GPU results. Historical measurements are subject to a 30-day freshness limit; once expired, use the recorded independent table and standard-library verifier, or bring fresh evidence. Do not change evidence timestamps.

## 3. Show the concrete correction

“The original evaluator waited for a warm-up-complete marker. The independent trials showed 62–86 extra qualified completions within 40 seconds, before that marker at roughly 49–52 seconds. I changed the accounting to count the whole transition, then checked the correction on held-out seeds.”

The keep baseline was zero qualified completions. This supports the accounting correction, not a policy advantage over immediate switching. Twenty-second WAIT did not avoid an observed loss.

## 4. Make the evidence inspectable

Show **independent measured validation** separately from the interactive forecast. Run the [standalone verifier](../scripts/verify_prospective_v4.py) against the release's sealed ZIP. It checks raw events, forecast freeze ordering and all nine seed/window comparisons without a GPU or application dependencies.

Checksums establish consistency, not independent authentication of the measurement environment. Ordinary quality failures, lateness and drops count as zero useful service; missing experimental arms are missing, not zero-throughput observations.

## 5. Explain where it fits

“Your optimizer proposes candidates. Your system owns rollout. TransitionBench consumes evidence, saves a conditional deployment review, and checks later matched outcomes. There is an explicit request-log adapter, so the input boundary can be reviewed without granting deployment access.”

Point to [the evidence contract](deployment-review.md) and [the external-log walkthrough](external-logs.md). The reference replay proves format conversion and native-result parity, not an existing customer integration.

The broader research did not establish StateAware superiority. In a separate sustained-traffic study, immediate switching beat the more complicated rule. The [results page](results.md) keeps these boundaries visible. There is no claim that Wafer lacks an equivalent internal tool or that this project improved its production system.
