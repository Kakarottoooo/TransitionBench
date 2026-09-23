"""Metrics over all offered requests in a predeclared common horizon."""
import math
from collections import Counter
from .schemas import RequestEvent, SLOSpec


def qualifies(row: RequestEvent, slo: SLOSpec, observation_s: float) -> bool:
    return (row.termination == "complete" and row.quality_valid and row.output_chars > 0
            and row.completed_s is not None and row.completed_s <= observation_s
            and row.first_content_s is not None
            and row.completed_s - row.scheduled_s <= slo.e2e_s
            and row.first_content_s - row.scheduled_s <= slo.first_content_s)


def summarize(rows: list[RequestEvent], slo: SLOSpec, observation_s: float) -> dict:
    if observation_s <= 0 or not math.isfinite(observation_s):
        raise ValueError("Positive finite observation duration required")
    if len({r.request_id for r in rows}) != len(rows):
        raise ValueError("Duplicate request IDs")
    qualified = [r for r in rows if qualifies(r, slo, observation_s)]
    latencies = [{"request_id": r.request_id,
                  "scheduled_e2e_s": r.completed_s - r.scheduled_s,
                  "api_e2e_s": r.completed_s - r.dispatch_s,
                  "client_queue_s": r.dispatch_s - r.scheduled_s,
                  "first_content_s": None if r.first_content_s is None else r.first_content_s - r.scheduled_s}
                 for r in rows if r.completed_s is not None and r.dispatch_s is not None]
    classes = {}
    for name in sorted({r.workload_class for r in rows}):
        group = [r for r in rows if r.workload_class == name]
        count = sum(qualifies(r, slo, observation_s) for r in group)
        classes[name] = {"offered": len(group), "qualified": count, "attainment": count / len(group)}
    return {"offered": len(rows), "qualified": len(qualified),
            "attainment": len(qualified) / len(rows) if rows else 0,
            "goodput_rps": len(qualified) / observation_s,
            "observation_s": observation_s, "by_class": classes,
            "terminations": dict(Counter(r.termination for r in rows)),
            "latencies": latencies,
            "cumulative": [{"at_s": r.completed_s, "qualified": i + 1}
                           for i, r in enumerate(sorted(qualified, key=lambda r: r.completed_s))],
            "definitions": {"qualified": "Complete, nonempty, quality-valid, within inclusive observation and both scheduled-arrival SLOs",
                            "attainment": "qualified / ALL offered requests",
                            "goodput_rps": "qualified / declared observation seconds",
                            "latency_unit": "seconds", "chunk_timing": "client chunks, not exact tokens",
                            "aggregation": "per run; paired inference uses runs, not individual requests"}}
