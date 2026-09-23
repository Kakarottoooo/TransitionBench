"""Sequence-preserving workloads; future entries never reach a policy observer."""
import json
import random
from dataclasses import dataclass
from pathlib import Path
from .schemas import WorkloadSpec


@dataclass(frozen=True)
class OfferedRequest:
    request_id: str
    scheduled_s: float
    workload_class: str
    prefix_group: str
    prompt: str
    expected: str
    input_token_upper_bound: int


def generate(spec: WorkloadSpec) -> list[OfferedRequest]:
    offsets = {"calibration": 100000, "tuning": 200000, "test": 300000}
    rng = random.Random(spec.seed + offsets[spec.split])
    rows, at, index = [], 0.0, 0
    while at < spec.injection_s - 1e-10:
        kind = "short" if spec.kind == "short" else "long"
        if spec.kind == "mixed-burst":
            kind = "short" if rng.random() < .6 else "long"
        epoch = int(at >= spec.injection_s / 2) if spec.kind == "prefix-shift" else 0
        group = rng.randrange(4) if kind == 'long' else index
        if kind == 'long' and spec.long_prefix_mode != 'legacy':
            # Consume the same RNG draw in both modes so class/arrival sequences
            # stay matched. Fixed-width groups keep prompt byte lengths equal;
            # tokenizer counts and actual cache hits still require measurement.
            group = f"{index if spec.long_prefix_mode == 'unique' else group:05d}"
        prefix = f"{spec.split}-{spec.seed}-{epoch}-{group}"
        request_id = f"{spec.split}-{spec.seed}-{index}"
        expected = f"TB:{request_id}:4"
        context = (f"Synthetic reference {prefix}. A deployment has two workers. " * 32) if kind == "long" else ""
        prompt = context + f"Return exactly {expected} and nothing else. The arithmetic answer to 2+2 is 4."
        rows.append(OfferedRequest(request_id, round(at, 9), kind, prefix, prompt, expected,
                                   len(prompt.encode("utf-8")) + 64))
        rate = spec.rate_rps * (2 if spec.kind == "mixed-burst" and .4 * spec.injection_s <= at < .6 * spec.injection_s else 1)
        at += 1 / rate
        index += 1
        if index > 20000:
            raise ValueError("Workload exceeds 20000 offered requests")
    return rows


def import_workload(path) -> list[OfferedRequest]:
    source = Path(path)
    if source.stat().st_size > 8 * 1024 * 1024:
        raise ValueError("Workload exceeds 8 MiB")
    rows = [OfferedRequest(**json.loads(line)) for line in source.read_text(encoding="utf-8").splitlines() if line]
    if len(rows) > 20000 or len({r.request_id for r in rows}) != len(rows):
        raise ValueError("Too many or duplicate requests")
    if any(r.scheduled_s < 0 or r.input_token_upper_bound < len(r.prompt.encode()) + 64 for r in rows):
        raise ValueError("Invalid workload timing or unsafe token reservation")
    if [r.scheduled_s for r in rows] != sorted(r.scheduled_s for r in rows):
        raise ValueError("Import must preserve chronological sequence")
    return rows
