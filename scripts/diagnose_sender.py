"""CPU-only, real HTTP sender soak. Never a GPU or policy performance result."""
import argparse
import asyncio
import collections
import gc
import hashlib
import importlib.metadata
import json
import math
import platform
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
import transitionbench

from transitionbench.endpoint import EndpointClient, NetworkPolicy
from transitionbench.schemas import EndpointSpec, ExperimentSpec
from transitionbench.service import Service
from transitionbench.workloads import generate


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


async def soak(output, base, minutes):
    service = Service(output / "service")
    count = math.ceil(minutes)
    spec = ExperimentSpec.model_validate({
        "mode": "LIVE_ENDPOINT", "endpoint_id": "local",
        "workload": {"kind": "mixed-burst", "seed": 1001, "split": "test",
                     "rate_rps": 128, "injection_s": 60},
        "observation_s": 65, "drain_s": 5,
        "budget": {"max_requests": 11000, "max_total_tokens": 20000000,
                   "max_output_tokens": 32, "max_concurrency": 64,
                   "max_duration_s": 120, "reserved_gpus": 0,
                   "max_reserved_gpu_seconds": 0},
    })
    client = EndpointClient(EndpointSpec(
        id="local", base_url=base + "/v1", model="local-arithmetic",
        streaming=True, supported_parameters=["max_tokens", "stream"],
    ), NetworkPolicy([base], ["127.0.0.1"]))
    transport_failures, epoch = [], None
    original_request = client.request
    def traced_request(*args, **kwargs):
        request = original_request(*args, **kwargs)
        async def trace(event, info):
            if event.endswith(".failed"):
                transport_failures.append({"stage": event,
                    "exception_type": type(info.get("exception")).__name__,
                    "elapsed_s": time.monotonic()-epoch if epoch is not None else None})
        request.extensions["trace"] = trace
        return request
    client.request = traced_request
    protocol = {
        "scope": "CPU real HTTP fixture with canonical production sender and journal",
        "python": sys.version, "platform": platform.platform(),
        "monotonic_clock": vars(time.get_clock_info("monotonic")),
        "dependencies": {name: importlib.metadata.version(name) for name in
                         ("httpx", "httpcore", "anyio", "sniffio", "pydantic", "uvicorn")},
        "code_revision": service.code_revision,
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "trial_count": count, "injection_duration_s": count * 60,
        "seed_order": list(range(1001, 1001 + count)),
        "experiment": spec.model_dump(mode="json"),
        "max_scheduling_lag_s": .1, "gc_enabled": gc.isenabled(),
        "failure_rule": "Stop on first invalid trial; no replacement or retry",
        "process_rule": "One client and fixture process for the entire soak; normal GC",
        "epoch_rule": "Discover before epoch; include session construction as in controlled traffic",
        "trace_rule": "Record HTTPcore failure stage/type only; no retries or success-stage timing",
        "created_unix_s": time.time(),
        "limits": ["No GPUs, model quality, transition, or policy advantage measured",
                   "Passing this host does not certify a cloud host",
                   "OS scheduling and co-resident load remain part of the observation"],
    }
    write_json(output / "protocol.json", protocol)
    (output / "producer.py").write_bytes(Path(__file__).read_bytes())
    source_dir = output / "source"
    source_dir.mkdir()
    for source in Path(transitionbench.__file__).parent.glob("*.py"):
        (source_dir / source.name).write_bytes(source.read_bytes())
    started = time.perf_counter()
    summary = {"status": "RUNNING", "completed_trials": 0, "offered": 0,
               "max_lag_s": 0, "injection_duration_s": 0,
               "protocol_sha256": hashlib.sha256((output / "protocol.json").read_bytes()).hexdigest()}
    try:
        for index in range(count):
            transport_failures, epoch = [], None
            spec.workload.seed = 1001 + index
            offered = generate(spec.workload)
            run_id, _ = service.store.create(spec, f"sender-soak-{index + 1}")
            await client.discover()
            pauses, begins = [], {}
            def gc_event(phase, info):
                generation = info["generation"]
                if phase == "start":
                    begins[generation] = (time.perf_counter(), time.monotonic() - epoch)
                elif generation in begins:
                    before, relative = begins.pop(generation)
                    pauses.append({"generation": generation,
                                   "start_s": relative,
                                   "duration_s": time.perf_counter() - before})
            trial_start = time.perf_counter()
            epoch = time.monotonic()
            gc.callbacks.append(gc_event)
            try:
                rows, validity = await service.measured_traffic(
                    run_id, spec, client, asyncio.Event(), offered=offered,
                    epoch=epoch, skip_discovery=True)
            finally:
                gc.callbacks.remove(gc_event)
            elapsed = time.perf_counter() - trial_start
            lags = sorted(r.scheduling_lag_s or 0 for r in rows)
            identities_match = [r.request_id for r in rows] == [r.request_id for r in offered]
            schedule_matches = [r.scheduled_s for r in rows] == [r.scheduled_s for r in offered]
            unexpected = [r.request_id for r in rows if r.termination != "client_drop"
                          and (r.termination != "complete" or not r.quality_valid)]
            journal = service.store.root / "runs" / run_id / "request-journal.jsonl"
            journal_ids = [json.loads(line)["request_id"] for line in journal.read_text(encoding="utf-8").splitlines()]
            journal_complete = len(journal_ids) == len(rows) and set(journal_ids) == {r.request_id for r in rows}
            valid = validity["valid"] and identities_match and schedule_matches and journal_complete and not unexpected
            result = {"trial": index + 1, "seed": spec.workload.seed, "run_id": run_id,
                      "elapsed_s": elapsed, "offered": len(rows), "valid": bool(valid),
                      "validity": validity, "identities_match": identities_match,
                      "schedule_matches": schedule_matches, "journal_complete": journal_complete,
                      "unexpected_requests": unexpected, "max_lag_s": lags[-1],
                      "p99_lag_s": lags[int(.99 * (len(lags) - 1))],
                      "lag_violations": sum(lag > .1 for lag in lags),
                      "terminations": dict(collections.Counter(r.termination for r in rows)),
                      "gc_max_s": max((p["duration_s"] for p in pauses), default=0),
                      "gc_pauses": pauses,
                      "transport_failures": transport_failures,
                      "journal_sha256": hashlib.sha256(journal.read_bytes()).hexdigest()}
            with (output / "trials.jsonl").open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(result) + "\n")
            summary["completed_trials"] += 1
            summary["offered"] += len(rows)
            summary["injection_duration_s"] += 60
            summary["max_lag_s"] = max(summary["max_lag_s"], lags[-1])
            summary["elapsed_s"] = time.perf_counter() - started
            if not valid:
                summary["status"] = "FAILED"
            write_json(output / "summary.json", summary)
            print(json.dumps({k: v for k, v in result.items() if k != "gc_pauses"}), flush=True)
            if not valid:
                break
            del rows, offered, journal_ids
        else:
            summary["status"] = "PASSED"
    except BaseException as exc:
        summary["status"] = "INTERRUPTED" if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)) else "ERROR"
        summary["error_type"] = type(exc).__name__
        raise
    finally:
        summary["elapsed_s"] = time.perf_counter() - started
        write_json(output / "summary.json", summary)
        await service.close()
    return summary["status"] == "PASSED"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minutes", type=int, default=90, choices=range(1, 181), metavar="1..180")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    with (args.output / "fixture.log").open("w") as log:
        server = subprocess.Popen([
            sys.executable, "-m", "uvicorn", "transitionbench.test_server:app",
            "--host", "127.0.0.1", "--port", str(port), "--log-level", "error", "--no-access-log",
        ], stdout=log, stderr=log, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        try:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                try:
                    urllib.request.urlopen(base + "/v1/models", timeout=.5).close()
                    break
                except Exception:
                    if server.poll() is not None:
                        raise RuntimeError("Owned fixture exited; inspect fixture.log")
                    time.sleep(.1)
            else:
                raise TimeoutError("Owned fixture startup deadline")
            return 0 if asyncio.run(soak(args.output, base, args.minutes)) else 1
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
