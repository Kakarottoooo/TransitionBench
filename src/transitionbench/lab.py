"""Explicitly authorized local Docker lab, without shell-string execution.

The API never imports Docker authority. Only the separate hook service constructs
this adapter. GPU execution is unavailable until doctor verifies the real host.
"""
import asyncio
import json
import os
import platform
import subprocess
import time
from pathlib import Path
import httpx
from .schemas import WorkerSnapshot, WarmupSpec
from .warmup import warm_worker
from .evidence import write_json
from .rollout import stable_hash

MODEL = "Qwen/Qwen2.5-3B-Instruct"
MODEL_REVISION = "aa8e72537993ba99e69dfaafa59ed015b17504d1"
IMAGE = "vllm/vllm-openai:v0.11.0"
ENGINE_CONFIGS = {"A": {"max_num_batched_tokens": 2048, "max_num_seqs": 32},
                  "B": {"max_num_batched_tokens": 4096, "max_num_seqs": 32}}


def command(args, timeout=30):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        # Commands contain no credentials. Do not expose arbitrary engine logs.
        raise RuntimeError(f"{args[0]} operation failed with exit code {result.returncode}")
    return result.stdout.strip()


def gpu_inventory():
    raw = command(["nvidia-smi", "--query-gpu=name,uuid,driver_version,memory.total", "--format=csv,noheader,nounits"])
    return [dict(zip(("device_model", "device_uuid", "driver", "memory_mib"), [p.strip() for p in row.split(",")])) for row in raw.splitlines()]


def doctor():
    status = {"python": platform.python_version(), "platform": platform.platform(), "gpus": [], "docker": None,
              "controlled_ready": False, "provider_live": "not-authorized-or-tested"}
    try:
        status["gpus"] = gpu_inventory()
    except (OSError, RuntimeError, subprocess.TimeoutExpired):
        status["gpu_error"] = "nvidia-smi unavailable"
    try:
        status["docker"] = command(["docker", "info", "--format", "{{.ServerVersion}}"], 10)
    except (OSError, RuntimeError, subprocess.TimeoutExpired):
        status["docker_error"] = "Docker daemon unavailable"
    status["controlled_ready"] = (len(status["gpus"]) == 2 and len({g["device_model"] for g in status["gpus"]}) == 1 and status["docker"] is not None and platform.system() == "Linux")
    return status


class DockerLabAdapter:
    def __init__(self, config, router):
        if config.get("experimental_authority") is not True or config.get("license_reviewed") is not True:
            raise ValueError("Explicit experimental authority and model license review required")
        if len(config["workers"]) != 2 or len({w["device_uuid"] for w in config["workers"]}) != 2:
            raise ValueError("Two distinct physical device UUIDs required")
        if len({w['port'] for w in config['workers']}) != 2:
            raise ValueError('Two distinct worker ports required')
        self.config, self.router = config, router
        self.owner = config["owner_id"]
        if not self.owner.isalnum() or len(self.owner) > 32:
            raise ValueError("Invalid owner ID")
        self.workers = {str(i): w for i, w in enumerate(config["workers"])}
        for worker in self.workers.values():
            if not str(worker["device_uuid"]).startswith("GPU-"):
                raise ValueError("Physical GPU UUID required")
            if worker["port"] not in range(1024, 65536):
                raise ValueError("Invalid lab port")
        self.image = config["image_digest"]
        if not self.image.startswith("vllm/vllm-openai@sha256:") or len(self.image.split("sha256:")[-1]) != 64:
            raise ValueError("Resolve the pinned image tag to an immutable digest before execution")
        self.cache = Path(config["cache_directory"]).resolve()
        self.cache.mkdir(parents=True, exist_ok=True)

    def name(self, worker):
        return f"transitionbench-{self.owner}-{worker}"

    def inspect(self, worker):
        data = json.loads(command(["docker", "inspect", self.name(worker)]))[0]
        if data["Config"]["Labels"].get("transitionbench.owner") != self.owner:
            raise ValueError("Container is not owned by this experiment")
        assigned = data["HostConfig"]["DeviceRequests"]
        if not assigned or assigned[0].get("DeviceIDs") != [self.workers[worker]["device_uuid"]]:
            raise ValueError("GPU assignment mismatch")
        if data["Config"]["Image"] != self.image:
            raise ValueError("Engine image changed")
        return data

    async def snapshot(self):
        inventory = {d["device_uuid"]: d for d in await asyncio.to_thread(gpu_inventory)}
        apps = await asyncio.to_thread(command, ["nvidia-smi", "--query-compute-apps=pid,gpu_uuid", "--format=csv,noheader,nounits"])
        gpu_pids = {int(row.split(",")[0]): row.split(",")[1].strip() for row in apps.splitlines() if "," in row}
        results = []
        for worker, settings in self.workers.items():
            data = await asyncio.to_thread(self.inspect, worker)
            labels = data["Config"]["Labels"]
            running = data['State']['Running']
            top = (await asyncio.to_thread(command, ["docker", "top", self.name(worker), "-eo", "pid"])) if running else ''
            owned_pids = {int(p.strip()) for p in top.splitlines()[1:] if p.strip().isdigit()}
            matched = [p for p in owned_pids if gpu_pids.get(p) == settings["device_uuid"]]
            ready = False
            async with httpx.AsyncClient(timeout=2, trust_env=False) as client:
                try:
                    ready = running and (await client.get(f"http://127.0.0.1:{settings['port']}/health")).status_code == 200
                except httpx.HTTPError:
                    pass
            hardware = inventory.get(settings["device_uuid"], {})
            results.append(WorkerSnapshot(worker_id=worker, config_id=labels["transitionbench.config"],
                         generation=int(labels["transitionbench.generation"]), ready=ready,
                         accepting=self.router.accepting[worker], in_flight=self.router.in_flight[worker],
                         device_uuid=settings["device_uuid"], device_model=hardware.get("device_model"),
                         process_id=matched[0] if matched else None, observed_at_unix_s=time.time(),
                         resource_evidence="independently-observed" if matched else "unknown"))
        return results

    async def start_worker(self, worker, config_id, generation):
        settings = self.workers[worker]
        params = ENGINE_CONFIGS[config_id]
        args = ["docker", "run", "-d", "--name", self.name(worker), "--gpus", "device=" + settings["device_uuid"],
                "--label", "transitionbench.owner=" + self.owner, "--label", "transitionbench.config=" + config_id,
                "--label", "transitionbench.generation=" + str(generation), "--shm-size", "2g",
                "-p", f"127.0.0.1:{settings['port']}:8000", "-v", f"{self.cache}:/root/.cache",
                "-e", "VLLM_NO_USAGE_STATS=1", "-e", "DO_NOT_TRACK=1",
                "-e", "CUDA_CACHE_PATH=/root/.cache/nvidia/ComputeCache", self.image,
                "--model", MODEL, "--revision", MODEL_REVISION, "--tokenizer-revision", MODEL_REVISION,
                "--dtype", "half", "--max-model-len", "4096", "--gpu-memory-utilization", "0.80",
                "--max-num-batched-tokens", str(params["max_num_batched_tokens"]),
                "--max-num-seqs", str(params["max_num_seqs"]), "--enable-prefix-caching",
                "--generation-config", "vllm", "--disable-log-requests"]
        await asyncio.to_thread(command, args, 60)

    async def operation(self, operation, worker_id, payload, key):
        if worker_id not in self.workers or payload["config_id"] not in ENGINE_CONFIGS:
            raise ValueError("Unknown worker/configuration")
        snapshots = await self.snapshot()
        worker = next(s for s in snapshots if s.worker_id == worker_id)
        if worker.generation != payload["expected_generation"]:
            raise ValueError("Generation conflict")
        if operation == "prepare":
            if len({s.device_model for s in snapshots}) != 1 or any(s.resource_evidence != "independently-observed" for s in snapshots):
                raise ValueError("Unverified physical GPU/process invariant")
        elif operation == "drain":
            self.router.accepting[worker_id] = False
            deadline = time.monotonic() + payload["drain_timeout_s"]
            while self.router.in_flight[worker_id]:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Drain deadline; no forced termination")
                await asyncio.sleep(.02)
        elif operation in ("apply", "rollback"):
            if self.router.accepting[worker_id] or self.router.in_flight[worker_id]:
                raise ValueError("Worker must be drained before reconstruction")
            await asyncio.to_thread(self.inspect, worker_id)
            await asyncio.to_thread(command, ["docker", "stop", "--time", "10", self.name(worker_id)])
            await asyncio.to_thread(command, ["docker", "rm", self.name(worker_id)])
            await self.start_worker(worker_id, payload["config_id"], worker.generation + 1)
        elif operation == "readiness":
            deadline = time.monotonic() + max(0, payload.get('operation_timeout_s', 60) - 5)
            while not next(s for s in await self.snapshot() if s.worker_id == worker_id).ready:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Readiness deadline")
                await asyncio.sleep(.5)
        elif operation == "warmup":
            policy = WarmupSpec.model_validate(payload['warmup'])
            async with httpx.AsyncClient(timeout=policy.max_duration_s, trust_env=False) as client:
                evidence = self.cache / ('transitionbench-' + self.owner) / 'warmup' / (stable_hash(key) + '.json')
                evidence.parent.mkdir(parents=True, exist_ok=True)
                await warm_worker(client, f"http://127.0.0.1:{self.workers[worker_id]['port']}/v1/chat/completions",
                    MODEL, payload['max_tokens'], policy,
                    record=lambda report: write_json(evidence, {**report, 'operation_key': key,
                        'worker_id': worker_id, 'generation': worker.generation, 'config_id': worker.config_id}))
        elif operation == "observe":
            if not worker.ready:
                raise ValueError("Cannot route to unready worker")
            self.router.accepting[worker_id] = True
        else:
            raise ValueError("Unsupported operation")
        return next(s.model_dump(mode="json") for s in await self.snapshot() if s.worker_id == worker_id)


class LabRouter:
    """Identical least-in-flight routing for all policies; no general proxy."""
    def __init__(self, worker_ports, model=MODEL):
        self.model = model
        self.ports = {str(i): port for i, port in enumerate(worker_ports)}
        self.accepting = {key: True for key in self.ports}
        self.in_flight = {key: 0 for key in self.ports}

    def install(self, app, token):
        import hmac
        from fastapi import Request, HTTPException
        from fastapi.responses import StreamingResponse

        clients = {}

        async def close_clients():
            await asyncio.gather(*(client.aclose() for client in clients.values()))
            clients.clear()

        async def start_clients():
            try:
                for worker in self.ports:
                    clients[worker] = httpx.AsyncClient(timeout=120, trust_env=False,
                        # Accommodate the largest allowed request concurrency,
                        # including a drain that sends all traffic to one worker.
                        limits=httpx.Limits(max_connections=256, max_keepalive_connections=64,
                                            keepalive_expiry=1.0))
            except BaseException:
                await close_clients()
                raise

        # The hook owns these clients for its lifespan. Building TLS/transport
        # machinery per request blocks routing even for loopback HTTP workers.
        app.add_event_handler('startup', start_clients)
        app.add_event_handler('shutdown', close_clients)

        @app.get("/v1/models")
        async def models(request: Request):
            if not hmac.compare_digest(request.headers.get("authorization", ""), "Bearer " + token):
                raise HTTPException(401)
            return {"data": [{"id": self.model}]}

        @app.post("/v1/chat/completions")
        async def route(request: Request):
            if not hmac.compare_digest(request.headers.get("authorization", ""), "Bearer " + token):
                raise HTTPException(401)
            raw = await request.body()
            if len(raw) > 65536:
                raise HTTPException(413)
            body = json.loads(raw)
            if body.get("model") != self.model or not 1 <= body.get("max_tokens", 0) <= 4096:
                raise HTTPException(422)
            available = [w for w in self.ports if self.accepting[w]]
            if not available:
                raise HTTPException(503)
            worker = min(available, key=self.in_flight.get)
            self.in_flight[worker] += 1
            client = clients[worker]
            try:
                response = await client.send(client.build_request("POST", f"http://127.0.0.1:{self.ports[worker]}/v1/chat/completions", json=body), stream=True)
            except asyncio.CancelledError:
                self.in_flight[worker] -= 1
                raise
            except Exception:
                self.in_flight[worker] -= 1
                raise HTTPException(502)
            async def stream():
                try:
                    async for part in response.aiter_bytes():
                        yield part
                finally:
                    self.in_flight[worker] -= 1
                    await response.aclose()
            return StreamingResponse(stream(), status_code=response.status_code,
                                     media_type=response.headers.get("content-type", "application/json"),
                                     headers={"x-transitionbench-worker": worker})
