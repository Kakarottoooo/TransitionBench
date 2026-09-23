"""Bounded OpenAI-compatible measurements, with no automatic generation retries."""
import asyncio
import ipaddress
import json
import os
import socket
import time
from urllib.parse import urlsplit
import httpx
from .schemas import EndpointSpec, ExperimentSpec, RequestEvent
from .workloads import OfferedRequest, generate
from .transport import measured_transport


class Refusal(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


class NetworkPolicy:
    def __init__(self, allowed_origins=(), private_hosts=()):
        self.allowed = set(allowed_origins)
        self.private_hosts = set(private_hosts)

    def resolve(self, url: str):
        parsed = urlsplit(url)
        if parsed.scheme not in ("https", "http") or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise Refusal("destination_refused", "Only clean HTTP(S) destinations are supported")
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if origin not in self.allowed:
            raise Refusal("destination_refused", "Destination is not operator-allowlisted")
        host = parsed.hostname
        if parsed.scheme != "https" and host not in self.private_hosts:
            raise Refusal("tls_required", "HTTP requires an explicitly configured lab host")
        addresses = sorted({r[4][0] for r in socket.getaddrinfo(host, parsed.port or (443 if parsed.scheme == "https" else 80), type=socket.SOCK_STREAM)})
        if not addresses:
            raise Refusal("destination_refused", "Destination did not resolve")
        for addr in addresses:
            ip = ipaddress.ip_address(addr)
            if ip.is_link_local or ip.is_multicast or ip.is_unspecified or (not ip.is_global and host not in self.private_hosts):
                raise Refusal("destination_refused", "Private or metadata destination refused")
        # Pin the validated address in the actual connection; retain TLS identity.
        return str(httpx.URL(url).copy_with(host=addresses[0])), parsed.netloc, host


class EndpointClient:
    def __init__(self, spec: EndpointSpec, network: NetworkPolicy):
        self.spec, self.network = spec, network
        if spec.provider == "wafer" and spec.base_url != "https://pass.wafer.ai/v1":
            raise Refusal("invalid_endpoint", "Wafer configuration must use its documented origin")
        if spec.require_zdr and spec.provider != "wafer":
            raise Refusal("unsupported_capability", "ZDR semantics are only defined for the Wafer adapter")

    def request(self, method, suffix, **kwargs):
        url, host, sni = self.network.resolve(self.spec.base_url.rstrip("/") + suffix)
        headers = {"Host": host}
        if self.spec.key_env:
            key = os.environ.get(self.spec.key_env)
            if not key:
                raise Refusal("credentials_missing", "Configured credential environment variable is unset")
            headers["Authorization"] = "Bearer " + key
        if self.spec.require_zdr:
            headers["Wafer-ZDR"] = "required"
        return httpx.Request(method, url, headers=headers, extensions={"sni_hostname": sni}, **kwargs)

    async def discover(self):
        async with httpx.AsyncClient(timeout=self.spec.timeout_s, follow_redirects=False, trust_env=False) as client:
            response = await client.send(self.request("GET", "/models"))
            if response.status_code != 200:
                raise Refusal("provider_error", f"Model discovery returned HTTP {response.status_code}")
            if len(response.content) > 2_000_000:
                raise Refusal("provider_error", "Model discovery response too large")
            data = response.json().get("data", [])
            selected = next((m for m in data if m.get("id") == self.spec.model), None)
            if selected is None:
                raise Refusal("model_unknown", "Selected model was not discovered at this endpoint")
            if self.spec.require_zdr and selected.get("zdr_supported") is not True:
                raise Refusal("zdr_unsupported", "Selected model does not advertise ZDR support")
            return [{k: m[k] for k in ("id", "zdr_supported") if k in m} for m in data]

    async def measure(self, item: OfferedRequest, max_tokens: int, epoch: float, session: httpx.AsyncClient, *, on_quality_failure=None):
        spec = self.spec
        if "max_tokens" not in spec.supported_parameters or (spec.streaming and "stream" not in spec.supported_parameters):
            raise Refusal("unsupported_parameter", "Output/stream constraint has not been contract-verified")
        now = lambda: max(item.scheduled_s, time.monotonic() - epoch)
        row = RequestEvent(request_id=item.request_id, scheduled_s=item.scheduled_s,
                           dispatch_s=now(), workload_class=item.workload_class, prefix_group=item.prefix_group,
                           origin="measured-black-box", quality_check="exact-synthetic-request-marker")
        row.scheduling_lag_s = row.dispatch_s - item.scheduled_s
        body = {"model": spec.model, "messages": [{"role": "user", "content": item.prompt}], "max_tokens": max_tokens}
        for parameter in ("temperature", "seed"):
            value = getattr(spec, parameter)
            if value is not None:
                if parameter not in spec.supported_parameters:
                    raise Refusal("unsupported_parameter", f"{parameter} has not been contract-verified")
                body[parameter] = value
        if spec.streaming:
            body["stream"] = True
        content, done, stream_id = "", False, None
        try:
            async with asyncio.timeout(spec.timeout_s):
                response = await session.send(self.request("POST", "/chat/completions", json=body), stream=True)
                try:
                    row.status_code = response.status_code
                    row.provider_request_id = response.headers.get("x-request-id", response.headers.get("request-id"))
                    # Router-reported attribution, not independent GPU identity.
                    # Ignore arbitrary remote values rather than retaining headers.
                    worker = response.headers.get("x-transitionbench-worker")
                    if worker in ('0', '1'):
                        row.worker_id = worker
                    if response.status_code != 200:
                        row.termination = "error"
                        return row
                    if spec.streaming:
                        if "text/event-stream" not in response.headers.get("content-type", ""):
                            row.termination = "error"
                            return row
                        data_lines, received = [], 0
                        async for line in response.aiter_lines():
                            received += len(line)
                            if received > 2_000_000:
                                raise ValueError("Response exceeded limit")
                            if done:
                                if line.strip() and not line.startswith(":"):
                                    raise ValueError("Data after stream completion")
                                continue
                            if line.startswith("data:"):
                                data_lines.append(line[5:].lstrip())
                            elif line == "" and data_lines:
                                payload = "\n".join(data_lines)
                                data_lines = []
                                if payload == "[DONE]":
                                    done = True
                                    # Consume bounded HTTP framing to release a reusable
                                    # connection; DONE alone does not end the HTTP body.
                                    continue
                                obj = json.loads(payload)
                                if obj.get("id"):
                                    if stream_id and stream_id != obj["id"]:
                                        raise ValueError("Response identity changed")
                                    stream_id = obj["id"]
                                if obj.get("model") and obj["model"] != spec.model:
                                    raise ValueError("Unexpected response model")
                                if obj.get("error"):
                                    raise ValueError("Stream error")
                                self._usage(row, obj)
                                for choice in obj.get("choices", []):
                                    if choice.get("index", 0) != 0:
                                        raise ValueError("Unexpected multiple completions")
                                    delta = choice.get("delta", {})
                                    if delta.get("reasoning_content") and row.first_reasoning_s is None:
                                        row.first_reasoning_s = now()
                                    text = delta.get("content")
                                    if text:
                                        if not isinstance(text, str):
                                            raise ValueError("Unsupported content type")
                                        at = now()
                                        row.first_content_s = row.first_content_s if row.first_content_s is not None else at
                                        row.final_content_s = at
                                        row.chunk_times_s.append(at)
                                        content += text
                                    if choice.get("finish_reason"):
                                        row.finish_reason = choice["finish_reason"]
                    else:
                        raw = bytearray()
                        async for part in response.aiter_bytes():
                            raw.extend(part)
                            if len(raw) > 2_000_000:
                                raise ValueError("Response exceeded limit")
                        obj = json.loads(raw)
                        if obj.get("model") and obj["model"] != spec.model:
                            raise ValueError("Unexpected response model")
                        choice = obj["choices"][0]
                        content = choice["message"].get("content") or ""
                        row.finish_reason = choice.get("finish_reason")
                        self._usage(row, obj)
                        if content:
                            row.first_content_s = row.final_content_s = now()
                        done = True
                        row.quality_check += "; first-content unavailable before full nonstreaming response"
                    row.termination = "complete" if done and row.finish_reason else "partial_stream"
                finally:
                    await response.aclose()
                    # HTTPX's bound stream points back to its response. After
                    # close, detach that cycle instead of deferring hundreds of
                    # completed response graphs to a stop-the-world GC pass.
                    response.stream = httpx.ByteStream(b"")
        except TimeoutError:
            row.termination = "timeout"
        except asyncio.CancelledError:
            row.termination = "cancelled"
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            row.termination = "partial_stream" if content else "error"
        finally:
            row.output_chars = len(content)
            row.completed_s = now()
            row.quality_valid = row.termination == "complete" and row.finish_reason == "stop" and content.strip() == item.expected
            # Explicit diagnostic opt-in only. The observer must be bounded and
            # memory-only; ordinary measurements never retain response text.
            if on_quality_failure is not None and row.termination == "complete" and not row.quality_valid:
                on_quality_failure(item, row, content)
        return row

    @staticmethod
    def _usage(row, obj):
        usage = obj.get("usage")
        if usage:
            for field, name in (("output_tokens", "completion_tokens"), ("input_tokens", "prompt_tokens")):
                value = usage.get(name)
                if value is not None:
                    if not isinstance(value, int) or value < 0:
                        raise ValueError("Invalid server token usage")
                    setattr(row, field, value)
            row.token_origin = "server"


async def run_endpoint(spec: ExperimentSpec, client: EndpointClient, cancel: asyncio.Event, on_event=None, offered=None, on_arrival=None, epoch=None, skip_discovery=False):
    items = offered if offered is not None else generate(spec.workload)
    if len(items) > spec.budget.max_requests:
        raise Refusal("budget_refusal", "Offered workload exceeds request budget")
    reserve = sum(i.input_token_upper_bound + spec.budget.max_output_tokens for i in items)
    if reserve > spec.budget.max_total_tokens:
        raise Refusal("budget_refusal", "Worst-case prompt/output token reservation exceeds budget")
    if not skip_discovery:
        await client.discover()
    rows, pending = {}, set()
    previous_completion = 0.0

    def save(row):
        # Keep complete evidence without growing the cyclic-GC graph throughout
        # timed arrivals. Rehydrate only after all network work has finished.
        rows[row.request_id] = row.model_dump_json()
        if on_event:
            on_event(row)

    async def one(item, session):
        row = await client.measure(item, spec.budget.max_output_tokens, epoch, session)
        if row.termination == "cancelled" and not cancel.is_set():
            row.termination = "unfinished"
        save(row)
        return row

    # Retire idle sockets before common 5 s server deadlines. Keep connection
    # assignment bounded as slow responses populate the HTTP/1.1 pool.
    transport=measured_transport(httpx.Limits(max_connections=spec.budget.max_concurrency,
                                             keepalive_expiry=1.0))
    async with httpx.AsyncClient(timeout=client.spec.timeout_s, follow_redirects=False,
                                trust_env=False, transport=transport) as session:
        # Construct TLS/connection machinery before the injection clock starts.
        # Setup is not a scheduled request and must not create artificial lag.
        epoch = epoch if epoch is not None else time.monotonic()
        for original in items:
            item = original
            if spec.workload.arrival_model == "scripted-session":
                from dataclasses import replace
                item = replace(item, scheduled_s=max(item.scheduled_s, previous_completion + spec.workload.think_s))
            delay = min(item.scheduled_s, spec.observation_s) - (time.monotonic() - epoch)
            if delay > 0 and not cancel.is_set():
                try:
                    await asyncio.wait_for(cancel.wait(), delay)
                except TimeoutError:
                    pass
            if cancel.is_set() or time.monotonic() - epoch >= spec.observation_s:
                save(RequestEvent(request_id=item.request_id, scheduled_s=item.scheduled_s,
                                  workload_class=item.workload_class, prefix_group=item.prefix_group,
                                  origin="measured-black-box", termination="cancelled" if cancel.is_set() else "unfinished"))
                continue
            pending = {t for t in pending if not t.done()}
            if on_arrival:
                on_arrival(item.workload_class, item.prefix_group, item.scheduled_s, len(pending))
            if len(pending) >= spec.budget.max_concurrency:
                save(RequestEvent(request_id=item.request_id, scheduled_s=item.scheduled_s,
                                  workload_class=item.workload_class, prefix_group=item.prefix_group,
                                  origin="measured-black-box", termination="client_drop",
                                  scheduling_lag_s=max(0, time.monotonic() - epoch - item.scheduled_s)))
                continue
            task = asyncio.create_task(one(item, session))
            pending.add(task)
            if spec.workload.arrival_model == "scripted-session":
                row = await task
                previous_completion = row.completed_s
        if pending:
            remaining = max(0, spec.observation_s - (time.monotonic() - epoch))
            stopper = asyncio.create_task(cancel.wait())
            group = asyncio.gather(*pending)
            await asyncio.wait([group, stopper], timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            stopper.cancel()
            await asyncio.gather(stopper, return_exceptions=True)
            await group
    result = [RequestEvent.model_validate_json(rows[i.request_id]) for i in items]
    errors = []
    if any((r.scheduling_lag_s or 0) > spec.max_dispatch_lag_s for r in result):
        errors.append("Client scheduling lag exceeded predeclared tolerance")
    if cancel.is_set():
        errors.append("Run cancelled; all offered requests retained")
    return result, {"valid": not errors, "errors": errors}
