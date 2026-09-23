import asyncio
import socket
import subprocess
import sys
import time
import httpx
import pytest
from transitionbench.schemas import EndpointSpec, ExperimentSpec, WorkloadSpec, ResourceBudget
from transitionbench.endpoint import EndpointClient, NetworkPolicy, run_endpoint


@pytest.fixture(scope="module")
def local_server():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    process = subprocess.Popen([sys.executable, "-m", "uvicorn", "transitionbench.test_server:app", "--host", "127.0.0.1", "--port", str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            if httpx.get(url + "/healthz").status_code == 200:
                break
        except httpx.TransportError:
            time.sleep(.05)
    else:
        process.terminate()
        raise RuntimeError("Local server did not start")
    yield url
    process.terminate()
    process.wait(timeout=10)


async def test_real_fragmented_stream_and_bounded_arrivals(local_server):
    endpoint = EndpointSpec(id="test", base_url=local_server + "/v1", model="local-arithmetic",
                            provider="local-test", streaming=True, supported_parameters=["max_tokens", "stream"])
    client = EndpointClient(endpoint, NetworkPolicy([local_server], ["127.0.0.1"]))
    models = await client.discover()
    assert "local-arithmetic" in [m["id"] for m in models]
    spec = ExperimentSpec(mode="LIVE_ENDPOINT", endpoint_id="test",
                           workload=WorkloadSpec(kind="short", injection_s=.4, rate_rps=10),
                           observation_s=1.4, drain_s=1, budget=ResourceBudget(max_requests=8, reserved_gpus=0))
    rows, validity = await run_endpoint(spec, client, asyncio.Event())
    assert len(rows) == 4
    assert all(r.termination == "complete" and r.quality_valid for r in rows)
    assert all(r.output_tokens is None and r.token_origin == "unknown" for r in rows)
    # Python 3.12 on Windows may round both observations to one clock tick.
    assert all(r.first_content_s >= r.dispatch_s for r in rows)
    assert validity["valid"]


@pytest.mark.parametrize("tail_delay,tail,termination", [
    (.02, b"", "complete"),
    (.5, b"", "timeout"),
    (.02, b"data: unexpected\n\n", "partial_stream"),
])
async def test_completed_sse_requests_reuse_connection(asgi_server, tail_delay, tail, termination):
    """DONE must not abandon HTTP framing and churn one socket per request."""
    from transitionbench.workloads import OfferedRequest

    async def app(scope, receive, send):
        if scope["type"] != "http":
            return
        while (await receive()).get("more_body", False):
            pass
        await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"content-type", b"text/event-stream")]})
        await send({"type": "http.response.body", "more_body": True,
                    "body": b'data: {"choices":[{"delta":{"content":"4"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'})
        # DONE is an SSE marker, not the HTTP end-of-message framing.
        await asyncio.sleep(tail_delay)
        await send({"type": "http.response.body", "body": tail})

    local_server = asgi_server(app)

    client = EndpointClient(
        EndpointSpec(id="test", base_url=local_server + "/v1", model="local-arithmetic",
                     streaming=True, timeout_s=.2, supported_parameters=["max_tokens", "stream"]),
        NetworkPolicy([local_server], ["127.0.0.1"]),
    )
    connects = []
    original = client.request

    async def trace(event, info):
        if event == "connection.connect_tcp.started":
            connects.append(event)

    def request(*args, **kwargs):
        result = original(*args, **kwargs)
        result.extensions["trace"] = trace
        return result

    client.request = request
    items = [OfferedRequest(str(i), 0, "short", str(i), "2+2", "4", 100) for i in range(3)]
    async with httpx.AsyncClient(trust_env=False) as session:
        for item in items:
            row = await client.measure(item, 32, time.monotonic(), session)
            assert row.termination == termination
            assert row.quality_valid == (termination == "complete")
    if termination == "complete":
        assert len(connects) == 1


async def test_async_backend_detection_has_no_repeated_import_misses(local_server):
    """HTTPcore probes optional sniffio on every new async primitive."""
    from transitionbench.workloads import OfferedRequest

    client = EndpointClient(
        EndpointSpec(id="test", base_url=local_server + "/v1", model="local-arithmetic",
                     streaming=True, supported_parameters=["max_tokens", "stream"]),
        NetworkPolicy([local_server], ["127.0.0.1"]),
    )
    item = OfferedRequest("probe", 0, "short", "probe", "Return exactly TB:probe:4", "TB:probe:4", 100)
    misses = []
    class ImportProbe:
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "sniffio":
                misses.append(fullname)
            return None

    probe = ImportProbe()
    async with httpx.AsyncClient(trust_env=False) as session:
        assert (await client.measure(item, 32, time.monotonic(), session)).quality_valid
        sys.meta_path.insert(0, probe)
        try:
            for _ in range(3):
                assert (await client.measure(item, 32, time.monotonic(), session)).quality_valid
        finally:
            sys.meta_path.remove(probe)
    assert not misses, "Async HTTP backend repeatedly searches for an absent optional dependency"


async def test_idle_connection_is_retired_before_peer_closes_during_reuse():
    """A peer can close an idle socket just as a later POST reuses it."""
    from transitionbench.workloads import OfferedRequest

    connections = []
    async def peer(reader, writer):
        connections.append(writer)
        served, last_reply = 0, 0
        try:
            while True:
                headers = await reader.readuntil(b"\r\n\r\n")
                length = next(int(line.split(b":", 1)[1]) for line in headers.split(b"\r\n")
                              if line.lower().startswith(b"content-length:"))
                await reader.readexactly(length)
                if served and time.monotonic() - last_reply > 1:
                    # Force the idle-expiry race before any response headers.
                    writer.transport.abort()
                    return
                body = (b'data: {"choices":[{"delta":{"content":"4"},"finish_reason":"stop"}]}\n\n'
                        b'data: [DONE]\n\n')
                writer.write(("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                              f"Content-Length: {len(body)}\r\n\r\n").encode() + body)
                await writer.drain()
                served += 1
                last_reply = time.monotonic()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    async with await asyncio.start_server(peer, "127.0.0.1", 0) as server:
        url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        client = EndpointClient(EndpointSpec(id="test", base_url=url+"/v1", model="local-arithmetic",
            streaming=True, supported_parameters=["max_tokens", "stream"]),
            NetworkPolicy([url], ["127.0.0.1"]))
        spec = ExperimentSpec(mode="LIVE_ENDPOINT", endpoint_id="test",
            workload=WorkloadSpec(kind="short", injection_s=2.4, rate_rps=1),
            observation_s=3, drain_s=.6, budget=ResourceBudget(max_requests=4, reserved_gpus=0))
        offered = [OfferedRequest(str(i), i*1.2, "short", str(i), "2+2", "4", 100) for i in range(2)]
        rows, validity = await run_endpoint(spec, client, asyncio.Event(), offered=offered, skip_discovery=True)
    assert all(row.termination == "complete" and row.quality_valid and row.attempt == 1 for row in rows)
    assert len(connections) == 2
    assert validity["valid"]
