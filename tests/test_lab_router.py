import asyncio
from urllib.parse import urlsplit
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from transitionbench.lab import LabRouter


async def test_router_reuses_worker_connection_and_respects_draining(asgi_server):
    peers=[set(),set()]
    def worker(index):
        app=FastAPI()
        @app.post('/v1/chat/completions')
        async def complete(request:Request):
            peers[index].add(request.client.port)
            async def stream():
                yield b'data: {"choices":[]}\n\n'
                yield b'data: [DONE]\n\n'
            return StreamingResponse(stream(),media_type='text/event-stream')
        return app
    ports=[urlsplit(asgi_server(worker(i))).port for i in range(2)]
    router=LabRouter(ports,model='fixture');app=FastAPI();router.install(app,'fixture-token')
    base=asgi_server(app)
    async with httpx.AsyncClient(base_url=base,trust_env=False) as client:
        for _ in range(3):
            r=await client.post('/v1/chat/completions',headers={'Authorization':'Bearer fixture-token'},
                json={'model':'fixture','max_tokens':32,'messages':[]})
            assert r.status_code==200 and '[DONE]' in r.text
        assert len(peers[0])==1  # Three real HTTP requests, one backend connection.
        router.accepting['0']=False
        r=await client.post('/v1/chat/completions',headers={'Authorization':'Bearer fixture-token'},
            json={'model':'fixture','max_tokens':32,'messages':[]})
        assert r.status_code==200 and r.headers['x-transitionbench-worker']=='1'
        assert len(peers[1])==1
    await asyncio.sleep(.05)
    assert router.in_flight=={'0':0,'1':0}

async def test_router_disconnect_and_error_release_capacity_without_retry(asgi_server):
    worker=FastAPI();calls=[]
    @worker.post('/v1/chat/completions')
    async def complete(request:Request):
        body=await request.json();calls.append(body)
        if body.get('fail'):
            return StreamingResponse(iter([b'upstream unavailable']),status_code=503)
        async def stream():
            yield b'data: first\n\n'
            if body.get('hold'):await asyncio.sleep(10)
            yield b'data: [DONE]\n\n'
        return StreamingResponse(stream(),media_type='text/event-stream')
    port=urlsplit(asgi_server(worker)).port
    router=LabRouter([port,port],model='fixture');app=FastAPI();router.install(app,'fixture-token')
    async with httpx.AsyncClient(base_url=asgi_server(app),trust_env=False) as client:
        headers={'Authorization':'Bearer fixture-token'}
        body={'model':'fixture','max_tokens':32,'messages':[]}
        unauthorized=await client.post('/v1/chat/completions',json=body)
        assert unauthorized.status_code==401 and not calls
        failed=await client.post('/v1/chat/completions',headers=headers,json={**body,'fail':True})
        assert failed.status_code==503 and len(calls)==1
        async with client.stream('POST','/v1/chat/completions',headers=headers,json={**body,'hold':True}) as response:
            async for data in response.aiter_bytes():
                assert b'first' in data
                break
        for _ in range(100):
            if sum(router.in_flight.values())==0:break
            await asyncio.sleep(.01)
        assert router.in_flight=={'0':0,'1':0}
        good=await client.post('/v1/chat/completions',headers=headers,json=body)
        assert good.status_code==200 and '[DONE]' in good.text and len(calls)==3


def test_router_closes_shared_clients_on_hook_shutdown(monkeypatch):
    from types import SimpleNamespace
    from fastapi.testclient import TestClient
    from transitionbench import lab
    made=[]
    def create(*a,**kw):
        client=httpx.AsyncClient(*a,**kw);made.append(client);return client
    monkeypatch.setattr(lab,'httpx',SimpleNamespace(AsyncClient=create,Limits=httpx.Limits))
    router=LabRouter([8801,8802]);app=FastAPI();router.install(app,'fixture-token')
    with TestClient(app):
        assert len(made)==2 and all(not c.is_closed for c in made)
    assert all(c.is_closed for c in made)


async def test_measurement_retains_router_worker_through_export(asgi_server, tmp_path):
    import time
    from transitionbench.endpoint import EndpointClient, NetworkPolicy
    from transitionbench.evidence import export_bundle
    from transitionbench.schemas import EndpointSpec, ExperimentSpec, RunManifest
    from transitionbench.workloads import OfferedRequest
    import json

    worker = FastAPI()
    @worker.post('/v1/chat/completions')
    async def complete():
        return {'choices': [{'message': {'content': '4'}, 'finish_reason': 'stop'}]}
    port = urlsplit(asgi_server(worker)).port
    router = LabRouter([port, port], model='fixture')
    app = FastAPI()
    router.install(app, 'fixture-token')
    base = asgi_server(app)
    endpoint = EndpointClient(EndpointSpec(id='fixture', base_url=base+'/v1', model='fixture'),
                              NetworkPolicy([base], ['127.0.0.1']))
    item = OfferedRequest('probe', 0, 'short', 'probe', '2+2', '4', 100)
    rows = []
    async with httpx.AsyncClient(trust_env=False) as client:
        for expected in ('0', '1'):
            router.accepting = {key: key == expected for key in router.ports}
            original = endpoint.request
            def request(*args, **kwargs):
                req = original(*args, **kwargs)
                req.headers['authorization'] = 'Bearer fixture-token'
                return req
            endpoint.request = request
            row = await endpoint.measure(item, 32, time.monotonic(), client)
            endpoint.request = original
            assert row.quality_valid and row.worker_id == expected
            row.request_id = 'probe-' + expected
            rows.append(row)
    manifest = RunManifest(run_id='attribution-fixture', mode='LIVE_ENDPOINT', origin='measured-black-box',
        experiment=ExperimentSpec(mode='LIVE_ENDPOINT'), offered_ids=[r.request_id for r in rows],
        created_at_unix_s=0, versions={})
    export_bundle(tmp_path/'bundle', manifest, rows, [], [])
    saved = [json.loads(line) for line in (tmp_path/'bundle/requests.jsonl').read_text().splitlines()]
    assert [r['worker_id'] for r in saved] == ['0', '1']
