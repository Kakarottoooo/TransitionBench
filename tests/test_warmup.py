import httpx
import pytest
from transitionbench.schemas import WarmupSpec
from transitionbench.warmup import warm_worker


async def test_warmup_requires_repeated_valid_stable_windows(monkeypatch):
    import transitionbench.warmup as module
    clock = [0.0]
    calls = []

    async def endpoint(request):
        import json
        body = json.loads(request.content)
        calls.append(body)
        clock[0] += .1
        expected = body['messages'][0]['content'].split('Return exactly ')[-1].split(' and nothing else.')[0]
        return httpx.Response(200, json={'choices': [{'finish_reason': 'stop', 'message': {'content': expected}}]})

    monkeypatch.setattr(module.time, 'monotonic', lambda: clock[0])
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        report = await warm_worker(client, 'http://worker/v1/chat/completions', 'fixture', 32, WarmupSpec())
    assert report['stable'] and len(calls) == 12
    assert {r['workload_class'] for r in report['requests']} == {'short', 'long'}
    assert all(r['quality_valid'] for r in report['requests'])
    assert 'content' not in str(report)


@pytest.mark.parametrize('failure', ['unstable', 'quality', 'timeout'])
async def test_warmup_refuses_bad_probes_and_retains_partial_evidence(monkeypatch, failure):
    import asyncio
    import json
    import transitionbench.warmup as module
    clock, calls, retained = [0.0], [], []
    async def endpoint(request):
        body = json.loads(request.content)
        index = len(calls)
        calls.append(body)
        if failure == 'timeout':
            await asyncio.sleep(.05)
        clock[0] += .1 if (index // 4) % 2 == 0 else .3
        expected = body['messages'][0]['content'].split('Return exactly ')[-1].split(' and nothing else.')[0]
        return httpx.Response(200, json={'choices': [{'finish_reason': 'stop', 'message': {
            'content': 'wrong' if failure == 'quality' else expected}}]})
    if failure != 'timeout':
        monkeypatch.setattr(module.time, 'monotonic', lambda: clock[0])
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        with pytest.raises(module.WarmupFailure):
            await warm_worker(client, 'http://worker/v1/chat/completions', 'fixture', 32,
                WarmupSpec(max_requests=12, max_duration_s=.01 if failure == 'timeout' else 45),
                record=retained.append)
    assert not retained[0]['stable']
    assert len(calls) == (12 if failure == 'unstable' else 1)
    assert len(retained[0]['requests']) == len(calls)


async def test_warmup_uses_real_local_http_and_keeps_text_out_of_receipt(asgi_server):
    from fastapi import FastAPI, Request
    app=FastAPI()
    @app.post('/v1/chat/completions')
    async def completion(request: Request):
        body=await request.json()
        return {'choices':[{'finish_reason':'stop','message':{
            'content':body['messages'][0]['content'].split('Return exactly ')[-1].split(' and nothing else.')[0]}}]}
    base=asgi_server(app)
    async with httpx.AsyncClient(trust_env=False) as client:
        report=await warm_worker(client,base+'/v1/chat/completions','fixture',32,
            WarmupSpec(relative_tolerance=.5,absolute_tolerance_s=.1))
    assert report['stable'] and len(report['requests'])>=12
    assert 'TB-WARM-' not in str(report)


async def test_fixed_sequence_does_not_exit_early_when_already_stable(monkeypatch):
    import json
    import transitionbench.warmup as module
    clock, calls = [0.0], []
    async def endpoint(request):
        body = json.loads(request.content)
        calls.append(body)
        clock[0] += .1
        expected = body['messages'][0]['content'].split('Return exactly ')[-1].split(' and nothing else.')[0]
        return httpx.Response(200, json={'choices':[{'finish_reason':'stop','message':{'content':expected}}]})
    monkeypatch.setattr(module.time, 'monotonic', lambda: clock[0])
    policy = WarmupSpec().model_copy(update={'complete_probe_sequence':True})
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        report = await warm_worker(client,'http://worker/v1/chat/completions','fixture',32,policy)
    assert report['stable'] and len(calls) == policy.max_requests


@pytest.mark.parametrize('drifting', [False, True])
async def test_four_sample_windows_reject_drift_and_tolerate_isolated_spikes(monkeypatch, drifting):
    import json
    import transitionbench.warmup as module
    clock, calls = [0.0], []
    async def endpoint(request):
        body=json.loads(request.content);index=len(calls);calls.append(body)
        # Four samples per class: one spike must not set the class median.
        latency=.075*(1.4**(index//8)) if drifting else (.15 if index%8 in (0,1) else .075)
        clock[0]+=latency
        expected=body['messages'][0]['content'].split('Return exactly ')[-1].split(' and nothing else.')[0]
        return httpx.Response(200,json={'choices':[{'finish_reason':'stop','message':{'content':expected}}]})
    monkeypatch.setattr(module.time,'monotonic',lambda:clock[0])
    policy=WarmupSpec(complete_probe_sequence=True,max_requests=32,samples_per_class=4)
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        if drifting:
            with pytest.raises(module.WarmupFailure):
                await warm_worker(client,'http://worker/completions','fixture',32,policy)
        else:
            result=await warm_worker(client,'http://worker/completions','fixture',32,policy)
            assert result['stable'] and len(result['windows'])==4
    assert len(calls)==32


def test_warmup_window_budget_requires_three_complete_windows():
    with pytest.raises(ValueError):
        WarmupSpec(max_requests=16,samples_per_class=4)
    with pytest.raises(ValueError):
        WarmupSpec(complete_probe_sequence=True,max_requests=28,samples_per_class=4)
