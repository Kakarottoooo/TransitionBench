import asyncio
import json
import time
import httpx
import pytest
from transitionbench.endpoint import EndpointClient,NetworkPolicy
from transitionbench.schemas import EndpointSpec,WorkloadSpec
from transitionbench.workloads import generate


class Bytes(httpx.AsyncByteStream):
    def __init__(self,data,delay=0):self.data,self.delay=data,delay
    async def __aiter__(self):
        for piece in self.data:
            if self.delay:await asyncio.sleep(self.delay)
            yield piece


@pytest.mark.parametrize('case,termination,valid',[('empty','complete',False),('partial','partial_stream',False),('wrong-id','partial_stream',False),('wrong-answer','complete',False),('length','complete',False),('timeout','timeout',False),('error','error',False),('ok','complete',True)])
async def test_stream_failures_and_quality_gate(case,termination,valid):
    item=generate(WorkloadSpec(kind='short',injection_s=.1,rate_rps=1))[0]
    spec=EndpointSpec(id='local',base_url='http://127.0.0.1:9998/v1',model='test',streaming=True,supported_parameters=['max_tokens','stream'],timeout_s=.02 if case=='timeout' else 3)
    client=EndpointClient(spec,NetworkPolicy(['http://127.0.0.1:9998'],['127.0.0.1']))
    content='' if case=='empty' else 'wrong' if case=='wrong-answer' else item.expected
    chunks=[{'id':'one','choices':[{'delta':{'role':'assistant','content':''}}]},
            {'id':'one','choices':[{'delta':{'content':content}}]},
            {'id':'two' if case=='wrong-id' else 'one','choices':[{'delta':{},'finish_reason':'length' if case=='length' else 'stop'}]},
            {'choices':[],'usage':{'prompt_tokens':10,'completion_tokens':5}}]
    data=''.join('data: '+json.dumps(c)+'\r\n\r\n' for c in chunks)+('' if case=='partial' else 'data: [DONE]\r\n\r\n')
    attempts=[]
    async def handler(request):
        attempts.append(request)
        return httpx.Response(429 if case=='error' else 200,headers={'content-type':'text/event-stream'},stream=Bytes([data[:25].encode(),data[25:].encode()],.03 if case=='timeout' else 0))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as session:
        row=await client.measure(item,32,time.monotonic(),session)
    assert row.termination==termination
    assert row.quality_valid==valid
    assert len(attempts)==1 and row.attempt==1
    if case=='ok':assert row.output_tokens==5 and row.token_origin=='server'


async def test_required_zdr_and_no_retry_downgrade(monkeypatch):
    monkeypatch.setenv('TEST_WAFER_KEY','sentinel-not-exported')
    spec=EndpointSpec(id='wafer',base_url='https://pass.wafer.ai/v1',provider='wafer',model='discovered',key_env='TEST_WAFER_KEY',require_zdr=True)
    network=NetworkPolicy(['https://pass.wafer.ai'])
    monkeypatch.setattr(network,'resolve',lambda url:(url,'pass.wafer.ai','pass.wafer.ai'))
    client=EndpointClient(spec,network)
    requests=[]
    async def handler(request):
        requests.append(request)
        assert request.headers['Wafer-ZDR']=='required'
        return httpx.Response(422,json={'error':{'code':'model_zdr_not_supported'}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as session:
        row=await client.measure(generate(WorkloadSpec(kind='short',injection_s=.1,rate_rps=1))[0],32,time.monotonic(),session)
    assert row.termination=='error' and len(requests)==1
    assert 'sentinel-not-exported' not in row.model_dump_json()
