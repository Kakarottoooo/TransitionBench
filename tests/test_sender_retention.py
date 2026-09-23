"""Finished evidence must not grow the GC object graph during timed arrivals."""
import asyncio
import weakref
from types import SimpleNamespace

from transitionbench.endpoint import run_endpoint
from transitionbench.schemas import ExperimentSpec, RequestEvent
from transitionbench.workloads import OfferedRequest
import httpx
from transitionbench.endpoint import EndpointClient, NetworkPolicy
from transitionbench.schemas import EndpointSpec


async def test_sender_releases_finished_objects_and_restores_exact_evidence():
    references=[]
    expected=[]

    async def measure(item, max_tokens, epoch, session):
        if references:
            assert references[-1]() is None
        row=RequestEvent(request_id=item.request_id, scheduled_s=item.scheduled_s,
            dispatch_s=item.scheduled_s, scheduling_lag_s=0, workload_class='short',
            prefix_group='test', origin='measured-black-box', termination='complete',
            completed_s=item.scheduled_s+.001, first_content_s=item.scheduled_s,
            final_content_s=item.scheduled_s+.001, chunk_times_s=[item.scheduled_s,item.scheduled_s+.001],
            output_chars=1, quality_valid=True, finish_reason='stop')
        references.append(weakref.ref(row))
        expected.append(row.model_dump(mode='json'))
        return row

    client=SimpleNamespace(spec=SimpleNamespace(timeout_s=1),measure=measure)
    spec=ExperimentSpec(mode='LIVE_ENDPOINT', endpoint_id='test',
        workload={'kind':'short','injection_s':.3,'rate_rps':10}, observation_s=1.3,drain_s=1,
        budget={'max_requests':3,'reserved_gpus':0})
    offered=[OfferedRequest(str(i),i*.1,'short','test','2+2','4',100) for i in range(3)]
    delivered=[]
    rows,validity=await run_endpoint(spec,client,asyncio.Event(),offered=offered,
        skip_discovery=True,on_event=lambda row:delivered.append(row.model_dump(mode='json')))
    assert validity['valid']
    assert len(rows)==3 and [r.model_dump(mode='json') for r in rows]==expected==delivered


async def test_closed_http_response_releases_its_stream_cycle_without_gc():
    item=OfferedRequest('cycle',0,'short','test','2+2','4',100)
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"choices":[{"delta":{"content":"4"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'
    async def handle(request):
        return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=Stream())
    client=EndpointClient(EndpointSpec(id='local',base_url='http://127.0.0.1:9000/v1',model='test',streaming=True,
        supported_parameters=['max_tokens','stream']),
        NetworkPolicy(['http://127.0.0.1:9000'],['127.0.0.1']))
    responses=[]
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as session:
        send=session.send
        async def observed_send(*args,**kwargs):
            response=await send(*args,**kwargs)
            responses.append(weakref.ref(response))
            return response
        session.send=observed_send
        import time
        row=await client.measure(item,32,time.monotonic(),session)
        assert row.termination=='complete' and row.quality_valid and row.output_chars==1
        assert responses[0]() is None
