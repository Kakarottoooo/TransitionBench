import asyncio
import time
from transitionbench.endpoint import EndpointClient,NetworkPolicy,run_endpoint
from transitionbench.schemas import EndpointSpec,ExperimentSpec,WorkloadSpec
from transitionbench.test_server import app


async def test_scripted_session_dependency_cannot_wait_beyond_budget(asgi_server):
    base=asgi_server(app)
    endpoint=EndpointSpec(id='test',base_url=base+'/v1',model='local-arithmetic')
    client=EndpointClient(endpoint,NetworkPolicy([base],['127.0.0.1']))
    spec=ExperimentSpec(mode='LIVE_ENDPOINT',workload=WorkloadSpec(kind='short',arrival_model='scripted-session',think_s=10,injection_s=.1,rate_rps=20),observation_s=.2,drain_s=.1)
    start=time.monotonic()
    rows,_=await run_endpoint(spec,client,asyncio.Event())
    assert time.monotonic()-start<1
    assert len(rows)==2 and all(r.termination=='unfinished' for r in rows)
