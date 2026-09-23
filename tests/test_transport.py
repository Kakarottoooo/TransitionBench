import httpcore
import httpx
import asyncio
import ssl
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from httpcore._async.connection_pool import AsyncPoolRequest
from transitionbench.transport import measured_transport


class Connection:
    def __init__(self, origin, *, idle=True, expired=False, closed=False):
        self._origin=origin
        self.idle,self.expired,self.closed=idle,expired,closed
        self.idle_checks=0
    def is_idle(self):
        self.idle_checks+=1
        return self.idle
    def is_closed(self):return self.closed
    def has_expired(self):return self.expired
    def is_available(self):return self.idle
    def can_handle_request(self,origin):return origin==self._origin
    async def aclose(self):self.closed=True


def request(host='one.test'):
    return AsyncPoolRequest(httpcore.Request('GET','https://'+host+'/'))


async def test_pool_scans_idle_state_linearly():
    async with measured_transport(httpx.Limits(max_connections=64,max_keepalive_connections=64)) as transport:
        pool=transport._pool
        pool._connections=[Connection(request().request.url.origin) for _ in range(64)]
        pool._requests=[request()]
        pool._assign_requests_to_connections()
        assert sum(c.idle_checks for c in pool._connections)<=3*64


async def test_pool_reserves_each_http1_connection_once_across_assignment_passes():
    async with measured_transport(httpx.Limits(max_connections=2)) as transport:
        pool=transport._pool;first=Connection(request().request.url.origin)
        pool._connections=[first];pool._requests=[request(),request(),request()]
        pool.create_connection=lambda origin:Connection(origin)
        pool._assign_requests_to_connections()
        assigned=[r.connection for r in pool._requests if r.connection is not None]
        assert len(assigned)==len(set(assigned))==2
        assert pool._requests[-1].connection is None
        pool._assign_requests_to_connections()
        assert pool._requests[-1].connection is None
        # The request leaves the pool only after its response stream is closed.
        pool._requests.pop(0)
        pool._assign_requests_to_connections()
        assert pool._requests[-1].connection is first


async def test_idle_limit_expiry_and_replacement_never_evict_a_reserved_connection():
    async with measured_transport(httpx.Limits(max_connections=2,max_keepalive_connections=1)) as transport:
        pool=transport._pool
        a,b,c=[request(name) for name in ('a.test','b.test','c.test')]
        claimed=Connection(a.request.url.origin);unused=Connection(b.request.url.origin)
        expired=Connection(b.request.url.origin,expired=True)
        a.assign_to_connection(claimed)
        pool._connections=[claimed,unused,expired];pool._requests=[a,c,b]
        pool.create_connection=lambda origin:Connection(origin)
        closing=pool._assign_requests_to_connections()
        assert set(closing)=={unused,expired}
        assert claimed in pool._connections and a.connection is claimed
        assert c.connection in pool._connections and c.connection._origin==c.request.url.origin
        assert b.connection is None and len(pool._connections)==2


async def test_idle_budget_counts_idle_not_active_connections():
    async with measured_transport(httpx.Limits(max_connections=8,max_keepalive_connections=1)) as transport:
        pool=transport._pool;origin=request().request.url.origin
        active=[Connection(origin,idle=False) for _ in range(4)]
        idle=[Connection(origin) for _ in range(2)]
        pool._connections=active+idle
        closing=pool._assign_requests_to_connections()
        assert closing==[idle[-1]] and pool._connections==active+idle[:1]


async def test_real_http_cancellation_releases_capacity_and_errors_are_not_retried(asgi_server):
    app=FastAPI();calls=[]
    @app.get('/')
    async def reply(request:Request):
        mode=request.query_params.get('mode');calls.append(mode)
        async def stream():
            yield b'first'
            if mode=='hold':await asyncio.sleep(10)
            yield b'last'
        return StreamingResponse(stream(),status_code=503 if mode=='fail' else 200)
    transport=measured_transport(httpx.Limits(max_connections=2,keepalive_expiry=1))
    async with httpx.AsyncClient(transport=transport,base_url=asgi_server(app),timeout=2) as client:
        held=[asyncio.create_task(client.get('/?mode=hold')) for _ in range(2)]
        queued=None
        try:
            for _ in range(100):
                if len(calls)==2:break
                await asyncio.sleep(.01)
            assert calls==['hold','hold']
            queued=asyncio.create_task(client.get('/?mode=cancelled'))
            await asyncio.sleep(.03)
            assert not queued.done() and len(calls)==2
            queued.cancel();await asyncio.gather(queued,return_exceptions=True)
            held[0].cancel();await asyncio.gather(held[0],return_exceptions=True)
            response=await client.get('/?mode=fail')
            assert response.status_code==503 and calls.count('fail')==1
            response=await client.get('/?mode=ok')
            assert response.text=='firstlast' and calls.count('ok')==1
        finally:
            for task in held+[queued]:
                if task is not None:task.cancel()
            await asyncio.gather(*held,*([queued] if queued else []),return_exceptions=True)
    assert transport._pool.connections==[]


async def test_transport_keeps_tls_verification_and_rejects_untested_versions(monkeypatch):
    async with measured_transport(httpx.Limits(max_connections=2)) as transport:
        assert transport._pool._ssl_context.verify_mode==ssl.CERT_REQUIRED
        assert transport._pool._ssl_context.check_hostname
        assert transport._pool._retries==0 and not transport._pool._http2
    monkeypatch.setattr(httpcore,'__version__','unsupported')
    with pytest.raises(RuntimeError,match='requires'):
        measured_transport(httpx.Limits())
