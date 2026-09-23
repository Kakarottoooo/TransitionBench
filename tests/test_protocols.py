import asyncio
import os
import sys
import time
import pytest
import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from transitionbench.api import create_app
from transitionbench.hook_server import create_hook_app
from transitionbench.endpoint import NetworkPolicy, Refusal
from transitionbench.rollout import HTTPHookAdapter, PlanStore, execute_plan
from test_rollout import FixtureAdapter


async def test_real_mcp_handshake_and_tools(asgi_server,tmp_path):
    api=asgi_server(create_app(tmp_path/'api'))
    params=StdioServerParameters(command=sys.executable,args=['-m','transitionbench.mcp_server'],env={**os.environ,'TRANSITIONBENCH_API':api})
    async with stdio_client(params) as (reader,writer):
        async with ClientSession(reader,writer) as session:
            initialized=await session.initialize()
            assert initialized.protocolVersion=='2025-11-25'
            available=await session.list_tools()
            names={t.name for t in available.tools}
            assert {'list_capabilities','validate_experiment','get_run','compare_runs','explain_decision','plan_transition','analyze_bundle'}==names
            result=await session.call_tool('list_capabilities',{})
            assert not result.isError
            result=await session.call_tool('validate_experiment',{'experiment':{}})
            assert not result.isError
            assert not any('execute' in n or 'approve' in n or 'shell' in n for n in names)


async def test_real_hook_lifecycle_duplicates_and_conflicts(asgi_server,tmp_path):
    local=FixtureAdapter()
    token='test-only-operator-token-123456789'
    base=asgi_server(create_hook_app(local,token,tmp_path/'hook.db'))
    adapter=HTTPHookAdapter(base,token,NetworkPolicy([base],['127.0.0.1']))
    snapshots=await adapter.snapshot()
    store=PlanStore(tmp_path/'plans.db')
    from transitionbench.schemas import ResourceBudget
    plan=store.create(base,'B',snapshots,ResourceBudget())
    store.approve(plan['id'],plan['hash'])
    result=await execute_plan(store,plan['id'],adapter)
    assert result['state']=='COMPLETE', result
    payload={'config_id':'B','expected_generation':1,'plan_id':plan['id'],'plan_hash':plan['hash'],'expires_at_unix_s':time.time()+30,'drain_timeout_s':1,'max_tokens':8}
    first=await adapter.operation('observe','0',payload,'same-operation')
    n=len(local.calls)
    assert await adapter.operation('observe','0',payload,'same-operation')==first
    assert len(local.calls)==n
    with pytest.raises(Refusal):
        await adapter.operation('observe','0',{**payload,'config_id':'A'},'same-operation')
    bad=HTTPHookAdapter(base,'wrong-token',NetworkPolicy([base],['127.0.0.1']))
    with pytest.raises(Refusal):await bad.snapshot()


async def test_hook_failure_cannot_be_blindly_replayed(asgi_server,tmp_path):
    adapter=FixtureAdapter();adapter.fail_readiness=True
    token='test-token-01234567890123456789'
    base=asgi_server(create_hook_app(adapter,token,tmp_path/'hook.db'))
    remote=HTTPHookAdapter(base,token,NetworkPolicy([base],['127.0.0.1']))
    payload={'config_id':'A','expected_generation':0,'plan_id':'a'*32,'plan_hash':'b'*64,'expires_at_unix_s':time.time()+30,'drain_timeout_s':1,'max_tokens':8}
    for _ in range(2):
        with pytest.raises(Refusal):await remote.operation('readiness','0',payload,'failed-id')
    assert adapter.calls==[('readiness','0')]


async def test_remote_timeout_remains_uncertain_and_cannot_be_replayed(asgi_server,tmp_path):
    class SlowAdapter(FixtureAdapter):
        async def operation(self, operation, worker_id, payload, key):
            self.calls.append((operation,worker_id))
            await asyncio.sleep(.1)
            return {}
    adapter=SlowAdapter()
    token='test-only-timeout-token-0123456789'
    base=asgi_server(create_hook_app(adapter,token,tmp_path/'hook.db'))
    remote=HTTPHookAdapter(base,token,NetworkPolicy([base],['127.0.0.1']))
    payload={'config_id':'B','expected_generation':0,'plan_id':'a'*32,'plan_hash':'b'*64,
        'expires_at_unix_s':time.time()+30,'drain_timeout_s':1,'max_tokens':32,'operation_timeout_s':.01}
    with pytest.raises(TimeoutError):await remote.operation('apply','0',payload,'uncertain')
    with pytest.raises(Refusal):await remote.operation('apply','0',payload,'uncertain')
    assert adapter.calls==[('apply','0')]


async def test_warmup_failure_reason_survives_real_http_without_output_text(asgi_server,tmp_path):
    from fastapi import FastAPI
    from transitionbench.warmup import warm_worker
    from transitionbench.schemas import WarmupSpec
    engine=FastAPI()
    @engine.post('/v1/chat/completions')
    async def wrong_output():
        return {'choices':[{'finish_reason':'stop','message':{'content':'private-model-output'}}]}
    engine_url=asgi_server(engine)
    class BadWarmup(FixtureAdapter):
        async def operation(self, operation, worker_id, payload, key):
            self.calls.append((operation,worker_id))
            async with httpx.AsyncClient(trust_env=False) as client:
                return await warm_worker(client,engine_url+'/v1/chat/completions','fixture',32,WarmupSpec())
    local=BadWarmup()
    token='test-only-warmup-token-0123456789'
    base=asgi_server(create_hook_app(local,token,tmp_path/'hook.db'))
    remote=HTTPHookAdapter(base,token,NetworkPolicy([base],['127.0.0.1']))
    payload={'config_id':'B','expected_generation':0,'plan_id':'a'*32,'plan_hash':'b'*64,
        'expires_at_unix_s':time.time()+30,'drain_timeout_s':1,'max_tokens':32}
    with pytest.raises(Refusal) as failure:
        await remote.operation('warmup','0',payload,'failed-warmup')
    assert 'warmup_failed:invalid_output' in str(failure.value)
    assert 'private-model-output' not in str(failure.value)
    with pytest.raises(Refusal,match='operation_requires_reconciliation'):
        await remote.operation('warmup','0',payload,'failed-warmup')
    assert local.calls==[('warmup','0')]


async def test_hook_untrusted_error_detail_is_not_reflected(asgi_server):
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    app=FastAPI()
    @app.get('/v1/snapshot')
    async def refuse():
        return JSONResponse(status_code=409,content={'detail':{'code':'warmup_failed',
            'reason':'secret-from-operator','message':'private-model-output'}})
    base=asgi_server(app)
    remote=HTTPHookAdapter(base,'test-only-token',NetworkPolicy([base],['127.0.0.1']))
    with pytest.raises(Refusal) as failure:
        await remote.snapshot()
    assert 'secret-from-operator' not in str(failure.value)
    assert 'private-model-output' not in str(failure.value)
