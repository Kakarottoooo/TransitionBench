"""CPU contracts for the measured 71s cold start; not GPU performance evidence."""
from types import SimpleNamespace
import httpx
import pytest
from transitionbench import lab, rollout
from transitionbench.hook_server import OperationPayload
from transitionbench.schemas import ResourceBudget, WorkerSnapshot
from test_rollout import FixtureAdapter


async def test_plan_sends_extended_readiness_only(tmp_path):
    adapter = FixtureAdapter()
    received = []
    original = adapter.operation
    async def operation(name, worker, payload, key):
        received.append((name, payload['operation_timeout_s']))
        return await original(name, worker, payload, key)
    adapter.operation = operation
    store = rollout.PlanStore(tmp_path/'plans.db')
    plan = store.create('fixture','B',await adapter.snapshot(),
                        ResourceBudget(max_duration_s=420,max_reserved_gpu_seconds=840))
    store.approve(plan['id'],plan['hash'])
    assert (await rollout.execute_plan(store,plan['id'],adapter))['state'] == 'COMPLETE'
    assert all(timeout == (90 if name == 'readiness' else 60) for name,timeout in received)


@pytest.mark.parametrize('timeout,passes',[(60,False),(90,True)])
async def test_gpu_adapter_waits_for_71s_within_approved_deadline(monkeypatch,timeout,passes):
    clock = [0.0]
    monkeypatch.setattr(lab,'time',SimpleNamespace(monotonic=lambda:clock[0]))
    async def sleep(seconds): clock[0] += seconds
    monkeypatch.setattr(lab,'asyncio',SimpleNamespace(sleep=sleep))
    worker = WorkerSnapshot(worker_id='0',config_id='A',generation=1,ready=False,
                            accepting=False,in_flight=0,device_uuid='GPU-fixture',observed_at_unix_s=0)
    adapter = object.__new__(lab.DockerLabAdapter)
    adapter.workers = {'0':{}}
    async def snapshot():
        worker.ready = clock[0] >= 71
        return [worker]
    adapter.snapshot = snapshot
    pending = adapter.operation('readiness','0',{'config_id':'A','expected_generation':1,
                                  'operation_timeout_s':timeout},'fixture')
    if passes:
        assert (await pending)['ready']
    else:
        with pytest.raises(TimeoutError): await pending


async def test_http_and_payload_preserve_90s_readiness(monkeypatch):
    payload = OperationPayload(config_id='A',expected_generation=1,plan_id='a'*32,
        plan_hash='a'*64,expires_at_unix_s=9999999999,drain_timeout_s=10,max_tokens=32,
        operation_timeout_s=90)
    timeouts = []
    client_class = httpx.AsyncClient
    def client(**kwargs):
        timeouts.append(kwargs['timeout'])
        kwargs['transport'] = httpx.MockTransport(lambda request:httpx.Response(200,json={}))
        return client_class(**kwargs)
    monkeypatch.setattr(rollout.httpx,'AsyncClient',client)
    network = SimpleNamespace(resolve=lambda url:(url,'127.0.0.1','127.0.0.1'))
    adapter = rollout.HTTPHookAdapter('http://127.0.0.1:8770','fixture-token',network)
    await adapter.operation('readiness','0',payload.model_dump(),'fixture')
    assert timeouts == [95]
