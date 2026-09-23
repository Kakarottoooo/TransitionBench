import asyncio
import json
import time
import pytest
from transitionbench.rollout import PlanStore,execute_plan
from transitionbench.schemas import ResourceBudget,ExperimentSpec
from transitionbench.store import JobStore
from test_rollout import FixtureAdapter


async def prepare(tmp_path,adapter):
    store=PlanStore(tmp_path/'plans.db')
    plan=store.create('fixture','B',await adapter.snapshot(),ResourceBudget())
    store.approve(plan['id'],plan['hash'])
    return store,plan


async def test_generation_change_after_approval_aborts_before_mutation(tmp_path):
    adapter=FixtureAdapter();store,plan=await prepare(tmp_path,adapter)
    adapter.workers['0'].generation+=1
    result=await execute_plan(store,plan['id'],adapter)
    assert result['state']=='ABORTED'
    assert not any(op in ('apply','rollback') for op,_ in adapter.calls)


async def test_unobserved_apply_acknowledgment_is_not_success(tmp_path):
    class AckOnly(FixtureAdapter):
        async def operation(self,op,worker,payload,key):
            if op=='apply':return {'accepted':True}
            return await super().operation(op,worker,payload,key)
    adapter=AckOnly();store,plan=await prepare(tmp_path,adapter)
    assert (await execute_plan(store,plan['id'],adapter))['state']=='ABORTED'


async def test_drain_timeout_does_not_force_terminate(tmp_path):
    class DrainFailure(FixtureAdapter):
        async def operation(self,op,worker,payload,key):
            if op=='drain':raise TimeoutError('Drain deadline')
            return await super().operation(op,worker,payload,key)
    adapter=DrainFailure();store,plan=await prepare(tmp_path,adapter)
    assert (await execute_plan(store,plan['id'],adapter))['state']=='ROLLBACK_PENDING'
    assert not any(op in ('apply','rollback') for op,_ in adapter.calls)


async def test_interrupted_rollout_is_not_replayed_and_lease_remains(tmp_path):
    adapter=FixtureAdapter();store,plan=await prepare(tmp_path,adapter)
    store.acquire(plan['id']);store.event(plan['id'],'RECONFIGURING','0')
    store.recover_interrupted()
    assert store.get(plan['id'])['state']=='ROLLBACK_PENDING'
    with pytest.raises(ValueError,match='recovery'):
        await execute_plan(store,plan['id'],adapter)
    assert adapter.calls==[]


async def test_same_physical_device_refused(tmp_path):
    adapter=FixtureAdapter();adapter.workers['1'].device_uuid=adapter.workers['0'].device_uuid
    with pytest.raises(ValueError,match='distinct'):
        await prepare(tmp_path,adapter)


def test_crashed_jobs_are_marked_interrupted(tmp_path):
    store=JobStore(tmp_path)
    run_id,_=store.create(ExperimentSpec(),'one')
    store.update(run_id,'RUNNING')
    store.acquire_owner()
    try:
        assert store.get(run_id)['state']=='INTERRUPTED'
        assert store.events(run_id)[-1]['state']=='INTERRUPTED'
        other=JobStore(tmp_path)
        with pytest.raises(OSError):other.acquire_owner()
        other.release_owner()
    finally:store.release_owner()


async def test_expired_approval_and_hash_change_refused(tmp_path):
    adapter=FixtureAdapter();store,plan=await prepare(tmp_path,adapter)
    with pytest.raises(ValueError):store.approve(plan['id'],'f'*64)
    with store.connect() as db:
        row=db.execute('SELECT body FROM plans WHERE id=?',(plan['id'],)).fetchone()
        body=json.loads(row[0]);body['config_id']='changed'
        db.execute('UPDATE plans SET body=? WHERE id=?',(json.dumps(body),plan['id']))
    with pytest.raises(ValueError,match='approval'):await execute_plan(store,plan['id'],adapter)


@pytest.mark.gpu
def test_real_two_gpu_managed_experiment_requires_resource_authority():
    pytest.skip('BLOCKED: one observed GPU; Docker daemon unavailable; no authorized two-GPU host')


@pytest.mark.live_provider
def test_real_wafer_smoke_requires_task_authorization():
    pytest.skip('BLOCKED: no task-authorized Wafer credentials and provider budget supplied')
