import pytest
from transitionbench.rollout import PlanStore, execute_plan
from transitionbench.schemas import ResourceBudget, WarmupSpec
from test_rollout import FixtureAdapter


async def test_exact_plan_reserves_forward_and_recovery_warmup(tmp_path):
    store, adapter = PlanStore(tmp_path/'plans.db'), FixtureAdapter()
    warmup = WarmupSpec(max_requests=24)
    with pytest.raises(ValueError, match='warmup'):
        store.create('fixture', 'B', await adapter.snapshot(), ResourceBudget(max_requests=95), warmup=warmup)
    plan = store.create('fixture', 'B', await adapter.snapshot(), ResourceBudget(), warmup=warmup)
    assert plan['warmup']['max_requests'] == 24
    assert plan['warmup_reservation']['max_requests'] == 96


async def test_uncertain_apply_retains_lease_without_automatic_recovery(tmp_path):
    class UncertainAdapter(FixtureAdapter):
        async def operation(self, operation, worker_id, payload, key):
            result = await super().operation(operation, worker_id, payload, key)
            if operation == 'apply':
                raise TimeoutError('Remote operation may still be running')
            return result
    store, adapter = PlanStore(tmp_path/'plans.db'), UncertainAdapter()
    plan = store.create('fixture', 'B', await adapter.snapshot(), ResourceBudget())
    store.approve(plan['id'], plan['hash'])
    result = await execute_plan(store, plan['id'], adapter)
    assert result['state'] == 'ROLLBACK_PENDING'
    assert not any(operation == 'rollback' for operation, _ in adapter.calls)
    assert not any(worker == '1' and operation == 'drain' for operation, worker in adapter.calls)
    # A new plan cannot silently take ownership while an operation is uncertain.
    adapter.workers['0'].accepting = True
    other = store.create('fixture', 'A', await adapter.snapshot(), ResourceBudget())
    store.approve(other['id'], other['hash'])
    with pytest.raises(ValueError, match='lease'):
        await execute_plan(store, other['id'], adapter)


def test_preparation_freezes_matched_order_and_refuses_unfunded_envelope():
    from transitionbench.preparation import prepare_study, StudyPreparation
    spec = StudyPreparation(machine_hourly_usd=2, spending_limit_usd=10)
    result = prepare_study(spec)
    assert result['status']=='READY_FOR_HOST_PREFLIGHT'
    assert len(result['test_order'])==20
    assert all(sum(r['seed']==seed for r in result['test_order'])==4 for seed in spec.test_seeds)
    assert prepare_study(spec)['protocol_hash']==result['protocol_hash']
    assert result['cloud_authorization'] is False
    refused=prepare_study(spec.model_copy(update={'spending_limit_usd':.1}))
    assert refused['status']=='BUDGET_REFUSED'
    assert refused['projected_usd'] > .1
    with pytest.raises(ValueError):
        prepare_study(spec.model_copy(update={'test_seeds':[11,102,103]}))


async def test_cancelled_measured_traffic_flushes_all_offered_records(asgi_server,tmp_path):
    import asyncio
    import json
    from transitionbench.service import Service
    from transitionbench.test_server import app
    from transitionbench.endpoint import EndpointClient, NetworkPolicy
    from transitionbench.schemas import EndpointSpec, ExperimentSpec, WorkloadSpec
    base=asgi_server(app)
    service=Service(tmp_path/'data')
    spec=ExperimentSpec(mode='LIVE_ENDPOINT',workload=WorkloadSpec(kind='short',rate_rps=10,injection_s=.2),
        observation_s=1.2,drain_s=1)
    run_id,_=service.store.create(spec,'cancelled-rehearsal')
    client=EndpointClient(EndpointSpec(id='fixture',base_url=base+'/v1',model='local-arithmetic'),NetworkPolicy([base],['127.0.0.1']))
    cancel=asyncio.Event();cancel.set()
    rows,validity=await service.measured_traffic(run_id,spec,client,cancel,skip_discovery=True)
    path=tmp_path/'data'/'runs'/run_id/'request-journal.jsonl'
    saved=[json.loads(line) for line in path.read_text().splitlines()]
    assert len(saved)==len(rows)==2 and not validity['valid']
    assert all(row['termination']=='cancelled' for row in saved)
    await service.close()
