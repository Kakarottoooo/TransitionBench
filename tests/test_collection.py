"""Real HTTP acquisition with explicitly fabricated GPU metadata for contract tests.

These tests exercise qualification plumbing; they are never hardware evidence.
"""
import asyncio
import json
import pytest
from transitionbench.collection import CollectionSpec, CalibrationCollector, collection_plan
from transitionbench.schemas import ExperimentSpec, WorkloadSpec, ResourceBudget, WarmupSpec
from transitionbench.service import Service
from transitionbench.calibration import qualify_calibration
from test_endpoint import local_server
from test_rollout import FixtureAdapter


def setup(tmp_path, base, *, costs=True):
    value = CollectionSpec(id='contract-fixture', experiment=ExperimentSpec(mode='CONTROLLED_ROLLOUT', endpoint_id='fixture',
        workload=WorkloadSpec(kind='short', injection_s=.15, rate_rps=20), observation_s=1, drain_s=.85,
        # This fixture checks acquisition contracts, not sender performance.
        # Windows/WSL filesystem and SQLite setup can exceed 300 ms.
        max_dispatch_lag_s=1,
        budget=ResourceBudget(max_duration_s=20, max_reserved_gpu_seconds=40, max_requests=60,
                              max_total_tokens=100000, max_concurrency=4)),
        warmup=WarmupSpec(max_requests=12, max_duration_s=2), transition_at_s=.04,
        transition_kinds=['short'] if costs else [], max_wall_s=2000, max_requests=10000, max_total_tokens=10000000)
    budget=value.experiment.budget.model_dump(mode='json')
    config={'allowed_origins':[base], 'private_hosts':['127.0.0.1'],
        'configurations':{'A':{'tokens':2048},'B':{'tokens':4096}},
        'hook':{'versions':{k:'TEST FIXTURE ONLY' for k in ('engine','driver','model_revision','tokenizer_revision')}},
        'endpoints':[{'spec':{'id':'fixture','base_url':base+'/v1','model':'local-arithmetic',
            'temperature':0,'seed':0,'streaming':True,'supported_parameters':['max_tokens','stream','temperature','seed']}, 'budget':budget}]}
    service=Service(tmp_path/'service',config)
    adapter=FixtureAdapter()
    for i,worker in adapter.workers.items():
        worker.device_uuid='GPU-TEST-FIXTURE-'+i
        worker.device_model='TEST FIXTURE ONLY'
        worker.process_id=int(i)+1
        worker.resource_evidence='independently-observed'
    plan=collection_plan(value, config, service.code_revision)
    return value,service,adapter,plan


@pytest.mark.parametrize('fresh', [False, True])
async def test_acquires_and_recomputes_full_calibration_with_real_http(tmp_path,local_server,monkeypatch,fresh):
    value,service,adapter,plan=setup(tmp_path,local_server)
    if fresh:
        value=value.model_copy(update={'initial_cache_policy':'fresh-workers',
            'warmup':value.warmup.model_copy(update={'complete_probe_sequence':True})})
        original=adapter.operation
        async def operation(op,worker,payload,key):
            result=await original(op,worker,payload,key)
            if op=='apply': adapter.workers[worker].process_id+=1000
            return result
        adapter.operation=operation
        plan=collection_plan(value,service.config,service.code_revision)
    collector=CalibrationCollector(service,adapter,value,tmp_path/'evidence',plan)
    try:
        result=await collector.run(plan['plan_hash'])
        assert result['state']=='QUALIFIED'
        assert result['reserved']['requests']<=plan['maximum_reservations']['requests']
        qualified=qualify_calibration(tmp_path/'evidence/index.json')
        assert len(qualified['evidence_ids'])==34
        assert qualified['static_best'] in ('A','B')
        samples=qualified['costs']['A>B:short']['by_queue']['idle']['samples']
        assert len(samples)==3 and all(loss>=0 for loss in samples)
        from transitionbench.schemas import ExperimentSpec
        registration=json.loads((tmp_path/'evidence/registration.json').read_text())
        service.config.update(registration)
        heldout=value.experiment.model_copy(update={'workload':value.experiment.workload.model_copy(update={'split':'test','seed':101}),
                                                   'calibration_id':value.id})
        before=await collector.prepare(qualified['static_best'])
        child=service.plans.create('held-out','B' if qualified['static_best']=='A' else 'A',before,
            heldout.budget,warmup=value.warmup,initial_condition=collector.initial_condition)
        heldout=heldout.model_copy(update={'plan_id':child['id']})
        assert service.measured_calibration(heldout)['id']==value.id
        other=service.plans.create('held-out','B',await adapter.snapshot(),heldout.budget,
                                  warmup=value.warmup.model_copy(update={'absolute_tolerance_s':.02}))
        with pytest.raises(ValueError,match='warmup differs'):
            service.measured_calibration(heldout.model_copy(update={'plan_id':other['id']}))
        assert set(qualified['calibration_seeds']).isdisjoint(value.test_seeds)
        assert len(list((tmp_path/'evidence/journals').glob('*/request-journal.jsonl')))==34
        # A completed response stream must not release a physical trial's
        # reserved observation interval before the next policy may start.
        import time
        monkeypatch.setattr(service,'hook_adapter',lambda:adapter)
        service.plans.approve(child['id'],child['hash'])
        heldout=heldout.model_copy(update={'observation_s':1.5,'drain_s':1.35})
        started=time.monotonic()
        job=await service.submit(heldout,'heldout-observation-contract')
        await service.tasks[job['id']]
        assert service.get(job['id'])['state']=='SUCCEEDED'
        assert time.monotonic()-started>=heldout.observation_s
    finally:await service.close()


async def test_rehearsal_does_not_become_gpu_calibration(tmp_path,local_server):
    value,service,adapter,plan=setup(tmp_path,local_server,costs=False)
    collector=CalibrationCollector(service,adapter,value,tmp_path/'evidence',plan,rehearsal=True)
    try:
        result=await collector.run(plan['plan_hash'])
        assert result['state']=='REHEARSAL_COMPLETE' and not result['qualified']
        assert not (tmp_path/'evidence/qualified.json').exists()
        with pytest.raises(ValueError,match='measured-controlled'):
            qualify_calibration(tmp_path/'evidence/index.json')
    finally:await service.close()


async def test_collector_retains_estimates_without_historical_request_arrays(tmp_path,local_server):
    from transitionbench.verifier import verify_bundle
    value,service,adapter,plan=setup(tmp_path,local_server)
    collector=CalibrationCollector(service,adapter,value,tmp_path/'evidence',plan)
    collector.output.mkdir()
    try:
        await collector.trial(0,plan['order'][0])
        retained=collector.measurements[0]
        checked=verify_bundle(collector.output/retained['bundle'])
        assert checked['integrity_valid'] and checked['experiment_valid']
        full=checked['recomputed']
        assert full['latencies']
        assert retained['metrics']=={key:full[key] for key in ('goodput_rps','qualified')}
    finally:await service.close()


async def test_transition_pair_is_checked_before_capacity_spend(tmp_path,local_server):
    value,service,adapter,plan=setup(tmp_path,local_server)
    try:
        first=plan['order'][:2]
        assert {r['role'] for r in first}=={'reference','transition'}
        assert first[0]['pair']==first[1]['pair']
        assert len(plan['order'])==34
        assert all(r['role']=='tuning' for r in plan['order'][-4:])
    finally:await service.close()


async def test_sweep_refuses_before_fixed_transition_or_tuning_operations(tmp_path, local_server):
    from transitionbench.collection import trial_spec
    from transitionbench.research import ResearchGateError
    value, service, adapter, _ = setup(tmp_path, local_server)
    data = value.model_dump(mode='json')
    data['capacity_rates_rps'] = [10, 20, 30]
    data.update(max_wall_s=4000, max_requests=20000, max_total_tokens=20000000)
    value = CollectionSpec.model_validate(data)
    plan = collection_plan(value, service.config, service.code_revision)
    collector = CalibrationCollector(service, adapter, value, tmp_path/'sweep', plan)
    assert len(plan['order']) == 58
    assert {t['rate_rps'] for t in plan['order'][:36]} == {10, 20, 30}
    assert all(t['role'] == 'capacity' for t in plan['order'][:36])
    assert trial_spec(value, plan['order'][0]).workload.rate_rps == plan['order'][0]['rate_rps']
    async def acquired(number, trial):
        assert trial['role'] == 'capacity'  # No later operation may be reached.
        collector.measurements.append({'trial': trial, 'capacity_point': {
            'config': trial['initial'], 'kind': 'short' if trial['kind']=='short' else 'long',
            'seed': trial['seed'], 'rate_rps': trial['rate_rps'], 'injection_s': 30,
            'observation_s': 40, 'offered': 300, 'qualified': 300, 'complete': 300,
            'quality_valid': 300, 'run_id': str(number)}})
    collector.trial = acquired
    try:
        with pytest.raises(ResearchGateError):
            await collector.run(plan['plan_hash'])
        assert not adapter.calls
        status = json.loads((tmp_path/'sweep/status.json').read_text())
        assert status['state'] == 'INCONCLUSIVE' and status['completed_trials'] == 36
        assert not (tmp_path/'sweep/qualified.json').exists()
        assert not json.loads((tmp_path/'sweep/capacity-diagnostic.json').read_text())['ready']
    finally:
        await service.close()


@pytest.mark.parametrize('loads', [[10, 20], [20, 10, 30], [10, 10, 20], [10, 30, 40]])
async def test_capacity_sweep_requires_frozen_matched_target_load(tmp_path, local_server, loads):
    value, service, _, _ = setup(tmp_path, local_server)
    try:
        data = value.model_dump(mode='json')
        data['capacity_rates_rps'] = loads
        with pytest.raises(ValueError, match='Capacity sweep'):
            CollectionSpec.model_validate(data)
    finally:
        await service.close()


@pytest.mark.parametrize('change',['hash','budget','config','pending','hardware'])
async def test_refuses_before_mutation(tmp_path,local_server,change):
    value,service,adapter,plan=setup(tmp_path,local_server,costs=False)
    digest=plan['plan_hash']
    if change=='hash':digest='0'*64
    elif change=='budget':
        value=value.model_copy(update={'max_requests':1})
        plan=collection_plan(value,service.config,service.code_revision);digest=plan['plan_hash']
    elif change=='config':service.config['configurations']['A']['tokens']=1
    elif change=='pending':
        child=service.plans.create('another','B',await adapter.snapshot(),value.experiment.budget,warmup=value.warmup)
        service.plans.approve(child['id'],child['hash']);service.plans.acquire(child['id'])
    else:adapter.workers['0'].resource_evidence='unknown'
    collector=CalibrationCollector(service,adapter,value,tmp_path/'evidence',plan)
    try:
        with pytest.raises(ValueError):await collector.run(digest)
        assert not adapter.calls
    finally:await service.close()


async def test_uncertain_reset_stops_and_preserves_lease(tmp_path,local_server):
    value,service,adapter,plan=setup(tmp_path,local_server)
    original=adapter.operation
    async def uncertain(op,worker,payload,key):
        if op=='apply':raise TimeoutError('Injected uncertain apply')
        return await original(op,worker,payload,key)
    adapter.operation=uncertain
    for w in adapter.workers.values():w.config_id='B' if plan['order'][0]['initial']=='A' else 'A'
    collector=CalibrationCollector(service,adapter,value,tmp_path/'evidence',plan)
    try:
        with pytest.raises(ValueError,match='reconciliation'):await collector.run(plan['plan_hash'])
        status=json.loads((tmp_path/'evidence/status.json').read_text())
        assert status['state']=='FAILED' and status['completed_trials']==0
        with service.plans.connect() as db:assert db.execute('SELECT COUNT(*) FROM leases').fetchone()[0]==1
        assert not any(op=='rollback' for op,_ in adapter.calls)
        assert (tmp_path/'evidence/collection-journal.jsonl').stat().st_size>0
    finally:await service.close()


async def test_cancelled_acquisition_keeps_partial_request_journal(tmp_path,local_server):
    value,service,adapter,plan=setup(tmp_path,local_server,costs=False)
    collector=CalibrationCollector(service,adapter,value,tmp_path/'evidence',plan)
    original=service.measured_traffic
    async def cancel_after_start(*args,**kwargs):
        async def cancel():
            await asyncio.sleep(.03);collector.cancel.set()
        task=asyncio.create_task(cancel())
        try:return await original(*args,**kwargs)
        finally:await task
    service.measured_traffic=cancel_after_start
    try:
        with pytest.raises(ValueError):await collector.run(plan['plan_hash'])
        journals=list((tmp_path/'evidence/journals').glob('*/request-journal.jsonl'))
        assert len(journals)==1 and journals[0].stat().st_size>0
        assert not (tmp_path/'evidence/qualified.json').exists()
    finally:await service.close()


def test_collection_rejects_holdout_and_duplicate_candidates(tmp_path,local_server):
    value,service,adapter,plan=setup(tmp_path,local_server)
    data=value.model_dump(mode='json');data['test_seeds']=[11]
    with pytest.raises(ValueError,match='disjoint'):CollectionSpec.model_validate(data)
    data=value.model_dump(mode='json');data['candidates']*=2
    with pytest.raises(ValueError,match='Duplicate'):CollectionSpec.model_validate(data)


async def test_warmup_failure_retains_plan_and_stops_before_traffic(tmp_path,local_server):
    value,service,adapter,plan=setup(tmp_path,local_server,costs=False)
    for w in adapter.workers.values():w.config_id=plan['order'][0]['initial']
    original=adapter.operation
    async def invalid(op,worker,payload,key):
        if op=='warmup':raise ValueError('Injected invalid warmup output')
        return await original(op,worker,payload,key)
    adapter.operation=invalid
    collector=CalibrationCollector(service,adapter,value,tmp_path/'evidence',plan)
    try:
        with pytest.raises(ValueError,match='invalid warmup'):await collector.run(plan['plan_hash'])
        assert service.store.list()==[]
        with service.plans.connect() as db:assert db.execute('SELECT COUNT(*) FROM leases').fetchone()[0]==1
        assert json.loads((tmp_path/'evidence/status.json').read_text())['completed_trials']==0
    finally:await service.close()


async def test_fresh_cache_rebuilds_even_matching_configuration_on_every_trial(tmp_path, local_server):
    value, service, adapter, _ = setup(tmp_path, local_server, costs=False)
    value = value.model_copy(update={'initial_cache_policy': 'fresh-workers',
        'warmup': value.warmup.model_copy(update={'complete_probe_sequence':True})})
    original = adapter.operation
    async def operation(op, worker, payload, key):
        result = await original(op, worker, payload, key)
        if op == 'apply':
            adapter.workers[worker].process_id += 1000
        return result
    adapter.operation = operation
    (tmp_path/'evidence').mkdir()
    collector = CalibrationCollector(service, adapter, value, tmp_path/'evidence', {'plan_hash':'test'})
    try:
        first = await collector.prepare('A')
        second = await collector.prepare('A')
        assert all(s.generation == 1 and s.process_id >= 1000 for s in first)
        assert all(s.generation == 2 and s.process_id >= 2000 for s in second)
        assert sum(op == 'apply' for op, _ in adapter.calls) == 4
        assert collector.initial_condition['policy'] == 'fresh-workers'
    finally:
        await service.close()

async def test_fresh_reset_rejects_unchanged_process_and_retains_lease(tmp_path,local_server):
    value,service,adapter,_=setup(tmp_path,local_server,costs=False)
    value=value.model_copy(update={'initial_cache_policy':'fresh-workers',
        'warmup':value.warmup.model_copy(update={'complete_probe_sequence':True})})
    (tmp_path/'evidence').mkdir()
    collector=CalibrationCollector(service,adapter,value,tmp_path/'evidence',{'plan_hash':'test'})
    try:
        with pytest.raises(ValueError,match='new generation'):
            await collector.prepare('A')
        assert service.store.list()==[]
        with service.plans.connect() as db:
            assert db.execute('SELECT COUNT(*) FROM leases').fetchone()[0]==1
    finally:
        await service.close()
