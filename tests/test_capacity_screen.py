"""Screen lifecycle contracts; fabricated GPU metadata is not hardware evidence."""
import json
import subprocess
import sys
from pathlib import Path
import pytest
from transitionbench.collection import CollectionSpec, CalibrationCollector, collection_plan
from transitionbench.preparation import ScreenBudget, prepare_capacity_screen
from transitionbench.research import ResearchGateError
from test_collection import setup
from test_endpoint import local_server


def screen(value):
    data = value.model_dump(mode='json')
    data.update(purpose='capacity-screen', calibration_seeds=[6001], transition_kinds=[],
                capacity_rates_rps=[10, 20, 30], initial_cache_policy='fresh-workers')
    data['warmup']['complete_probe_sequence'] = True
    return CollectionSpec.model_validate(data)


async def test_bounded_screen_cannot_be_promoted_to_qualification(tmp_path, local_server):
    value, service, _, _ = setup(tmp_path, local_server)
    try:
        data = screen(value).model_dump(mode='json')
        data.update(purpose='qualification',screen_metric='bounded-system',calibration_seeds=[6001,6002,6003])
        with pytest.raises(ValueError,match='exploratory only'):
            CollectionSpec.model_validate(data)
        data.update(purpose='capacity-screen',screen_metric='gpu-service',calibration_seeds=[6001],capacity_rates_rps=[20])
        with pytest.raises(ValueError,match='3-5'):
            CollectionSpec.model_validate(data)
        data['screen_quality_policy'] = 'score-invalid-as-zero'
        with pytest.raises(ValueError,match='explicit bounded-system'):
            CollectionSpec.model_validate(data)
        data.update(purpose='qualification', screen_metric='bounded-system', calibration_seeds=[6001,6002,6003])
        with pytest.raises(ValueError,match='explicit bounded-system'):
            CollectionSpec.model_validate(data)
    finally:
        await service.close()


@pytest.mark.parametrize('bounded,score_quality_failures', [(False, False), (True, False), (True, True)])
async def test_screen_only_acquires_twelve_pairs_and_never_registers_calibration(tmp_path, local_server, bounded, score_quality_failures, monkeypatch):
    value, service, adapter, _ = setup(tmp_path, local_server)
    value = screen(value)
    if bounded:
        data = value.model_dump(mode='json')
        data.update(screen_metric='bounded-system', capacity_rates_rps=[20])
        if score_quality_failures:
            data['screen_quality_policy'] = 'score-invalid-as-zero'
        value = CollectionSpec.model_validate(data)
    if score_quality_failures:
        from dataclasses import replace
        from transitionbench.endpoint import EndpointClient
        measure = EndpointClient.measure
        async def wrong_output(client, item, *args, **kwargs):
            if item.request_id.startswith('calibration-') and item.request_id.endswith('-0'):
                item = replace(item, prompt='Return exactly WRONG and nothing else.')
            return await measure(client, item, *args, **kwargs)
        monkeypatch.setattr(EndpointClient, 'measure', wrong_output)
    # The loopback fixture holds responses open; give it enough client slots so
    # this success-path test does not intentionally exercise client shedding.
    value = value.model_copy(update={'experiment': value.experiment.model_copy(update={
        'budget': value.experiment.budget.model_copy(update={'max_concurrency': 2 if bounded and not score_quality_failures else 16})})})
    service.config['endpoints'][0]['budget']['max_concurrency'] = value.experiment.budget.max_concurrency
    original = adapter.operation
    async def operation(op, worker, payload, key):
        result = await original(op, worker, payload, key)
        if op == 'apply': adapter.workers[worker].process_id += 1000
        return result
    adapter.operation = operation
    plan = collection_plan(value, service.config, service.code_revision)
    expected = 4 if bounded else 12
    assert len(plan['order']) == expected
    assert all(t['role'] == 'capacity' for t in plan['order'])
    assert all(t['rate_rps'] == 20 for t in plan['order'][:4])
    for a,b in zip(plan['order'][::2], plan['order'][1::2]):
        assert a['kind'] == b['kind'] and a['rate_rps'] == b['rate_rps']
        assert {a['initial'], b['initial']} == {'A', 'B'}
    root = tmp_path/'campaign'
    output = root/'calibration'
    collector = CalibrationCollector(service, adapter, value, output, plan)
    try:
        result = await collector.run(plan['plan_hash'])
        assert result['state'] == 'SCREEN_COMPLETE'
        assert result['completed_trials'] == expected
        assert not result['qualified'] and not result['research_ready']
        assert not result['automatic_continuation']
        assert not (output/'registration.json').exists()
        assert not (output/'qualified.json').exists()
        assert len(list((output/'journals').glob('*/request-journal.jsonl'))) == expected
        if bounded and not score_quality_failures:
            assert any(m['capacity_point']['client_dropped'] > 0 for m in collector.measurements)
        (root/'frozen-study.json').write_text(json.dumps({'collection':value.model_dump(mode='json'),
                                                       'code_revision':service.code_revision}))
        (root/'status.json').write_text(json.dumps({'state':'SCREEN_COMPLETE'}))
        (root/'study-plan.json').write_text(json.dumps({'test_order':[]}))
        verifier = Path(__file__).resolve().parents[1]/'scripts/verify_gpu_study.py'
        checked = subprocess.run([sys.executable,str(verifier),str(root)],capture_output=True,text=True)
        assert checked.returncode == 0, checked.stdout + checked.stderr
        receipt = json.loads((root/'independent-gpu-verification.json').read_text())
        assert receipt['screen_protocol_complete'] and not receipt['full_protocol_complete']
        assert receipt['expected_held_out_trials'] == 0
        if bounded:
            assert receipt['screen_metric'] == 'bounded-system'
        if bounded and not score_quality_failures:
            assert any(r['terminations'].get('client_drop', 0) for r in receipt['rows'])
        if score_quality_failures:
            assert receipt['screen_quality_policy'] == 'score-invalid-as-zero'
            for row in receipt['rows']:
                assert row['quality_valid_requests'] == row['terminations']['complete'] - 1
                assert row['metrics']['qualified'] <= row['quality_valid_requests']
                assert row['metrics']['offered'] == row['terminations']['complete']
            # Even an internally consistent bundle cannot silently change the
            # frozen prefix treatment at independent campaign verification.
            frozen = json.loads((root/'frozen-study.json').read_text())
            frozen['collection']['experiment']['workload']['long_prefix_mode'] = 'unique'
            (root/'frozen-study.json').write_text(json.dumps(frozen))
            rejected = subprocess.run([sys.executable,str(verifier),str(root)],capture_output=True,text=True)
            assert rejected.returncode == 1
            receipt = json.loads((root/'independent-gpu-verification.json').read_text())
            assert any('differs from frozen grid' in str(e) for e in receipt['errors'])
    finally:
        await service.close()


@pytest.mark.parametrize('bounded,complete,quality,dropped,quality_policy',[(False,400,399,0,'require-all-valid'),
    (False,399,399,1,'require-all-valid'),(True,399,398,1,'require-all-valid'),
    (True,398,398,1,'require-all-valid'),(True,398,398,1,'score-invalid-as-zero')])
async def test_screen_stops_at_first_contaminated_point_and_keeps_evidence(tmp_path, local_server,
                                                                       bounded,complete,quality,dropped,quality_policy):
    value, service, adapter, _ = setup(tmp_path, local_server)
    value = screen(value)
    if bounded:
        value = CollectionSpec.model_validate({**value.model_dump(mode='json'),'screen_metric':'bounded-system',
                                              'screen_quality_policy':quality_policy})
    plan = collection_plan(value, service.config, service.code_revision)
    collector = CalibrationCollector(service, adapter, value, tmp_path/'screen', plan)
    async def contaminated(number, trial):
        assert number == 0
        collector.measurements.append({'trial': trial, 'capacity_point': dict(
            config=trial['initial'], kind='short', seed=6001, rate_rps=20,
            injection_s=20, observation_s=30, offered=400, qualified=399,
            complete=complete, quality_valid=quality, client_dropped=dropped, run_id='fabricated-contract')})
    collector.trial = contaminated
    try:
        with pytest.raises(ResearchGateError, match='confound'):
            await collector.run(plan['plan_hash'])
        status = json.loads((tmp_path/'screen/status.json').read_text())
        assert status['completed_trials'] == 1 and status['state'] == 'INCONCLUSIVE'
        assert (tmp_path/'screen/capacity-diagnostic.json').exists()
        assert not adapter.calls
    finally:
        await service.close()


async def test_screen_budget_includes_setup_reset_warmup_export_and_stop(tmp_path, local_server):
    value, service, _, _ = setup(tmp_path, local_server)
    data = screen(value).model_dump(mode='json')
    data['experiment'].update(observation_s=30, drain_s=10)
    data['experiment']['workload']['injection_s'] = 20
    data['experiment']['budget'].update(max_requests=1000, max_total_tokens=3000000,
        max_duration_s=150, max_reserved_gpu_seconds=300)
    data.update(max_wall_s=6000, max_requests=100000, max_total_tokens=100000000)
    data['warmup']['max_duration_s'] = 45
    value = CollectionSpec.model_validate(data)
    plan = collection_plan(value, service.config, service.code_revision)
    try:
        budget = ScreenBudget(machine_hourly_usd=1.464074074, spending_limit_usd=3)
        result = prepare_capacity_screen(value, plan, budget)
        assert result['status'] == 'READY_FOR_HOST_PREFLIGHT'
        assert result['collection_maximum_s'] == 4680
        assert result['maximum_billed_s_including_stop'] == 5790
        assert result['projected_usd'] == pytest.approx(2.854719)
        assert result['test_order'] == [] and not result['cloud_authorization']
        assert prepare_capacity_screen(value, plan, budget.model_copy(update={'spending_limit_usd': 2}))['status'] == 'BUDGET_REFUSED'
        data['purpose'] = 'qualification'
        with pytest.raises(ValueError, match='three'):
            CollectionSpec.model_validate(data)
    finally:
        await service.close()
