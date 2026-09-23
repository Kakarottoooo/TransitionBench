"""Fabricated metadata fixtures test qualification, never constitute GPU evidence."""
import json
import pytest
from transitionbench.calibration import qualify_calibration
from transitionbench.evidence import export_bundle
from transitionbench.schemas import ExperimentSpec, WorkloadSpec, RunManifest, RequestEvent


def bundle(root, name, config='A', seed=11, split='calibration', kind='short', origin='measured-controlled', rate=2):
    path = root/name
    spec = ExperimentSpec(mode='CONTROLLED_ROLLOUT' if origin == 'measured-controlled' else 'SIMULATION',
        endpoint_id='fixture',
        policy='FixedHysteresis' if split=='tuning' else 'StaticBest',
        workload=WorkloadSpec(kind=kind, seed=seed, split=split, rate_rps=rate, injection_s=2), observation_s=3, drain_s=1)
    rows = [RequestEvent(request_id=f'{split}-{seed}-{i}', workload_class='short' if kind == 'short' else 'long',
        scheduled_s=i/rate, dispatch_s=i/rate, first_content_s=i/rate+.1, final_content_s=i/rate+.2,
        completed_s=i/rate+.2, termination='complete', quality_valid=True, output_chars=10, origin=origin) for i in range(int(2*rate))]
    manifest = RunManifest(run_id=name, mode=spec.mode, origin=origin, experiment=spec,
        offered_ids=[r.request_id for r in rows], created_at_unix_s=0,
        versions={k:'TEST FIXTURE ONLY' for k in ('model_revision','tokenizer_revision','engine','driver','code_revision')},
        hardware=[{'device_uuid':f'GPU-fixture-{i}', 'device_model':'fixture', 'process_id':i+1,
                   'resource_evidence':'independently-observed', 'config_id':config} for i in range(2)],
        configurations={'A':{'tokens':2048}, 'B':{'tokens':4096}},
        resource_intervals=[{'start_s':0, 'end_s':3, 'reserved_gpus':2}],
        policy_parameters={'endpoint_contract':{'model':'fixture','temperature':0,'seed':0,'streaming':False},
            'hysteresis':{'advantage_fraction':.1,'persistence_s':2,'dwell_s':10}})
    export_bundle(path, manifest, rows, [], [])
    return path.name


def study(root):
    trials = []
    for config in ('A','B'):
        for kind in ('short','long-prefix'):
            for seed in (11,12,13):
                trials.append({'role':'capacity','config_id':config,
                    'bundle':bundle(root,f'capacity-{config}-{kind}-{seed}',config,seed,kind=kind)})
        for seed in (11,12,13):
            trials.append({'role':'fixed','config_id':config,
                'bundle':bundle(root,f'fixed-{config}-{seed}',config,seed)})
    for seed in (21,22):
        trials.append({'role':'tuning','hysteresis':{'advantage_fraction':.1,'persistence_s':2,'dwell_s':10},
            'bundle':bundle(root,f'tuning-{seed}',seed=seed,split='tuning')})
    path=root/'calibration-study.json'
    path.write_text(json.dumps({'id':'fixture-calibration','trials':trials}))
    return path


def test_calibration_recomputes_strong_static_and_keeps_missing_cost_unknown(tmp_path):
    result=qualify_calibration(study(tmp_path))
    assert result['static_best']=='A'
    assert result['rates']['A']['short']==pytest.approx(2)  # Four arrivals / two injection seconds, not drain padding.
    assert not result['research_readiness']['ready']
    assert result['costs']=={}  # No invented transition estimates.
    assert result['calibration_seeds']==[11,12,13]
    assert result['tuning_seeds']==[21,22]
    assert result['certified'] is False


def test_load_sweep_qualification_uses_target_load_not_mean_of_different_loads(tmp_path):
    path = study(tmp_path)
    data = json.loads(path.read_text())
    for config in ('A', 'B'):
        for kind in ('short', 'long-prefix'):
            for seed in (11, 12, 13):
                for rate in (1, 4):
                    data['trials'].append({'role': 'capacity', 'config_id': config,
                        'bundle': bundle(tmp_path, f'sweep-{config}-{kind}-{seed}-{rate}',
                                         config, seed, kind=kind, rate=rate)})
    path.write_text(json.dumps(data))
    result = qualify_calibration(path)
    assert result['rates'] == {'A': {'short': 2, 'long': 2}, 'B': {'short': 2, 'long': 2}}
    assert len(result['capacity_diagnostic']['curves']['A:short']['points']) == 3
    assert not result['research_readiness']['ready']  # All loads pass: only a lower bound.


@pytest.mark.parametrize('change',['synthetic','tamper','test_leak','duplicate','missing_config'])
def test_calibration_rejects_unqualified_evidence(tmp_path,change):
    path=study(tmp_path)
    data=json.loads(path.read_text())
    if change=='synthetic':
        data['trials'][0]['bundle']=bundle(tmp_path,'synthetic',origin='synthetic')
    elif change=='tamper':
        (tmp_path/data['trials'][0]['bundle']/'requests.jsonl').write_text('')
    elif change=='test_leak':
        data['trials'][0]['bundle']=bundle(tmp_path,'holdout',split='test',seed=101)
    elif change=='duplicate':data['trials'].append(data['trials'][0])
    else:data['trials']=[t for t in data['trials'] if t.get('config_id')!='B']
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError):qualify_calibration(path)


def test_transition_deficit_uses_candidate_reference_and_observed_queue(tmp_path):
    path=study(tmp_path)
    data=json.loads(path.read_text())
    data['transition_pairs']=[]
    for seed in (11,12,13):
        reference=bundle(tmp_path,f'reference-{seed}',config='B',seed=seed)
        moving=bundle(tmp_path,f'moving-{seed}',config='A',seed=seed)
        root=tmp_path/moving
        manifest=RunManifest.model_validate_json((root/'manifest.json').read_text())
        rows=[RequestEvent.model_validate_json(line) for line in (root/'requests.jsonl').read_text().splitlines()]
        rows[0].quality_valid=False
        events=[{'worker_id':'0','state':'DRAINING_ONE_WORKER','at_s':0},
                {'worker_id':'0','state':'COMPLETE','at_s':.1}]
        moving += '-with-events'
        export_bundle(tmp_path/moving,manifest,rows,events,[])
        data['transition_pairs'].append({'source':'A','target':'B','reference_bundle':reference,'transition_bundle':moving})
    path.write_text(json.dumps(data))
    result=qualify_calibration(path)
    cost=result['costs']['A>B:short']
    assert cost['reference']=='candidate-steady'
    assert cost['by_queue']['idle']=={'mean':1, 'spread':0, 'samples':[1,1,1]}
    assert 'backlogged' not in cost['by_queue']


def test_tuning_parameters_must_match_run_evidence(tmp_path):
    path=study(tmp_path)
    data=json.loads(path.read_text())
    data['trials'][-1]['hysteresis']['dwell_s']=99
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError,match='parameters'):qualify_calibration(path)


async def test_service_rechecks_calibration_pins_and_holdout_before_traffic(tmp_path):
    from transitionbench.service import Service
    path=study(tmp_path)
    qualified=qualify_calibration(path)
    entry={'source':str(path),'source_sha256':qualified['source_sha256'],
        'qualification_hash':qualified['qualification_hash']}
    config={'calibrations':{'fixture-calibration':entry},'configurations':qualified['scope']['configurations'],
        'endpoints':[{'spec':{'id':'fixture','base_url':'http://127.0.0.1:1/v1','model':'fixture',
            'temperature':0,'seed':0,'streaming':False}}]}
    service=Service(tmp_path/'service',config)
    spec=ExperimentSpec(mode='CONTROLLED_ROLLOUT',endpoint_id='fixture',calibration_id='fixture-calibration',
        workload=WorkloadSpec(kind='short',seed=101))
    assert service.measured_calibration(spec)['static_best']=='A'
    changed_prefix=spec.model_copy(update={'workload':spec.workload.model_copy(update={'long_prefix_mode':'unique'})})
    with pytest.raises(ValueError,match='contract differs'):
        service.measured_calibration(changed_prefix)
    leaked=spec.model_copy(update={'workload':spec.workload.model_copy(update={'seed':11})})
    with pytest.raises(ValueError,match='held-out'):service.measured_calibration(leaked)
    entry['qualification_hash']='changed'
    with pytest.raises(ValueError,match='changed'):service.measured_calibration(spec)
    await service.close()
