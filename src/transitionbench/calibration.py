"""Offline qualification from independently recomputed, operator-owned bundles.

Metadata consistency is not hardware authenticity. No inference or GPU operation
is performed here. Missing transition buckets remain unavailable to StateAware.
"""
import hashlib
import json
import statistics
from pathlib import Path
from typing import Literal
from pydantic import Field
from .schemas import Record
from .verifier import verify_bundle
from .rollout import stable_hash
from .research import capacity_diagnostic, research_gate


class Hysteresis(Record):
    advantage_fraction: float = Field(ge=0, le=1)
    persistence_s: float = Field(ge=0, le=3600)
    dwell_s: float = Field(ge=0, le=3600)


class CalibrationTrial(Record):
    role: Literal['capacity', 'fixed', 'tuning']
    bundle: str
    config_id: Literal['A', 'B'] | None = None
    hysteresis: Hysteresis | None = None


class CalibrationStudy(Record):
    id: str = Field(pattern=r'^[A-Za-z0-9_-]{1,80}$')
    trials: list[CalibrationTrial] = Field(min_length=1, max_length=200)
    transition_pairs: list['TransitionPair'] = Field(default_factory=list, max_length=96)


class TransitionPair(Record):
    source: Literal['A', 'B']
    target: Literal['A', 'B']
    reference_bundle: str
    transition_bundle: str


def _scope(manifest):
    spec = manifest['experiment']
    endpoint = manifest['policy_parameters'].get('endpoint_contract', {})
    if not endpoint.get('model') or endpoint.get('temperature') != 0 or endpoint.get('seed') is None:
        raise ValueError('Calibration needs a fixed model and deterministic sampling parameters')
    result = {'versions': manifest['versions'], 'configurations': manifest['configurations'],
        'devices': sorted((h['device_uuid'], h['device_model']) for h in manifest['hardware']),
        'slo': spec['slo'], 'output_tokens': spec['budget']['max_output_tokens'],
        'concurrency': spec['budget']['max_concurrency'], 'endpoint_id': spec['endpoint_id'],
        'warmup': manifest['policy_parameters'].get('warmup'),
        'initial_cache_policy': manifest['policy_parameters'].get('initial_cache_policy', 'legacy'),
        'traffic_concurrency_limit': manifest['policy_parameters'].get('traffic_concurrency_limit'),
        'endpoint_contract': {k:endpoint.get(k) for k in ('model','temperature','seed','streaming')}}
    mode = spec['workload'].get('long_prefix_mode', 'legacy')
    if mode != 'legacy':
        result['long_prefix_mode'] = mode
    return result


def _read(root, relative):
    path = (root / relative).resolve()
    if not path.is_relative_to(root):
        raise ValueError('Calibration bundles must remain within the index directory')
    checked = verify_bundle(path)
    if not checked['integrity_valid'] or not checked['experiment_valid']:
        raise ValueError('Invalid calibration bundle: ' + relative)
    manifest = json.loads((path/'manifest.json').read_text(encoding='utf-8'))
    if manifest['origin'] != 'measured-controlled' or manifest['mode'] != 'CONTROLLED_ROLLOUT':
        raise ValueError('Only original measured-controlled bundles qualify')
    return path, manifest, checked


def qualify_calibration(source, *, allow_pending_tuning=False):
    source = Path(source).resolve()
    if source.stat().st_size > 1024 * 1024:
        raise ValueError('Calibration index exceeds 1 MiB')
    raw = source.read_bytes()
    study = CalibrationStudy.model_validate_json(raw)
    seen, seeds, capacities, fixed, tuning = set(), {'calibration': set(), 'tuning': set()}, {}, {}, {}
    evidence, scope, fixed_contract = [], None, None
    capacity_contracts, tuning_initial = {}, []
    capacity_points = []
    for trial in study.trials:
        path, manifest, checked = _read(source.parent, trial.bundle)
        spec = manifest['experiment']
        if manifest['origin'] != 'measured-controlled' or manifest['mode'] != 'CONTROLLED_ROLLOUT':
            raise ValueError('Only original measured-controlled bundles qualify')
        identity = manifest['run_id']
        if identity in seen:
            raise ValueError('Duplicate calibration evidence')
        seen.add(identity)
        split = 'tuning' if trial.role == 'tuning' else 'calibration'
        if spec['workload']['split'] != split:
            raise ValueError('Calibration/tuning holdout violation')
        seed = spec['workload']['seed']
        seeds[split].add(seed)
        current_scope = _scope(manifest)
        if scope is None:
            scope = current_scope
        elif scope != current_scope:
            raise ValueError('Calibration resource/model/SLO/endpoint contract mismatch')
        value = checked['recomputed']
        if trial.role in ('capacity', 'fixed'):
            if trial.config_id is None or any(h.get('config_id') != trial.config_id for h in manifest['hardware']):
                raise ValueError('Fixed configuration is not supported by observed worker metadata')
            if (path/'transitions.jsonl').read_text(encoding='utf-8').strip():
                raise ValueError('Fixed measurements must not contain transitions')
        if trial.role == 'capacity':
            classes = list(value['by_class'])
            if len(classes) != 1 or classes[0] not in ('short', 'long') or value['goodput_rps'] <= 0:
                raise ValueError('Capacity requires positive single-class measured goodput')
            key = (trial.config_id, classes[0])
            contract = {k:v for k,v in spec.items() if k not in ('policy','plan_id','calibration_id')}
            contract['workload'] = {k:v for k,v in spec['workload'].items() if k not in ('seed','split')}
            prior = capacity_contracts.setdefault((classes[0], spec['workload']['rate_rps']),contract)
            if prior != contract:
                raise ValueError('Capacity configurations must use matched workload contracts')
            group = capacities.setdefault(key, {})
            sample_key = (seed, spec['workload']['rate_rps'])
            if sample_key in group:
                raise ValueError('Duplicate capacity seed')
            group[sample_key] = value['qualified']/spec['workload']['injection_s']
            quality_valid = complete = 0
            with (path/'requests.jsonl').open(encoding='utf-8') as stream:
                for line in stream:
                    if line.strip():
                        row = json.loads(line)
                        quality_valid += bool(row['quality_valid'])
                        complete += row['termination'] == 'complete'
            capacity_points.append({'config': trial.config_id, 'kind': classes[0], 'seed': seed,
                'rate_rps': spec['workload']['rate_rps'], 'injection_s': spec['workload']['injection_s'],
                'observation_s': spec['observation_s'], 'offered': value['offered'],
                'qualified': value['qualified'], 'complete': complete,
                'quality_valid': quality_valid, 'run_id': identity})
        else:
            contract = {k: v for k, v in spec.items() if k not in ('policy', 'plan_id', 'calibration_id')}
            contract['workload'] = {k: v for k, v in spec['workload'].items() if k not in ('seed', 'split')}
            if fixed_contract is None:
                fixed_contract = contract
            elif fixed_contract != contract:
                raise ValueError('Fixed selection and tuning must use the same declared workload contract')
            if trial.role == 'fixed':
                group = fixed.setdefault(trial.config_id, {})
            else:
                if trial.hysteresis is None:
                    raise ValueError('Tuning requires explicit hysteresis parameters')
                params = trial.hysteresis.model_dump(exclude={'schema_version'})
                if spec['policy'] != 'FixedHysteresis' or manifest['policy_parameters'].get('hysteresis') != params:
                    raise ValueError('Tuning parameters must match the recorded policy and parameters')
                tuning_initial.extend(h.get('config_id') for h in manifest['hardware'])
                key = json.dumps(params, sort_keys=True)
                group = tuning.setdefault(key, {})
            if seed in group:
                raise ValueError('Duplicate fixed/tuning seed')
            group[seed] = value['qualified']
        evidence.append({'run_id': identity, 'bundle': trial.bundle,
            'checksums_sha256': hashlib.sha256((path/'checksums.json').read_bytes()).hexdigest()})
    if seeds['calibration'] & seeds['tuning']:
        raise ValueError('Calibration and tuning seeds overlap')
    for config in ('A', 'B'):
        for kind in ('short', 'long'):
            if len(capacities.get((config, kind), {})) < 3:
                raise ValueError('Three distinct measured seeds required per configuration/class')
        if len(fixed.get(config, {})) < 3:
            raise ValueError('Strong static selection requires three seeds for both configurations')
    if set(fixed['A']) != set(fixed['B']):
        raise ValueError('Static configurations need matched seeds')
    for kind in ('short','long'):
        if set(capacities['A',kind]) != set(capacities['B',kind]):
            raise ValueError('Capacity configurations need matched seeds')
    if (not tuning and not allow_pending_tuning) or any(len(group) < 2 or set(group) != seeds['tuning'] for group in tuning.values()):
        raise ValueError('Each declared tuning candidate needs at least two matched tuning seeds')
    scores = {config: statistics.mean(values.values()) for config, values in fixed.items()}
    best = max(sorted(scores), key=scores.get)
    if any(config != best for config in tuning_initial):
        raise ValueError('Tuning must start from the same calibration-selected static configuration')
    tuned = max(sorted(tuning), key=lambda key: statistics.mean(tuning[key].values())) if tuning else None
    buckets, signed, cost_seeds = {}, {}, {}
    timings = {}
    for pair in study.transition_pairs:
        if pair.source == pair.target:
            raise ValueError('Transition source and target must differ')
        reference, moving = [_read(source.parent, p) for p in (pair.reference_bundle, pair.transition_bundle)]
        for path, manifest, checked in (reference, moving):
            if _scope(manifest) != scope or manifest['experiment']['workload']['split'] != 'calibration':
                raise ValueError('Transition calibration scope/holdout mismatch')
            if manifest['run_id'] in seen:
                raise ValueError('Transition pairs require distinct evidence runs')
            seen.add(manifest['run_id'])
            evidence.append({'run_id': manifest['run_id'], 'bundle': path.relative_to(source.parent).as_posix(),
                'checksums_sha256': hashlib.sha256((path/'checksums.json').read_bytes()).hexdigest()})
        refspec, movspec = reference[1]['experiment'], moving[1]['experiment']
        if {k:v for k,v in refspec.items() if k not in ('plan_id','policy')} != {k:v for k,v in movspec.items() if k not in ('plan_id','policy')}:
            raise ValueError('Candidate reference and transition must have matched contracts')
        seed = movspec['workload']['seed']
        if seed in seeds['tuning']:
            raise ValueError('Transition calibration overlaps tuning seeds')
        seeds['calibration'].add(seed)
        if any(h.get('config_id') != pair.target for h in reference[1]['hardware']) or any(h.get('config_id') != pair.source for h in moving[1]['hardware']):
            raise ValueError('Reference must start at candidate; transition must start at source')
        if (reference[0]/'transitions.jsonl').read_text(encoding='utf-8').strip():
            raise ValueError('Candidate reference must remain fixed')
        events = [json.loads(line) for line in (moving[0]/'transitions.jsonl').read_text(encoding='utf-8').splitlines()]
        starts = [e['at_s'] for e in events if e['state']=='DRAINING_ONE_WORKER']
        completions = [e['at_s'] for e in events if e['state']=='COMPLETE']
        if not starts or not completions:
            raise ValueError('Transition requires observed start and completion')
        start = min(starts)
        if any(end < start or end > movspec['observation_s'] for end in completions):
            raise ValueError('Transition must complete inside the observation window')
        if not 0 <= start < movspec['observation_s']:
            raise ValueError('Transition window must lie inside observation')
        classes = list(moving[2]['recomputed']['by_class'])
        if len(classes)!=1 or classes[0] not in ('short','long'):
            raise ValueError('Cost bucket requires single-class workload')
        rows = [[json.loads(line) for line in (entry[0]/'requests.jsonl').read_text(encoding='utf-8').splitlines()] for entry in (reference,moving)]
        depth = sum(r['dispatch_s'] is not None and r['dispatch_s'] < start and (r['completed_s'] is None or r['completed_s'] > start) for r in rows[1])
        bucket = 'backlogged' if depth > 2 else 'idle'
        key = (f'{pair.source}>{pair.target}:{classes[0]}', bucket)
        if seed in cost_seeds.setdefault(key,set()):
            raise ValueError('Duplicate transition seed in state bucket')
        cost_seeds[key].add(seed)
        def count(events):
            return sum(r['termination']=='complete' and r['quality_valid'] and r['output_chars']>0 and
                r['completed_s'] is not None and start <= r['completed_s'] <= movspec['observation_s'] and
                r['first_content_s'] is not None and r['completed_s']-r['scheduled_s'] <= movspec['slo']['e2e_s'] and
                r['first_content_s']-r['scheduled_s'] <= movspec['slo']['first_content_s'] for r in events)
        loss = count(rows[0])-count(rows[1])
        signed.setdefault(key,[]).append(loss)
        buckets.setdefault(key,[]).append(max(0,loss))
        completed = max(completions)
        injection = movspec['workload']['injection_s']
        from .metrics import qualifies
        from .schemas import RequestEvent, SLOSpec
        slo = SLOSpec.model_validate(movspec['slo'])
        tail = sum(qualifies(RequestEvent.model_validate(r), slo, movspec['observation_s'])
            and r['scheduled_s'] >= completed and r['completed_s'] <= injection for r in rows[1])
        timings.setdefault(key, []).append({'start_s': start, 'complete_s': completed,
            'injection_s': injection, 'rate_rps': movspec['workload']['rate_rps'],
            'post_transition_qualified_rps': tail/(injection-completed) if injection > completed else 0})
    costs = {}
    for (pair,bucket),samples in buckets.items():
        if len(samples)<3:
            raise ValueError('Each supplied cost bucket requires three distinct paired seeds')
        cost = costs.setdefault(pair, {'reference':'candidate-steady', 'by_queue':{}, 'signed_samples':{},
            'uncertainty_kind':'observed range, not confidence interval', 'negative_deficit_rule':'clamped to zero; signed values retained'})
        cost['by_queue'][bucket] = {'mean':statistics.mean(samples),'spread':max(samples)-min(samples),'samples':samples}
        cost['signed_samples'][bucket] = signed[pair,bucket]
        cost.setdefault('timing', {})[bucket] = timings[pair,bucket]
    diagnostic = capacity_diagnostic(capacity_points, fixed_contract['workload']['rate_rps'])
    if any(set(diagnostic['rates'][c]) != {'short', 'long'} for c in ('A', 'B')):
        raise ValueError('Capacity sweep must include the fixed/tuning offered load')
    result = {'id': study.id, 'origin': 'measured-controlled', 'certified': False,
        'rates': diagnostic['rates'], 'capacity_diagnostic': diagnostic,
        'costs': costs, 'static_best': best, 'static_qualified': scores,
        'hysteresis': json.loads(tuned) if tuned else None, 'workload_kind': fixed_contract['workload']['kind'],
        'calibration_seeds': sorted(seeds['calibration']), 'tuning_seeds': sorted(seeds['tuning']),
        'evidence_ids': sorted(seen), 'evidence': evidence, 'scope': scope,
        'source_sha256': hashlib.sha256(raw).hexdigest(), 'qualification_hash': stable_hash(evidence),
        'limitations': ['Operator-provided hardware metadata is not independent certification',
            'Rates are qualified cohort counts / injection seconds at the target load, not universal service capacity',
            'Missing transition buckets cause StateAware to decline switching',
            'Probe stability and arithmetic validity do not establish broad model quality']}
    result['research_readiness'] = research_gate(result)
    return result
