"""Bounded calibration acquisition using the existing traffic and rollout paths.

The CLI owns the analysis service directory while collecting. The separate,
operator-owned hook retains deployment authority. Rehearsals never qualify as GPU
calibration, even when their HTTP requests and CPU process changes are real.
"""
import asyncio
import importlib.metadata
import json
import platform
import random
import shutil
import statistics
import time
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, model_validator
from . import __version__
from .calibration import Hysteresis, qualify_calibration
from .endpoint import EndpointClient
from .evidence import append_jsonl, export_bundle, write_json
from .policy import OnlinePolicy
from .rollout import execute_plan, stable_hash
from .schemas import Record, ExperimentSpec, EndpointSpec, RunManifest, WarmupSpec, Mode
from .verifier import verify_bundle
from .warmup import warmup_reservation
from .workloads import generate
from .research import capacity_diagnostic, require_research_ready, ResearchGateError


class CollectionSpec(Record):
    id: str = Field(pattern=r'^[A-Za-z0-9_-]{1,80}$')
    purpose: Literal['qualification', 'capacity-screen'] = 'qualification'
    screen_metric: Literal['gpu-service', 'bounded-system'] = 'gpu-service'
    screen_quality_policy: Literal['require-all-valid', 'score-invalid-as-zero'] = 'require-all-valid'
    experiment: ExperimentSpec
    warmup: WarmupSpec = Field(default_factory=WarmupSpec)
    initial_cache_policy: Literal['legacy', 'fresh-workers'] = 'legacy'
    calibration_seeds: list[int] = Field(default_factory=lambda: [11, 12, 13], min_length=1, max_length=10)
    tuning_seeds: list[int] = Field(default_factory=lambda: [21, 22], min_length=2, max_length=10)
    test_seeds: list[int] = Field(default_factory=lambda: [101, 102, 103, 104, 105], min_length=1, max_length=30)
    candidates: list[Hysteresis] = Field(default_factory=lambda: [
        Hysteresis(advantage_fraction=.1, persistence_s=2, dwell_s=10),
        Hysteresis(advantage_fraction=.2, persistence_s=4, dwell_s=20)], min_length=1, max_length=8)
    transition_kinds: list[Literal['short', 'long-prefix']] = Field(default_factory=lambda: ['short', 'long-prefix'])
    transition_at_s: float = Field(default=2, ge=0)
    capacity_rates_rps: list[Annotated[float, Field(gt=0, le=500)]] = Field(default_factory=list, max_length=5)
    order_seed: int = Field(default=713, ge=0)
    max_wall_s: float = Field(gt=0, le=86400)
    max_requests: int = Field(gt=0, le=2_000_000)
    max_total_tokens: int = Field(gt=0, le=2_000_000_000)

    @model_validator(mode='after')
    def boundaries(self):
        if self.screen_quality_policy != 'require-all-valid' and (self.purpose != 'capacity-screen' or self.screen_metric != 'bounded-system'):
            raise ValueError('Scored quality failures require an explicit bounded-system screen')
        if self.screen_metric == 'bounded-system' and self.purpose != 'capacity-screen':
            raise ValueError('Bounded-system evidence is exploratory only; not a qualified GPU calibration')
        if self.purpose == 'capacity-screen':
            if len(self.calibration_seeds) != 1 or not self.capacity_rates_rps or self.transition_kinds:
                raise ValueError('Capacity screen requires one scout seed, a load ladder and no transition trials')
            if self.initial_cache_policy != 'fresh-workers':
                raise ValueError('Capacity screen requires fresh workers')
        elif len(self.calibration_seeds) < 3:
            raise ValueError('Qualification requires at least three calibration seeds')
        minimum_loads = 1 if self.screen_metric == 'bounded-system' else 3
        if self.capacity_rates_rps and (len(self.capacity_rates_rps) < minimum_loads or
                self.capacity_rates_rps != sorted(set(self.capacity_rates_rps)) or
                self.experiment.workload.rate_rps not in self.capacity_rates_rps):
            raise ValueError('Capacity sweep needs 3-5 increasing distinct loads including the target load')
        if self.initial_cache_policy == 'fresh-workers' and not self.warmup.complete_probe_sequence:
            raise ValueError('Fresh-worker comparison requires the full fixed warmup sequence')
        groups = [self.calibration_seeds, self.tuning_seeds, self.test_seeds]
        if any(len(g) != len(set(g)) or any(s < 0 for s in g) for g in groups):
            raise ValueError('Unique nonnegative seeds required')
        if any(set(a) & set(b) for i, a in enumerate(groups) for b in groups[i+1:]):
            raise ValueError('Calibration, tuning and held-out test seeds must be disjoint')
        if len(set(self.transition_kinds)) != len(self.transition_kinds):
            raise ValueError('Duplicate transition kinds')
        if len({stable_hash(c.model_dump()) for c in self.candidates}) != len(self.candidates):
            raise ValueError('Duplicate tuning candidates')
        spec = self.experiment
        if spec.mode != 'CONTROLLED_ROLLOUT' or not spec.endpoint_id or spec.plan_id or spec.calibration_id:
            raise ValueError('Collection needs a controlled endpoint template without a prior plan/calibration')
        if (spec.budget.reserved_gpus != 2 or spec.budget.max_concurrency < 2 or
                spec.budget.max_output_tokens < 32 or
                spec.budget.max_reserved_gpu_seconds < 2 * spec.budget.max_duration_s):
            raise ValueError('A full two-device duration envelope and warmup concurrency slot are required')
        if self.transition_at_s >= min(spec.workload.injection_s, spec.budget.max_duration_s / 2):
            raise ValueError('Forced transition must start during injection and before recovery reserve')
        return self


def schedule(value):
    rng, trials = random.Random(value.order_seed), []
    for seed in value.calibration_seeds:
        block = [{'role': role, 'kind': kind, 'seed': seed, 'initial': config}
                 for role, kinds in [('capacity', ['short', 'long-prefix']), ('fixed', [value.experiment.workload.kind])]
                 for kind in kinds for config in ('A', 'B')]
        rng.shuffle(block)
        for trial in block:
            if trial['role'] == 'capacity' and value.capacity_rates_rps:
                trials.extend({**trial, 'rate_rps': rate} for rate in value.capacity_rates_rps)
            else:
                trials.append(trial)
    if value.purpose == 'capacity-screen':
        # Matched A/B pairs at each load; no selection, tuning, transitions or
        # held-out runs. A scout can never become a qualified calibration.
        trials = []
        loads = [value.experiment.workload.rate_rps] + [r for r in value.capacity_rates_rps
            if r != value.experiment.workload.rate_rps]
        for rate in loads:
            kinds = ['short', 'long-prefix']
            rng.shuffle(kinds)
            for kind in kinds:
                configs = ['A', 'B']
                rng.shuffle(configs)
                trials.extend(dict(role='capacity', kind=kind, seed=value.calibration_seeds[0],
                                   initial=config, rate_rps=rate) for config in configs)
        return trials
    for kind in value.transition_kinds:
        for source, target in [('A', 'B'), ('B', 'A')]:
            for seed in value.calibration_seeds:
                pair = f'{source}-{target}-{kind}-{seed}'
                block = [dict(role='reference', kind=kind, seed=seed, initial=target, pair=pair, source=source, target=target),
                         dict(role='transition', kind=kind, seed=seed, initial=source, pair=pair, source=source, target=target)]
                rng.shuffle(block)
                trials.extend(block)
    # Validate the expensive deployment boundary before spending on capacity.
    # Keep the seeded order within each matched transition/reference pair.
    if value.capacity_rates_rps:
        # New studies first establish whether there is anything worth switching.
        trials.sort(key=lambda trial: trial['role'] != 'capacity')
    else:
        trials.sort(key=lambda trial: trial['role'] not in ('reference', 'transition'))
    for seed in value.tuning_seeds:
        block = [{'role': 'tuning', 'kind': value.experiment.workload.kind, 'seed': seed,
                  'initial': 'selected-static', 'hysteresis': c.model_dump(exclude={'schema_version'})} for c in value.candidates]
        rng.shuffle(block)
        trials.extend(block)
    if len(trials) > 200:
        raise ValueError('Collection exceeds the 200-trial evidence envelope')
    return trials


def trial_spec(value, trial):
    data = value.experiment.model_dump(mode='json')
    data['workload'].update(kind=trial['kind'], seed=trial['seed'], split='tuning' if trial['role'] == 'tuning' else 'calibration')
    if 'rate_rps' in trial:
        data['workload']['rate_rps'] = trial['rate_rps']
    data['policy'] = 'FixedHysteresis' if trial['role'] == 'tuning' else 'StaticBest'
    return ExperimentSpec.model_validate(data)


def collection_plan(value, operator_config, code_revision):
    trials = schedule(value)
    warm_requests, warm_tokens = warmup_reservation(value.warmup, value.experiment.budget.max_output_tokens)
    requests = tokens = 0
    for trial in trials:
        spec = trial_spec(value, trial)
        items = generate(spec.workload)
        traffic_tokens = sum(i.input_token_upper_bound + spec.budget.max_output_tokens for i in items)
        if len(items) + warm_requests > spec.budget.max_requests or traffic_tokens + warm_tokens > spec.budget.max_total_tokens:
            raise ValueError('Per-trial budget cannot cover workload plus rollout/recovery probes')
        # One possible reset, two initial warmups, and one measured rollout/recovery.
        requests += spec.budget.max_requests + 1.5 * warm_requests
        tokens += spec.budget.max_total_tokens + 1.5 * warm_tokens
    seconds = len(trials) * (2 * value.experiment.budget.max_duration_s + 2 * value.warmup.max_duration_s)
    result = {'protocol': value.model_dump(mode='json'), 'order': trials, 'code_revision': code_revision,
              'operator_config_hash': stable_hash(operator_config),
              'maximum_reservations': {'requests': int(requests), 'tokens': int(tokens), 'wall_s': seconds},
              'status': 'READY' if requests <= value.max_requests and tokens <= value.max_total_tokens and seconds <= value.max_wall_s else 'BUDGET_REFUSED',
              'cloud_authorization': False, 'hardware_validated': False,
              'limitations': ['Conservative local operation envelope; no provider billing cap or shutdown',
                             'No retries or automatic continuation after failure', 'No held-out test traffic is collected']}
    slot = 2 * value.experiment.budget.max_duration_s + 2 * value.warmup.max_duration_s
    result['stage_maximum_s'] = {role: sum(t['role'] == role for t in trials)*slot
                                 for role in ('capacity', 'fixed', 'reference', 'transition', 'tuning')}
    result['plan_hash'] = stable_hash(result)
    return result


class CalibrationCollector:
    def __init__(self, service, adapter, value, output, plan, *, rehearsal=False, require_discrimination=False):
        self.service, self.adapter, self.value = service, adapter, value
        self.output, self.plan, self.rehearsal = Path(output).resolve(), plan, rehearsal
        self.cancel = asyncio.Event()
        self.index = {'id': value.id, 'trials': [], 'transition_pairs': []}
        self.measurements, self.pairs = [], {}
        self.reserved = {'requests': 0, 'tokens': 0}
        self.started = time.monotonic()
        self.origin = 'measured-black-box' if rehearsal else 'measured-controlled'
        self.require_discrimination = require_discrimination or bool(value.capacity_rates_rps)
        self.active_run = None
        self.run_ids = set()
        self.identity = None
        self.initial_condition = None

    def receipt(self, event, **fields):
        append_jsonl(self.output / 'collection-journal.jsonl', {'event': event, 'elapsed_s': time.monotonic()-self.started, **fields})

    def reserve(self, stage, requests, tokens, seconds):
        if (self.reserved['requests'] + requests > self.value.max_requests or
                self.reserved['tokens'] + tokens > self.value.max_total_tokens or
                time.monotonic() - self.started + seconds > self.value.max_wall_s):
            raise ValueError('Campaign budget refused before ' + stage)
        self.reserved['requests'] += requests
        self.reserved['tokens'] += tokens
        self.receipt('reservation', stage=stage, requests=requests, tokens=tokens, maximum_s=seconds)

    async def snapshots(self):
        async with asyncio.timeout(min(30, self.value.experiment.budget.max_duration_s)):
            rows = await self.adapter.snapshot()
        if len(rows) != 2 or any(not s.ready or not s.accepting for s in rows):
            raise ValueError('Two ready, accepting workers required')
        if not self.rehearsal and (len({s.device_uuid for s in rows}) != 2 or
                len({s.device_model for s in rows}) != 1 or any(not s.device_uuid or not s.device_uuid.startswith('GPU-') or
                not s.device_model or not s.process_id or s.resource_evidence != 'independently-observed' for s in rows)):
            raise ValueError('Independent physical two-GPU/process evidence required')
        identity = sorted((s.worker_id, s.device_uuid, s.device_model) for s in rows)
        if self.identity is not None and identity != self.identity:
            raise ValueError('Worker/device identity changed within calibration')
        self.identity = identity
        return [s.model_copy(deep=True) for s in rows]

    def make_plan(self, target, snapshots):
        plan = self.service.plans.create('calibration-lab', target, snapshots, self.value.experiment.budget,
                                         warmup=self.value.warmup, ttl_s=600, initial_condition=self.initial_condition)
        # Authorized by the exact campaign hash: allowed targets and per-step ceilings
        # are frozen before execution. Each concrete child plan remains auditable.
        self.service.plans.approve(plan['id'], plan['hash'])
        self.receipt('child_plan', plan_id=plan['id'], plan_hash=plan['hash'], campaign_hash=self.plan['plan_hash'], target=target)
        return plan

    async def prepare(self, initial):
        self.initial_condition = None
        if self.value.initial_cache_policy == 'fresh-workers' and not self.value.warmup.complete_probe_sequence:
            raise ValueError('Fresh-worker comparison requires the full fixed warmup sequence')
        snapshots = await self.snapshots()
        before = [s.model_dump(mode='json') for s in snapshots]
        warm_requests, warm_tokens = warmup_reservation(self.value.warmup, self.value.experiment.budget.max_output_tokens)
        fresh = self.value.initial_cache_policy == 'fresh-workers'
        if fresh or any(s.config_id != initial for s in snapshots):
            self.reserve('reset', warm_requests, warm_tokens, self.value.experiment.budget.max_duration_s)
            plan = self.make_plan(initial, snapshots)
            result = await execute_plan(self.service.plans, plan['id'], self.adapter, self.cancel)
            self.receipt('reset_result', plan=result)
            if result['state'] != 'COMPLETE':
                raise ValueError('Reset requires reconciliation: ' + result['state'])
            snapshots = await self.snapshots()
        if any(s.in_flight for s in snapshots):
            raise ValueError('Initial warmup requires an idle owned router')
        self.reserve('initial_warmup', warm_requests // 2, warm_tokens // 2, 2*self.value.warmup.max_duration_s)
        plan = self.make_plan(initial, snapshots)
        self.service.plans.acquire(plan['id'])
        try:
            if fresh:
                originals = {s['worker_id']: s for s in before}
                if any(s.generation != originals[s.worker_id]['generation'] + 1 or
                       not s.process_id or s.process_id == originals[s.worker_id]['process_id'] or
                       s.config_id != initial for s in snapshots):
                    raise ValueError('Fresh reset lacks a new generation and independently observed process')
            for s in snapshots:
                payload = {'config_id': initial, 'expected_generation': s.generation,
                           'plan_id': plan['id'], 'plan_hash': plan['hash'], 'expires_at_unix_s': plan['expires_at_unix_s'],
                           'drain_timeout_s': 10, 'max_tokens': 32, 'warmup': self.value.warmup.model_dump(mode='json'),
                           'operation_timeout_s': min(60, self.value.warmup.max_duration_s)}
                async with asyncio.timeout(payload['operation_timeout_s']):
                    await self.adapter.operation('warmup', s.worker_id, payload, f"{plan['id']}:{s.worker_id}:initial-warmup")
            after = await self.snapshots()
            if fresh and sorted((s.worker_id,s.generation,s.process_id) for s in after) != sorted((s.worker_id,s.generation,s.process_id) for s in snapshots):
                raise ValueError('Worker changed during initial warmup')
            self.service.plans.event(plan['id'], 'STAYED', detail='Bounded initial warmup completed')
        except BaseException:
            self.service.plans.event(plan['id'], 'ROLLBACK_PENDING', detail='Initial warmup interrupted; no automatic continuation')
            raise
        if fresh:
            self.initial_condition = {'policy': 'fresh-workers', 'before': before,
                'after': [s.model_dump(mode='json') for s in after],
                'warmup': self.value.warmup.model_dump(mode='json')}
            self.receipt('initial_condition', **self.initial_condition)
        return after

    def estimates(self):
        diagnostic = self.capacity_diagnostic()
        rates = diagnostic['rates']
        if any(set(row) != {'short', 'long'} for row in rates.values()):
            raise ValueError('Capacity sweep lacks the target offered load')
        if any(v <= 0 for row in rates.values() for v in row.values()):
            raise ValueError('Capacity collection produced zero qualified goodput')
        scores = {c: statistics.mean(m['metrics']['qualified'] for m in self.measurements
                  if m['trial']['role'] == 'fixed' and m['trial']['initial'] == c) for c in ('A', 'B')}
        return {'id': self.value.id, 'origin': self.origin, 'rates': rates, 'static_best': max(sorted(scores), key=scores.get)}

    def capacity_diagnostic(self):
        return capacity_diagnostic([m['capacity_point'] for m in self.measurements
            if m['trial']['role'] == 'capacity'], self.value.experiment.workload.rate_rps)

    async def trial(self, number, trial):
        spec = trial_spec(self.value, trial)
        estimates = self.estimates() if trial['role'] == 'tuning' else None
        initial = estimates['static_best'] if estimates else trial['initial']
        snapshots = await self.prepare(initial)
        self.reserve('measured_trial', spec.budget.max_requests, spec.budget.max_total_tokens, spec.budget.max_duration_s)
        run_id, _ = self.service.store.create(spec, f"{self.value.id}:{number}")
        self.active_run = run_id
        self.run_ids.add(run_id)
        self.service.store.update(run_id, 'RUNNING')
        endpoint = EndpointSpec.model_validate(self.service.endpoints[spec.endpoint_id]['spec'])
        client = EndpointClient(endpoint, self.service.network)
        await client.discover()
        target = trial.get('target', 'B' if initial == 'A' else 'A')
        plan = self.make_plan(target, snapshots)
        self.service.plans.acquire(plan['id'])
        self.service.plans.event(plan['id'], 'WAITING_POLICY')
        spec = spec.model_copy(update={'plan_id': plan['id']})
        warm = plan['warmup_reservation']
        traffic_spec = spec.model_copy(update={'budget': spec.budget.model_copy(update={
            'max_requests': spec.budget.max_requests-warm['max_requests'],
            'max_total_tokens': spec.budget.max_total_tokens-warm['max_total_tokens'],
            'max_concurrency': spec.budget.max_concurrency-1})})
        policy = OnlinePolicy('FixedHysteresis', {**estimates, 'hysteresis': trial['hysteresis']}, spec.horizon_s) if estimates else None
        arrivals = asyncio.Queue()
        def arrived(kind, prefix, at, depth):
            arrivals.put_nowait((kind, prefix, at, depth))
        started = time.monotonic()
        traffic = asyncio.create_task(self.service.measured_traffic(run_id, traffic_spec, client, self.cancel,
            on_arrival=arrived, epoch=started, skip_discovery=True))
        decisions, result = [], None
        try:
            async with asyncio.timeout(spec.budget.max_duration_s):
                while not traffic.done() and not self.cancel.is_set():
                    if trial['role'] == 'transition' and time.monotonic()-started >= self.value.transition_at_s:
                        result = await execute_plan(self.service.plans, plan['id'], self.adapter, self.cancel, lease_held=True, started_at=started)
                        break
                    try:
                        kind, prefix, at, depth = await asyncio.wait_for(arrivals.get(), .01)
                    except TimeoutError:
                        continue
                    if policy:
                        policy.observe_arrival(kind, prefix, at)
                        chosen, _ = policy.choose(at, initial, depth)
                        decisions.append({'at_s': at, 'chosen': chosen, 'observed_queue': depth})
                        if chosen != initial:
                            if chosen != target:
                                raise ValueError('Tuning decision outside approved target')
                            result = await execute_plan(self.service.plans, plan['id'], self.adapter, self.cancel, lease_held=True, started_at=started)
                            break
                rows, validity = await traffic
                await asyncio.sleep(max(0, spec.observation_s-(time.monotonic()-started)))
            if result is None:
                self.service.plans.event(plan['id'], 'STAYED', detail='No measured transition')
                result = self.service.plans.get(plan['id'])
        except BaseException:
            self.cancel.set()
            await traffic
            self.service.plans.event(plan['id'], 'ROLLBACK_PENDING', detail='Acquisition interrupted; inspect journals before any restart')
            raise
        self.receipt('trial_plan_result', run_id=run_id, plan=result)
        if result['state'] not in ('COMPLETE', 'STAYED'):
            validity = {'valid': False, 'errors': ['Rollout '+result['state']]}
        if trial['role'] == 'transition' and (result['state'] != 'COMPLETE' or result['elapsed_s'] > spec.observation_s):
            validity = {'valid': False, 'errors': ['Forced transition did not complete inside the observation window']}
        transitions = [{'state': e['state'], 'worker_id': e['worker_id'], 'at_s': e['elapsed_s'],
                        'origin': self.origin, 'detail': e['detail']} for e in result['events']] if result['state'] != 'STAYED' else []
        for row in rows:
            row.origin = self.origin
        elapsed = max(spec.observation_s, time.monotonic()-started)
        versions = {'transitionbench': __version__, 'python': platform.python_version(), 'code_revision': self.service.code_revision,
                    **{n: importlib.metadata.version(n) for n in ('fastapi', 'pydantic', 'httpx', 'mcp')},
                    **self.service.config.get('hook', {}).get('versions', {})}
        if self.rehearsal:
            spec = spec.model_copy(update={'mode': Mode.LIVE_ENDPOINT, 'budget': spec.budget.model_copy(update={'reserved_gpus': 0, 'max_reserved_gpu_seconds': 0})})
        manifest = RunManifest(run_id=run_id, mode=spec.mode, origin=self.origin, experiment=spec,
            offered_ids=[r.request_id for r in rows], created_at_unix_s=time.time(), versions=versions,
            hardware=[s.model_dump(mode='json') for s in snapshots], configurations=self.service.config['configurations'],
            resource_intervals=[{'start_s': 0, 'end_s': elapsed, 'reserved_gpus': 0 if self.rehearsal else 2, 'active_gpu_seconds': None}],
            policy_parameters={'endpoint_contract': {k: getattr(endpoint, k) for k in ('model', 'temperature', 'seed', 'streaming')},
                'hysteresis': trial.get('hysteresis'), 'warmup': self.value.warmup.model_dump(mode='json'),
                'traffic_concurrency_limit': traffic_spec.budget.max_concurrency, 'campaign_hash': self.plan['plan_hash']},
            limitations=['CPU rehearsal; not GPU calibration' if self.rehearsal else 'Operator-owned measurements; not independent certification',
                         'One transition maximum per trial; arithmetic task validity only'])
        manifest.policy_parameters['initial_cache_policy'] = self.value.initial_cache_policy
        manifest.policy_parameters['initial_condition'] = self.initial_condition
        relative = f'bundles/{number:03d}-{run_id}'
        path = self.output / relative
        export_bundle(path, manifest, rows, transitions, decisions, validity)
        checked = verify_bundle(path)
        write_json(path.parent / (path.name+'.verification.json'), checked)
        self.service.store.update(run_id, 'SUCCEEDED' if checked['experiment_valid'] else 'INVALID')
        self.active_run = None
        if not checked['integrity_valid'] or not checked['experiment_valid']:
            raise ValueError('Invalid acquired trial; preserved ' + relative)
        # Later estimates need only these scalars. Full per-request arrays stay
        # in the verified bundle, outside subsequent timed trials' live heap.
        metrics = {key: checked['recomputed'][key] for key in ('goodput_rps', 'qualified')}
        retained = {'trial': trial, 'metrics': metrics, 'bundle': relative}
        if trial['role'] == 'capacity':
            retained['capacity_point'] = {'config': initial, 'kind': 'short' if trial['kind'] == 'short' else 'long',
                'seed': trial['seed'], 'rate_rps': spec.workload.rate_rps,
                'injection_s': spec.workload.injection_s, 'observation_s': spec.observation_s,
                'offered': len(rows), 'qualified': metrics['qualified'],
                'complete': sum(r.termination == 'complete' for r in rows),
                'client_dropped': sum(r.termination == 'client_drop' for r in rows),
                'quality_valid': sum(r.quality_valid for r in rows), 'run_id': run_id}
        self.measurements.append(retained)
        if trial['role'] in ('capacity', 'fixed', 'tuning'):
            entry = {'role': trial['role'], 'bundle': relative}
            entry.update(hysteresis=trial['hysteresis']) if policy else entry.update(config_id=initial)
            self.index['trials'].append(entry)
        else:
            pair = self.pairs.setdefault(trial['pair'], {'source': trial['source'], 'target': trial['target']})
            pair['reference_bundle' if trial['role'] == 'reference' else 'transition_bundle'] = relative
            if 'reference_bundle' in pair and 'transition_bundle' in pair:
                self.index['transition_pairs'].append(dict(pair))
        write_json(self.output/'index.json', self.index)
        self.receipt('trial_complete', number=number, role=trial['role'], run_id=run_id, bundle=relative)

    async def run(self, approved_hash):
        if approved_hash != self.plan['plan_hash'] or self.plan['status'] != 'READY':
            raise ValueError('Exact ready collection plan hash required before any operations')
        if self.plan != collection_plan(self.value, self.service.config, self.service.code_revision):
            raise ValueError('Collection protocol, code or operator configuration changed')
        self.output.mkdir(parents=True, exist_ok=False)
        write_json(self.output/'plan.json', self.plan)
        source_root = self.output/'producer'
        source_root.mkdir()
        for source in Path(__file__).parent.glob('*.py'):
            shutil.copyfile(source, source_root/source.name)
        status = {'state': 'RUNNING', 'origin': self.origin, 'hardware_validated': False, 'qualified': False}
        write_json(self.output/'status.json', status)
        try:
            with self.service.plans.connect() as db:
                if db.execute('SELECT 1 FROM leases').fetchone():
                    raise ValueError('An existing plan needs reconciliation; no acquisition allowed')
            endpoint = EndpointSpec.model_validate(self.service.endpoints[self.value.experiment.endpoint_id]['spec'])
            if endpoint.temperature != 0 or endpoint.seed is None:
                raise ValueError('Fixed deterministic endpoint parameters required')
            envelope = self.service.endpoints[endpoint.id]['budget']
            for field in ('max_requests', 'max_total_tokens', 'max_output_tokens', 'max_concurrency', 'max_duration_s'):
                if getattr(self.value.experiment.budget, field) > envelope[field]:
                    raise ValueError('Collection exceeds operator endpoint envelope: '+field)
            versions = self.service.config.get('hook', {}).get('versions', {})
            if not self.rehearsal and any(not versions.get(k) or 'REPLACE' in versions[k]
                                        for k in ('engine', 'driver', 'model_revision', 'tokenizer_revision')):
                raise ValueError('Pinned runtime, driver, model and tokenizer versions required')
            async with asyncio.timeout(max(.001, self.value.max_wall_s-(time.monotonic()-self.started))):
                for number, trial in enumerate(self.plan['order']):
                    if self.cancel.is_set():
                        raise ValueError('Collection cancelled')
                    if self.require_discrimination and trial['role'] == 'tuning' and not any(
                            m['trial']['role'] == 'tuning' for m in self.measurements):
                        candidate = qualify_calibration(self.output/'index.json', allow_pending_tuning=True)
                        write_json(self.output/'research-readiness.json', candidate['research_readiness'])
                        require_research_ready(candidate)
                    await self.trial(number, trial)
                    if self.value.purpose == 'capacity-screen':
                        point = self.measurements[-1]['capacity_point']
                        admitted_drops = point.get('client_dropped', 0) if self.value.screen_metric == 'bounded-system' else 0
                        quality_abort = self.value.screen_quality_policy == 'require-all-valid' and point['quality_valid'] != point['complete']
                        if point['complete'] + admitted_drops != point['offered'] or quality_abort:
                            write_json(self.output/'capacity-diagnostic.json', self.capacity_diagnostic())
                            raise ResearchGateError('Screen stopped: transport/client/quality failures confound capacity')
                    if self.value.purpose != 'capacity-screen' and self.require_discrimination and trial['role'] == 'capacity' and not any(
                            t['role'] == 'capacity' for t in self.plan['order'][number+1:]):
                        diagnostic = self.capacity_diagnostic()
                        write_json(self.output/'capacity-diagnostic.json', diagnostic)
                        if not diagnostic['ready']:
                            raise ResearchGateError('Capacity discrimination refused: ' + '; '.join(diagnostic['reasons']))
            if self.value.purpose == 'capacity-screen':
                write_json(self.output/'capacity-diagnostic.json', self.capacity_diagnostic())
                status.update(state='REHEARSAL_COMPLETE' if self.rehearsal else 'SCREEN_COMPLETE',
                    qualified=False, research_ready=False, automatic_continuation=False,
                    limitations=['One scout seed cannot qualify capacity or authorize a formal study'])
            elif self.rehearsal:
                try:
                    qualify_calibration(self.output/'index.json')
                except ValueError:
                    status.update(state='REHEARSAL_COMPLETE', qualification_refused_as_expected=True)
                else:
                    raise AssertionError('CPU evidence must not qualify')
            else:
                qualified = qualify_calibration(self.output/'index.json')
                write_json(self.output/'qualified.json', qualified)
                registration = {'calibrations': {self.value.id: {k: qualified[k] for k in ('source_sha256', 'qualification_hash')}}}
                registration['calibrations'][self.value.id]['source'] = str(self.output/'index.json')
                write_json(self.output/'registration.json', registration)
                status.update(state='QUALIFIED', qualified=True, hardware_validated=True)
            return status
        except BaseException as exc:
            status.update(state='INCONCLUSIVE' if isinstance(exc, ResearchGateError) else 'FAILED',
                          error_type=type(exc).__name__, error=str(exc)[:300])
            if self.active_run:
                self.service.store.update(self.active_run, 'FAILED', status['error'])
            self.receipt('failure', **status)
            raise
        finally:
            status.update(completed_trials=len(self.measurements), reserved=self.reserved, elapsed_s=time.monotonic()-self.started)
            write_json(self.output/'status.json', status)
            # Include raw partial journals and child plans even when no bundle exists.
            for path in (self.service.store.root/'runs').glob('*/request-journal.jsonl'):
                if path.parent.name in self.run_ids:
                    destination = self.output/'journals'/path.parent.name
                    destination.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(path, destination/path.name)
