"""Workload/measurement contracts only; no GPU performance claims."""
import hashlib
import json
from dataclasses import asdict

import pytest

from transitionbench.schemas import WorkloadSpec
from transitionbench.workloads import generate


def test_shared_and_unique_long_inputs_keep_arrivals_answers_and_lengths_matched():
    common = dict(kind='long-prefix', seed=6201, split='calibration', rate_rps=192, injection_s=20)
    shared = generate(WorkloadSpec(**common, long_prefix_mode='shared'))
    unique = generate(WorkloadSpec(**common, long_prefix_mode='unique'))
    assert len(shared) == len(unique) == 3840
    assert len({r.prefix_group for r in shared}) == 4
    assert len({r.prefix_group for r in unique}) == 3840
    for a, b in zip(shared, unique):
        assert (a.request_id, a.scheduled_s, a.expected) == (b.request_id, b.scheduled_s, b.expected)
        assert len(a.prompt.encode()) == len(b.prompt.encode())
        assert a.input_token_upper_bound == b.input_token_upper_bound
        assert a.prefix_group in a.prompt and b.prefix_group in b.prompt
        # The distinguishing group occurs before the repeated context, not just
        # at the end of an otherwise cacheable long prefix.
        assert a.prompt.index(a.prefix_group) < 32
        assert b.prompt.index(b.prefix_group) < 32
    assert unique == generate(WorkloadSpec(**common, long_prefix_mode='unique'))


@pytest.mark.parametrize('kind', ['short', 'mixed-burst', 'prefix-shift'])
def test_prefix_control_preserves_class_schedule_and_split_isolation(kind):
    common = dict(kind=kind, seed=6201, rate_rps=20, injection_s=2)
    shared = generate(WorkloadSpec(**common, long_prefix_mode='shared'))
    unique = generate(WorkloadSpec(**common, long_prefix_mode='unique'))
    assert [(r.scheduled_s, r.workload_class, r.expected) for r in shared] == [
        (r.scheduled_s, r.workload_class, r.expected) for r in unique]
    assert all(a.prompt == b.prompt for a, b in zip(shared, unique) if a.workload_class == 'short')
    splits = [{r.prefix_group for r in generate(WorkloadSpec(**common, split=s, long_prefix_mode='unique'))}
              for s in ('calibration', 'tuning', 'test')]
    assert all(not a & b for i, a in enumerate(splits) for b in splits[i+1:])


def test_legacy_workload_remains_byte_identical():
    # Digest captured from the pre-change generator for all four workload kinds.
    rows = [asdict(r) for kind in ('short', 'long-prefix', 'prefix-shift', 'mixed-burst')
            for r in generate(WorkloadSpec(kind=kind, seed=6201, rate_rps=20, injection_s=2))]
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()
    assert digest == '81fd7d0a5aaf6ab40053896fab70fe0df8347b0a7a520763d8e05b8131ee1d8a'


def test_calibration_rejects_mixed_prefix_regimes(tmp_path):
    from test_calibration import study
    from test_boundaries import rewrite_checks
    from transitionbench.calibration import qualify_calibration
    path = study(tmp_path)
    data = json.loads(path.read_text())
    bundle = tmp_path / data['trials'][0]['bundle']
    manifest = json.loads((bundle/'manifest.json').read_text())
    manifest['experiment']['workload']['long_prefix_mode'] = 'unique'
    (bundle/'manifest.json').write_text(json.dumps(manifest))
    rewrite_checks(bundle)
    with pytest.raises(ValueError, match='contract mismatch'):
        qualify_calibration(path)


def test_explicit_legacy_preserves_old_calibration_identity(tmp_path):
    from test_calibration import bundle
    from transitionbench.calibration import _scope
    name = bundle(tmp_path, 'scope-fixture')
    manifest = json.loads((tmp_path/name/'manifest.json').read_text())
    explicit = _scope(manifest)
    del manifest['experiment']['workload']['long_prefix_mode']
    assert _scope(manifest) == explicit
    assert 'long_prefix_mode' not in explicit


def test_collection_freezes_prefix_mode_and_reserves_complete_input_budget():
    from transitionbench.collection import CollectionSpec, collection_plan
    from transitionbench.schemas import ExperimentSpec, ResourceBudget, WarmupSpec
    experiment = ExperimentSpec(mode='CONTROLLED_ROLLOUT', endpoint_id='fixture',
        workload=WorkloadSpec(kind='short', long_prefix_mode='unique', split='calibration',
                              seed=6201, rate_rps=192, injection_s=20),
        observation_s=30, drain_s=10,
        budget=ResourceBudget(max_duration_s=420, max_reserved_gpu_seconds=840,
                              max_requests=6000, max_concurrency=129, max_total_tokens=20000000))
    screen = CollectionSpec(id='prefix-contract', purpose='capacity-screen', screen_metric='bounded-system',
        initial_cache_policy='fresh-workers', warmup=WarmupSpec(complete_probe_sequence=True),
        experiment=experiment, calibration_seeds=[6201], transition_kinds=[], capacity_rates_rps=[192],
        max_wall_s=4000, max_requests=100000, max_total_tokens=400000000)
    plan = collection_plan(screen, {}, 'fixture')
    assert plan['status'] == 'READY'
    assert plan['protocol']['experiment']['workload']['long_prefix_mode'] == 'unique'
    shared = screen.model_copy(update={'experiment': experiment.model_copy(update={
        'workload': experiment.workload.model_copy(update={'long_prefix_mode': 'shared'})})})
    other = collection_plan(shared, {}, 'fixture')
    assert other['maximum_reservations'] == plan['maximum_reservations']
    assert other['plan_hash'] != plan['plan_hash']
