"""Recompute an exported GPU campaign without the producer or third-party packages.

Usage: python scripts/verify_gpu_study.py reports/vast-51687766/final/evidence/CAMPAIGN
Incomplete campaigns are retained and reported as incomplete, never as a full pass.
"""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import statistics


def read(path):
    return json.loads(path.read_text(encoding='utf-8'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('study', type=Path)
    args = parser.parse_args()
    root = args.study.resolve()
    module = importlib.util.spec_from_file_location('standalone_verifier',
        Path(__file__).resolve().parents[1] / 'src/transitionbench/verifier.py')
    verifier = importlib.util.module_from_spec(module)
    module.loader.exec_module(verifier)
    status = read(root / 'status.json')
    frozen = read(root / 'frozen-study.json')
    screen = frozen['collection'].get('purpose') == 'capacity-screen'
    collection = read(root / 'calibration/plan.json')
    order = collection['order']
    errors, rows, held_out = [], [], []
    digest = hashlib.sha256()
    for source in sorted((root / 'calibration/producer').glob('*.py')):
        digest.update(source.name.encode())
        digest.update(source.read_bytes())
    source_matches = 'sha256:' + digest.hexdigest() == frozen['code_revision']
    if not source_matches:
        errors.append('Frozen producer source hash mismatch')
    paths = sorted((root / 'calibration/bundles').glob('*'))
    paths = [p for p in paths if p.is_dir()]
    paths += sorted((root / 'service/runs').glob('*/bundle'))
    for path in paths:
        checked = verifier.verify_bundle(path)
        if not checked['integrity_valid'] or not checked['experiment_valid']:
            errors.append({'bundle': str(path.relative_to(root)),
                'integrity': checked['integrity_errors'], 'experiment': checked['experiment_errors']})
        manifest = read(path / 'manifest.json')
        spec = manifest['experiment']
        actual = checked['recomputed']
        if manifest['versions']['code_revision'] != frozen['code_revision']:
            errors.append('Bundle source revision mismatch: ' + manifest['run_id'])
        requests = [json.loads(line) for line in (path / 'requests.jsonl').read_text(encoding='utf-8').splitlines() if line]
        transitions = [json.loads(line) for line in (path / 'transitions.jsonl').read_text(encoding='utf-8').splitlines() if line]
        decisions = [json.loads(line) for line in (path / 'decisions.jsonl').read_text(encoding='utf-8').splitlines() if line]
        calibration_number = int(path.name.split('-')[0]) if path.parent.name == 'bundles' else None
        role = order[calibration_number]['role'] if calibration_number is not None else 'held-out'
        if screen and calibration_number is not None:
            planned = order[calibration_number]
            if (planned['role'] != 'capacity' or spec['workload']['seed'] != planned['seed'] or
                    spec['workload']['kind'] != planned['kind'] or
                    spec['workload'].get('long_prefix_mode', 'legacy') != frozen['collection']['experiment']['workload'].get('long_prefix_mode', 'legacy') or
                    spec['workload']['rate_rps'] != planned['rate_rps'] or
                    any(h['config_id'] != planned['initial'] for h in manifest['hardware']) or transitions):
                errors.append('Screen trial differs from frozen grid: ' + manifest['run_id'])
            bounded = frozen['collection'].get('screen_metric', 'gpu-service') == 'bounded-system'
            score_invalid = bounded and frozen['collection'].get('screen_quality_policy', 'require-all-valid') == 'score-invalid-as-zero'
            if any(not ((r['termination'] == 'complete' and (r['quality_valid'] or score_invalid)) or
                        (bounded and r['termination'] == 'client_drop' and r['dispatch_s'] is None
                         and r['status_code'] is None and not r['quality_valid'])) for r in requests):
                errors.append('Screen contains client/transport/quality confounding: ' + manifest['run_id'])
        row = {'bundle': path.relative_to(root).as_posix(), 'run_id': manifest['run_id'],
            'role': role, 'calibration_number': calibration_number,
            'seed': spec['workload']['seed'], 'kind': spec['workload']['kind'],
            'long_prefix_mode': spec['workload'].get('long_prefix_mode', 'legacy'),
            'policy': spec['policy'], 'initial': [h['config_id'] for h in manifest['hardware']],
            'integrity_valid': checked['integrity_valid'], 'experiment_valid': checked['experiment_valid'],
            'metrics': {k:v for k,v in actual.items() if k != 'latencies'},
            'terminations': dict(Counter(r['termination'] for r in requests)),
            'quality_valid_requests': sum(r['quality_valid'] is True for r in requests),
            'transition_completed': any(t['state'] == 'COMPLETE' for t in transitions),
            'transition_end_s': max((t['at_s'] for t in transitions if t['state']=='COMPLETE'), default=None),
            'decision_records': len(decisions)}
        rows.append(row)
        if role == 'held-out':
            held_out.append(path)
    test_order = read(root / 'study-plan.json')['test_order']
    rank = {(r['seed'],r['policy']):i for i,r in enumerate(test_order)}
    held_out.sort(key=lambda p: rank[(read(p/'manifest.json')['experiment']['workload']['seed'],
                                   read(p/'manifest.json')['experiment']['policy'])])
    comparison = verifier.compare_bundles(held_out) if held_out else None
    independent_intervals = []
    if comparison:
        by_id = {r['run_id']:r for r in rows}
        for item in comparison['comparisons']:
            differences = [by_id[a]['metrics']['qualified']-by_id[b]['metrics']['qualified'] for a,b in item['run_pairs']]
            interval = None
            if len(differences) >= 3:
                rng = random.Random(713)
                draws = sorted(statistics.mean(rng.choices(differences, k=len(differences))) for _ in range(2000))
                interval = [draws[49], draws[1949]]
            independent_intervals.append({**item, 'differences': differences,
                'paired_bootstrap_95_interval': interval})
        if not comparison['integrity_valid'] or not comparison['experiment_valid']:
            for problem in comparison['errors']:
                checked = problem.get('verification')
                errors.append({'bundle': problem['bundle'],
                    'integrity': checked['integrity_errors'],
                    'experiment': checked['experiment_errors']} if checked else problem)
        if (root / 'paired-comparison.json').exists():
            for expected in read(root / 'paired-comparison.json'):
                actual = next(r for r in independent_intervals if r['baseline'] == expected['baseline'])
                if (actual['matched_trials'] != expected['matched_trials'] or
                    actual['mean_difference_requests'] != expected['difference_requests'] or
                    sorted(actual['run_pairs']) != sorted(expected['run_pairs'])):
                    errors.append('Paired comparison mismatch: ' + expected['baseline'])
                # Bootstrap uses the producer's declared pair order, not filesystem order.
                diffs = [by_id[a]['metrics']['qualified']-by_id[b]['metrics']['qualified'] for a,b in expected['run_pairs']]
                rng = random.Random(713)
                draws = sorted(statistics.mean(rng.choices(diffs, k=len(diffs))) for _ in range(2000)) if diffs else []
                if len(diffs) >= 3 and [draws[49], draws[1949]] != expected['paired_bootstrap_95_interval']:
                    errors.append('Bootstrap mismatch: ' + expected['baseline'])
    expected_test = set() if screen else {(s,p) for s in frozen['collection']['test_seeds']
        for p in ('StaticBest','SteadyStateFirst','FixedHysteresis','StateAware')}
    observed_test = [(r['seed'],r['policy']) for r in rows if r['role']=='held-out']
    complete = (status['state']==('SCREEN_COMPLETE' if screen else 'COMPLETE') and len(rows)==len(order)+len(expected_test)
        and len(observed_test)==len(expected_test) and set(observed_test)==expected_test)
    if screen:
        complete &= sorted(r['calibration_number'] for r in rows if r['role'] != 'held-out') == list(range(len(order)))
    result = {'campaign_state':status['state'], 'full_protocol_complete':complete and not screen,
        'screen_protocol_complete':complete and screen, 'purpose':'capacity-screen' if screen else 'qualification',
        'screen_metric':frozen['collection'].get('screen_metric','gpu-service') if screen else None,
        'screen_quality_policy':frozen['collection'].get('screen_quality_policy','require-all-valid') if screen else None,
        'verified_bundle_count':len(rows), 'expected_calibration_trials':len(order),
        'expected_held_out_trials':len(expected_test), 'errors':errors,
        'all_bundle_integrity_valid':all(r['integrity_valid'] for r in rows),
        'all_bundle_experiment_valid':all(r['experiment_valid'] for r in rows),
        'all_present_bundles_valid':not errors, 'producer_source_matches':source_matches,
        'rows':rows, 'paired_comparisons':independent_intervals,
        'certified':False, 'limitations':['Integrity is not source authenticity or independent certification',
            'Only declared arithmetic-task quality metadata is recomputed; no raw text regrading',
            'An incomplete protocol cannot establish the planned policy comparison']}
    (root/'independent-gpu-verification.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k not in ('rows','paired_comparisons')},indent=2))
    return 0 if complete and not errors else 1


if __name__ == '__main__':
    raise SystemExit(main())
