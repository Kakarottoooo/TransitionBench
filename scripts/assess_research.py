"""Offline admission/repayment analysis from verified calibration bundles.

Never contacts an endpoint or changes the source campaign. Exit 2 means valid
measurements are insufficient for the proposed research comparison.
"""
import argparse
import hashlib
import json
from pathlib import Path
from transitionbench.calibration import qualify_calibration
from transitionbench.research import research_gate


def assess(source):
    value = qualify_calibration(source)
    estimates = []
    for pair, cost in value['costs'].items():
        direction, kind = pair.split(':')
        current, candidate = direction.split('>')
        delta = value['rates'][candidate][kind] - value['rates'][current][kind]
        for bucket, sample in cost['by_queue'].items():
            timing = cost['timing'][bucket]
            estimates.append({'direction': pair, 'queue_bucket': bucket,
                'measured_gain_rps': delta, 'mean_deficit_requests': sample['mean'],
                'observed_deficit_samples': sample['samples'],
                'break_even_s': sample['mean']/delta if delta > 0 else None,
                'conservative_break_even_s': (sample['mean']+sample['spread'])/delta if delta > 0 else None,
                'reason': 'NO_POSITIVE_MEASURED_GAIN' if delta <= 0 else 'ESTIMATE_NOT_HELD_OUT_PROOF',
                'all_changes_finish_during_injection': all(t['complete_s'] < t['injection_s'] for t in timing),
                'timing': timing})
    package = Path(__file__).resolve().parents[1]/'src/transitionbench'
    digest = hashlib.sha256()
    for path in sorted(package.glob('*.py')):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return {'campaign_id': value['id'], 'source_index_sha256': value['source_sha256'],
        'evidence_hash': value['qualification_hash'], 'analysis_source_revision': 'sha256:'+digest.hexdigest(),
        'source_bundle_count': len(value['evidence_ids']), 'analysis_only': True,
        'new_gpu_runs': 0, 'certified': False,
        'capacity': value['capacity_diagnostic'], 'repayment_estimates': estimates,
        'admission': research_gate(value),
        'historical_results': 'Original campaign reports and frozen producer remain unchanged'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Output exists; retain prior analyses rather than overwriting evidence')
    result = assess(args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
        stream.write('\n')
    print(json.dumps({'ready': result['admission']['ready'], 'output': str(args.output),
                      'reasons': result['admission']['reasons']}))
    return 0 if result['admission']['ready'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
