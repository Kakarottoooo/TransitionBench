"""Recompute an entire study with only the independent verifier + stdlib."""
import argparse
import importlib.util
import json
from pathlib import Path

parser=argparse.ArgumentParser();parser.add_argument('study');args=parser.parse_args()
root=Path(args.study).resolve()
spec=importlib.util.spec_from_file_location('standalone_verifier',Path(__file__).resolve().parents[1]/'src'/'transitionbench'/'verifier.py')
verifier=importlib.util.module_from_spec(spec);spec.loader.exec_module(verifier)
index=json.loads((root/'synthetic-trials.json').read_text(encoding='utf-8'))
producer=json.loads((root/'paired-comparisons.json').read_text(encoding='utf-8'))
results={}
for case in sorted({r['case'] for r in index}):
    result=verifier.compare_bundles([root/r['bundle'] for r in index if r['case']==case])
    for actual,expected in zip(result['comparisons'],producer[case]):
        if actual['mean_difference_requests']!=expected['difference_requests'] or actual['matched_trials']!=expected['matched_trials']:
            raise ValueError('Independent comparison disagreement')
    if not result['integrity_valid'] or not result['experiment_valid']:raise ValueError(result)
    results[case]=result
local=json.loads((root/'local-http-trials.json').read_text(encoding='utf-8'))
results['local_http']=[{'run_id':r['id'],**verifier.verify_bundle(root/r['bundle'])} for r in local]
(root/'independent-study-verification.json').write_text(json.dumps(results,indent=2),encoding='utf-8')
print(json.dumps({'synthetic_bundles':len(index),'cases':len(results)-1,'local_http_bundles':len(local),
                  'local_http_valid':sum(r['experiment_valid'] for r in results['local_http']),
                  'comparison_recomputation':'PASS','certified':False},indent=2))
