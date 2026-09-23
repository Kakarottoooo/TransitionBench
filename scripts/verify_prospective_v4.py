"""Recompute the sealed v4 archive without trusting reported metrics."""
import hashlib,importlib.util,json,subprocess,sys,zipfile
from pathlib import Path
import argparse
parser=argparse.ArgumentParser(description="CPU-only verification of the sealed TransitionBench v4 experiment. No GPU, API key or extra packages.")
parser.add_argument("archive",type=Path)
parser.add_argument("extract_to",type=Path,help="A new directory; existing directories are refused")
args=parser.parse_args()
read=lambda p:json.loads(p.read_text(encoding='utf-8-sig'))
archive=args.archive
target=args.extract_to
receipt={'sha256':'d445b19c1b7b9e9dff384a442d8697ce9bc57dd9be181b25512cdea278158849'}
assert hashlib.sha256(archive.read_bytes()).hexdigest()==receipt['sha256']
target.mkdir(exist_ok=False)
with zipfile.ZipFile(archive) as z:
    hashes=json.loads(z.read('manifest-sha256.json'))
    assert set(z.namelist())==set(hashes)|{'manifest-sha256.json'}
    for name,sha in hashes.items():
        assert (target/name).resolve().is_relative_to(target.resolve())
        assert hashlib.sha256(z.read(name)).hexdigest()==sha
    z.extractall(target)
loader=importlib.util.spec_from_file_location('replay',target/'work/analyze-local-switch-study.py')
replay=importlib.util.module_from_spec(loader);loader.loader.exec_module(replay)
run=target/'outputs/transitionbench/reports/local-cache-prospective-v4'
expected=read(run/'analysis.json')
replayed=replay.analyze(run)
assert replayed==expected
folder=target/'outputs/transitionbench/reports/deployment-prospective-v4'
for name,sha in read(folder/'freeze-sha256.json').items():
    assert hashlib.sha256((folder/name).read_bytes()).hexdigest()==sha
assert read(folder/'prospective-protocol.json')['frozen_at_unix_s']<read(run/'status.json')['started_unix_s']
checks=[]
for h in (20,40,120):
    cmd=[sys.executable,str(target/'outputs/transitionbench/scripts/verify_proposal_arithmetic.py'),
         str(folder/f'forecast-{h}.json'),str(target/'outputs/transitionbench/reports/deployment-review-v1/bundles')]
    checks.append(json.loads(subprocess.check_output(cmd,text=True)))
def good(policy,seed):
    bundle=run/f'prospective-{seed}-{policy}'
    spec=read(bundle/'manifest.json')['experiment']
    rows=[json.loads(x) for x in (bundle/'requests.jsonl').read_text().splitlines() if x]
    return sorted(r['completed_s'] for r in rows if replay.qualified(r,spec))
rows=[]
for r in read(folder/'result.json')['comparisons']:
    seed,h=r['seed'],r['horizon_s']
    events=[json.loads(x) for x in (run/f'prospective-{seed}-forced/transitions.jsonl').read_text().splitlines() if x]
    start=next(e['at_s'] for e in events if e['state']=='STOPPING')
    ready=next(e['at_s'] for e in events if e['state']=='COMPLETE')-start
    a,b=good('keep',seed),good('forced',seed)
    curve=[sum(start<=x<=start+t for x in b)-sum(start<=x<=start+t for x in a) for t in range(h+1)]
    payback=next((t for t in range(h+1) if curve[t]>0 and min(curve[t:])>=0),None)
    assert curve[-1]==r['measured_switch_minus_keep'] and payback==r['repayment_s']
    rows.append(dict(seed=seed,horizon_s=h,net=curve[-1],cumulative_payback_s=payback,
        first_positive_grid_s=next((t for t,v in enumerate(curve) if v>0),None)))
result=dict(status='PASS',members=len(hashes),archive_sha256=receipt['sha256'],
    raw_trial_replay_equal=True,freeze_precedes_measurement=True,frozen_forecasts=checks,
    independent_outcomes=rows,scope='Raw arithmetic and integrity, not hardware authenticity or policy superiority')
(target/'verification.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))


