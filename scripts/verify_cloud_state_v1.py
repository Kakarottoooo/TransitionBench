"""Verify cloud-state-v1 archive and recompute both stages using only Python stdlib."""
import hashlib,json,subprocess,sys,tarfile,time,zipfile
from pathlib import Path,PurePosixPath
archive=Path(sys.argv[1]).resolve();target=Path(sys.argv[2]).resolve();assert not target.exists(),'Use a new, short extraction directory'
start=time.time();sha=hashlib.sha256(archive.read_bytes()).hexdigest()
expected=archive.with_name(archive.name+'.sha256').read_text(encoding='ascii').split()[0];assert sha==expected
with zipfile.ZipFile(archive) as z:
 manifest=json.loads(z.read('SHA256SUMS.json'));assert set(z.namelist())==set(manifest)|{'SHA256SUMS.json'} and len(z.namelist())==len(manifest)+1
 for name,h in manifest.items():
  p=PurePosixPath(name);assert not p.is_absolute() and '..' not in p.parts and ':' not in name
  assert hashlib.sha256(z.read(name)).hexdigest()==h
 target.mkdir(parents=True);z.extractall(target)
base=target/'outputs/transitionbench/reports/cloud-state-v1';checks={}
for stage in ('screen','mechanism'):
 # Preserve raw transport archives, including incidental producer bytecode omitted from the outer file tree.
 with tarfile.open(base/(stage+'-evidence.tgz')) as tar:
  for m in tar:
   p=PurePosixPath(m.name);assert not p.is_absolute() and '..' not in p.parts and ':' not in m.name and (m.isfile() or m.isdir())
   dest=base/stage/m.name
   if m.isfile():
    blob=tar.extractfile(m).read()
    if dest.exists():assert dest.read_bytes()==blob
    else:dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(blob)
 original=json.loads((base/(stage+'-analysis.json')).read_text(encoding='utf-8'))
 proc=subprocess.run([sys.executable,str(target/'work/cloud-state-stage.py'),stage],capture_output=True,text=True,timeout=90)
 assert proc.returncode==0,(proc.stdout,proc.stderr)
 fresh=json.loads((base/(stage+'-analysis.json')).read_text(encoding='utf-8'));assert fresh==original,stage+' recomputation mismatch'
 checks[stage]={'exact_analysis_match':True,'valid_contracts':fresh['valid'],'engine_crash_trials':fresh['engine_crash_trials'],'deployment_usable':fresh['deployment_usable']}
assert hashlib.sha256(archive.read_bytes()).hexdigest()==sha
receipt={'archive':archive.name,'sha256':sha,'bytes':archive.stat().st_size,'members':len(manifest)+1,'every_member_sha256_verified':True,'extracted_to':str(target),'extracted_analysis':checks,'elapsed_s':time.time()-start,'no_gpu_or_network_used_for_recomputation':True}
(target/'verification.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8');print(json.dumps(receipt,indent=2))
