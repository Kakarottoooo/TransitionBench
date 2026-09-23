"""Install built packages into fresh environments and exercise real interfaces."""
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
import zipfile
import tomllib
from pathlib import Path
import httpx

ROOT=Path(__file__).resolve().parents[1]
WORK=ROOT/'work'/('clean-'+uuid.uuid4().hex[:8]);WORK.mkdir(parents=True)
env={**os.environ,'UV_CACHE_DIR':str(ROOT/'work'/'uv-cache'),'npm_config_cache':str(ROOT/'work'/'npm-cache'),'TRANSITIONBENCH_API':'http://127.0.0.1:8785'}
env.pop('PYTHONPATH',None)
checks=[]


def run(args,cwd=WORK,timeout=90,expected_exit=0):
    started=time.monotonic()
    result=subprocess.run([str(a) for a in args],cwd=cwd,env=env,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=timeout)
    checks.append({'command':[str(a) for a in args],'exit_code':result.returncode,'elapsed_s':time.monotonic()-started,'stdout_tail':result.stdout[-1200:],'stderr_tail':result.stderr[-1200:]})
    if result.returncode!=expected_exit:raise RuntimeError(result.stdout+'\n'+result.stderr)
    return result.stdout


def main():
    run(['uv','venv','--python',sys.executable,'--seed',WORK/'python'])
    python=WORK/'python'/('Scripts/python.exe' if os.name=='nt' else 'bin/python')
    cli=WORK/'python'/('Scripts/transitionbench.exe' if os.name=='nt' else 'bin/transitionbench')
    version=tomllib.loads((ROOT/'pyproject.toml').read_text())['project']['version']
    wheel=ROOT/'dist'/f'transitionbench-{version}-py3-none-any.whl'
    run(['uv','pip','install','--python',python,wheel])
    install_path=run([python,'-c','import transitionbench;print(transitionbench.__file__)']).strip()
    if str(WORK) not in install_path:raise ValueError('Package leaked from development environment')
    run([cli,'--help']);run([cli,'doctor'])
    prepared=json.loads(run([cli,'prepare-study',ROOT/'examples'/'study-preparation.json']))
    assert prepared['cloud_authorization'] is False and len(prepared['test_order'])==20
    refused=json.loads(run([cli,'calibration-plan',ROOT/'examples/calibration-collection.json',
        '--operator',ROOT/'examples/lab-operator.json','--data-dir',WORK/'collection-state',
        '--output',WORK/'collection-plan.json'],expected_exit=1))
    assert refused['status']=='BUDGET_REFUSED' and len(refused['order'])==34
    assert refused['maximum_reservations']['wall_s']==19380
    run([cli,'calibration-collect','--help'])
    runtime=json.loads(run([python,'-c','import sys,sysconfig,json;print(json.dumps([sys._base_executable,sysconfig.get_path("purelib")]))']))
    bootstrap="import sys;sys.path.insert(0,sys.argv[1]);from transitionbench.cli import main;main(['demo','--port','8785','--data-dir',sys.argv[2]])"
    log=(WORK/'server.log').open('w',encoding='utf-8')
    process=subprocess.Popen([runtime[0],'-c',bootstrap,runtime[1],str(WORK/'data')],cwd=WORK,env=env,stdout=log,stderr=log)
    try:
        base='http://127.0.0.1:8785'
        for _ in range(200):
            try:
                if httpx.get(base+'/healthz',timeout=1).status_code==200:break
            except httpx.HTTPError:pass
            time.sleep(.05)
        else:raise RuntimeError('Fresh-installed service did not start')
        html=httpx.get(base+'/').text
        assert '<div id="root">' in html and 'assets/index-' in html
        command=[cli,'--api',base]
        run(command+['validate',ROOT/'examples'/'simulation.json'])
        result=json.loads(run(command+['run',ROOT/'examples'/'simulation.json','--wait']))
        assert result['state']=='SUCCEEDED' and result['origin']=='synthetic'
        run(command+['inspect',result['id']])
        run(command+['export',result['id'],WORK/'evidence.zip'])
        with zipfile.ZipFile(WORK/'evidence.zip') as archive:archive.extractall(WORK/'bundle')
        verified=json.loads(run([cli,'verify',WORK/'bundle']))
        assert verified['integrity_valid'] and verified['experiment_valid']
        run(command+['cancel',result['id']])
        run(command+['decision',ROOT/'examples'/'decision.json'])
        run([python,ROOT/'examples'/'python_client.py'])
        run([python,ROOT/'examples'/'mcp_client.py'])
        spec=WORK/'curl.json';spec.write_text('{"mode":"SIMULATION"}',encoding='utf-8')
        curl=shutil.which('curl.exe') or shutil.which('curl')
        curl_run=json.loads(run([curl,'--fail-with-body','--silent','--show-error',base+'/api/v1/runs','-H','Content-Type: application/json','-H','X-TransitionBench: 1','-H','Idempotency-Key: clean-curl','--data-binary','@'+str(spec)]))
        assert curl_run['id']
        node=WORK/'node';node.mkdir();(node/'package.json').write_text('{"private":true,"type":"module"}',encoding='utf-8')
        npm=shutil.which('npm.cmd') or shutil.which('npm')
        run([npm,'install','--no-audit','--no-fund',ROOT/'dist'/'transitionbench-local-client-0.1.0.tgz'],node)
        shutil.copyfile(ROOT/'examples'/'typescript_client.mjs',node/'check.mjs')
        run(['node','check.mjs'],node)
        contracts=httpx.get(base+'/openapi.json').json()
        pinned=json.loads((ROOT/'docs'/'openapi.json').read_text(encoding='utf-8'))
        assert contracts==pinned
        write={'status':'PASS','fresh_directory':str(WORK),'wheel_import_path':install_path,'run_id':result['id'],
               'interfaces':['wheel CLI','packaged web assets','HTTP','Python SDK','npm tarball SDK','curl','MCP stdio handshake/tool'],
               'openapi_agreement':True,'checks':checks}
        (ROOT/'reports'/'clean-install.json').write_text(json.dumps(write,indent=2),encoding='utf-8')
        print(json.dumps({'status':'PASS','checks':len(checks),'directory':str(WORK)},indent=2))
    finally:
        process.terminate();process.wait(timeout=15);log.close()


if __name__=='__main__':main()
