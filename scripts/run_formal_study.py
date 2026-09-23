"""Run one frozen two-GPU campaign, retain every attempt, and export on exit.

Provider shutdown is owned by the separately armed local budget watchdog.
"""
import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tarfile
import time
from pathlib import Path
from transitionbench.collection import CalibrationCollector, CollectionSpec, collection_plan, trial_spec
from transitionbench.lab import DockerLabAdapter, LabRouter, doctor
from transitionbench.preparation import StudyPreparation, prepare_study, ScreenBudget, prepare_capacity_screen
from transitionbench.rollout import stable_hash
from transitionbench.service import Service, paired_comparison
from transitionbench.verifier import verify_bundle
from transitionbench.research import require_research_ready, ResearchGateError
from run_cloud_short_check import SyntheticFailureCapture


def write(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, default=str) + '\n')
    temporary.replace(path)


def require_frozen_host(health, expected_driver):
    if not expected_driver or not health['controlled_ready'] or {g['driver'] for g in health['gpus']} != {expected_driver}:
        raise ValueError('Two-GPU identity/driver preflight refused')


def retain_worker_startup_evidence(out, adapter):
    """Capture owned engine startup failures before cleanup removes live state."""
    directory = out/'operator'
    directory.mkdir(exist_ok=True)
    evidence = {}
    for worker in adapter.workers:
        row = evidence[worker] = {}
        try:
            inspected = adapter.inspect(worker)  # Checks owner, device and pinned image.
            row['state'] = {k: inspected['State'].get(k) for k in
                            ('Status', 'Running', 'OOMKilled', 'ExitCode', 'StartedAt', 'FinishedAt')}
            result = subprocess.run(['docker', 'logs', '--timestamps', '--tail', '200', adapter.name(worker)],
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                    encoding='utf-8', errors='replace', timeout=5)
            text = result.stdout or ''
            row.update(log_exit_code=result.returncode, truncated=len(text) > 65536)
            (directory/f'worker-{worker}-startup.log').write_text(text[-65536:], encoding='utf-8')
        except Exception as exc:
            row['error_type'] = type(exc).__name__
    write(directory/'worker-startup.json', evidence)
    # Fixed read-only queries only: preserve the failure text that command()
    # intentionally omits from public errors. Never export environment/credentials.
    host = {}
    queries = {
        'nvidia_smi': ['nvidia-smi', '--query-gpu=name,uuid,driver_version', '--format=csv,noheader'],
        'loaded_driver': ['cat', '/proc/driver/nvidia/version'],
        'installed_driver_packages': ['dpkg-query', '-W', '-f=${binary:Package}\t${Version}\n',
                                      'nvidia-*', 'libnvidia-*'],
    }
    for name, args in queries.items():
        try:
            result = subprocess.run(args, capture_output=True, text=True, encoding='utf-8',
                                    errors='replace', timeout=5)
            stdout, stderr = result.stdout or '', result.stderr or ''
            host[name] = {'exit_code': result.returncode, 'stdout': stdout[-8192:],
                          'stderr': stderr[-8192:], 'truncated': max(len(stdout), len(stderr)) > 8192}
        except Exception as exc:
            host[name] = {'error_type': type(exc).__name__}
    write(directory/'host-gpu-diagnostics.json', host)


def retain_operator_evidence(out, adapter):
    """Keep only this campaign's referenced receipts, including failed probes."""
    directory = out/'operator'
    directory.mkdir(exist_ok=True)
    index = {'retained_warmup_receipts': [], 'missing_warmup_receipts': []}
    database = directory/'hook.db'
    index['hook_database_present'] = database.is_file()
    if database.is_file():
        with sqlite3.connect(database.as_uri()+'?mode=ro', uri=True) as db:
            keys = [row[0] for row in db.execute('SELECT key FROM operations ORDER BY rowid')]
        for key in keys:
            parts = key.split(':')
            if not ((len(parts) == 4 and parts[2] == 'warmup') or
                    (len(parts) == 3 and parts[2] == 'initial-warmup')):
                continue
            name = stable_hash(key)+'.json'
            source = adapter.cache/('transitionbench-'+adapter.owner)/'warmup'/name
            if source.is_file():
                (directory/'warmup').mkdir(exist_ok=True)
                shutil.copyfile(source, directory/'warmup'/name)
                index['retained_warmup_receipts'].append(key)
            else:
                # A pre-probe generation conflict legitimately has no probe receipt.
                index['missing_warmup_receipts'].append(key)
    write(directory/'evidence.json', index)


async def execute(root, protocol, approval, stop_at):
    frozen = json.loads(protocol.read_text())
    if stable_hash(frozen) != approval:
        raise ValueError('Exact frozen protocol hash required')
    value = CollectionSpec.model_validate(frozen['collection'])
    if not value.capacity_rates_rps or value.experiment.workload.injection_s < 20:
        raise ResearchGateError('Formal studies require a frozen multi-load capacity sweep before host operations')
    if value.experiment.workload.kind not in ('short', 'long-prefix'):
        raise ResearchGateError('This single-transition runner cannot admit a mixed-load or phase-change study; freeze and validate its scenario runner first')
    screen = value.purpose == 'capacity-screen'
    if screen:
        planned = collection_plan(value, frozen['operator_config'], frozen['code_revision'])
        if planned['plan_hash'] != frozen['collection_plan_hash']:
            raise ValueError('Screen plan differs from frozen acquisition')
        study = prepare_capacity_screen(value, planned, ScreenBudget.model_validate(frozen['screen_budget']))
        if stop_at-time.time() > study['stop_after_start_s']:
            raise ValueError('Screen deadline exceeds the frozen cost envelope')
    else:
        study = prepare_study(StudyPreparation.model_validate(frozen['preparation']))
    if study['status'] != 'READY_FOR_HOST_PREFLIGHT' or value.initial_cache_policy != 'fresh-workers':
        raise ValueError('Full budget and fresh initial condition required')
    out = root / 'evidence' / value.id
    out.mkdir(exist_ok=False)
    write(out/'frozen-study.json', frozen)
    write(out/'study-plan.json', study)
    state = {'state':'STARTING', 'approved_hash':approval, 'started_at_unix_s':time.time(), 'test_runs':[]}
    service = adapter = hook = None
    timers = {}
    active_capture = None
    slot = 2*value.experiment.budget.max_duration_s + 2*value.warmup.max_duration_s
    def status(**fields):
        state.update(fields)
        write(out/'status.json', state)
        print(json.dumps(fields), flush=True)
    def reserve(seconds):
        if time.time()+seconds+300 > stop_at:
            raise TimeoutError('Provider deadline cannot cover next stage and export; no retry')
    try:
        status(state='HOST_PREFLIGHT')
        reserve(600 + slot)
        health = doctor()
        write(out/'host.json', health)
        require_frozen_host(health, frozen['operator_config']['hook']['versions']['driver'])
        versions = {n:importlib.metadata.version(n) for n in frozen['dependencies']}
        if versions != frozen['dependencies']:
            raise ValueError('Dependency identity differs from frozen local validation')
        write(out/'runtime.json', {'python':sys.version,'dependencies':versions})
        config = frozen['operator_config'] if screen else json.loads((root/'formal-v5/operator.json').read_text())
        if stable_hash(config) != frozen['operator_config_hash']:
            raise ValueError('Operator configuration changed')
        service = Service(out/'service', config)
        service.store.acquire_owner()
        if service.code_revision != frozen['code_revision']:
            raise ValueError('Producer source changed')
        plan = collection_plan(value, config, service.code_revision)
        if plan['plan_hash'] != frozen['collection_plan_hash']:
            raise ValueError('Collection plan changed')
        for unit in ('apt-daily.service','apt-daily-upgrade.service'):
            if subprocess.run(['systemctl','is-active','--quiet',unit]).returncode == 0:
                raise ValueError('Package manager active')
        for unit in ('apt-daily.timer','apt-daily-upgrade.timer'):
            timers[unit] = subprocess.run(['systemctl','is-active',unit],capture_output=True,text=True).stdout.strip()
            subprocess.run(['systemctl','stop',unit],check=True,timeout=15)
        write(out/'timer-state.json', timers)
        adapter = DockerLabAdapter(json.loads((root/'lab.json').read_text()), LabRouter([8801,8802]))
        for worker in adapter.workers:
            obj = adapter.inspect(worker)
            if obj['Config']['Labels'].get('transitionbench.owner') != adapter.owner:
                raise ValueError('Worker ownership differs')
            if not obj['State']['Running']:
                subprocess.run(['docker','start',adapter.name(worker)],check=True,capture_output=True,timeout=30)
        status(state='WORKER_STARTUP')
        async with asyncio.timeout(360):
            while True:
                rows = await adapter.snapshot()
                if all(s.ready and s.process_id for s in rows):
                    break
                await asyncio.sleep(3)
        write(out/'initial-workers.json', [s.model_dump(mode='json') for s in rows])
        key, secret = (root/'hook.env').read_text().strip().split('=',1)
        os.environ[key] = secret
        with socket.socket() as sock:
            if sock.connect_ex(('127.0.0.1',8770)) == 0:
                raise ValueError('Unexpected existing hook; do not replace another owner')
        with (out/'hook.log').open('w') as log:
            hook = subprocess.Popen([str(root/'venv/bin/transitionbench'),'lab-hook',str(root/'lab.json'),
                '--port','8770','--data-dir',str(out/'operator')],stdin=subprocess.DEVNULL,
                stdout=log,stderr=log,env=os.environ.copy(),start_new_session=True)
        for _ in range(100):
            with socket.socket() as sock:
                if sock.connect_ex(('127.0.0.1',8770)) == 0:
                    break
            if hook.poll() is not None:
                raise ValueError('Owned hook failed')
            await asyncio.sleep(.1)
        else:
            raise TimeoutError('Hook startup')
        original_traffic = service.measured_traffic
        async def captured_traffic(run_id,spec,client,*args,**kwargs):
            original = client.measure
            capture = active_capture
            if capture is None:
                raise ValueError('Missing prebuilt synthetic output allowlist')
            async def measured(*a,**k):
                return await original(*a,**k,on_quality_failure=capture.record)
            client.measure = measured
            try:
                return await original_traffic(run_id,spec,client,*args,**kwargs)
            finally:
                client.measure = original
                write(out/(run_id+'-quality-failures.json'), capture.evidence())
        service.measured_traffic = captured_traffic
        collector = CalibrationCollector(service,service.hook_adapter(),value,out/'calibration',plan,
                                         require_discrimination=True)
        original_trial = collector.trial
        async def bounded_trial(number,trial):
            nonlocal active_capture
            reserve(slot)
            active_capture = SyntheticFailureCapture(trial_spec(value,trial).workload,formal_study=True)
            status(state='CALIBRATING',trial_number=number,trial=trial)
            async with asyncio.timeout(slot):
                await original_trial(number,trial)
            status(completed_calibration_trials=len(collector.measurements))
        collector.trial = bounded_trial
        result = await collector.run(plan['plan_hash'])
        if result['state'] != ('SCREEN_COMPLETE' if screen else 'QUALIFIED'):
            raise ValueError('Real GPU calibration required')
        if not screen:
            calibration = json.loads((out/'calibration/qualified.json').read_text())
            write(out/'research-readiness.json', calibration['research_readiness'])
            require_research_ready(calibration)
            service.config.update(json.loads((out/'calibration/registration.json').read_text()))
        write(out/'registered-operator.json', service.config)
        for number,trial in enumerate(study['test_order']):
            reserve(slot)
            workload = value.experiment.workload.model_copy(update={'seed':trial['seed'],'split':'test'})
            active_capture = SyntheticFailureCapture(workload,formal_study=True)
            status(state='TESTING',trial_number=number,trial=trial)
            async with asyncio.timeout(slot):
                before = await collector.prepare(calibration['static_best'])
                target = 'B' if calibration['static_best']=='A' else 'A'
                child = collector.make_plan(target,before)
                spec = value.experiment.model_copy(update={'policy':trial['policy'],'workload':workload,
                    'calibration_id':value.id,'plan_id':child['id']})
                collector.reserve('held_out',spec.budget.max_requests,spec.budget.max_total_tokens,spec.budget.max_duration_s)
                job = await service.submit(spec,'held-out-'+str(number))
                await service.tasks[job['id']]
            job = service.get(job['id'])
            bundle = service.store.root/'runs'/job['id']/'bundle'
            checked = verify_bundle(bundle) if bundle.exists() else None
            state['test_runs'].append({'number':number,'seed':trial['seed'],'policy':trial['policy'],
                'run_id':job['id'],'state':job['state'],'error':job.get('error'),'verification':checked})
            status(completed_test_trials=len(state['test_runs']))
            if job['state']!='SUCCEEDED' or not checked or not checked['integrity_valid'] or not checked['experiment_valid']:
                raise ValueError('Invalid held-out trial; retained, no replacement or paid retry')
        if not screen:
            write(out/'paired-comparison.json',paired_comparison([service.get(r['run_id']) for r in state['test_runs']]))
        status(state='SCREEN_COMPLETE' if screen else 'COMPLETE', certified=False)
    except BaseException as exc:
        status(state='INCONCLUSIVE' if isinstance(exc, ResearchGateError) else 'FAILED',
               error_type=type(exc).__name__,error=str(exc)[:500])
    finally:
        if service is not None:
            await service.close()
        if hook is not None:
            hook.terminate()
            try: hook.wait(timeout=10)
            except subprocess.TimeoutExpired:
                hook.kill()
                hook.wait(timeout=10)
        if adapter is not None:
            if state['state'] == 'FAILED':
                try:
                    retain_worker_startup_evidence(out, adapter)
                except Exception as exc:
                    state['worker_evidence_error'] = type(exc).__name__
            for worker in adapter.workers:
                try:
                    adapter.inspect(worker)
                    subprocess.run(['docker','stop','--time','10',adapter.name(worker)],capture_output=True,timeout=25)
                except Exception as exc:
                    state.setdefault('cleanup_errors',[]).append(type(exc).__name__)
        for unit,previous in timers.items():
            if previous=='active':
                subprocess.run(['systemctl','start',unit],capture_output=True,timeout=15)
        if adapter is not None:
            try:
                retain_operator_evidence(out, adapter)
            except Exception as exc:
                status(state='FAILED', export_evidence_error=type(exc).__name__)
        status(completed_at_unix_s=time.time())
        archive = root/(value.id+'.tgz')
        with tarfile.open(archive,'w:gz') as tar:
            tar.add(out,arcname=value.id)
        receipt = {'archive':str(archive),'bytes':archive.stat().st_size,
            'sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'state':state['state']}
        write(root/(value.id+'-export.json'),receipt)
        print(json.dumps(receipt),flush=True)
    return 0 if state['state'] in ('COMPLETE', 'SCREEN_COMPLETE') else 1


def cli():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path('/opt/transitionbench'))
    parser.add_argument('--protocol',type=Path,required=True)
    parser.add_argument('--approve',required=True)
    parser.add_argument('--stop-at',type=float,required=True)
    args=parser.parse_args()
    return asyncio.run(execute(args.root,args.protocol,args.approve,args.stop_at))


if __name__=='__main__':
    raise SystemExit(cli())
