"""Bounded compatibility diagnostic; cloud execution requires separate authorization."""
import argparse, asyncio, gc, hashlib, importlib.metadata, json, os, socket
import sqlite3, subprocess, sys, tarfile, time
from pathlib import Path
from contextlib import contextmanager
from transitionbench.collection import CalibrationCollector, CollectionSpec, trial_spec
from transitionbench.lab import DockerLabAdapter, LabRouter, doctor
from transitionbench.rollout import stable_hash
from transitionbench.service import Service
from transitionbench.verifier import verify_bundle
from transitionbench.workloads import generate

args=None
R=Path('/opt/transitionbench')
RUN_ID='sender-cloud-quality-v2'
O=R/'evidence'/RUN_ID
EXPECTED='sha256:9c89b8c6c98c3b698d713daaf316a8d167b561defdf97a941afc73132f5141a7'
state={}
timers={}; adapter=None; service=None


class DiagnosticFailure(ValueError):
    def __init__(self, acceptance):
        super().__init__('Short diagnostic quality/request acceptance failed')
        self.acceptance = acceptance


class SyntheticFailureCapture:
    """Bounded diagnostic sidecar; never persist prompts or unrecognized requests."""
    MAX_RECORDS = 32
    MAX_CHARS = 256

    def __init__(self, workload, *, formal_study=False):
        self.split = workload.split
        allowed_splits = ('calibration', 'tuning', 'test') if formal_study else ('calibration',)
        if workload.split not in allowed_splits or workload.arrival_model != 'open-loop':
            raise ValueError('Output capture requires generated open-loop calibration requests')
        # Build before the collector starts its injection clock, not per response.
        self.allowed = {item.request_id: (item.expected, hashlib.sha256(item.prompt.encode()).hexdigest())
                        for item in generate(workload)}
        self.records = []
        self.omitted = 0
        self.rejected = 0

    def record(self, item, row, content):
        if row.termination != 'complete' or row.quality_valid:
            return
        expected = self.allowed.get(item.request_id)
        if (expected is None or expected[0] != item.expected or row.request_id != item.request_id
                or expected[1] != hashlib.sha256(item.prompt.encode()).hexdigest()):
            self.rejected += 1
            return
        if len(self.records) >= self.MAX_RECORDS:
            self.omitted += 1
            return
        self.records.append({'request_id': item.request_id, 'expected': item.expected,
            'prompt_sha256': expected[1], 'actual_content': content[:self.MAX_CHARS],
            'actual_chars': len(content), 'truncated': len(content) > self.MAX_CHARS,
            'quality_valid': row.quality_valid, 'quality_check': row.quality_check,
            'finish_reason': row.finish_reason, 'status_code': row.status_code,
            'scheduled_s': row.scheduled_s, 'completed_s': row.completed_s})

    def evidence(self):
        return {'schema_version': 'diagnostic-quality-1', 'scope': 'generated ' + self.split + ' failures only',
                'max_records': self.MAX_RECORDS, 'max_content_chars': self.MAX_CHARS,
                'captured': len(self.records), 'omitted_due_to_cap': self.omitted,
                'rejected_unknown_request': self.rejected, 'records': self.records}


@contextmanager
def capture_outputs(client, capture, run_id):
    original = client.measure

    async def measured(item, *pos, **kw):
        return await original(item, *pos, **kw, on_quality_failure=capture.record)

    client.measure = measured
    try:
        yield
    finally:
        client.measure = original
        # Persist after traffic finishes, outside the response callback.
        name = run_id + '-quality-failures.json'
        save(name, capture.evidence())
        state.setdefault('quality_capture_files', []).append({
            'path': name, 'sha256': hashlib.sha256((O / name).read_bytes()).hexdigest()})


def assess_requests(path):
    """Correct completed responses are mandatory; offered overload drops remain losses."""
    rows = [json.loads(line) for line in Path(path).read_text(encoding='utf-8').splitlines() if line]
    completed = [r for r in rows if r.get('termination') == 'complete']
    invalid = [r['request_id'] for r in completed if not (
        r.get('quality_valid') is True and r.get('finish_reason') == 'stop'
        and r.get('status_code') == 200 and r.get('output_chars', 0) > 0)]
    failed = [r['request_id'] for r in rows if r.get('termination') not in ('complete', 'client_drop')]
    passed = bool(completed) and not invalid and not failed
    return {'status': 'PASSED' if passed else ('FAILED_QUALITY_GATE' if invalid else 'FAILED_REQUEST_GATE'),
            'offered': len(rows), 'complete': len(completed),
            'client_drop': sum(r.get('termination') == 'client_drop' for r in rows),
            'quality_invalid_complete_ids': invalid, 'failed_request_ids': failed,
            'no_completed_responses': not completed}


def assess_bundle(path):
    """Read-only replay; never initialize cloud services or mutate the bundle."""
    checked = verify_bundle(path)
    if not checked['integrity_valid'] or not checked['experiment_valid']:
        return {'status': 'FAILED_MEASUREMENT_GATE',
                'integrity_errors': checked['integrity_errors'],
                'experiment_errors': checked['experiment_errors']}
    return {**assess_requests(Path(path) / 'requests.jsonl'),
            'integrity_valid': True, 'experiment_valid': True,
            'qualified': checked['recomputed']['qualified']}


def save(name,value): (O/name).write_text(json.dumps(value,indent=2,default=str)+'\n')
def status(**kw): state.update(kw); save('status.json',state); print(json.dumps(kw),flush=True)
def reserve(seconds):
    if time.time()+seconds+180>args.stop_at: raise TimeoutError('Insufficient remaining time plus 180s export/stop reserve')
def command(*argv,timeout=30): return subprocess.check_output(argv,text=True,timeout=timeout).strip()

async def run_trial(collector, number, trial):
    await collector.trial(number, trial)
    measurement = collector.measurements[-1]
    state['runs'].append(measurement)
    # Collection has already verified the measurement and exported its journal.
    # Preserve the trial before enforcing the stricter diagnostic acceptance.
    measurement['acceptance'] = assess_requests(O / measurement['bundle'] / 'requests.jsonl')
    save('status.json', state)
    if measurement['acceptance']['status'] != 'PASSED':
        raise DiagnosticFailure(measurement['acceptance'])


async def main():
    global adapter,service
    reserve(90)
    versions={n:importlib.metadata.version(n) for n in ('httpx','httpcore','anyio','sniffio','uvicorn','pydantic')}
    save('runtime.json',{'python':sys.version,'versions':versions,'clock':vars(time.get_clock_info('monotonic'))})
    expected=json.loads((R/'short-check/expected-dependencies.json').read_text())
    if versions != expected: raise ValueError('Dependency identity differs from local sustained validation')
    if not gc.isenabled(): raise ValueError('Normal GC required')
    health=doctor();save('host.json',health)
    if not health['controlled_ready']: raise ValueError('GPU doctor refused')
    if {g['driver'] for g in health['gpus']} != {'580.178.04'}: raise ValueError('Driver identity changed')
    for unit in ('apt-daily.service','apt-daily-upgrade.service'):
        if subprocess.run(['systemctl','is-active','--quiet',unit]).returncode==0: raise ValueError('Package manager active')
    for unit in ('apt-daily.timer','apt-daily-upgrade.timer'):
        timers[unit]=subprocess.run(['systemctl','is-active',unit],capture_output=True,text=True).stdout.strip()
        command('systemctl','stop',unit)
    save('timer-state.json',timers)
    status(state='CPU_CONTROL')
    cpu_label=RUN_ID+'-cpu-9051'
    with (O/'cpu-control.log').open('w') as log:
        result=subprocess.run([sys.executable,str(R/'short-check/transport-stage-probe.py'),'--mode','baseline','--seed','9051','--seconds','60','--label',cpu_label],stdout=log,stderr=log,timeout=90)
    cpu_path=R/'short-check/dispatch-stage'/cpu_label
    import shutil
    shutil.copytree(cpu_path,O/'cpu-control')
    cpu=json.loads((cpu_path/'result.json').read_text())
    if result.returncode or not cpu['validity']['valid'] or cpu['terminations']!={'complete':9216} or cpu['trace_failures']:
        raise ValueError('CPU control failed; no GPU traffic will run')
    state['cpu_acceptance'] = assess_requests(O / 'cpu-control/requests.jsonl')
    save('status.json', state)
    if state['cpu_acceptance']['status'] != 'PASSED':
        raise DiagnosticFailure(state['cpu_acceptance'])
    reserve(360)
    config=json.loads((R/'lab.json').read_text())
    adapter=DockerLabAdapter(config,LabRouter([8801,8802]))
    for worker in adapter.workers:
        obj=adapter.inspect(worker)
        if obj['Config']['Labels'].get('transitionbench.owner')!='vast51687766': raise ValueError('Worker ownership differs')
        if not obj['State']['Running']: command('docker','start',adapter.name(worker))
    status(state='WORKER_STARTUP')
    async with asyncio.timeout(360):
        while True:
            rows=await adapter.snapshot()
            if all(s.ready for s in rows): break
            await asyncio.sleep(3)
    save('initial-workers.json',[s.model_dump(mode='json') for s in rows])
    initial={s.config_id for s in rows}
    if len(initial)!=1 or not initial <= {'A','B'}: raise ValueError('Workers must start at one existing known configuration')
    initial=initial.pop(); target='A' if initial=='B' else 'B'
    key,secret=(R/'hook.env').read_text().strip().split('=',1);os.environ[key]=secret
    with socket.socket() as sock: listening=sock.connect_ex(('127.0.0.1',8770))==0
    if listening: raise ValueError('Expected stopped hook after VM start; inspect existing owner before replacement')
    with (O/'hook.log').open('w') as log:
        hook=subprocess.Popen([str(R/'venv/bin/transitionbench'),'lab-hook',str(R/'lab.json'),'--port','8770','--data-dir',str(R/'hook')],env=os.environ.copy(),stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
    (R/'hook.pid').write_text(str(hook.pid))
    for _ in range(50):
        with socket.socket() as sock: ready=sock.connect_ex(('127.0.0.1',8770))==0
        if ready: break
        if hook.poll() is not None: raise ValueError('Owned hook failed')
        await asyncio.sleep(.1)
    else: raise TimeoutError('Hook not listening')
    frozen=json.loads((R/'replacement-study.json').read_text())
    data=frozen['collection'];data['id']=RUN_ID;data['max_wall_s']=900
    data['max_requests']=100000;data['max_total_tokens']=200000000
    value=CollectionSpec.model_validate(data)
    operator=json.loads((R/'evidence/vast51687766-mixed-v3/registered-operator.json').read_text())
    service=Service(O/'service',operator); service.store.acquire_owner()
    if service.code_revision!=EXPECTED: raise ValueError('Source identity differs')
    protocol={'source':EXPECTED,'versions':versions,'initial':initial,'target':target,'seeds':[9052,9053],
              'stop_at_unix_s':args.stop_at,'export_reserve_s':180,'experiment':value.experiment.model_dump(mode='json'),
              'claim':'Two compatibility measurements, not a qualified calibration or held-out study',
              'cache_scope':'Retained initial cache; no comparative policy claim',
              'failure_rule':'Stop on invalid measurement, any incorrect completed response or request failure; no replacement',
              'diagnostic_acceptance':'Every completed response must pass exact quality, HTTP 200 and finish=stop; at least one completion; client_drop remains in denominator',
              'failure_output_capture': {'scope':'generated calibration failures only',
                  'max_records_per_trial':SyntheticFailureCapture.MAX_RECORDS,
                  'max_content_chars':SyntheticFailureCapture.MAX_CHARS,
                  'prompts_retained':False, 'successful_outputs_retained':False},
              'runner_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    save('protocol.json',protocol)
    collector=CalibrationCollector(service,service.hook_adapter(),value,O,{'plan_hash':stable_hash(protocol)})
    original=service.measured_traffic
    captures={}
    async def measured(run_id,spec,client,*pos,**kw):
        start=time.monotonic(); stages={}; pending={}; failures=[]; gaps=[]; pauses=[]; gc_starts={}
        def record(name,begin):
            elapsed=time.monotonic()-begin; item=stages.setdefault(name,{'count':0,'max_s':0,'sum_s':0})
            item['count']+=1;item['sum_s']+=elapsed;item['max_s']=max(item['max_s'],elapsed)
        request=client.request
        def traced(*a,**k):
            begin=time.monotonic();req=request(*a,**k);record('construct_and_resolve',begin);identity=id(req)
            async def trace(event,info):
                stage=event.rsplit('.',1)[0]
                if event.endswith('.started'):pending[identity,stage]=time.monotonic()
                elif event.endswith(('.complete','.failed')):
                    before=pending.pop((identity,stage),None)
                    if before is not None:record(stage,before)
                    if event.endswith('.failed'):failures.append({'stage':event,'type':type(info.get('exception')).__name__,'at_s':time.monotonic()-start})
            req.extensions['trace']=trace;return req
        client.request=traced
        def gc_event(phase,info):
            gen=info['generation'];now=time.monotonic()
            if phase=='start':gc_starts[gen]=now
            elif gen in gc_starts:pauses.append({'at_s':gc_starts[gen]-start,'duration_s':now-gc_starts[gen]})
        async def beat():
            before=time.monotonic()
            while True:
                await asyncio.sleep(.01);now=time.monotonic()
                if now-before>.025:gaps.append({'at_s':before-start,'gap_s':now-before})
                before=now
        gc.callbacks.append(gc_event);task=asyncio.create_task(beat())
        try:
            with capture_outputs(client,captures[spec.workload.seed],run_id):
                return await original(run_id,spec,client,*pos,**kw)
        finally:
            gc.callbacks.remove(gc_event);task.cancel();await asyncio.gather(task,return_exceptions=True)
            save(run_id+'-timing.json',{'stages':stages,'http_failures':failures,'gc':pauses,'loop_gaps':gaps})
    service.measured_traffic=measured
    for number,role,seed in ((1,'fixed',9052),(2,'transition',9053)):
        reserve(390)
        status(state='MEASURING',role=role,seed=seed)
        trial={'role':role,'initial':initial,'kind':'mixed-burst','seed':seed,'target':target,'source':initial,'pair':'diagnostic-only'}
        captures[seed]=SyntheticFailureCapture(trial_spec(value,trial).workload)
        await run_trial(collector,number,trial)
    status(state='PASSED')

async def run_and_close():
    try:await main()
    finally:
        if service is not None:await service.close()

def execute_cloud(stop_at):
    global args, state
    args = argparse.Namespace(stop_at=stop_at)
    O.mkdir(exist_ok=False)
    state = {'state':'PREFLIGHT','started_unix_s':time.time(),'runs':[],
             'scope':'Compatibility only; no policy or qualified calibration claim'}
    try:asyncio.run(run_and_close())
    except DiagnosticFailure as exc:
        status(state=exc.acceptance['status'],acceptance=exc.acceptance,error_type=type(exc).__name__,error=str(exc))
    except BaseException as exc:status(state='FAILED',error_type=type(exc).__name__,error=str(exc))
    finally:
        cleanup={'leases':[],'pending':[]}
        plans=O/'service/plans.db'
        if plans.exists():
            with sqlite3.connect(f'file:{plans}?mode=ro',uri=True) as db:
                cleanup['leases']=db.execute('SELECT target,plan_id FROM leases').fetchall()
                cleanup['pending']=db.execute("SELECT id,state FROM plans WHERE state='ROLLBACK_PENDING'").fetchall()
        if adapter is not None:
            for worker in adapter.workers:
                name=adapter.name(worker)
                obj=adapter.inspect(worker)
                if obj['Config']['Labels'].get('transitionbench.owner')=='vast51687766':
                    subprocess.run(['docker','stop','--time','10',name],capture_output=True,timeout=20)
        for unit,previous in timers.items():
            if previous=='active':subprocess.run(['systemctl','start',unit],capture_output=True,timeout=10)
        save('cleanup.json',cleanup)
        if cleanup['leases'] or cleanup['pending']:status(state='FAILED',cleanup_requires_reconciliation=True)
        state['ended_unix_s']=time.time();save('status.json',state)
        archive=R/(RUN_ID+'.tgz')
        with tarfile.open(archive,'w:gz') as tar:tar.add(O,arcname=O.name)
        print(json.dumps({'archive':str(archive),'bytes':archive.stat().st_size,'sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'state':state['state']}),flush=True)
    return 0 if state['state']=='PASSED' else 1


def cli():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--stop-at', type=float, help='Authorized cloud execution only; external provider-stop guard required')
    mode.add_argument('--verify-bundle', type=Path, nargs='+', help='Read-only local acceptance replay; no cloud operations')
    options = parser.parse_args()
    if options.verify_bundle is not None:
        results = [dict(bundle=str(path), **assess_bundle(path)) for path in options.verify_bundle]
        print(json.dumps({'bundles': results}, indent=2))
        return 0 if all(r['status'] == 'PASSED' for r in results) else 1
    return execute_cloud(options.stop_at)


if __name__ == '__main__':
    sys.exit(cli())
