"""Run calibration orchestration against owned CPU processes and real HTTP.

No fake GPU identity is emitted. Qualification must reject these bundles.
"""
import argparse
import asyncio
import importlib.util
import json
import os
import secrets
import socket
import threading
import time
from pathlib import Path
import httpx
import uvicorn
from fastapi import FastAPI
from transitionbench.collection import CollectionSpec, CalibrationCollector, collection_plan
from transitionbench.evidence import write_json
from transitionbench.lab import LabRouter
from transitionbench.schemas import ExperimentSpec, WorkloadSpec, ResourceBudget, WarmupSpec
from transitionbench.service import Service
from transitionbench.warmup import warm_worker

ROOT=Path(__file__).resolve().parents[1]
module_spec=importlib.util.spec_from_file_location('owned_process_example',ROOT/'examples/process_hook.py')
module=importlib.util.module_from_spec(module_spec);module_spec.loader.exec_module(module)


async def rehearse(output):
    output=Path(output).resolve();output.mkdir(parents=True,exist_ok=False)
    class OwnedCPU(module.ProcessAdapter):
        async def operation(self,operation,worker,payload,key):
            if operation!='warmup':return await super().operation(operation,worker,payload,key)
            current=await self.ready(worker)
            if current['generation']!=payload['expected_generation']:raise ValueError('CPU generation changed')
            async with httpx.AsyncClient(timeout=2,trust_env=False) as client:
                await warm_worker(client,f'http://127.0.0.1:{self.ports[worker]}/v1/chat/completions',
                    'local-arithmetic',32,WarmupSpec.model_validate(payload['warmup']),
                    record=lambda report:write_json(output/'warmup'/(key.replace(':','-')+'.json'),report))
            return next(s.model_dump(mode='json') for s in await self.snapshot() if s.worker_id==worker)
    (output/'warmup').mkdir()
    adapter=OwnedCPU();server=None;thread=None;sock=None
    started=time.monotonic()
    try:
        await adapter.initialize()
        initial=[s.model_dump(mode='json') for s in await adapter.snapshot()]
        router=LabRouter(list(adapter.ports.values()),model='local-arithmetic')
        router.accepting=adapter.accepting;router.in_flight=adapter.in_flight
        token=secrets.token_urlsafe(32);os.environ['TB_REHEARSAL_TOKEN']=token
        app=FastAPI();router.install(app,token)
        sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
        server=uvicorn.Server(uvicorn.Config(app,log_level='error',access_log=False))
        thread=threading.Thread(target=server.run,kwargs={'sockets':[sock]},daemon=True);thread.start()
        deadline=time.monotonic()+10
        while not server.started:
            if time.monotonic()>deadline:raise TimeoutError('Router startup')
            await asyncio.sleep(.02)
        base=f'http://127.0.0.1:{port}'
        value=CollectionSpec(id='cpu-rehearsal',experiment=ExperimentSpec(mode='CONTROLLED_ROLLOUT',endpoint_id='cpu',
            workload=WorkloadSpec(kind='short',injection_s=5,rate_rps=4), observation_s=6,drain_s=1,
            max_dispatch_lag_s=.3,budget=ResourceBudget(max_requests=200,max_total_tokens=250000,max_duration_s=30,
                max_reserved_gpu_seconds=60,max_concurrency=4)),warmup=WarmupSpec(max_requests=24,max_duration_s=3,
                absolute_tolerance_s=.02), transition_kinds=['short'],transition_at_s=.1,
            max_wall_s=3000,max_requests=15000,max_total_tokens=15000000)
        config={'allowed_origins':[base],'private_hosts':['127.0.0.1'],
            'configurations':{'A':{'fixture_only':True},'B':{'fixture_only':True}},
            'endpoints':[{'spec':{'id':'cpu','base_url':base+'/v1','provider':'local-test','model':'local-arithmetic',
                'key_env':'TB_REHEARSAL_TOKEN','temperature':0,'seed':0,'streaming':True,
                'supported_parameters':['max_tokens','stream','temperature','seed']},'budget':value.experiment.budget.model_dump(mode='json')}]}
        service=Service(output/'state',config);service.store.acquire_owner()
        try:
            plan=collection_plan(value,config,service.code_revision)
            collector=CalibrationCollector(service,adapter,value,output/'campaign',plan,rehearsal=True)
            status=await collector.run(plan['plan_hash'])
            final=[s.model_dump(mode='json') for s in await adapter.snapshot()]
            if any(a['process_id']==b['process_id'] for a,b in zip(initial,final)):
                raise AssertionError('Expected real owned process replacement')
            # Second campaign injects an uncertain apply acknowledgment at the
            # adapter boundary. No retry, further trial or forced rollback may run.
            failure_value=value.model_copy(update={'id':'cpu-failure-rehearsal'})
            failure_plan=collection_plan(failure_value,config,service.code_revision)
            original=adapter.operation
            calls=[]
            async def uncertain(operation,worker,payload,key):
                calls.append(operation)
                if operation=='apply':raise TimeoutError('Injected uncertain apply acknowledgment; CPU rehearsal')
                return await original(operation,worker,payload,key)
            adapter.operation=uncertain
            failed=CalibrationCollector(service,adapter,failure_value,output/'failure-campaign',failure_plan,rehearsal=True)
            try:
                await failed.run(failure_plan['plan_hash'])
            except ValueError:
                failure=json.loads((output/'failure-campaign/status.json').read_text())
            else:raise AssertionError('Injected failure was ignored')
            with service.plans.connect() as db:
                pending=db.execute("SELECT COUNT(*) FROM plans WHERE state='ROLLBACK_PENDING'").fetchone()[0]
                leases=db.execute('SELECT COUNT(*) FROM leases').fetchone()[0]
            if not pending or not leases or 'rollback' in calls:raise AssertionError('Uncertain operation was replayed/released')
            report={'origin':'measured-black-box','target':'owned CPU processes and actual loopback HTTP; not GPU/model evidence',
                'success':status,'failure':failure,'failure_injection':'adapter apply timeout',
                'pending_plans':pending,'retained_leases':leases,'automatic_rollback':False,
                'initial_workers':initial,'final_workers':final,'elapsed_s':time.monotonic()-started,
                'hardware_validated':False,'paid_instances':0,'external_provider_requests':0}
            write_json(output/'result.json',report)
            print(json.dumps(report,indent=2))
        finally:await service.close()
    finally:
        if server:server.should_exit=True
        if thread:thread.join(10)
        if sock:sock.close()
        adapter.close()
        os.environ.pop('TB_REHEARSAL_TOKEN',None)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True)
    args=parser.parse_args();asyncio.run(rehearse(args.output))
