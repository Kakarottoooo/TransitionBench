"""Predeclared held-out synthetic trials and real loopback HTTP plumbing trials.

This program does not use GPUs, model credentials or external model endpoints.
"""
import argparse
import asyncio
import json
import random
import socket
import statistics
import threading
import time
import zipfile
from pathlib import Path
import httpx
import uvicorn
from transitionbench.endpoint import EndpointClient,NetworkPolicy
from transitionbench.evidence import write_json
from transitionbench.schemas import ExperimentSpec,WorkloadSpec,ResourceBudget,EndpointSpec
from transitionbench.service import Service,paired_comparison
from transitionbench.test_server import app
from transitionbench.workloads import generate

POLICIES=['StaticBest','SteadyStateFirst','FixedHysteresis','StateAware']
CASES=[{'id':kind,'kind':kind,'duration':30,'transition_s':3,'rate':9} for kind in ('long-prefix','short','prefix-shift','mixed-burst')]+[
    {'id':'low-transition-cost','kind':'long-prefix','duration':30,'transition_s':.1,'rate':9},
    {'id':'long-stable-regime','kind':'long-prefix','duration':90,'transition_s':3,'rate':9},
    {'id':'spare-capacity','kind':'long-prefix','duration':30,'transition_s':3,'rate':4}]


async def wait(service,run,accept_invalid=False):
    await service.tasks[run['id']]
    result=service.get(run['id'])
    if result['state']!='SUCCEEDED' and not (accept_invalid and result['state']=='INVALID'):
        raise RuntimeError(result)
    return result


async def overhead(base):
    """Same fixture, payload, connection policy and serial request count."""
    spec=EndpointSpec(id='local',base_url=base+'/v1',model='local-arithmetic',streaming=True,supported_parameters=['max_tokens','stream'])
    adapter=EndpointClient(spec,NetworkPolicy([base],['127.0.0.1']))
    item=generate(WorkloadSpec(kind='short',injection_s=.1,rate_rps=1))[0]
    samples=[]
    for block in range(5):
        pair={}
        for mode in (['minimal','instrumented'] if block%2==0 else ['instrumented','minimal']):
            async with httpx.AsyncClient(timeout=10,trust_env=False) as session:
                durations=[]
                for _ in range(12):
                    started=time.monotonic()
                    if mode=='instrumented':
                        row=await adapter.measure(item,32,started,session)
                        assert row.quality_valid
                    else:
                        body={'model':'local-arithmetic','messages':[{'role':'user','content':item.prompt}],'max_tokens':32,'stream':True}
                        async with session.stream('POST',base+'/v1/chat/completions',json=body) as response:
                            content=''
                            async for line in response.aiter_lines():
                                if line.startswith('data: ') and line!='data: [DONE]':
                                    obj=json.loads(line[6:])
                                    for c in obj.get('choices',[]):content+=c.get('delta',{}).get('content','')
                            assert content==item.expected
                    durations.append(time.monotonic()-started)
                pair[mode]={'mean_s':statistics.mean(durations),'samples_s':durations}
        pair['difference_s']=pair['instrumented']['mean_s']-pair['minimal']['mean_s']
        samples.append(pair)
    return {'origin':'measured-black-box','target':'loopback protocol fixture, not a model','paired_blocks':5,
            'requests_per_arm_per_block':12,'blocks':samples,'mean_added_s':statistics.mean(p['difference_s'] for p in samples),
            'limitations':['Serial low-load measurement; not GPU router overhead or a production estimate','Timing noise can make a block difference negative']}


async def main(output,local_only=False):
    output=Path(output).resolve();output.mkdir(parents=True,exist_ok=True)
    protocol={'schema_version':'1.0','frozen_before_runs_unix_s':time.time(),'origin':'synthetic',
              'test_seeds':[101,102,103,104,105],'calibration_seeds':[11,12,13],'tuning_seeds':[21,22],
              'policies':POLICIES,'cases':CASES,'slo':{'e2e_s':2,'first_content_s':1},
              'trial_order_seed':20260919,'smallest_meaningful_gain_requests':2,
              'stopping_rule':'Exactly five matched test seeds per condition, no adaptive stopping',
              'precision_target':'Report paired run bootstrap intervals; five runs is exploratory precision only',
              'not_claimed':['GPU effectiveness','live Wafer compatibility','production p99 SLA']}
    protocol_path=output/'predeclared-protocol.json'
    if protocol_path.exists():raise ValueError('Output already contains a protocol; use a fresh directory')
    write_json(protocol_path,protocol)
    # Preserve exact producer source, rather than pointing at a mutable checkout.
    import transitionbench
    with zipfile.ZipFile(output/'producer-source.zip','w',zipfile.ZIP_DEFLATED) as archive:
        for source in sorted(Path(transitionbench.__file__).parent.glob('*.py')):
            archive.write(source,'transitionbench/'+source.name)
        archive.write(Path(__file__),'run_cpu_experiments.py')
        lock=Path(__file__).resolve().parents[1]/'uv.lock'
        if lock.exists():archive.write(lock,'uv.lock')
    service=Service(output/'evidence')
    all_runs,comparisons=[],{}
    rng=random.Random(20260919)
    for case in ([] if local_only else CASES):
        group=[]
        for seed in protocol['test_seeds']:
            order=POLICIES.copy();rng.shuffle(order)
            for policy in order:
                spec=ExperimentSpec(policy=policy,workload=WorkloadSpec(kind=case['kind'],seed=seed,rate_rps=case['rate'],injection_s=case['duration']),
                     observation_s=case['duration']+10,drain_s=10,transition_s=case['transition_s'],
                     budget=ResourceBudget(max_requests=2000,max_total_tokens=6000000,max_duration_s=case['duration']+10,
                                           max_reserved_gpu_seconds=(case['duration']+10)*2))
                run=await wait(service,await service.submit(spec,key=f"{case['id']}:{seed}:{policy}"))
                group.append(run)
                all_runs.append({'case':case['id'],'id':run['id'],'policy':policy,'seed':seed,'origin':run['origin'],
                                 'qualified':run['summary']['qualified'],'offered':run['summary']['offered'],
                                 'bundle':f"evidence/runs/{run['id']}/bundle"})
        comparisons[case['id']]=paired_comparison(group)
        print(case['id'],{p:statistics.mean(r['summary']['qualified'] for r in group if r['spec']['policy']==p) for p in POLICIES},flush=True)
    write_json(output/'synthetic-trials.json',all_runs)
    write_json(output/'paired-comparisons.json',comparisons)
    await service.close()
    sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    server=uvicorn.Server(uvicorn.Config(app,log_level='error',access_log=False))
    thread=threading.Thread(target=server.run,kwargs={'sockets':[sock]},daemon=True);thread.start()
    while not server.started:await asyncio.sleep(.02)
    base=f'http://127.0.0.1:{port}'
    try:
        spec=EndpointSpec(id='local-fixture',base_url=base+'/v1',provider='local-test',model='local-arithmetic',streaming=True,supported_parameters=['max_tokens','stream'])
        config={'allowed_origins':[base],'private_hosts':['127.0.0.1'],'endpoints':[{'spec':spec.model_dump(mode='json'),'budget':ResourceBudget(max_requests=500,max_total_tokens=1000000,reserved_gpus=0).model_dump(mode='json')}]}
        service=Service(output/'local-http',config)
        measured=[]
        for rate in (10,100,250):
            for seed in (201,202,203):
                experiment=ExperimentSpec(mode='LIVE_ENDPOINT',endpoint_id='local-fixture',workload=WorkloadSpec(kind='short',seed=seed,rate_rps=rate,injection_s=.8),
                                          observation_s=1.8,drain_s=1,budget=ResourceBudget(max_requests=500,max_concurrency=1,max_total_tokens=1000000,reserved_gpus=0))
                run=await wait(service,await service.submit(experiment),accept_invalid=True)
                measured.append({'id':run['id'],'state':run['state'],'rate_rps':rate,'seed':seed,'summary':run['summary'],'bundle':f"local-http/runs/{run['id']}/bundle"})
        write_json(output/'local-http-trials.json',measured)
        write_json(output/'instrumentation-overhead.json',await overhead(base))
        await service.close()
    finally:
        server.should_exit=True;thread.join(timeout=10);sock.close()
    print('Complete: synthetic results and measured loopback results are separated.',flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);parser.add_argument('--local-only',action='store_true')
    args=parser.parse_args()
    asyncio.run(main(args.output,args.local_only))
