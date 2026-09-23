"""Paired loopback diagnosis; never a model/GPU benchmark or a production claim."""
import argparse
import asyncio
import cProfile
import hashlib
import json
import platform
import pstats
import random
import socket
import statistics
import threading
import time
from pathlib import Path
import httpx
import uvicorn
from transitionbench.endpoint import EndpointClient, NetworkPolicy
from transitionbench.evidence import write_json
from transitionbench.schemas import EndpointSpec, WorkloadSpec
from transitionbench.test_server import app
from transitionbench.workloads import generate


def interval(values, seed=29):
    rng=random.Random(seed)
    samples=sorted(statistics.mean(rng.choices(values,k=len(values))) for _ in range(10000))
    return [samples[250],samples[9749]]


async def diagnose(output, blocks=12, count=12):
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    protocol={'target':'owned loopback arithmetic HTTP fixture; not a model', 'blocks':blocks,
        'requests_per_arm':count,'order_seed':908,'arms':['minimal_eof','minimal_done','instrumented','instrumented_fixed_destination'],
        'hypotheses':['Historical difference is timing/order noise: matched block intervals include zero',
            'EOF versus DONE termination changes connection reuse and tail time',
            'Destination validation has stable cost: prevalidated destination removes it'],
        'frozen_at_unix_s':time.time(),'python':platform.python_version(),'platform':platform.platform(),
        'limitations':['Serial low load; no GPU or workload quality claim',
            'Fixed-destination arm is a diagnostic ablation only; production destination checks remain enabled',
            'A block is the resampling unit; requests within a block are not independent replicates',
            'No timing threshold is a functional acceptance gate']}
    write_json(output/'protocol.json',protocol)
    source=Path(__file__).read_bytes();(output/'producer.py').write_bytes(source)
    sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    server=uvicorn.Server(uvicorn.Config(app,log_level='error',access_log=False))
    thread=threading.Thread(target=server.run,kwargs={'sockets':[sock]},daemon=True);thread.start()
    base=f'http://127.0.0.1:{port}'
    try:
        deadline=time.monotonic()+10
        while not server.started:
            if time.monotonic()>deadline:raise TimeoutError('Fixture startup')
            await asyncio.sleep(.02)
        spec=EndpointSpec(id='local',base_url=base+'/v1',model='local-arithmetic',streaming=True,supported_parameters=['max_tokens','stream'])
        network=NetworkPolicy([base],['127.0.0.1'])
        adapter=EndpointClient(spec,network)
        destination=network.resolve(base+'/v1/chat/completions')
        class FixedDestination:
            def resolve(self,url):
                if url!=base+'/v1/chat/completions':raise ValueError('Diagnostic destination changed')
                return destination
        fixed=EndpointClient(spec,FixedDestination())
        item=generate(WorkloadSpec(kind='short',injection_s=.1,rate_rps=1))[0]
        body={'model':spec.model,'messages':[{'role':'user','content':item.prompt}],'max_tokens':32,'stream':True}
        async def one(mode,session):
            before=time.perf_counter()
            if mode.startswith('instrumented'):
                row=await (fixed if mode.endswith('destination') else adapter).measure(item,32,time.monotonic(),session)
                if not row.quality_valid:raise ValueError('Instrumented task check failed')
            else:
                async with session.stream('POST',base+'/v1/chat/completions',json=body) as response:
                    response.raise_for_status();content='';finish=None;done=False
                    async for line in response.aiter_lines():
                        if line=='data: [DONE]':
                            done=True
                            if mode=='minimal_done':break
                        elif line.startswith('data: '):
                            obj=json.loads(line[6:])
                            for choice in obj.get('choices',[]):
                                content+=choice.get('delta',{}).get('content','')
                                finish=choice.get('finish_reason') or finish
                    if content!=item.expected or finish!='stop' or not done:raise ValueError('Minimal task check failed')
            return time.perf_counter()-before
        rng=random.Random(protocol['order_seed']);samples=[]
        for block in range(blocks):
            order=protocol['arms'][:];rng.shuffle(order)
            result={'block':block,'order':order,'arms':{}}
            for mode in order:
                async with httpx.AsyncClient(timeout=10,trust_env=False) as session:
                    # Identical untimed connection/protocol warmup in every arm.
                    await one(mode,session)
                    values=[await one(mode,session) for _ in range(count)]
                result['arms'][mode]={'mean_s':statistics.mean(values),'samples_s':values}
            samples.append(result)
            write_json(output/'samples.json',samples)
        comparisons={}
        for a,b in [('instrumented','minimal_eof'),('instrumented','minimal_done'),('minimal_done','minimal_eof'),
                    ('instrumented','instrumented_fixed_destination')]:
            differences=[s['arms'][a]['mean_s']-s['arms'][b]['mean_s'] for s in samples]
            comparisons[a+' minus '+b]={'mean_ms':statistics.mean(differences)*1000,
                'paired_block_bootstrap_95_ms':[v*1000 for v in interval(differences)],
                'block_differences_ms':[v*1000 for v in differences]}
        # CPU profile is a separate run, excluded from all paired latency results.
        profiler=cProfile.Profile()
        async with httpx.AsyncClient(timeout=10,trust_env=False) as session:
            profiler.enable()
            for _ in range(12):await one('instrumented',session)
            profiler.disable()
        profiler.dump_stats(str(output/'instrumented.prof'))
        with (output/'profile.txt').open('w',encoding='utf-8') as stream:
            pstats.Stats(profiler,stream=stream).strip_dirs().sort_stats('cumulative').print_stats(35)
        micro={}
        for name,call in [('destination_validation',lambda:network.resolve(base+'/v1/chat/completions')),
                          ('validated_request_construction',lambda:adapter.request('POST','/chat/completions',json=body))]:
            values=[]
            for _ in range(1000):
                before=time.perf_counter();call();values.append(time.perf_counter()-before)
            micro[name]={'mean_us':statistics.mean(values)*1e6,'median_us':statistics.median(values)*1e6,'samples':1000}
        result={'origin':'measured-black-box','comparisons':comparisons,'microbenchmarks':micro,
            'producer_sha256':hashlib.sha256(source).hexdigest(),'successful_requests':blocks*len(protocol['arms'])*(count+1)+12,
            'production_code_modified':False,'limitations':protocol['limitations']}
        write_json(output/'result.json',result)
        print(json.dumps(result,indent=2))
        return result
    finally:
        server.should_exit=True;thread.join(10);sock.close()
        if thread.is_alive():raise RuntimeError('Owned diagnostic server failed to stop')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True)
    args=parser.parse_args();asyncio.run(diagnose(args.output))
