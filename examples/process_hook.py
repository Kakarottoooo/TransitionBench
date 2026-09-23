"""Working operator-owned CPU process hook example, NOT a GPU/model backend.

Run: python examples/process_hook.py --port 8771
Supply TRANSITIONBENCH_HOOK_TOKEN through the environment (at least 24 chars).
Only subprocess handles created by this instance can be stopped. Physical GPU
evidence remains unknown, so this example cannot pass CONTROLLED_ROLLOUT gates.
"""
import argparse
import asyncio
import os
import socket
import subprocess
import sys
import sysconfig
import time
from pathlib import Path
import httpx
import uvicorn
import transitionbench
from transitionbench.hook_server import create_hook_app
from transitionbench.schemas import WorkerSnapshot


class ProcessAdapter:
    def __init__(self):
        self.processes={};self.ports={};self.accepting={'0':True,'1':True};self.in_flight={'0':0,'1':0}
        for worker in ('0','1'):
            with socket.socket() as sock:
                sock.bind(('127.0.0.1',0));self.ports[worker]=sock.getsockname()[1]

    async def start(self,worker,config='A',generation=0):
        if worker in self.processes and self.processes[worker].poll() is None:
            raise ValueError('Owned worker already running')
        environment={**os.environ,'TB_FIXTURE_CONFIG':config,'TB_FIXTURE_GENERATION':str(generation)}
        # On Windows the venv python.exe is a redirector spawning another PID.
        # Launch the real interpreter with the exact current dependency paths so
        # the owned process handle is also the observed serving PID.
        bootstrap="import sys;sys.path[:0]=sys.argv[1:3];import uvicorn;uvicorn.run('transitionbench.test_server:app',host='127.0.0.1',port=int(sys.argv[3]),access_log=False)"
        self.processes[worker]=subprocess.Popen([sys._base_executable,'-c',bootstrap,
            sysconfig.get_path('purelib'),str(Path(transitionbench.__file__).parent.parent),str(self.ports[worker])],env=environment,
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

    async def ready(self,worker):
        deadline=time.monotonic()+10
        async with httpx.AsyncClient(timeout=1,trust_env=False) as client:
            while time.monotonic()<deadline:
                try:
                    response=await client.get(f'http://127.0.0.1:{self.ports[worker]}/healthz')
                    if response.status_code==200:return response.json()
                except httpx.HTTPError:pass
                await asyncio.sleep(.025)
        raise TimeoutError('Owned CPU fixture did not become ready')

    async def snapshot(self):
        rows=[]
        for worker in ('0','1'):
            observed=await self.ready(worker)
            if observed['process_id']!=self.processes[worker].pid:
                raise ValueError('Port was occupied by an unowned process')
            rows.append(WorkerSnapshot(worker_id=worker,config_id=observed['config_id'],generation=observed['generation'],
                ready=True,accepting=self.accepting[worker],in_flight=self.in_flight[worker],
                device_uuid='cpu-fixture-slot-'+worker,device_model=None,process_id=observed['process_id'],
                observed_at_unix_s=time.time(),resource_evidence='unknown'))
        return rows

    async def operation(self,operation,worker,payload,key):
        if worker not in self.ports or payload['config_id'] not in ('A','B'):raise ValueError('Unknown fixture target')
        current=await self.ready(worker)
        if current['generation']!=payload['expected_generation']:raise ValueError('Generation conflict')
        if operation=='drain':
            self.accepting[worker]=False
            deadline=time.monotonic()+payload['drain_timeout_s']
            while self.in_flight[worker]:
                if time.monotonic()>deadline:raise TimeoutError('Drain deadline')
                await asyncio.sleep(.01)
        elif operation in ('apply','rollback'):
            if self.accepting[worker] or self.in_flight[worker]:raise ValueError('Worker is not drained')
            process=self.processes[worker]
            process.terminate();await asyncio.to_thread(process.wait,10)
            await self.start(worker,payload['config_id'],current['generation']+1)
            await self.ready(worker)
        elif operation=='readiness':await self.ready(worker)
        elif operation=='warmup':
            async with httpx.AsyncClient(timeout=2,trust_env=False) as client:
                response=await client.post(f'http://127.0.0.1:{self.ports[worker]}/v1/chat/completions',json={
                    'model':'local-arithmetic','messages':[{'role':'user','content':'Return exactly TB:warm:4'}],
                    'max_tokens':payload['max_tokens']})
                response.raise_for_status()
                if response.json()['choices'][0]['message']['content']!='TB:warm:4':raise ValueError('Warmup failed')
        elif operation=='observe':self.accepting[worker]=True
        elif operation!='prepare':raise ValueError('Unsupported operation')
        return next(s.model_dump(mode='json') for s in await self.snapshot() if s.worker_id==worker)

    async def initialize(self):
        for worker in ('0','1'):
            await self.start(worker)
            await self.ready(worker)

    def close(self):
        for process in self.processes.values():
            if process.poll() is None:
                process.terminate();process.wait(timeout=10)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--port',type=int,default=8771);parser.add_argument('--database',default='work/process-hook.db')
    args=parser.parse_args();Path(args.database).parent.mkdir(parents=True,exist_ok=True)
    adapter=ProcessAdapter()
    try:
        asyncio.run(adapter.initialize())
        app=create_hook_app(adapter,os.environ.get('TRANSITIONBENCH_HOOK_TOKEN',''),args.database)
        uvicorn.run(app,host='127.0.0.1',port=args.port,access_log=False)
    finally:adapter.close()
