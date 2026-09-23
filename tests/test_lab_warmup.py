"""Real HTTP probes with a fake external Docker/NVIDIA CLI boundary; no GPU claim."""
import json
from urllib.parse import urlsplit
from fastapi import FastAPI, Request
from transitionbench.lab import DockerLabAdapter, LabRouter
from transitionbench.schemas import WarmupSpec


async def test_adapter_persists_warmup_receipt_in_owned_cache(asgi_server,tmp_path,monkeypatch):
    from transitionbench import lab
    app=FastAPI()
    @app.get('/health')
    async def health():return {'status':'ok'}
    @app.post('/v1/chat/completions')
    async def completion(request:Request):
        body=await request.json()
        return {'choices':[{'finish_reason':'stop','message':{
            'content':body['messages'][0]['content'].split('Return exactly ')[-1].split(' and nothing else.')[0]}}]}
    ports=[urlsplit(asgi_server(app)).port for _ in range(2)]
    image='vllm/vllm-openai@sha256:'+'0'*64
    running = [True, True]
    def command(args,timeout=30):
        if args[0]=='nvidia-smi':
            if args[1].startswith('--query-gpu='):
                return 'fixture, GPU-fixture-0, test-driver, 48000\nfixture, GPU-fixture-1, test-driver, 48000'
            return '100, GPU-fixture-0\n101, GPU-fixture-1'
        worker=int(args[2].rsplit('-',1)[1])
        if args[1]=='inspect':
            return json.dumps([{'Config':{'Image':image,'Labels':{'transitionbench.owner':'fixture',
                'transitionbench.config':'A','transitionbench.generation':'0'}},
                'HostConfig':{'DeviceRequests':[{'DeviceIDs':[f'GPU-fixture-{worker}']}]},
                'State':{'Running':running[worker]}}])
        if args[1]=='top':
            if not running[worker]:
                raise RuntimeError('docker top cannot inspect an exited container')
            return f'PID\n{100+worker}'
        raise AssertionError('Unexpected external mutation: '+str(args))
    monkeypatch.setattr(lab,'command',command)
    adapter=DockerLabAdapter({'experimental_authority':True,'license_reviewed':True,'owner_id':'fixture',
        'image_digest':image,'cache_directory':str(tmp_path/'cache'),
        'workers':[{'device_uuid':f'GPU-fixture-{i}','port':p} for i,p in enumerate(ports)]},LabRouter(ports))
    result=await adapter.operation('warmup','0',{'expected_generation':0,'config_id':'A','max_tokens':32,
        'warmup':WarmupSpec(relative_tolerance=.5,absolute_tolerance_s=.1).model_dump()},'fixture-warmup')
    assert result['generation']==0
    receipts=list((tmp_path/'cache'/'transitionbench-fixture'/'warmup').glob('*.json'))
    assert len(receipts)==1
    receipt=json.loads(receipts[0].read_text())
    assert receipt['stable'] and len(receipt['requests'])>=12
    assert receipt['worker_id']=='0'
    # A crashed worker must remain observable for reconciliation, even when
    # its former port happens to answer health and a stale PID is reported.
    running[0] = False
    stopped, healthy = await adapter.snapshot()
    assert not stopped.ready and stopped.process_id is None
    assert stopped.resource_evidence != 'independently-observed'
    assert healthy.ready and healthy.resource_evidence == 'independently-observed'
