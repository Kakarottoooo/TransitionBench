import asyncio
import hashlib
import json
import time
import zipfile
import pytest
from pydantic import ValidationError
from transitionbench.endpoint import EndpointClient, NetworkPolicy, Refusal, run_endpoint
from transitionbench.evidence import export_bundle, import_bundle
from transitionbench.metrics import summarize
from transitionbench.schemas import DecisionInput, ExperimentSpec, RequestEvent, RunManifest, SLOSpec, EndpointSpec, WorkloadSpec, ResourceBudget
from transitionbench.simulation import simulate, calibrate
from transitionbench.policy import OnlinePolicy
from transitionbench.verifier import verify_bundle
from transitionbench.workloads import generate
from transitionbench.test_server import app as fixture_app


@pytest.mark.parametrize('mutation',[{'quality_valid':False},{'output_chars':0},{'completed_s':4,'final_content_s':4},{'termination':'timeout'}])
def test_nonqualifying_events_never_disappear(mutation):
    body=dict(request_id='x',scheduled_s=0,dispatch_s=0,first_content_s=.1,final_content_s=.2,completed_s=.2,quality_valid=True,output_chars=1,termination='complete')
    body.update(mutation)
    result=summarize([RequestEvent(**body)],SLOSpec(),3)
    assert result['offered']==1 and result['qualified']==0


def test_duplicate_and_impossible_lifecycle_refused():
    row=RequestEvent(request_id='x',scheduled_s=0)
    with pytest.raises(ValueError,match='Duplicate'):
        summarize([row,row],SLOSpec(),3)
    with pytest.raises(ValidationError):
        RequestEvent(request_id='x',scheduled_s=2,dispatch_s=1)
    with pytest.raises(ValidationError):
        RequestEvent(request_id='x',scheduled_s=float('nan'))
    with pytest.raises(ValidationError):
        DecisionInput(current_goodput_rps=1,candidate_goodput_rps=2,horizon_s=10,deficit_reference='current-steady')


@pytest.mark.parametrize('url',['http://169.254.169.254/latest','http://127.0.0.1:8999/v1','https://user:secret@example.com/v1','file:///etc/passwd','https://example.com/v1?secret=x'])
def test_nonallowlisted_or_unsafe_destinations_refused(url):
    with pytest.raises((Refusal,OSError)):
        NetworkPolicy(['https://example.com']).resolve(url)


async def test_overload_preserves_open_loop_arrivals_and_cancellation(asgi_server):
    base=asgi_server(fixture_app)
    client=EndpointClient(EndpointSpec(id='local',base_url=base+'/v1',model='local-arithmetic'),NetworkPolicy([base],['127.0.0.1']))
    spec=ExperimentSpec(mode='LIVE_ENDPOINT',workload=WorkloadSpec(kind='short',injection_s=.2,rate_rps=200),observation_s=1.2,drain_s=1,budget=ResourceBudget(max_concurrency=1,reserved_gpus=0))
    rows,_=await run_endpoint(spec,client,asyncio.Event())
    assert len(rows)==40
    assert [r.scheduled_s for r in rows]==[i.scheduled_s for i in generate(spec.workload)]
    assert any(r.termination=='client_drop' for r in rows)
    cancel=asyncio.Event()
    async def stop():
        await asyncio.sleep(.07)
        cancel.set()
    stopper=asyncio.create_task(stop())
    start=time.monotonic()
    spec=ExperimentSpec(mode='LIVE_ENDPOINT',workload=WorkloadSpec(kind='short',injection_s=2,rate_rps=10),observation_s=3,drain_s=1)
    rows,validity=await run_endpoint(spec,client,cancel)
    await stopper
    assert time.monotonic()-start<1
    assert len(rows)==20 and any(r.termination=='cancelled' for r in rows)
    assert not validity['valid']


def test_holdouts_and_policy_do_not_receive_future_schedule():
    groups=[{r.prefix_group for r in generate(WorkloadSpec(split=s,seed=1))} for s in ('calibration','tuning','test')]
    assert not groups[0]&groups[1] and not groups[1]&groups[2]
    cal=calibrate()
    a,b=OnlinePolicy('StateAware',cal,40),OnlinePolicy('StateAware',cal,40)
    for n in range(20):
        a.observe_arrival('long',str(n%3));b.observe_arrival('long',str(n%3))
        da,db=a.choose(n,'A',0),b.choose(n,'A',0)
        assert da[0]==db[0]
        if da[1]:
            assert da[1].action==db[1].action


def test_simulated_drain_waits_for_existing_requests():
    spec=ExperimentSpec(workload=WorkloadSpec(kind='long-prefix',rate_rps=10,injection_s=10),observation_s=20,drain_s=10,transition_s=2)
    rows,events,_=simulate(spec,fixed='A',switch_at=2)
    for worker in ('0','1'):
        drain=next(e.at_s for e in events if e.worker_id==worker and e.state=='DRAINING_ONE_WORKER')
        apply=next(e.at_s for e in events if e.worker_id==worker and e.state=='RECONFIGURING')
        observe=next(e.at_s for e in events if e.worker_id==worker and e.state=='OBSERVING')
        before=[r.completed_s for r in rows if r.worker_id==worker and r.dispatch_s<drain]
        assert apply>=max(before,default=0)
        assert not any(r.worker_id==worker and drain<=r.dispatch_s<observe for r in rows if r.dispatch_s is not None)


def rewrite_checks(root):
    checks={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in root.iterdir() if p.name!='checksums.json'}
    (root/'checksums.json').write_text(json.dumps(checks),encoding='utf-8')


@pytest.mark.parametrize('change',['missing','duplicate','order','schema','summary','claim'])
def test_independent_verifier_rejects_structural_fraud_even_with_new_checksums(tmp_path,change):
    root=tmp_path/'bundle'
    manifest=RunManifest(run_id='test',mode='SIMULATION',origin='synthetic',experiment=ExperimentSpec(),offered_ids=['a'],created_at_unix_s=0,versions={})
    row=RequestEvent(request_id='a',scheduled_s=0,dispatch_s=0,first_content_s=.1,final_content_s=.2,completed_s=.2,termination='complete',quality_valid=True,output_chars=2)
    export_bundle(root,manifest,[row],[],[])
    path=root/'requests.jsonl';raw=json.loads(path.read_text())
    if change=='missing':path.write_text('')
    elif change=='duplicate':path.write_text(json.dumps(raw)+'\n'+json.dumps(raw)+'\n')
    elif change=='order':raw['dispatch_s']=10;path.write_text(json.dumps(raw)+'\n')
    elif change=='schema':raw['schema_version']='99';path.write_text(json.dumps(raw)+'\n')
    elif change=='summary':
        s=json.loads((root/'summary.json').read_text());s['qualified']=99;(root/'summary.json').write_text(json.dumps(s))
    else:
        m=json.loads((root/'manifest.json').read_text());m['origin']='measured-controlled';(root/'manifest.json').write_text(json.dumps(m))
    rewrite_checks(root)
    assert not verify_bundle(root)['integrity_valid']


def test_unsafe_archive_and_invalid_zip(tmp_path):
    archive=tmp_path/'unsafe.zip'
    with zipfile.ZipFile(archive,'w') as z:z.writestr('../secret','x')
    with pytest.raises(ValueError):import_bundle(archive,tmp_path/'target')
    archive.write_text('invalid')
    with pytest.raises(ValueError):import_bundle(archive,tmp_path/'target')
