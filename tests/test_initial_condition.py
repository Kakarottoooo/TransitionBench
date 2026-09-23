"""Frozen initial-condition verifier rejects missing or altered reset evidence."""
import copy
import pytest
from transitionbench.verifier import initial_condition_errors

@pytest.mark.parametrize('mutation', ['missing','same_pid','generation','device','duplicate','warmup','observed'])
def test_fresh_reset_receipt_refuses_invalid_evidence(mutation):
    before=[dict(worker_id=str(i),process_id=i+1,generation=1,device_uuid='GPU-'+str(i),
        device_model='fixture',config_id='A',ready=True,accepting=True,in_flight=0) for i in range(2)]
    after=[{**r,'process_id':r['process_id']+10,'generation':2} for r in before]
    warm={'complete_probe_sequence':True,'max_requests':24}
    condition=dict(policy='fresh-workers',before=before,after=after,warmup=warm)
    hardware=copy.deepcopy(after)
    assert initial_condition_errors(condition,hardware,warm)==[]
    if mutation=='missing':condition=None
    elif mutation=='same_pid':after[0]['process_id']=before[0]['process_id']
    elif mutation=='generation':after[0]['generation']=1
    elif mutation=='device':after[0]['device_uuid']='GPU-other'
    elif mutation=='duplicate':after.append(copy.deepcopy(after[0]))
    elif mutation=='warmup':warm['complete_probe_sequence']=False
    else:hardware[0]['process_id']+=1
    assert initial_condition_errors(condition,hardware,warm)
