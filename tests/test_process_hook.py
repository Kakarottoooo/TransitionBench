import importlib.util
from pathlib import Path
from transitionbench.endpoint import NetworkPolicy
from transitionbench.hook_server import create_hook_app
from transitionbench.rollout import HTTPHookAdapter,PlanStore,execute_plan
from transitionbench.schemas import ResourceBudget
from transitionbench.evidence import write_json


async def test_real_owned_process_restart_observed_through_http_hook(asgi_server,tmp_path):
    source=Path(__file__).resolve().parents[1]/'examples'/'process_hook.py'
    module_spec=importlib.util.spec_from_file_location('process_example',source)
    module=importlib.util.module_from_spec(module_spec);module_spec.loader.exec_module(module)
    local=module.ProcessAdapter()
    try:
        await local.initialize()
        before=await local.snapshot()
        token='test-only-process-hook-0123456789'
        base=asgi_server(create_hook_app(local,token,tmp_path/'hook.db'))
        remote=HTTPHookAdapter(base,token,NetworkPolicy([base],['127.0.0.1']))
        store=PlanStore(tmp_path/'plans.db')
        plan=store.create(base,'B',before,ResourceBudget())
        store.approve(plan['id'],plan['hash'])
        result=await execute_plan(store,plan['id'],remote)
        after=await remote.snapshot()
        assert result['state']=='COMPLETE',result
        assert all(s.config_id=='B' and s.generation==1 and s.ready for s in after)
        assert all(a.process_id!=b.process_id for a,b in zip(after,before))
        assert all(s.resource_evidence=='unknown' for s in after)
        report={'origin':'measured-black-box','target':'owned CPU HTTP fixture subprocesses; not model/GPU evidence',
                'before':[s.model_dump(mode='json') for s in before],'after':[s.model_dump(mode='json') for s in after],
                'rollout':result,'claim':'Lifecycle plumbing only; G5 remains blocked'}
        write_json(tmp_path/'cpu-process-transition.json',report)
    finally:local.close()
