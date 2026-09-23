from transitionbench.sdk import Client
import os

with Client(os.environ.get('TRANSITIONBENCH_API','http://127.0.0.1:8765')) as api:
    print(api.capabilities()['integration_level'])
    run=api.run({'mode':'SIMULATION','policy':'StateAware'})
    result=api.wait(run['id'])
    assert result['state']=='SUCCEEDED'
    print(result['mode'],result['origin'],result['summary']['qualified'],result['summary']['offered'])
