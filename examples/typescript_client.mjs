import {TransitionBench} from '@transitionbench/local-client';
const client=new TransitionBench(process.env.TRANSITIONBENCH_API||'http://127.0.0.1:8765');
const run=await client.run({mode:'SIMULATION',policy:'StaticBest'});
const result=await client.wait(run.id);
if(result.state!=='SUCCEEDED')throw new Error(JSON.stringify(result));
console.log(JSON.stringify({mode:result.mode,origin:result.origin,qualified:result.summary.qualified}));
