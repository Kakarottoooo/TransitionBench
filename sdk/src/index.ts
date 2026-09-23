/** Thin language-neutral HTTP boundary. Schemas are generated from Python OpenAPI. */
export * as Schemas from './schemas.js';
import type {ExperimentSpec,DecisionInput,ResourceBudget} from './schemas.js';
export type Mode = 'SIMULATION'|'RECORDED_REPLAY'|'LIVE_ENDPOINT'|'CONTROLLED_ROLLOUT';
export type Policy = 'StaticBest'|'SteadyStateFirst'|'FixedHysteresis'|'StateAware';
export interface Run {id:string;state:string;mode:Mode;origin:string;summary:Record<string,unknown>|null;error:string|null;spec:Record<string,unknown>}
export class TransitionBench {
  constructor(public baseUrl='http://127.0.0.1:8765',private operatorToken?:string){}
  async request<T>(method:string,path:string,body?:unknown,key:string=crypto.randomUUID()):Promise<T>{
    const response=await fetch(this.baseUrl+path,{method,headers:{'Content-Type':'application/json','X-TransitionBench':'1','Idempotency-Key':key,...(this.operatorToken?{'X-Operator-Token':this.operatorToken}:{})},...(body===undefined?{}:{body:JSON.stringify(body)})});
    const result=await response.json();
    if(!response.ok)throw new Error(JSON.stringify(result));
    return result as T;
  }
  capabilities(){return this.request<Record<string,unknown>>('GET','/api/v1/capabilities');}
  validate(spec:ExperimentSpec){return this.request<{valid:boolean;errors:string[]}>('POST','/api/v1/experiments/validate',spec);}
  run(spec:ExperimentSpec,key?:string){return this.request<Run>('POST','/api/v1/runs',spec,key);}
  getRun(id:string){return this.request<Run>('GET',`/api/v1/runs/${encodeURIComponent(id)}`);}
  cancel(id:string){return this.request<Run>('POST',`/api/v1/runs/${encodeURIComponent(id)}/cancel`,{});}
  evaluate(input:DecisionInput){return this.request<Record<string,unknown>>('POST','/api/v1/decisions/evaluate',input);}
  records(id:string){return this.request<Record<string,unknown>>('GET',`/api/v1/runs/${encodeURIComponent(id)}/records`);}
  compare(ids:string[]){return this.request<unknown[]>('POST','/api/v1/runs/compare',{run_ids:ids});}
  plan(configId:string,budget:ResourceBudget){return this.request<Record<string,unknown>>('POST','/api/v1/transition-plans',{config_id:configId,budget});}
  async wait(id:string,timeoutMs=120000):Promise<Run>{
    const deadline=Date.now()+timeoutMs;
    while(Date.now()<deadline){const run=await this.getRun(id);if(!['QUEUED','RUNNING','CANCELLING'].includes(run.state))return run;await new Promise(resolve=>setTimeout(resolve,100));}
    throw new Error('Run continues on server; poll its durable ID');
  }
}
