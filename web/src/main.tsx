import React, {useEffect, useRef, useState} from 'react';
import {createRoot} from 'react-dom/client';
import './style.css';
import DeploymentReview from './DeploymentReview';
import './theme.css';

type Policy='StaticBest'|'SteadyStateFirst'|'FixedHysteresis'|'StateAware';
type Point={at_s:number;qualified:number};
type Summary={offered:number;qualified:number;attainment:number;goodput_rps:number;observation_s:number;cumulative:Point[];by_class:Record<string,{offered:number;qualified:number;attainment:number}>;terminations:Record<string,number>};
type Run={id:string;state:string;mode:string;origin:string;summary:Summary|null;error:string|null;spec:any};
type EventRow={request_id:string;scheduled_s:number;dispatch_s:number|null;completed_s:number|null;first_content_s:number|null;quality_valid:boolean;termination:string;origin:string};
type Decision={action:string;gain_requests:number|null;break_even_s:number|null;horizon_s:number;transition_deficit_requests:number|null;uncertainty_requests:number|null;reasons:string[];evidence_ids:string[];sensitivity:{horizon_s:number;gain_requests:number|null}[]};
type Records={requests:EventRow[];transitions:{at_s:number;worker_id:string;state:string}[];decisions:Decision[];manifest:any};
const POLICIES:Policy[]=['StaticBest','SteadyStateFirst','FixedHysteresis','StateAware'];
const COLORS=['#c8d8e1','#eac38c','#cbb8ee','#9be7c5'];
const LABELS=['Best fixed','Steady-state first','Fixed hysteresis','State-aware'];
const pause=(ms:number)=>new Promise(r=>setTimeout(r,ms));
async function api<T>(path:string,body?:unknown):Promise<T>{
  const response=await fetch('/api/v1'+path,{method:body===undefined?'GET':'POST',headers:{'Content-Type':'application/json','X-TransitionBench':'1','Idempotency-Key':crypto.randomUUID()},...(body===undefined?{}:{body:JSON.stringify(body)})});
  const data=await response.json();
  if(!response.ok)throw new Error(data.error?.message||data.detail?.message||JSON.stringify(data.detail||data));
  return data;
}
const fmt=(n:number|null|undefined,d=1)=>n==null?'Unknown':n.toFixed(d);

function App(){
  const [view,setView]=useState('Evaluate Deployment');
  const [kind,setKind]=useState('prefix-shift');
  const [duration,setDuration]=useState(30);
  const [rate,setRate]=useState(6);
  const [transition,setTransition]=useState(3);
  const [horizon,setHorizon]=useState(40);
  const [slo,setSlo]=useState(2);
  const [runs,setRuns]=useState<Run[]>([]);
  const [selected,setSelected]=useState(3);
  const [records,setRecords]=useState<Records|null>(null);
  const [busy,setBusy]=useState(false);
  const [progress,setProgress]=useState('Ready to compute. No key or GPU needed.');
  const [error,setError]=useState('');
  const [caps,setCaps]=useState<any>(null);
  const [endpoints,setEndpoints]=useState<any[]>([]);
  const [endpoint,setEndpoint]=useState('');
  const [bundleId,setBundleId]=useState('');
  const [importInfo,setImportInfo]=useState('');
  const [guided,setGuided]=useState(0);
  const [playback,setPlayback]=useState(1);
  const [playing,setPlaying]=useState(false);
  const [cursor,setCursor]=useState(1);
  const stop=useRef(false), active=useRef<string|null>(null);
  const chosen=runs[selected]||runs[0];
  const decision=records?.decisions.at(-1);
  const totalHorizon=chosen?.summary?.observation_s||duration+10;
  useEffect(()=>{api('/capabilities').then(setCaps).catch(e=>setError(e.message));api<any[]>('/endpoints').then(e=>{setEndpoints(e);if(e[0])setEndpoint(e[0].id);}).catch(e=>setError(e.message));},[]);
  useEffect(()=>{let current=true;setRecords(null);if(chosen?.summary)api<Records>(`/runs/${chosen.id}/records`).then(value=>{if(current)setRecords(value);}).catch(e=>{if(current)setError(e.message);});return()=>{current=false;};},[chosen?.id,chosen?.state]);
  useEffect(()=>{if(!playing)return;const timer=setInterval(()=>setCursor(old=>{if(old>=1){setPlaying(false);return 1;}return Math.min(1,old+.012*playback);}),100);return()=>clearInterval(timer);},[playing,playback]);
  async function wait(run:Run){
    active.current=run.id;
    while(['QUEUED','RUNNING','CANCELLING'].includes(run.state)){
      if(stop.current)await api(`/runs/${run.id}/cancel`,{});
      await pause(90);run=await api<Run>(`/runs/${run.id}`);
    }
    if(run.state==='FAILED'||run.state==='INVALID')throw new Error(run.error||'Experiment validity failed; inspect the exported evidence.');
    return run;
  }
  function spec(policy:Policy){return {mode:'SIMULATION',policy,workload:{kind,seed:101,split:'test',rate_rps:rate,injection_s:duration},observation_s:duration+10,drain_s:10,horizon_s:horizon,transition_s:transition,slo:{e2e_s:slo,first_content_s:slo/2},budget:{max_requests:2000,max_total_tokens:6000000,max_duration_s:duration+10,reserved_gpus:2,max_reserved_gpu_seconds:(duration+10)*2}};}
  async function compare(){
    stop.current=false;setBusy(true);setError('');setRuns([]);setRecords(null);setCursor(1);setSelected(0);
    const completed:Run[]=[];
    try{for(let i=0;i<POLICIES.length;i++){
      if(stop.current)break;
      setProgress(`Computing ${LABELS[i]} · ${i+1} of 4 sequential trials`);
      const run=await wait(await api<Run>('/runs',spec(POLICIES[i])));
      completed.push(run);setRuns([...completed]);
    }setSelected(Math.max(0,completed.length-1));setProgress(stop.current?'Cancelled. Offered requests remain in the evidence.':'Four runs computed. All curves below come from their raw events.');}
    catch(e){setError((e as Error).message);setProgress('Run failed. Adjust the input or inspect the error.');}
    finally{setBusy(false);active.current=null;}
  }
  async function cancel(){stop.current=true;setProgress('Stopping new work and accounting for in-flight requests…');if(active.current)await api(`/runs/${active.current}/cancel`,{});}
  async function importFile(file:File|undefined){
    if(!file)return;setError('');setImportInfo('Verifying checksums, records, and claim levels…');
    try{const form=new FormData();form.append('file',file);const r=await fetch('/api/v1/bundles/import',{method:'POST',headers:{'X-TransitionBench':'1'},body:form});const data=await r.json();if(!r.ok)throw new Error(data.error?.message||'Import rejected');setBundleId(data.bundle_id);setImportInfo(`Integrity verified · experiment ${data.verification.experiment_valid?'valid':'invalid; inspect limitations'} · ${data.verification.recomputed.offered} offered requests`);}
    catch(e){setError((e as Error).message);setImportInfo('Import failed. Use a TransitionBench evidence ZIP.');}
  }
  async function replay(){
    stop.current=false;setBusy(true);setError('');
    try{const run=await wait(await api<Run>('/runs',{mode:'RECORDED_REPLAY',replay_bundle_id:bundleId,slo:{e2e_s:slo,first_content_s:slo/2}}));setRuns([run]);setSelected(0);setView('Inspect Evidence');setProgress('Recorded events recomputed. Original provenance preserved.');}
    catch(e){setError((e as Error).message);}finally{setBusy(false);}
  }
  async function smoke(){
    stop.current=false;setBusy(true);setError('');setProgress('Sending one authorized synthetic arithmetic request…');
    try{const run=await wait(await api<Run>('/runs',{mode:'LIVE_ENDPOINT',endpoint_id:endpoint,workload:{kind:'short',rate_rps:5,injection_s:.2},observation_s:10.2,drain_s:10,budget:{max_requests:1,max_total_tokens:512,max_output_tokens:32,max_concurrency:1,max_duration_s:11,reserved_gpus:0,max_reserved_gpu_seconds:0}}));setRuns([run]);setSelected(0);setView('Inspect Evidence');setProgress('Endpoint smoke completed. No physical resource equality is inferred.');}catch(e){setError((e as Error).message);}finally{setBusy(false);}
  }
  const steps=[['Start with the question','A configuration can be faster after warm-up and still lose requests while it is being deployed. The observation horizon decides whether that loss can be recovered.'],['Inspect the experiment','Four policies run sequentially on the same simulated two-worker budget and matched workload seed. Calibration and tuning use separate prefix groups.'],['Read the transition','The two worker lanes show when each worker drains, restarts and warms. The other worker continues serving. This lifecycle costs time and capacity.'],['Challenge the conclusion','Inspect all offered requests, including drops. Try short inputs or a long stable regime. A tie or loss is a valid result. Hardware evidence is still required.']];
  return <div className="app app-surface">
    <aside className="app-sidebar">
      <header><a className="brand" href="#main" aria-label="TransitionBench home"><span className="brand-mark" aria-hidden="true">↳</span><span>transition<span>bench</span></span></a><span className="edition">DEPLOYMENT REVIEW <span>v0.4.4</span></span></header>
      <nav aria-label="Main views">{['Evaluate Deployment','Inspect Evidence','Run on Your Setup','Understand'].map(name=><button key={name} aria-current={view===name?'page':undefined} onClick={()=>setView(name)}><svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">{name==='Understand'?<><path d="M4 19V5m0 14h16M8 14l4-5 4 3 4-7"/></>:name==='Run on Your Setup'?<><rect x="4" y="4" width="16" height="6" rx="2"/><rect x="4" y="14" width="16" height="6" rx="2"/><path d="M8 7h.01M8 17h.01"/></>:name==='Inspect Evidence'?<><path d="M14 3H5v18h14V8zM14 3v5h5M8 12h8M8 16h5"/></>:<><rect x="3" y="3" width="18" height="18" rx="5"/><path d="m7 12 3 3 7-7"/></>}</svg>{name}</button>)}</nav>
      <div className="sidebar-bottom"><a className="api-link" href="/docs" target="_blank" rel="noreferrer">API reference <span>↗</span></a><span className="local"><i/> Local · no telemetry</span><p>Evidence before deployment.</p></div>
    </aside>
    <main id="main" className={view==='Evaluate Deployment'?'review-view':'standard-view'}>
    {view!=='Evaluate Deployment'&&<div className="intro"><div><h1>{view==='Understand'?'Understand the deployment tradeoff.':view==='Inspect Evidence'?'Trace the result to the requests.':'Bring your deployment evidence.'}</h1><p>{view==='Understand'?'Explore transition costs in a CPU simulation. Use matched measurements to support a deployment decision.':view==='Inspect Evidence'?'Inspect raw events, failures and qualified completions from an imported or simulated run.':'Use your existing measurements to review a configuration change. Your serving system stays in control.'}</p></div>{(view==='Understand'||chosen)&&<div className="evidence-stamp"><span className="stamp-label">EVIDENCE MODE</span><strong>{chosen?.mode||'SIMULATION'}</strong><span>{chosen?.origin||'synthetic'} · {chosen?'inspectable raw events':'no model calls'}</span><small>{chosen?.origin?.startsWith('measured')?'Recorded client observations. Check resource limitations.':'An explanation model, not a GPU performance claim.'}</small></div>}</div>}
    {error&&<div className="error" role="alert"><strong>Unable to complete this action.</strong> {error}<button onClick={()=>setError('')} aria-label="Dismiss error">×</button></div>}
    {view!=='Evaluate Deployment'&&<div className="status" role="status" aria-label="Experiment status" aria-live="polite"><span className={busy?'spinner':'status-dot'}/>{progress}{busy&&<button className="quiet" onClick={cancel}>Cancel run</button>}</div>}
    {view==='Evaluate Deployment'&&<DeploymentReview/>}
    {view==='Understand'&&<>
      <section className="guide"><div><span className="eyebrow">A 60–90 SECOND WALKTHROUGH</span><h2>{steps[guided][0]}</h2><p>{steps[guided][1]}</p></div><div className="guide-controls"><div aria-label="Walkthrough progress">{steps.map((_,i)=><button aria-label={`Walkthrough step ${i+1}`} aria-pressed={guided===i} key={i} onClick={()=>setGuided(i)}>{i+1}</button>)}</div><button className="text-button" onClick={()=>setGuided((guided+1)%4)}>Next step →</button></div></section>
      <div className="experiment-layout"><aside className="controls"><div className="section-heading"><span className="eyebrow">EXPERIMENT CONTRACT</span><span className="pill">Synthetic</span></div><h2>Change the conditions.</h2><label>Workload<select value={kind} onChange={e=>setKind(e.target.value)} disabled={busy}><option value="prefix-shift">Popular prefixes shift</option><option value="long-prefix">Stable long-prefix reuse</option><option value="short">Short inputs · neutral control</option><option value="mixed-burst">Mixed lengths + load burst</option></select></label><p className="hint">{kind==='prefix-shift'?'Same rate and lengths; popular prefix groups change halfway through. The controller does not know when.':kind==='short'?'Tests when the faster long-input configuration is unnecessary.':'A reproducible synthetic workload with held-out prefix groups.'}</p>
      <label>Arrival rate <output>{rate} req/s</output><input aria-label="Arrival rate" type="range" min="2" max="12" step="1" value={rate} onChange={e=>setRate(+e.target.value)} disabled={busy}/></label>
      <label>Injection duration <output>{duration} s</output><input type="range" min="20" max="120" step="10" value={duration} onChange={e=>setDuration(+e.target.value)} disabled={busy}/></label>
      <label>Reconstruction per worker <output>{transition} s</output><input type="range" min="0" max="10" step=".5" value={transition} onChange={e=>setTransition(+e.target.value)} disabled={busy}/></label>
      <label>Assumed planning horizon<select value={horizon} onChange={e=>setHorizon(+e.target.value)} disabled={busy}>{[10,20,40,80,120].map(v=><option key={v} value={v}>{v} seconds</option>)}</select></label>
      <label>End-to-end objective <span className="unit">seconds</span><input type="number" min=".2" max="10" step=".1" value={slo} onChange={e=>setSlo(+e.target.value)} disabled={busy}/></label>
      <dl><div><dt>Reserved capacity</dt><dd>2 simulated workers</dd></div><div><dt>Arrival rate</dt><dd>{rate} requests/s{kind==='mixed-burst'?' + burst':''}</dd></div><div><dt>Observation</dt><dd>{duration+10}s, including 10s drain</dd></div><div><dt>Configuration</dt><dd>A ↔ B · same output model</dd></div></dl>
      <button className="primary" disabled={busy||slo<=0} onClick={compare}>{busy?'Computing…':'Compare four policies'}<span>→</span></button><p className="hint">Runs sequentially. No external endpoint, GPU, key, or account is used.</p></aside>
      <div className="results"><section className="chart-panel"><div className="section-heading"><div><span className="eyebrow">PRIMARY OUTCOME</span><h2>Requests that finish within the objective</h2></div><span className="small-note">ALL OFFERED REQUESTS COUNT</span></div>
      {runs.some(r=>r.summary)?<>
      <div className="legend">{runs.map((r,i)=><button onClick={()=>setSelected(i)} className={selected===i?'chosen':''} key={r.id}><i style={{background:COLORS[POLICIES.indexOf(r.spec.policy as Policy)]||COLORS[0]}}/>{LABELS[POLICIES.indexOf(r.spec.policy as Policy)]||'Recorded run'}</button>)}</div>
      <Cumulative runs={runs} cursor={cursor}/><div className="chart-footer"><span>Elapsed seconds from first scheduled arrival</span><div><button className="quiet" onClick={()=>{setCursor(0);setPlaying(true);}}>Replay events</button><select aria-label="Playback speed" value={playback} onChange={e=>setPlayback(+e.target.value)}>{[1,2,4].map(v=><option key={v} value={v}>{v}×</option>)}</select></div></div>
      <div className="score-row">{runs.map((r,i)=><button className={selected===i?'score selected':'score'} key={r.id} onClick={()=>setSelected(i)} style={{borderTopColor:COLORS[i]}}><span>{LABELS[POLICIES.indexOf(r.spec.policy)]||r.spec.policy}</span><strong>{r.summary?.qualified??'—'}<small> / {r.summary?.offered??'—'}</small></strong><span>{fmt((r.summary?.attainment||0)*100)}% attainment</span></button>)}</div>
      <p className="chart-note">Each curve is a separate matched run. These are {chosen?.origin==='synthetic'?'computed synthetic outcomes':'recorded observations'}, not measured counterfactuals. <button className="text-button" onClick={()=>setView('Inspect Evidence')}>Trace the numbers ↗</button></p></>:<div className="empty-chart"><div className="empty-axis"><span>qualifying completions</span><span>elapsed time →</span></div><h3>The answer starts with a run.</h3><p>Choose a workload, then compare the four policies.<br/>Every point will trace back to a request event.</p><button className="text-button" onClick={compare} disabled={busy}>Run the default experiment →</button></div>}
      </section>
      {chosen?.summary&&<section className="lifecycle"><div className="section-heading"><div><span className="eyebrow">THE TRANSITION COST</span><h2>{records?.transitions.length?'One worker changes. One keeps serving.':'Both workers kept serving.'}</h2></div><span className="pill">{chosen.spec.policy}</span></div><Timeline records={records} horizon={totalHorizon}/><p className="hint">Shaded intervals include reconstruction and warm-up. A second worker follows the first worker’s readiness check. No third prewarming GPU.</p></section>}
      </div></div>
      {chosen?.summary&&<div className="detail-grid"><section><div className="section-heading"><div><span className="eyebrow">WHY THIS ACTION?</span><h2>{decision?.action?.replaceAll('_',' ')||'No state-aware decision recorded'}</h2></div><span className="decision-mark">{decision?.action==='SWITCH'?'↗':decision?.action==='WAIT'?'Ⅱ':'—'}</span></div><p>{decision?.reasons?.join(' ')||'This policy held its configuration or did not produce a state-aware estimate. Inspect the lifecycle for actual changes.'}</p>{decision&&<><dl className="decision-numbers"><div><dt>Predicted net gain</dt><dd>{fmt(decision.gain_requests)} requests</dd></div><div><dt>Break-even</dt><dd>{fmt(decision.break_even_s)} s</dd></div><div><dt>Assumed horizon</dt><dd>{decision.horizon_s} s</dd></div></dl><div className="sensitivity"><span>Horizon sensitivity · predicted, not observed</span>{decision.sensitivity.map(s=><div key={s.horizon_s}><span>{s.horizon_s}s</span><span className="dashed-line"/><b>{fmt(s.gain_requests)} requests</b></div>)}</div><p className="hint">Evidence: {decision.evidence_ids.join(', ')}. Range uncertainty is not a confidence interval.</p></>}</section><section><span className="eyebrow">LATENCY CHECK</span><h2>Queueing stays in the picture.</h2><Latency records={records} slo={chosen.spec.slo.e2e_s} horizon={totalHorizon}/><p className="hint">Scheduled arrival → completion, in seconds. Dashed line: declared objective. Points beyond it fail the latency gate.</p></section></div>}
    </>}
    {view==='Run on Your Setup'&&<>
      <div className="setup-intro"><h2>Start with a matched evaluation.</h2><p>For a deployment recommendation, import current, candidate and measured-transition bundles from at least three paired seeds in <button className="text-button" onClick={()=>setView('Evaluate Deployment')}>Evaluate Deployment</button>. To inspect a single bundle, use the importer below. <a href="https://github.com/Kakarottoooo/TransitionBench/blob/main/docs/integration.md" target="_blank" rel="noreferrer">Read the integration guide</a>.</p><p>Endpoint smoke tests and managed experiments are separate capabilities with their own authorization requirements.</p></div><div className="setup-grid">
      <section><span className="lane">B · EVIDENCE & SHADOW</span><h2>Inspect an evidence bundle</h2><p>Import a ZIP exported by TransitionBench. Checksums, lifecycle order, schema and claim levels are verified before replay.</p><label className="file-input">Choose evidence ZIP<input type="file" accept=".zip" onChange={e=>importFile(e.target.files?.[0])}/></label><p className="hint" role="status">{importInfo||'Maximum 32 MiB. No files are uploaded to an external service.'}</p><label>Recompute end-to-end SLO (s)<input type="number" value={slo} min=".2" step=".1" onChange={e=>setSlo(+e.target.value)}/></label><button className="primary" disabled={!bundleId||busy||slo<=0} onClick={replay}>Replay imported evidence →</button><p className="hint">Only SLO attainment is recomputed. A different policy or workload needs a new experiment.</p></section>
      <section><span className="lane">A · INFERENCE ONLY</span><h2>Test an owned endpoint</h2><p>Endpoint origins, credentials and hard limits are configured on the local backend. Credentials never enter this page.</p>{endpoints.length?<><label>Approved endpoint<select value={endpoint} onChange={e=>setEndpoint(e.target.value)}>{endpoints.map(e=><option key={e.id}>{e.id}</option>)}</select></label><div className="preview"><strong>Traffic preview</strong><p>1 synthetic arithmetic request · ≤ 32 output tokens · 512 total-token reservation · concurrency 1 · 11-second budget.</p></div><button className="primary" onClick={smoke} disabled={busy}>Run one-request smoke test →</button></>:<div className="limitation"><strong>No endpoint authorized</strong><p>Add the exact endpoint and budget to an operator config, then launch:</p><code>transitionbench serve --config examples/operator.json</code></div>}<p className="hint">API latency cannot establish physical GPU equality or private cache-transition behavior.</p></section>
      <section><span className="lane">C · MANAGED EXPERIMENT</span><h2>Use the two-GPU lab</h2><p>An operator-owned hook performs draining, reconstruction, readiness, warm-up and rollback. Every mutation requires an exact, unexpired plan.</p><div className="limitation"><strong>{caps?.features?.controlled_rollout?.state==='supported'?'Hook configured; resource verification required':'Managed control unavailable'}</strong><p>{caps?.features?.controlled_rollout?.reason||'Loading capability manifest…'}</p><code>transitionbench doctor</code></div><p className="hint">Read docs/deployment-adapter.md. A public Wafer key provides inference access, not backend deployment authority.</p><a className="text-button" href="/docs" target="_blank" rel="noreferrer">Inspect the plan API ↗</a></section></div>
      <section className="capabilities"><h2>Capability check</h2>{caps?<table><thead><tr><th>Capability</th><th>Status</th><th>Evidence</th><th>Boundary</th></tr></thead><tbody>{Object.entries(caps.features).map(([name,c]:[string,any])=><tr key={name}><td>{name.replaceAll('_',' ')}</td><td><span className={'cap '+c.state}>{c.state}</span></td><td>{c.evidence}</td><td>{c.reason}</td></tr>)}</tbody></table>:<p>Loading local capabilities…</p>}</section>
    </>}
    {view==='Inspect Evidence'&&<section className="evidence-view"><div className="section-heading"><div><span className="eyebrow">RECOMPUTABLE, NOT SELF-CERTIFIED</span><h2>The number is only the beginning.</h2></div>{chosen?.summary&&<a className="primary" href={`/api/v1/runs/${chosen.id}/artifacts/evidence.zip`}>Export evidence ZIP ↓</a>}</div>{chosen?.summary?<>
    <div className="run-selector"><label>Inspect run<select value={selected} onChange={e=>setSelected(+e.target.value)}>{runs.map((r,i)=><option key={r.id} value={i}>{r.spec.policy} · {r.id.slice(0,12)}</option>)}</select></label><span>{chosen.mode} · {chosen.origin}</span><a href={`/api/v1/runs/${chosen.id}/artifacts/report.html`}>Self-contained HTML report ↗</a></div>
    <div className="evidence-counts"><div><strong>{chosen.summary.qualified}</strong><span>qualified completions</span></div><div><strong>{chosen.summary.offered}</strong><span>all offered requests</span></div><div><strong>{fmt(chosen.summary.goodput_rps,3)}</strong><span>qualified requests / second</span></div><div><strong>{chosen.summary.observation_s}s</strong><span>common observation horizon</span></div></div>
    <p className="formula">Qualified = completed within horizon ∩ scheduled-arrival latency objectives ∩ nonempty output ∩ validity gate</p><div className="detail-grid"><div><h3>Who was helped or harmed?</h3><table><thead><tr><th>Class</th><th>Offered</th><th>Qualified</th><th>Attainment</th></tr></thead><tbody>{Object.entries(chosen.summary.by_class).map(([k,v])=><tr key={k}><td>{k}</td><td>{v.offered}</td><td>{v.qualified}</td><td>{fmt(v.attainment*100)}%</td></tr>)}</tbody></table><p>Termination counts: {Object.entries(chosen.summary.terminations).map(([k,v])=>`${k}: ${v}`).join(' · ')}</p><p className="hint">This view has one trial per policy. Confidence intervals require repeated matched runs; no production p99 claim is made.</p></div><div><h3>Independent verification</h3><code>transitionbench verify PATH_TO_EXTRACTED_BUNDLE</code><p>The standalone verifier reconstructs counts and latencies from raw records. Checksums prove integrity relative to the supplied manifest, not authenticity.</p><a href={`/api/v1/runs/${chosen.id}/artifacts/verification.json`}>Read verification result ↗</a><p className="hint">Unavailable: exact inter-token timing, observed active GPU seconds, monetary cost. Missing values remain null.</p></div></div>
    <h3>Raw event excerpt</h3><pre>{JSON.stringify(records?.requests.slice(0,3)||[],null,2)}</pre><details><summary>Assumptions, configurations and provenance</summary><pre>{JSON.stringify(records?.manifest||{},null,2)}</pre></details><details><summary>Decision records</summary><pre>{JSON.stringify(records?.decisions||[],null,2)}</pre></details>
    </>:<div className="empty"><h3>No evidence selected yet.</h3><p>Run a synthetic comparison or import an existing evidence bundle.</p><button className="primary" onClick={()=>setView('Understand')}>Set up an experiment →</button></div>}</section>}
    <footer><span>TransitionBench · an experimental decision-support tool</span><span>No official Wafer affiliation · Conclusions require workload-matched evidence</span></footer>
    </main>
  </div>;
}

function Cumulative({runs,cursor}:{runs:Run[];cursor:number}){
  const h=Math.max(...runs.map(r=>r.summary?.observation_s||1)),max=Math.max(1,...runs.map(r=>r.summary?.offered||1));
  const x=(v:number)=>55+v/h*795,y=(v:number)=>275-v/max*235;
  return <svg className="cumulative" viewBox="0 0 890 315" role="img" aria-label="Cumulative SLO-qualified completions from raw events">{[0,.25,.5,.75,1].map(f=><g key={f}><line x1="55" x2="850" y1={y(f*max)} y2={y(f*max)} className="grid-line"/><text x="42" y={y(f*max)+4} textAnchor="end">{Math.round(f*max)}</text><text x={x(f*h)} y="301" textAnchor="middle">{Math.round(f*h)}s</text></g>)}{runs.map((r,i)=><polyline key={r.id} points={`55,275 ${(r.summary?.cumulative||[]).filter(p=>p.at_s<=h*cursor).map(p=>`${x(p.at_s)},${y(p.qualified)}`).join(' ')}`} fill="none" stroke={COLORS[POLICIES.indexOf(r.spec.policy)]||COLORS[i]} strokeWidth="3"/>)}<text x="55" y="20">qualifying completions</text>{cursor<1&&<line x1={x(h*cursor)} x2={x(h*cursor)} y1="35" y2="275" stroke="#243c51" strokeDasharray="4 4"/>}</svg>;
}
function Timeline({records,horizon}:{records:Records|null;horizon:number}){
  return <div className="timeline">{['0','1'].map(worker=>{const events=records?.transitions.filter(e=>e.worker_id===worker)||[];return <div className="worker" key={worker}><span>Worker {+worker+1}</span><div className="track"><span className="serving">Serving</span>{events.filter(e=>e.state==='RECONFIGURING').map((e,i)=>{const end=events.find(n=>n.at_s>=e.at_s&&n.state==='OBSERVING')?.at_s||horizon;return <div title={`Reconstruction and warm-up: ${fmt(e.at_s)}–${fmt(end)}s`} key={i} className="transition-block" style={{left:`${Math.min(100,e.at_s/horizon*100)}%`,width:`${Math.max(1,Math.min(horizon,end)-e.at_s)/horizon*100}%`}}/>;})}</div></div>;})}<div className="timeline-scale"><span>0s</span><span>{horizon}s · shared clock</span></div></div>;
}
function Latency({records,slo,horizon}:{records:Records|null;slo:number;horizon:number}){
  const rows=(records?.requests||[]).filter(r=>r.completed_s!=null);const max=Math.max(slo*1.3,...rows.map(r=>r.completed_s!-r.scheduled_s),.1);const y=(v:number)=>160-v/max*135;
  return <svg viewBox="0 0 500 195" className="latency" role="img" aria-label="Scheduled-arrival latency relative to SLO"><line x1="40" x2="485" y1={y(slo)} y2={y(slo)} stroke="#b6662c" strokeDasharray="5 4"/><text x="40" y={y(slo)-7}>objective {slo}s</text>{rows.filter((_,i)=>i%Math.max(1,Math.floor(rows.length/160))===0).map(r=><circle key={r.request_id} cx={40+r.scheduled_s/horizon*440} cy={y(r.completed_s!-r.scheduled_s)} r="2.4" fill={r.completed_s!-r.scheduled_s>slo?'#b6662c':'#057b85'} opacity=".65"/>)}<text x="40" y="186">0s</text><text x="440" y="186">{horizon}s</text><text x="5" y="25">{fmt(max)}s</text></svg>;
}
createRoot(document.getElementById('root')!).render(<App/>);

