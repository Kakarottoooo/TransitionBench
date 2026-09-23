"""Write source-backed human reports from the frozen release study."""
import html
import json
import statistics
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
study=ROOT/'reports'/'release-study'
trials=json.loads((study/'synthetic-trials.json').read_text(encoding='utf-8'))
comparisons=json.loads((study/'paired-comparisons.json').read_text(encoding='utf-8'))
local=json.loads((study/'local-http-trials.json').read_text(encoding='utf-8'))
overhead=json.loads((study/'instrumentation-overhead.json').read_text(encoding='utf-8'))
policies=['StaticBest','SteadyStateFirst','FixedHysteresis','StateAware']
rows=[]
htmlrows=[]
for case in dict.fromkeys(r['case'] for r in trials):
    group=[r for r in trials if r['case']==case]
    values=[statistics.mean(r['qualified'] for r in group if r['policy']==p) for p in policies]
    rows.append('| '+case+' | '+' | '.join(f'{v:g}' for v in values)+' |')
    first=group[0]
    htmlrows.append('<tr><td>'+html.escape(case)+'</td>'+''.join(f'<td>{v:g}</td>' for v in values)+f'<td><a href="release-study/{first["bundle"]}/report.html">raw run ↗</a></td></tr>')
table='\n'.join(rows)
local_table='\n'.join('| '+str(rate)+' | '+', '.join(str(r['summary']['qualified'])+'/'+str(r['summary']['offered']) for r in local if r['rate_rps']==rate)+' |' for rate in (10,100,250))
burst=comparisons['mixed-burst']
ms=overhead['mean_added_s']*1000
report=f'''# TransitionBench 研究与交付报告

日期：2026-09-19。结论：**证据不足**。当前数据不支持“状态感知控制器优于强静态或合理滞回基线”的真实 GPU 主张。

## 已实现与证据边界

交付了 Python 核心、可交互英文应用、版本化 HTTP、CLI、Python/TypeScript SDK、官方 SDK stdio MCP、
证据导出/回放、独立验证器、部署 hook 和共用滚动执行器。CPU 子进程 hook 实际重启了本任务创建的
两个服务，观察到 PID、配置代次和就绪状态变化；这证明生命周期路径能执行，不证明 GPU 推理性能。

本机只有一张 RTX 3080 Ti 12 GiB，Docker Linux daemon 未运行。没有本任务授权的 Wafer 凭据或
在线流量预算。没有购买资源、发布包、公开部署、联系 Wafer，或操作生产基础设施。

## 预声明试验及最终结果

最终数据源：[冻结协议](release-study/predeclared-protocol.json)、[140 个试验索引](release-study/synthetic-trials.json)、
[配对比较](release-study/paired-comparisons.json)、[独立复算](release-study/independent-study-verification.json)。
每个条件 5 个配对 test seeds，四策略随机顺序、顺序运行；不是同时用八个 worker 冒充两张 GPU。
固定预算/模型服务分布/输出假设，统一观察窗口，统计单位为整次配对试验。

下表是**仿真**每次试验平均 SLO 合格请求数，不是 GPU 测量：

| 条件 | StaticBest | SteadyStateFirst | FixedHysteresis | StateAware |
|---|---:|---:|---:|---:|
{table}

StateAware 相对 FixedHysteresis 在全部这些条件中的配对差值为 0。突发混合条件下，相对强静态基线
平均差值 {burst[0]['difference_requests']:g} 个请求，run-level bootstrap 95% 区间为
{burst[0]['paired_bootstrap_95_interval']}；相对 SteadyStateFirst 为 {burst[1]['difference_requests']:g}，
区间 {burst[1]['paired_bootstrap_95_interval']}。后一个差异不能作为状态感知模型的独有收益，因为固定滞回
取得了相同结果。五个种子和简单环境不足以支持生产 p99 或总体有效性的推断。

全为零的配对区间只表示这些有限合成样本恰好相同，不是无限精度保证。按请求类别的伤害、原始时序、
终止原因均保存在每个 bundle 内，丢弃、超时、未完成及质量失败不会被移出分母。

## 方法审查与负面证据

早期探索使用跨类别合并校准的静态配置，导致长前缀场景中静态配置不合适，制造出明显的表观切换收益。
最终将 StaticBest 加强为“每个预声明负载类别都只用 calibration seeds 选择最优固定配置”，并让四策略
从相同初始配置开始。该收益随之消失。滞回阈值、持续时间和最短驻留在与最终基础负载相同的 9 req/s
条件上使用独立 tuning seeds 选择，未用 test 结果选参数。较早试验保留为探索记录，不纳入最终主张。

还修正了需求预测错误：额外空闲容量不是额外完成请求。所有在线策略现在都用过去的到达率约束预期吞吐。
StateAware 的切换损失按配置对、工作负载类别及当前在途请求桶估计；未校准桶返回证据不足。
这些仍是简单仿真假设，不能被解释为已经观察到 GPU KV 缓存状态。

另一个实际失败是同步证据持久化阻塞发压线程，部分早期本地试验超过调度误差容忍度并被判 INVALID。
保留了无效试验，而不是放宽门槛把它们改成有效。修复将写入移到后台，并在注入时钟之前初始化 HTTP 客户端。
修复后最终 9 次真实 loopback HTTP 试验均有效。

## 真实本地 HTTP 测量

来源：[本地试验](release-study/local-http-trials.json)。目标是带真实 socket/SSE 的协议夹具，不运行语言模型。
单并发、0.8 秒开放式注入、1 秒 drain；以下为合格/全部 offered 请求：

| 到达率 req/s | 三次重复 |
|---|---|
{local_table}

高到达率下的丢弃是预算/过载事实，不会通过放慢输入抹掉。每条请求记录计划时间、实际发送、首个可见内容、
最终内容、结束原因、字符数及有效性检查；usage 缺失保持 null。字符块不是精确 token。

五个配对块、每块每臂 12 个同内容请求的本地仪表开销，平均增加 **{ms:.3f} ms/请求**。
原始样本：[instrumentation-overhead.json](release-study/instrumentation-overhead.json)。这是串行、低负载、
loopback 夹具测量，存在 Windows 调度噪声；这个开销不能忽略，也不能外推为 GPU 路由器开销。
真实 GPU 路由/仪表扰动、warmup 充分性和任务质量重复性仍未验证。

## 独立验证与集成

独立验证器不调用生产方 summarize 函数。它从原始记录复算请求数、分母、延迟、资源区间和配对差值，
发现 checksum 错误、缺失/重复请求、不可能的生命周期、版本不兼容及不支持的证据级别。
140 个最终仿真 bundle 与 9 个本地 HTTP bundle 已通过复算。checksum 只证明相对 manifest 的完整性，
不证明来源真实或第三方认证。默认不保留文本，因此不能仅凭 metadata 重新评估语义答案。

HTTP/Python/TypeScript/CLI/MCP 共享一个核心。MCP 用官方 SDK 完成真实 2025-11-25 握手与工具调用，
默认只分析和规划，不提供运行、审批、执行、任意 URL 或 shell 工具。外部 hook 通过真实 HTTP 验证了
生命周期、重复操作、参数冲突和失败后不盲重放。详细测试与安装结果在 [acceptance.json](acceptance.json)。

## 明确未满足的条件

- G5：没有双同型号物理 GPU 上的有效重复试验；没有验证 vLLM Docker 实际启动、充分预热、模型质量
  重复性、真实 cache/compile 因果机制、active GPU usage 或路由开销。当前 managed policy trial 限制为
  最多一次精确批准的候选切换，尚未验证多次往返切换实验。
- G6：没有执行授权 Wafer 在线 smoke。公开文档和 fixture 合约不替代真实兼容性。
- GPU 校准需要授权环境中的实际测量和证据输入；工具会拒绝缺失校准，不会把仿真结果重新标为测量。
- 当前仿真只适合解释和开发。它未以硬件数据验证，区间也不能证明现实收益。

## 下一步与研究判断

优先取得一个可独立观察资源的、授权且有明确预算的双 GPU 实验环境。先完成固定配置校准、正常预热及
输出有效性评估，再运行匹配条件的四策略重复试验。如果合理静态/滞回已经足够，应保留简单方案；不要
通过增加重启频率、选择性轨迹、额外隐藏 GPU 或私有 Wafer 能力维护一个薄弱结论。

更强通用模型可能吸收简单的参数建议与可视化。较持久的价值是授权边界、独立原始证据、实际部署状态、
输出/资源核验和可追溯的失败历史；本次实现优先保留这些边界，而不是宣称一个未经证实的优化算法。

结论：**证据不足**。在已测合成条件中，额外 StateAware 复杂度未超过强基线。能够改变这一结论的是
满足相同资源、模型、质量和统计契约的真实受控数据，而不是更漂亮的演示或更大的单次百分比。
'''
(ROOT/'reports'/'research-report.zh-CN.md').write_text(report,encoding='utf-8')
first=next(r for r in trials if r['case']=='mixed-burst' and r['policy']=='StateAware')
bundle=study/first['bundle']
summary=json.loads((bundle/'summary.json').read_text(encoding='utf-8'))
points='0,210 '+' '.join(f"{p['at_s']/summary['observation_s']*900:.2f},{210-p['qualified']/summary['offered']*190:.2f}" for p in summary['cumulative'])
page=f'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; img-src data:"><title>TransitionBench — inspect the evidence</title><style>
body{{font:16px/1.6 system-ui,sans-serif;max-width:1120px;margin:auto;padding:40px;color:#20354b;background:#f0f4f7}}h1{{font-size:48px;line-height:1.12;letter-spacing:-1.5px}}h1 em{{color:#057b85;font-style:normal}}h2{{font-size:24px}}section{{border:1px solid #d3dfe6;background:white;border-radius:12px;padding:28px;margin:25px 0}}.badge{{display:inline-block;background:#ffebcf;border:1px solid #dcbd92;padding:7px 12px;border-radius:5px;font:12px monospace}}table{{width:100%;border-collapse:collapse;font-size:13px}}td,th{{text-align:left;border-bottom:1px solid #dde6ec;padding:12px 8px}}a{{color:#08758a}}pre{{white-space:pre-wrap;background:#edf3f6;padding:15px;font-size:12px}}svg{{width:100%}}.table{{overflow:auto}}small{{color:#60798c}}@media(max-width:600px){{body{{padding:20px}}h1{{font-size:34px}}section{{padding:18px}}}}
</style><p>TRANSITIONBENCH / EXPERIMENT NOTEBOOK</p><h1>Should we deploy the<br/><em>faster configuration</em> now?</h1><p>Compare steady-state gains with the requests lost during the transition.</p><p class="badge">SIMULATION · synthetic policy comparisons</p><p>140 matched trials, seven conditions, five seeds. No GPU improvement claim. Live Wafer compatibility was not tested.</p>
<section><h2>The strong baselines are sufficient in this synthetic model.</h2><p>StateAware did not outperform tuned FixedHysteresis in any tested condition. In the mixed burst, its mean difference versus StaticBest was −0.6 qualifying requests. These findings do not establish effectiveness on GPUs.</p><div class="table"><table><tr><th>Condition</th><th>StaticBest</th><th>SteadyStateFirst</th><th>FixedHysteresis</th><th>StateAware</th><th>Evidence</th></tr>{''.join(htmlrows)}</table></div><small>Mean SLO-qualified completions per trial. Every denominator includes client drops and unfinished requests. Conditions have different offered loads; compare policies within each row.</small></section>
<section><h2>A curve you can recompute</h2><p>Synthetic mixed-burst run <code>{first['id']}</code>: {summary['qualified']} / {summary['offered']} offered requests qualify.</p><svg viewBox="0 0 900 240" role="img" aria-label="Cumulative qualifying completions in a synthetic mixed-burst run"><path d="M0 210H900" stroke="#bacad5"/><polyline points="{points}" fill="none" stroke="#057b85" stroke-width="3"/><text x="0" y="235" fill="#60798c">0s</text><text x="840" y="235" fill="#60798c">40s</text></svg><a href="release-study/{first['bundle']}/report.html">Inspect this run, assumptions and raw events ↗</a></section>
<section><h2>Measured loopback evidence is separate.</h2><p>Nine actual local HTTP/SSE trials validate the scheduler and protocol. The target is an arithmetic fixture, not a language model. Instrumentation added a mean {ms:.3f} ms/request in five low-load paired blocks; this is not GPU router overhead.</p><a href="release-study/local-http-trials.json">Read raw trial summaries</a> · <a href="release-study/instrumentation-overhead.json">Read overhead samples</a> · <a href="cpu-process-transition.json">Inspect actual CPU process transitions</a></section>
<section><h2>Try it, verify it, disagree with it.</h2><pre>python -m pip install dist/transitionbench-0.3.0-py3-none-any.whl\ntransitionbench demo\ntransitionbench verify PATH_TO_EXTRACTED_BUNDLE\npython scripts/verify_study.py reports/release-study</pre><p>The demo needs no key or GPU. Import evidence or use an operator-approved endpoint. Managed deployment requires a separate exact plan approval and observed resources.</p><a href="../README.md">Quickstart</a> · <a href="../docs/reproduction.md">Owned endpoint / two-GPU paths</a> · <a href="acceptance.json">Acceptance gates</a> · <a href="research-report.zh-CN.md">Chinese research report</a></section>
<p><small>Integrity is not authenticity or independent certification. No analytics, remote assets or scripts. This file is a self-contained read-only report; evidence links resolve within the delivered repository.</small></p></html>'''
(ROOT/'reports'/'overview.html').write_text(page,encoding='utf-8')
print('Generated Chinese research report and self-contained English HTML overview')
