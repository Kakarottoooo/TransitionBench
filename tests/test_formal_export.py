"""Operator failure evidence must survive the campaign export boundary."""
import importlib.util
import json
import sqlite3
import subprocess
import pytest
from pathlib import Path
from types import SimpleNamespace

from transitionbench.rollout import stable_hash


def test_worker_startup_evidence_is_bounded_and_owner_checked(tmp_path, monkeypatch):
    scripts = Path(__file__).resolve().parents[1]/'scripts'
    monkeypatch.syspath_prepend(str(scripts))
    spec = importlib.util.spec_from_file_location('startup_evidence_runner', scripts/'run_formal_study.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    calls = []
    def inspect(worker):
        if worker == 'unowned':
            raise ValueError('Container ownership changed')
        return {'State': {'Status': 'running', 'ExitCode': 0, 'OOMKilled': False},
                'Config': {'Env': ['PRIVATE=value']}}
    def capture(args, **kwargs):
        calls.append(args)
        assert kwargs['timeout'] == 5 and kwargs['errors'] == 'replace'
        if args[0] != 'docker':
            return SimpleNamespace(returncode=0, stdout='host-version', stderr='')
        assert args[1:5] == ['logs', '--timestamps', '--tail', '200']
        if args[-1] == 'owned-slow':
            raise subprocess.TimeoutExpired(args, 5)
        return SimpleNamespace(returncode=0, stdout='x'*70000+'startup-marker')
    monkeypatch.setattr(runner.subprocess, 'run', capture)
    adapter = SimpleNamespace(workers={'unowned': {}, 'slow': {}, 'ok': {}},
                              inspect=inspect, name=lambda worker: 'owned-'+worker)
    runner.retain_worker_startup_evidence(tmp_path, adapter)
    assert [args[-1] for args in calls if args[0] == 'docker'] == ['owned-slow', 'owned-ok']
    index = json.loads((tmp_path/'operator/worker-startup.json').read_text())
    assert index['unowned']['error_type'] == 'ValueError'
    assert index['slow']['error_type'] == 'TimeoutExpired'
    assert index['ok']['state']['OOMKilled'] is False
    text = (tmp_path/'operator/worker-ok-startup.log').read_text()
    assert len(text) == 65536 and text.endswith('startup-marker')
    assert 'PRIVATE' not in json.dumps(index)


def test_gpu_failure_export_preserves_diagnostic_and_continues_after_timeout(tmp_path, monkeypatch):
    scripts = Path(__file__).resolve().parents[1]/'scripts'
    monkeypatch.syspath_prepend(str(scripts))
    spec = importlib.util.spec_from_file_location('gpu_failure_export_runner', scripts/'run_formal_study.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    calls = []
    def capture(args, **kwargs):
        calls.append(args[0])
        assert kwargs['timeout'] == 5 and not kwargs.get('shell', False)
        if args[0] == 'nvidia-smi':
            return SimpleNamespace(returncode=18, stdout='', stderr='x'*9000+'Driver/library version mismatch')
        if args[0] == 'cat':
            raise subprocess.TimeoutExpired(args, 5)
        assert args == ['dpkg-query', '-W', '-f=${binary:Package}\t${Version}\n', 'nvidia-*', 'libnvidia-*']
        return SimpleNamespace(returncode=0, stdout='libnvidia-compute-580\t580.178.04\n', stderr='')
    monkeypatch.setattr(runner.subprocess, 'run', capture)
    runner.retain_worker_startup_evidence(tmp_path, SimpleNamespace(workers={}))
    evidence = json.loads((tmp_path/'operator/host-gpu-diagnostics.json').read_text())
    assert calls == ['nvidia-smi', 'cat', 'dpkg-query']
    assert evidence['nvidia_smi']['exit_code'] == 18
    assert evidence['nvidia_smi']['stderr'].endswith('Driver/library version mismatch')
    assert len(evidence['nvidia_smi']['stderr']) == 8192
    assert evidence['nvidia_smi']['truncated']
    assert evidence['loaded_driver']['error_type'] == 'TimeoutExpired'
    assert '580.178.04' in evidence['installed_driver_packages']['stdout']


def test_new_host_must_match_its_frozen_driver(monkeypatch):
    scripts = Path(__file__).resolve().parents[1]/'scripts'
    monkeypatch.syspath_prepend(str(scripts))
    spec = importlib.util.spec_from_file_location('formal_host_runner', scripts/'run_formal_study.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    health = {'controlled_ready': True, 'gpus': [{'driver': '580.178.04'}, {'driver': '580.178.04'}]}
    runner.require_frozen_host(health, '580.178.04')
    with pytest.raises(ValueError):
        runner.require_frozen_host(health, 'different-driver')
    health['gpus'] = [{'driver': 'new-pinned-driver'}, {'driver': 'new-pinned-driver'}]
    runner.require_frozen_host(health, 'new-pinned-driver')
    health['controlled_ready'] = False
    with pytest.raises(ValueError):
        runner.require_frozen_host(health, 'new-pinned-driver')


def test_export_retains_failed_warmup_and_reports_missing_receipts(tmp_path,monkeypatch):
    scripts=Path(__file__).resolve().parents[1]/'scripts'
    monkeypatch.syspath_prepend(str(scripts))
    spec=importlib.util.spec_from_file_location('formal_export_runner',scripts/'run_formal_study.py')
    runner=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    out=tmp_path/'campaign'
    (out/'operator').mkdir(parents=True)
    failed='a'*32+':0:warmup:72'
    missing='b'*32+':0:warmup:73'
    initial='c'*32+':1:initial-warmup'
    with sqlite3.connect(out/'operator/hook.db') as db:
        db.execute('CREATE TABLE operations (key TEXT PRIMARY KEY, status TEXT)')
        db.executemany('INSERT INTO operations VALUES (?,?)',[(failed,'failed'),(missing,'failed'),(initial,'complete')])
    cache=tmp_path/'cache'
    source=cache/'transitionbench-fixture/warmup'
    source.mkdir(parents=True)
    report={'reason':'request_limit','stable':False,'operation_key':failed,'requests':[{'quality_valid':True}]}
    (source/(stable_hash(failed)+'.json')).write_text(json.dumps(report))
    (source/(stable_hash(initial)+'.json')).write_text(json.dumps({'operation_key':initial,'stable':True}))
    (source/'unrelated.json').write_text('private-unrelated-data')
    runner.retain_operator_evidence(out,SimpleNamespace(cache=cache,owner='fixture'))
    retained=out/'operator/warmup'/(stable_hash(failed)+'.json')
    assert json.loads(retained.read_text())==report
    index=json.loads((out/'operator/evidence.json').read_text())
    assert index['missing_warmup_receipts']==[missing]
    assert index['retained_warmup_receipts']==[failed,initial]
    assert not (out/'operator/warmup/unrelated.json').exists()


@pytest.mark.parametrize('sweep,kind', [(False, 'short'), (True, 'mixed-burst')])
async def test_formal_preflight_rejects_uninformative_or_unsupported_protocol_before_host(tmp_path, monkeypatch, sweep, kind):
    scripts = Path(__file__).resolve().parents[1]/'scripts'
    monkeypatch.syspath_prepend(str(scripts))
    spec = importlib.util.spec_from_file_location('formal_gate_runner', scripts/'run_formal_study.py')
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    collection = json.loads((scripts.parent/'examples/calibration-collection.json').read_text())
    collection['experiment']['workload']['kind'] = kind
    if sweep:
        collection['capacity_rates_rps'] = [1, 2, 3]
    frozen = {'collection': collection}
    path = tmp_path/'frozen.json'
    path.write_text(json.dumps(frozen))
    def forbidden():
        pytest.fail('Research protocol must be refused before hardware operations')
    monkeypatch.setattr(runner, 'doctor', forbidden)
    with pytest.raises(runner.ResearchGateError):
        await runner.execute(tmp_path, path, stable_hash(frozen), 0)
    assert not (tmp_path/'evidence').exists()
