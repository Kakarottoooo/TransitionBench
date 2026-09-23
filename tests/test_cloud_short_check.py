"""Short-diagnostic acceptance is stricter than general measurement validity."""
import importlib.util
import json
import time
import tarfile
import hashlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from transitionbench.endpoint import EndpointClient, NetworkPolicy
from transitionbench.schemas import EndpointSpec, WorkloadSpec, ExperimentSpec, ResourceBudget
from transitionbench.workloads import generate


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def runner():
    spec = importlib.util.spec_from_file_location('cloud_short_check', ROOT / 'scripts/run_cloud_short_check.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_completed_wrong_response_stops_runner_and_preserves_trial(runner, tmp_path):
    """The collector returns a valid measurement even when useful output fails."""
    bundle = tmp_path / 'bundles/001'
    bundle.mkdir(parents=True)
    (bundle / 'requests.jsonl').write_text(json.dumps({
        'request_id': 'calibration-9053-3032', 'termination': 'complete',
        'quality_valid': False, 'status_code': 200, 'finish_reason': 'stop', 'output_chars': 26,
    }) + '\n')
    measurement = {'bundle': 'bundles/001', 'trial': {'role': 'fixed'}}
    async def trial(number, value):
        collector.measurements.append(measurement)
    collector = SimpleNamespace(trial=trial, measurements=[])
    runner.O = tmp_path
    runner.state = {'state': 'MEASURING', 'runs': []}
    # The collector already verified this bundle. Preserve that valid-measurement
    # signal while exercising the actual caller that used to mark it PASSED.
    with pytest.raises(ValueError, match='quality'):
        await runner.run_trial(collector, 1, measurement['trial'])
    assert runner.state['runs'][0]['bundle'] == 'bundles/001'
    saved = json.loads((tmp_path / 'status.json').read_text())
    assert saved['runs'][0]['acceptance']['quality_invalid_complete_ids'] == ['calibration-9053-3032']


@pytest.mark.parametrize('termination', ['error', 'timeout', 'partial_stream', 'cancelled', 'unfinished', 'budget_refusal'])
def test_request_failures_cannot_be_hidden_by_correct_completions(runner, tmp_path, termination):
    rows = [dict(request_id='ok', termination='complete', quality_valid=True,
                 finish_reason='stop', status_code=200, output_chars=26),
            dict(request_id='failed', termination=termination)]
    path = tmp_path / 'requests.jsonl'
    path.write_text('\n'.join(json.dumps(r) for r in rows))
    result = runner.assess_requests(path)
    assert result['status'] == 'FAILED_REQUEST_GATE'
    assert result['failed_request_ids'] == ['failed']


@pytest.mark.parametrize('completion', [True, False])
def test_overload_drops_stay_in_denominator_but_cannot_alone_pass(runner, tmp_path, completion):
    rows = [dict(request_id='drop', termination='client_drop', quality_valid=False)]
    if completion:
        rows.append(dict(request_id='ok', termination='complete', quality_valid=True,
                         finish_reason='stop', status_code=200, output_chars=26))
    path = tmp_path / 'requests.jsonl'
    path.write_text('\n'.join(json.dumps(r) for r in rows))
    result = runner.assess_requests(path)
    assert result['status'] == ('PASSED' if completion else 'FAILED_REQUEST_GATE')
    assert result['offered'] == len(rows) and result['client_drop'] == 1


@pytest.mark.parametrize('change', [
    {'quality_valid': 'true'}, {'quality_valid': 1}, {'quality_valid': None},
    {'finish_reason': 'length'}, {'status_code': 500}, {'output_chars': 0},
])
def test_completion_requires_explicit_quality_and_success_contract(runner, tmp_path, change):
    row = dict(request_id='wrong', termination='complete', quality_valid=True,
               finish_reason='stop', status_code=200, output_chars=26)
    row.update(change)
    path = tmp_path / 'requests.jsonl'
    path.write_text(json.dumps(row) + '\n')
    assert runner.assess_requests(path)['status'] == 'FAILED_QUALITY_GATE'


def test_empty_journal_cannot_pass(runner, tmp_path):
    path = tmp_path / 'requests.jsonl'
    path.write_text('')
    assert runner.assess_requests(path)['status'] == 'FAILED_REQUEST_GATE'


@pytest.mark.parametrize('valid', [False, True])
def test_final_exit_exports_and_stops_before_next_trial_on_failure(runner, tmp_path, monkeypatch, valid):
    """Exercise the final runner catch/cleanup/archive/exit path without cloud IO."""
    runner.R, runner.O = tmp_path, tmp_path / 'evidence'
    launched = []

    async def main():
        bundle = runner.O / 'bundles/001'
        bundle.mkdir(parents=True)
        (bundle / 'requests.jsonl').write_text(json.dumps(dict(request_id='wrong',
            termination='complete', quality_valid=valid, status_code=200,
            finish_reason='stop', output_chars=26)) + '\n')
        async def trial(number, value):
            launched.append(number)
            collector.measurements.append({'bundle': 'bundles/001', 'trial': value})
        collector = SimpleNamespace(trial=trial, measurements=[])
        for number in (1, 2):
            await runner.run_trial(collector, number, {'role': 'fixed'})
        runner.status(state='PASSED')

    monkeypatch.setattr(runner, 'main', main)
    assert runner.execute_cloud(time.time() + 1200) == (0 if valid else 1)
    assert launched == ([1, 2] if valid else [1])
    assert json.loads((runner.O / 'status.json').read_text())['state'] == ('PASSED' if valid else 'FAILED_QUALITY_GATE')
    assert json.loads((runner.O / 'cleanup.json').read_text()) == {'leases': [], 'pending': []}
    assert (tmp_path / (runner.RUN_ID + '.tgz')).is_file()


@pytest.fixture(scope='module')
def failed_items():
    # Reconstruct the synthetic request contract; actual cloud outputs were not saved.
    return {item.request_id: item for item in generate(WorkloadSpec(
        kind='mixed-burst', split='calibration', seed=9053, rate_rps=128, injection_s=60))}


@pytest.mark.parametrize('index', [3032, 3037, 8675])
@pytest.mark.parametrize('capture_enabled', [False, True])
@pytest.mark.parametrize('variant,valid', [
    ('exact', True), ('whitespace', True), ('wrong_marker', False),
    ('wrong_arithmetic', False), ('explanation', False), ('truncated', False),
    ('length_finish', False), ('missing_done', False),
])
async def test_failed_request_contract_with_fragmented_responses(runner, tmp_path, failed_items, index, variant, valid, capture_enabled):
    request_id = f'calibration-9053-{index}'
    item = failed_items[request_id]
    expected = f'TB:{request_id}:4'
    assert item.expected == expected
    assert item.prompt.count(f'Return exactly {expected} and nothing else.') == 1
    assert item.prompt.endswith('The arithmetic answer to 2+2 is 4.')
    text = {'whitespace': '\n ' + expected + ' \n', 'wrong_marker': expected.replace('9053', '9052'),
            'wrong_arithmetic': expected[:-1] + '5', 'explanation': expected + ' because 2+2=4',
            'truncated': expected[:-1]}.get(variant, expected)

    class Fragmented(httpx.AsyncByteStream):
        async def __aiter__(self):
            events = []
            for start in range(0, len(text), 3):
                events.append('data: ' + json.dumps({'id': 'fixture', 'choices': [
                    {'index': 0, 'delta': {'content': text[start:start+3]}, 'finish_reason': None}]}) + '\n\n')
            events.append('data: ' + json.dumps({'id': 'fixture', 'choices': [{'index': 0,
                'delta': {}, 'finish_reason': 'length' if variant == 'length_finish' else 'stop'}]}) + '\n\n')
            if variant != 'missing_done':
                events.append('data: [DONE]\n\n')
            raw = ''.join(events).encode()
            for start in range(0, len(raw), 7):
                yield raw[start:start+7]

    def respond(request):
        body = json.loads(request.content)
        assert body['messages'][0]['content'] == item.prompt
        assert body['max_tokens'] == 32
        return httpx.Response(200, headers={'content-type': 'text/event-stream'}, stream=Fragmented())

    client = EndpointClient(EndpointSpec(id='fixture', base_url='http://127.0.0.1/v1', model='fixture',
        streaming=True, supported_parameters=['max_tokens', 'stream']),
        NetworkPolicy(['http://127.0.0.1'], ['127.0.0.1']))
    capture = runner.SyntheticFailureCapture(WorkloadSpec(kind='mixed-burst', split='calibration',
        seed=9053, rate_rps=128, injection_s=60))
    runner.O = tmp_path
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as session:
        if capture_enabled:
            with runner.capture_outputs(client, capture, 'fixture'):
                row = await client.measure(item, 32, time.monotonic(), session)
        else:
            row = await client.measure(item, 32, time.monotonic(), session)
    assert row.quality_valid is valid
    assert row.output_chars == len(text)
    assert row.termination == ('partial_stream' if variant == 'missing_done' else 'complete')
    expected_capture = capture_enabled and not valid and variant != 'missing_done'
    assert bool(capture.records) is expected_capture
    if expected_capture:
        assert capture.records[0]['actual_content'] == text
        assert capture.records[0]['expected'] == expected
    if not capture_enabled:
        assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('actual,finish,captured', [('wrong', 'stop', True), ('4', 'stop', False), ('4', 'length', True)])
async def test_quality_observer_receives_exact_checked_content_only_on_failure(actual, finish, captured):
    from transitionbench.workloads import OfferedRequest
    item = OfferedRequest('synthetic', 0, 'short', 'synthetic', 'Return 4', '4', 100)
    client = EndpointClient(EndpointSpec(id='fixture', base_url='http://127.0.0.1/v1', model='fixture',
        streaming=False, supported_parameters=['max_tokens']),
        NetworkPolicy(['http://127.0.0.1'], ['127.0.0.1']))
    events = []
    def observe(request, row, content):
        events.append((request.request_id, row.quality_valid, content, row.completed_s))
    response = lambda request: httpx.Response(200, json={'choices': [
        {'message': {'content': actual}, 'finish_reason': finish}]})
    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as session:
        row = await client.measure(item, 32, time.monotonic(), session, on_quality_failure=observe)
    assert bool(events) is captured
    if captured:
        assert events == [('synthetic', False, actual, row.completed_s)]
    assert row.quality_valid is (not captured)


def test_capture_is_bounded_and_rejects_altered_or_unknown_prompts(runner):
    workload = WorkloadSpec(split='calibration', kind='short', seed=9053, rate_rps=40, injection_s=1)
    capture = runner.SyntheticFailureCapture(workload)
    items = generate(workload)
    row = SimpleNamespace(termination='complete', quality_valid=False, quality_check='exact-synthetic-request-marker',
        finish_reason='stop', status_code=200, scheduled_s=0, completed_s=1)
    for item in items:
        row.request_id = item.request_id
        capture.record(item, row, 'x' * 10000)
    assert len(capture.records) == 32 and capture.omitted == 8
    assert all(len(r['actual_content']) == 256 and r['actual_chars'] == 10000 and r['truncated'] for r in capture.records)
    row.request_id = items[0].request_id
    capture.record(replace(items[0], prompt='PRIVATE_INPUT_SENTINEL'), row, 'PRIVATE_OUTPUT_SENTINEL')
    capture.record(replace(items[0], request_id='private'), row, 'PRIVATE_OUTPUT_SENTINEL')
    capture.record(replace(items[0], expected='PRIVATE_EXPECTED_SENTINEL'), row, 'PRIVATE_OUTPUT_SENTINEL')
    evidence = json.dumps(capture.evidence())
    assert capture.rejected == 3 and 'PRIVATE_' not in evidence
    assert items[0].prompt not in evidence
    assert len(evidence.encode()) < 40000


def test_capture_restores_client_and_exports_even_on_exception(runner, tmp_path):
    runner.O = tmp_path
    client = SimpleNamespace(measure=object())
    original = client.measure
    capture = runner.SyntheticFailureCapture(WorkloadSpec(split='calibration', injection_s=.1))
    with pytest.raises(RuntimeError):
        with runner.capture_outputs(client, capture, 'interrupted'):
            raise RuntimeError('test interruption')
    assert client.measure is original
    assert json.loads((tmp_path / 'interrupted-quality-failures.json').read_text())['captured'] == 0


def test_real_http_failure_capture_reaches_final_archive(runner, tmp_path, monkeypatch, asgi_server):
    """Actual local HTTP/SSE -> quality hook -> sidecar -> failure gate -> archive."""
    import asyncio
    from transitionbench.endpoint import run_endpoint
    workload = WorkloadSpec(kind='short', split='calibration', seed=9053, injection_s=.3, rate_rps=10)
    items = generate(workload)
    expected = {item.prompt: item.expected for item in items}
    actual_wrong = items[1].expected.replace(':4', ':5')

    async def app(scope, receive, send):
        raw = b''
        while True:
            event = await receive()
            raw += event.get('body', b'')
            if not event.get('more_body', False):
                break
        prompt = json.loads(raw)['messages'][0]['content']
        text = actual_wrong if prompt == items[1].prompt else expected[prompt]
        await send({'type': 'http.response.start', 'status': 200,
                    'headers': [(b'content-type', b'text/event-stream')]})
        for token in text:
            data = json.dumps({'choices': [{'delta': {'content': token}, 'finish_reason': None}]})
            await send({'type': 'http.response.body', 'body': ('data: '+data+'\n\n').encode(), 'more_body': True})
        await send({'type': 'http.response.body', 'body':
                    b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'})

    base = asgi_server(app)
    runner.R, runner.O = tmp_path, tmp_path / 'evidence'
    spec = ExperimentSpec(mode='LIVE_ENDPOINT', endpoint_id='fixture', workload=workload,
        observation_s=2, drain_s=1.7, max_dispatch_lag_s=.3,
        budget=ResourceBudget(max_requests=4, max_total_tokens=10000, reserved_gpus=0))
    client = EndpointClient(EndpointSpec(id='fixture', base_url=base+'/v1', model='fixture', streaming=True,
        supported_parameters=['max_tokens', 'stream']), NetworkPolicy([base], ['127.0.0.1']))
    original = client.measure

    async def main():
        capture = runner.SyntheticFailureCapture(workload)
        collector = SimpleNamespace(measurements=[])
        async def trial(number, value):
            with runner.capture_outputs(client, capture, 'local-http'):
                rows, validity = await run_endpoint(spec, client, asyncio.Event(), skip_discovery=True)
            assert validity['valid'] and len(rows) == 3
            assert [r.quality_valid for r in rows] == [True, False, True]
            bundle = runner.O / 'bundles/001'
            bundle.mkdir(parents=True)
            (bundle / 'requests.jsonl').write_text(''.join(r.model_dump_json()+'\n' for r in rows))
            collector.measurements.append({'bundle': 'bundles/001', 'trial': value})
        collector.trial = trial
        await runner.run_trial(collector, 1, {'role': 'local-http-fixture'})
        runner.status(state='PASSED')

    monkeypatch.setattr(runner, 'main', main)
    assert runner.execute_cloud(time.time()+1200) == 1
    assert client.measure == original
    with tarfile.open(tmp_path / (runner.RUN_ID+'.tgz')) as archive:
        sidecar = archive.extractfile('evidence/local-http-quality-failures.json').read()
        saved = json.loads(archive.extractfile('evidence/status.json').read())
        assert saved['state'] == 'FAILED_QUALITY_GATE'
        assert saved['quality_capture_files'][0]['sha256'] == hashlib.sha256(sidecar).hexdigest()
        records = json.loads(sidecar)['records']
        assert len(records) == 1 and records[0]['request_id'] == items[1].request_id
        assert records[0]['actual_content'] == actual_wrong and records[0]['expected'] == items[1].expected
        assert not records[0]['truncated']
        assert all(item.prompt.encode() not in sidecar for item in items)
