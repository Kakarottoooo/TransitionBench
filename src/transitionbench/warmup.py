"""Bounded short/long probe stability, not proof of arbitrary workload warmup."""
import asyncio
import statistics
import time
from .schemas import WarmupSpec

INPUT_TOKEN_RESERVATION = 1024  # UTF-8 prompt bytes plus conservative chat overhead.
WARMUP_FAILURE_REASONS = frozenset({'request_limit', 'invalid_output', 'deadline', 'cancelled',
    'ConnectError', 'ConnectTimeout', 'ReadError', 'ReadTimeout', 'WriteError', 'WriteTimeout',
    'PoolTimeout', 'RemoteProtocolError', 'HTTPStatusError', 'KeyError', 'IndexError',
    'TypeError', 'ValueError', 'JSONDecodeError', 'unclassified'})


def warmup_reservation(policy: WarmupSpec, output_tokens: int):
    # Two forward workers plus two recovery workers; unused allowance is not use.
    requests = 4 * policy.max_requests
    return requests, requests * (INPUT_TOKEN_RESERVATION + output_tokens)


class WarmupFailure(ValueError):
    def __init__(self, report):
        self.report = report
        super().__init__("Warmup refused: " + report['reason'])


async def warm_worker(client, url, model, max_tokens, policy: WarmupSpec, record=None):
    report = {'stable': False, 'reason': 'request_limit', 'requests': [], 'windows': [],
              'criterion': policy.model_dump(mode='json'),
              'limitation': 'Probe stability only; GPU workload adequacy requires measurement'}
    started, stable_comparisons = time.monotonic(), 0
    try:
        async with asyncio.timeout(policy.max_duration_s):
            while len(report['requests']) + 2 * policy.samples_per_class <= policy.max_requests:
                window = {'short': [], 'long': []}
                for kind in ('short', 'long') * policy.samples_per_class:
                    index = len(report['requests'])
                    expected = f'TB-WARM-{kind}-{index}:4'
                    prompt = (('context ' * 48 if kind == 'long' else '') + 'Return exactly ' + expected
                              + ' and nothing else. The arithmetic answer to 2+2 is 4.')
                    before = time.monotonic()
                    row = {'index': index, 'workload_class': kind, 'quality_valid': False,
                           'latency_s': None, 'termination': 'unfinished'}
                    report['requests'].append(row)  # Reserve before dispatch, including failure.
                    try:
                        response = await client.post(url, json={'model': model,
                            'messages': [{'role': 'user', 'content': prompt}],
                            'max_tokens': max_tokens, 'temperature': 0, 'seed': 0})
                        response.raise_for_status()
                        choice = response.json()['choices'][0]
                        row['quality_valid'] = (choice.get('finish_reason') == 'stop' and
                            choice['message'].get('content', '').strip() == expected)
                        row['termination'] = 'complete'
                    finally:
                        row['latency_s'] = time.monotonic() - before
                    if not row['quality_valid']:
                        report['reason'] = 'invalid_output'
                        raise WarmupFailure(report)
                    window[kind].append(row['latency_s'])
                medians = {kind: statistics.median(values) for kind, values in window.items()}
                if report['windows']:
                    previous = report['windows'][-1]
                    stable = all(abs(medians[k] - previous[k]) <= max(
                        policy.absolute_tolerance_s, policy.relative_tolerance * previous[k]) for k in medians)
                    stable_comparisons = stable_comparisons + 1 if stable else 0
                report['windows'].append(medians)
                if stable_comparisons >= 2 and (not policy.complete_probe_sequence or len(report['requests']) == policy.max_requests):
                    report.update(stable=True, reason='two_consecutive_stable_comparisons')
                    return report
    except WarmupFailure:
        raise
    except asyncio.CancelledError:
        report['reason'] = 'cancelled'
        raise
    except Exception as exc:
        report['reason'] = 'deadline' if isinstance(exc, TimeoutError) else type(exc).__name__
        raise WarmupFailure(report) from None
    finally:
        report['elapsed_s'] = time.monotonic() - started
        if record:
            record(report)
    raise WarmupFailure(report)
