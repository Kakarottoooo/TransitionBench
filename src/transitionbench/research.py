"""Scientific admission checks; valid measurements need not identify a benefit.

These fixed rules must be frozen before acquiring new evidence. Observed ranges
are screening bounds, not confidence intervals or universal capacity limits.
"""
import statistics


class ResearchGateError(ValueError):
    pass


def capacity_diagnostic(rows, target_rate_rps):
    curves, rates, advantages, reasons = {}, {'A': {}, 'B': {}}, {}, []
    seed_contract = None
    load_contract = None
    if len({(p['injection_s'], p['observation_s']) for p in rows}) != 1:
        reasons.append('Capacity load points must share the same injection and observation durations')
    for kind in ('short', 'long'):
        target = {}
        for config in ('A', 'B'):
            group = [p for p in rows if p['config'] == config and p['kind'] == kind]
            loads = sorted({p['rate_rps'] for p in group})
            if load_contract is None:
                load_contract = loads
            elif load_contract != loads:
                reasons.append('Unmatched load ladder')
            points = []
            for load in loads:
                samples = sorted((p for p in group if p['rate_rps'] == load), key=lambda p: p['seed'])
                seeds = [p['seed'] for p in samples]
                if seed_contract is None:
                    seed_contract = seeds
                if len(seeds) < 3 or len(seeds) != len(set(seeds)) or seeds != seed_contract:
                    reasons.append('Each load/configuration/class needs the same three or more distinct seeds')
                if any(p['complete'] != p['offered'] or p['quality_valid'] != p['offered'] for p in samples):
                    reasons.append('Transport/client/quality failures cannot identify a GPU capacity limit')
                if any(p['injection_s'] < 20 or p['offered'] <= 0 for p in samples):
                    reasons.append('Capacity screening needs at least 20 seconds of offered traffic')
                if any(abs(p['offered']-load*p['injection_s']) > 1 for p in samples):
                    reasons.append('Offered counts do not match the declared constant load')
                fractions = [p['qualified']/max(1, p['offered']) for p in samples]
                point = {'offered_rps': load, 'seeds': seeds,
                    'qualified_cohort_rps': [p['qualified']/p['injection_s'] for p in samples],
                    'observation_goodput_rps': [p['qualified']/p['observation_s'] for p in samples],
                    'attainment': fractions,
                    'status': 'PASS' if min(fractions) >= .99 else 'FAIL' if max(fractions) < .99 else 'UNCERTAIN',
                    'run_ids': [p['run_id'] for p in samples]}
                points.append(point)
                if load == target_rate_rps:
                    target[config] = point
                    rates[config][kind] = statistics.mean(point['qualified_cohort_rps'])
            passes = [p['offered_rps'] for p in points if p['status'] == 'PASS']
            fails = [p['offered_rps'] for p in points if p['status'] == 'FAIL']
            bracketed = bool(passes and fails and max(passes) < min(fails))
            status = 'BRACKETED' if bracketed else 'LOWER_BOUND_ONLY' if passes and not fails else 'UNRESOLVED'
            curves[f'{config}:{kind}'] = {'status': status, 'points': points,
                'slo_load_lower_rps': max(passes) if passes else None,
                'slo_load_upper_rps': min(fails) if bracketed else None}
            if len(loads) < 3 or not bracketed or any(p['status'] == 'UNCERTAIN' for p in points):
                reasons.append(f'{config}:{kind} lacks a repeatable SLO load bracket')
        if set(target) == {'A', 'B'} and target['A']['seeds'] == target['B']['seeds']:
            diffs = [a-b for a,b in zip(target['A']['qualified_cohort_rps'], target['B']['qualified_cohort_rps'])]
            winner = 'A' if min(diffs) > 0 else 'B' if max(diffs) < 0 else None
            if winner:
                source = 'B' if winner == 'A' else 'A'
                gains = diffs if winner == 'A' else [-d for d in diffs]
                floor = min(gains)
                practical = .05 * statistics.mean(target[source]['qualified_cohort_rps'])
                advantages[kind] = {'source': source, 'target': winner, 'gain_lower_rps': floor,
                    'mean_gain_rps': statistics.mean(gains), 'paired_gain_rps': gains}
                if floor < max(1, practical):
                    reasons.append(f'{kind} advantage below the preregistered 5% / 1 req/s screen')
            else:
                reasons.append(f'{kind} has no consistent matched configuration advantage')
        else:
            reasons.append(f'{kind} lacks matched samples at the declared scenario load')
    if len(advantages) != 2 or len({v['target'] for v in advantages.values()}) != 2:
        reasons.append('No complementary short/long winners: switching is not justified')
    return {'version': 'load-response-v1', 'ready': not reasons, 'reasons': sorted(set(reasons)),
        'target_rate_rps': target_rate_rps, 'curves': curves, 'rates': rates, 'advantages': advantages,
        'rules': {'minimum_loads': 3, 'minimum_seeds': 3, 'minimum_injection_s': 20,
                  'slo_attainment': .99, 'minimum_advantage_fraction': .05, 'minimum_gain_rps': 1},
        'rate_definition': 'qualified offered cohort / injection seconds at the declared target load',
        'limitations': ['A finite SLO load bracket is not a universal hardware capacity bound',
            'Observed paired minima are screening bounds, not confidence intervals',
            'No extrapolation to other loads, mixed classes, models, or longer phases is verified']}


def research_gate(calibration):
    diagnostic = calibration.get('capacity_diagnostic', {})
    reasons = list(diagnostic.get('reasons', []))
    if not diagnostic.get('ready'):
        reasons.append('Capacity discrimination gate did not pass')
    directions = []
    for kind, advantage in diagnostic.get('advantages', {}).items():
        pair = f"{advantage['source']}>{advantage['target']}:{kind}"
        cost = calibration.get('costs', {}).get(pair, {})
        if cost.get('reference') != 'candidate-steady' or not cost.get('by_queue'):
            reasons.append(pair + ' lacks candidate-relative transition evidence')
            continue
        for bucket, estimate in cost['by_queue'].items():
            timing = cost.get('timing', {}).get(bucket, [])
            gain = advantage['gain_lower_rps']
            loss = estimate['mean']
            conservative_loss = loss + estimate['spread']
            eligible = len(timing) >= 3 and gain > 0
            duration = max((t['complete_s']-t['start_s'] for t in timing), default=0)
            # Loss is relative to candidate from the start, so deployment time
            # must NOT be added to loss/delta. It is a separate feasibility bound.
            breakeven = loss/gain if gain > 0 else None
            conservative = conservative_loss/gain if gain > 0 else None
            minimum = max(conservative, duration+10) if conservative is not None else None
            candidate_rate = diagnostic.get('rates', {}).get(advantage['target'], {}).get(kind, 0)
            for t in timing:
                eligible &= (t['rate_rps'] == diagnostic.get('target_rate_rps') and
                    t['injection_s']-t['complete_s'] >= 10 and
                    t['post_transition_qualified_rps'] >= .95*candidate_rate and
                    minimum is not None and minimum <= t['injection_s']-t['start_s'])
            if not eligible:
                reasons.append(pair + ':' + bucket + ' lacks matched-load recovery and observed repayment horizon')
            directions.append({**advantage, 'kind': kind, 'queue_bucket': bucket,
                'mean_deficit_requests': loss, 'conservative_deficit_requests': conservative_loss,
                'break_even_s': breakeven, 'conservative_break_even_s': conservative,
                'transition_duration_max_s': duration, 'minimum_phase_s': minimum,
                'eligible': bool(eligible)})
    if len({d['kind'] for d in directions if d['eligible']}) != 2:
        reasons.append('Both crossover directions need observed cost/recovery support')
    return {'version': 'research-admission-v1', 'ready': not reasons,
        'reasons': sorted(set(reasons)), 'directions': directions,
        'cloud_authorization': False,
        'limitations': ['Repayment is an estimate, not demonstrated held-out benefit',
            'Cost uncertainty is an observed range, not a confidence interval',
            'Only listed queue buckets and the measured offered load are supported',
            'Transient return switches need separate evidence; one-switch runner cannot certify round trips']}


def require_research_ready(calibration):
    result = research_gate(calibration)
    if not result['ready']:
        raise ResearchGateError('Research admission refused: ' + '; '.join(result['reasons']))
    return result
