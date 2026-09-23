"""Synthetic contracts, never GPU performance evidence."""
import copy
import pytest
from transitionbench.research import capacity_diagnostic, research_gate, ResearchGateError, require_research_ready


def points():
    rows = []
    for kind in ('short', 'long'):
        for config in ('A', 'B'):
            ceiling = 160 if (config == 'A') == (kind == 'short') else 80
            for rate in (40, 100, 200):
                for seed in (11, 12, 13):
                    rows.append(dict(config=config, kind=kind, seed=seed, rate_rps=rate,
                        injection_s=60, observation_s=150, offered=rate*60,
                        qualified=min(rate, ceiling)*60, complete=rate*60,
                        quality_valid=rate*60, run_id=f'{config}-{kind}-{rate}-{seed}'))
    return rows


def qualified():
    costs = {}
    for pair in ('B>A:short', 'A>B:long'):
        costs[pair] = {'reference': 'candidate-steady', 'by_queue': {
            'backlogged': {'mean': 400, 'spread': 100, 'samples': [350, 400, 450]}},
            'timing': {'backlogged': [dict(start_s=2, complete_s=12, injection_s=60,
                rate_rps=100, post_transition_qualified_rps=100) for _ in range(3)]}}
    return {'capacity_diagnostic': capacity_diagnostic(points(), 100), 'costs': costs}


def test_arrival_limited_results_cannot_be_called_capacity_or_admit_study():
    rows = [p for p in points() if p['rate_rps'] == 40]
    result = capacity_diagnostic(rows, 40)
    assert not result['ready']
    assert result['curves']['A:short']['status'] == 'LOWER_BOUND_ONLY'
    assert result['curves']['A:short']['points'][0]['qualified_cohort_rps'] == [40]*3
    assert result['curves']['A:short']['points'][0]['observation_goodput_rps'] == [16]*3
    with pytest.raises(ResearchGateError):
        require_research_ready({'capacity_diagnostic': result, 'costs': {}})


def test_matched_crossover_and_candidate_relative_break_even():
    result = research_gate(qualified())
    assert result['ready']
    assert {x['target'] for x in result['directions']} == {'A', 'B'}
    row = result['directions'][0]
    assert row['gain_lower_rps'] == 20
    assert row['break_even_s'] == 20
    assert row['conservative_break_even_s'] == 25
    # Candidate-relative deficit already includes switching losses: do not add
    # the 10-second deployment duration to loss/delta a second time.
    assert row['minimum_phase_s'] == 25


@pytest.mark.parametrize('change', ['same_winner', 'unmatched', 'client_limited', 'quality_failure', 'nonmonotone'])
def test_capacity_gate_refuses_uninformative_or_confounding_curves(change):
    rows = points()
    if change == 'same_winner':
        for p in rows:
            p['qualified'] = min(p['rate_rps'], 160 if p['config']=='A' else 80)*60
    elif change == 'unmatched': rows.pop()
    elif change == 'client_limited': rows[-1]['complete'] -= 1
    elif change == 'quality_failure': rows[-1]['quality_valid'] -= 1
    else:
        for p in rows:
            if p['rate_rps']==200: p['qualified']=p['offered']
    assert not capacity_diagnostic(rows, 100)['ready']


@pytest.mark.parametrize('change', ['missing', 'late', 'wrong_load', 'no_recovery', 'no_gain'])
def test_repayment_gate_fails_closed(change):
    value = qualified()
    cost = value['costs']['B>A:short']
    if change == 'missing': cost['timing'] = {}
    elif change == 'no_gain': value['capacity_diagnostic']['ready'] = False
    else:
        for timing in cost['timing']['backlogged']:
            if change == 'late': timing['complete_s'] = 65
            elif change == 'wrong_load': timing['rate_rps'] = 40
            else: timing['post_transition_qualified_rps'] = 1
    assert not research_gate(value)['ready']


def test_padding_observation_does_not_change_capacity_estimates():
    before = capacity_diagnostic(points(), 100)
    padded = copy.deepcopy(points())
    for p in padded: p['observation_s'] *= 2
    after = capacity_diagnostic(padded, 100)
    assert before['rates'] == after['rates']
    assert before['advantages'] == after['advantages']


@pytest.mark.parametrize('kind,rate', [('mixed', 100), ('long', 40)])
def test_online_policy_does_not_extrapolate_single_load_pure_class_rates(kind, rate):
    from transitionbench.policy import OnlinePolicy
    value = qualified()
    value.update(id='fixture', rates=value['capacity_diagnostic']['rates'],
                 hysteresis={'advantage_fraction': .1, 'persistence_s': 0, 'dwell_s': 0})
    for name in ('StateAware', 'SteadyStateFirst', 'FixedHysteresis'):
        policy = OnlinePolicy(name, value, 40)
        for i in range(40):
            policy.observe_arrival('short' if kind == 'mixed' and i % 2 else 'long', None, i/rate)
        chosen, decision = policy.choose(40/rate, 'A', 10)
        assert chosen == 'A'
        if name == 'StateAware':
            assert decision.action == 'INSUFFICIENT_EVIDENCE'
