from transitionbench.schemas import DecisionInput
from transitionbench.policy import evaluate_decision


def test_candidate_reference_deficit_is_subtracted_exactly_once():
    decision = evaluate_decision(DecisionInput(current_goodput_rps=10, candidate_goodput_rps=12,
                               transition_deficit_requests=30, horizon_s=20,
                               uncertainty_requests=2, evidence_ids=["calibration-1"]))
    assert decision.gain_requests == 10
    assert decision.break_even_s == 15
    assert decision.action == "SWITCH"


def test_unknown_and_negative_advantage():
    unknown = evaluate_decision(DecisionInput(current_goodput_rps=10, candidate_goodput_rps=12, horizon_s=20))
    assert unknown.action == "INSUFFICIENT_EVIDENCE"
    worse = evaluate_decision(DecisionInput(current_goodput_rps=12, candidate_goodput_rps=10, horizon_s=20))
    assert worse.action == "KEEP"


def test_spare_capacity_is_not_predicted_as_additional_completed_demand():
    from transitionbench.policy import OnlinePolicy
    from transitionbench.simulation import calibrate
    policy=OnlinePolicy('StateAware',calibrate(),40)
    for n in range(40):
        policy.observe_arrival('long',str(n%4),n/4)
    target,decision=policy.choose(10,'A',0)
    assert target=='A'
    assert decision.action=='KEEP'
