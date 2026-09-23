from transitionbench.simulation import calibrate,simulate
from transitionbench.schemas import ExperimentSpec,WorkloadSpec


def test_best_fixed_is_calibrated_per_family_and_shared_as_initial_condition():
    calibration=calibrate()
    assert calibration['static_best_by_workload']['long-prefix']=='B'
    assert calibration['static_best_by_workload']['short']=='A'
    for policy in ('StaticBest','SteadyStateFirst','FixedHysteresis','StateAware'):
        rows,_,_=simulate(ExperimentSpec(policy=policy,workload=WorkloadSpec(kind='long-prefix')),calibration)
        assert rows[0].config_id=='B'
