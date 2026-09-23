"""Deterministic, offline trial ordering and cost envelope. Never rents a host."""
import random
from pydantic import Field
from .schemas import Record, ExperimentSpec, WarmupSpec
from .rollout import stable_hash
from .warmup import warmup_reservation
from .workloads import generate


class ScreenBudget(Record):
    machine_hourly_usd: float = Field(gt=0, le=1000)
    spending_limit_usd: float = Field(gt=0, le=100)
    non_compute_allowance_usd: float = Field(default=.5, ge=.5, le=100)
    setup_allowance_s: float = Field(default=600, ge=600, le=3600)
    export_allowance_s: float = Field(default=300, ge=300, le=3600)
    stop_reserve_s: float = Field(default=210, ge=210, le=600)


def prepare_capacity_screen(value, plan, budget: ScreenBudget):
    if value.purpose != 'capacity-screen' or value.experiment.workload.injection_s < 20:
        raise ValueError('Screen budget requires a capacity-screen protocol with at least 20s injection')
    expected = 4*len(value.capacity_rates_rps)
    if len(plan['order']) != expected or any(t['role'] != 'capacity' for t in plan['order']):
        raise ValueError('Screen must contain only the exact matched load/configuration/class grid')
    collection_s = plan['maximum_reservations']['wall_s']
    stop_after_s = budget.setup_allowance_s + collection_s + budget.export_allowance_s
    billed_s = stop_after_s + budget.stop_reserve_s
    projected = billed_s/3600*budget.machine_hourly_usd + budget.non_compute_allowance_usd
    return {'purpose': 'capacity-screen', 'status': 'READY_FOR_HOST_PREFLIGHT'
        if plan['status'] == 'READY' and projected <= budget.spending_limit_usd else 'BUDGET_REFUSED',
        'trial_count': expected, 'test_order': [], 'research_ready': False,
        'collection_plan_hash': plan['plan_hash'], 'budget': budget.model_dump(mode='json'),
        'collection_maximum_s': collection_s, 'stop_after_start_s': stop_after_s,
        'maximum_billed_s_including_stop': billed_s, 'projected_usd': round(projected, 6),
        'spending_limit_usd': budget.spending_limit_usd, 'cloud_authorization': False,
        'automatic_continuation': False,
        'limitations': ['Budget estimate and local watchdog are not a provider-enforced dollar cap',
            'One scout seed is not confirmation; all results, including failures, remain retained']}


class StudyPreparation(Record):
    experiment: ExperimentSpec = Field(default_factory=ExperimentSpec)
    warmup: WarmupSpec = Field(default_factory=WarmupSpec)
    calibration_seeds: list[int] = Field(default_factory=lambda:[11,12,13], min_length=3, max_length=30)
    tuning_seeds: list[int] = Field(default_factory=lambda:[21,22], min_length=2, max_length=30)
    test_seeds: list[int] = Field(default_factory=lambda:[101,102,103,104,105], min_length=3, max_length=30)
    order_seed: int = Field(default=713, ge=0)
    machine_hourly_usd: float = Field(gt=0, le=1000)
    spending_limit_usd: float = Field(gt=0, le=10000)
    non_compute_allowance_usd: float = Field(default=0, ge=0, le=10000)
    setup_allowance_s: float = Field(default=1800, ge=0, le=86400)
    calibration_allowance_s: float = Field(default=3600, gt=0, le=86400)
    tuning_allowance_s: float = Field(default=1800, gt=0, le=86400)
    reset_allowance_per_trial_s: float = Field(default=60, ge=0, le=3600)
    export_teardown_allowance_s: float = Field(default=300, gt=0, le=3600)


def prepare_study(value: StudyPreparation):
    groups = [value.calibration_seeds,value.tuning_seeds,value.test_seeds]
    if any(len(set(group)) != len(group) or any(seed<0 for seed in group) for group in groups):
        raise ValueError('Seeds must be unique nonnegative integers within each split')
    if any(set(a)&set(b) for i,a in enumerate(groups) for b in groups[i+1:]):
        raise ValueError('Calibration, tuning and test seeds must be disjoint')
    spec = value.experiment
    if spec.budget.reserved_gpus != 2 or spec.budget.max_reserved_gpu_seconds < 2*spec.budget.max_duration_s:
        raise ValueError('Reference preparation requires a full two-GPU trial envelope')
    warm_requests,warm_tokens = warmup_reservation(value.warmup,spec.budget.max_output_tokens)
    if spec.budget.max_output_tokens<32:
        raise ValueError('Warmup markers require a 32-token output allowance')
    rng = random.Random(value.order_seed)
    order = []
    for seed in value.test_seeds:
        policies = ['StaticBest','SteadyStateFirst','FixedHysteresis','StateAware']
        rng.shuffle(policies)
        workload = spec.workload.model_copy(update={'seed':seed,'split':'test'})
        offered = generate(workload)
        tokens = sum(r.input_token_upper_bound+spec.budget.max_output_tokens for r in offered)
        if len(offered)+warm_requests>spec.budget.max_requests or tokens+warm_tokens>spec.budget.max_total_tokens:
            raise ValueError('Trial budget cannot cover traffic plus forward/recovery warmup')
        order.extend({'seed':seed,'policy':policy,'offered_requests':len(offered),
            'traffic_token_reservation':tokens,'initial_configuration':'calibration-selected fixed best',
            'execution':'NOT_RUN'} for policy in policies)
    # Initial warmup occurs before each observation; rollout/recovery warmup is
    # inside each approved trial's duration cap, not a second hidden allocation.
    per_trial = spec.budget.max_duration_s + value.reset_allowance_per_trial_s + 2*value.warmup.max_duration_s
    phases = {'setup':value.setup_allowance_s,'calibration':value.calibration_allowance_s,
        'tuning':value.tuning_allowance_s,'matched_tests':len(order)*per_trial,
        'export_and_teardown':value.export_teardown_allowance_s}
    total = sum(phases.values())
    cost = total/3600*value.machine_hourly_usd+value.non_compute_allowance_usd
    return {'schema_version':'1.0','status':'READY_FOR_HOST_PREFLIGHT' if cost<=value.spending_limit_usd else 'BUDGET_REFUSED',
        'cloud_authorization':False,'hardware_validated':False,'protocol_hash':stable_hash(value.model_dump(mode='json')),
        'input':value.model_dump(mode='json'),'test_order':order,'phase_allowances_s':phases,
        'planned_reserved_gpu_seconds':2*total,'projected_usd':round(cost,6),
        'spending_limit_usd':value.spending_limit_usd,
        'latest_termination_after_s':max(0,(value.spending_limit_usd-value.non_compute_allowance_usd)/value.machine_hourly_usd*3600),
        'per_trial_warmup_reservation':{'max_requests':warm_requests,'max_total_tokens':warm_tokens},
        'limitations':['Planning assumptions, not a billing cap or measured runtime',
            'Setup, download, compilation, idle, reset, recovery and teardown consume rental time',
            'Stop before the next phase if remaining allowance cannot cover it and teardown',
            'A stopped process does not terminate the cloud instance; verify provider state',
            'Calibration/tuning allowances are stage ceilings; incomplete stages block tests',
            'No favorable-result stopping; retain failed/invalid attempts within this envelope']}
