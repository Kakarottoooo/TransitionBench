"""Canonical public records. Durations and timestamps use seconds, never milliseconds."""
from enum import StrEnum
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator


class Record(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    schema_version: Literal["1.0"] = "1.0"


class Mode(StrEnum):
    SIMULATION = "SIMULATION"
    RECORDED_REPLAY = "RECORDED_REPLAY"
    LIVE_ENDPOINT = "LIVE_ENDPOINT"
    CONTROLLED_ROLLOUT = "CONTROLLED_ROLLOUT"


Origin = Literal["synthetic", "measured-black-box", "measured-controlled"]
PolicyName = Literal["StaticBest", "SteadyStateFirst", "FixedHysteresis", "StateAware"]


class Capability(Record):
    state: Literal["supported", "unsupported", "unknown"]
    evidence: Literal["documented", "contract-tested", "live-verified", "none"] = "none"
    reason: str


class Capabilities(Record):
    integration_level: Literal["A", "B", "C"] = "B"
    features: dict[str, Capability]


class EndpointSpec(Record):
    id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    base_url: str = Field(max_length=512)
    provider: Literal["openai-compatible", "wafer", "local-test"] = "openai-compatible"
    model: str = Field(min_length=1, max_length=256)
    key_env: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]{0,63}$")
    require_zdr: bool = False
    streaming: bool = False
    timeout_s: float = Field(default=15, gt=0, le=120)
    temperature: float | None = Field(default=None, ge=0, le=2)
    seed: int | None = None
    # An operator verifies optional fields before registering this contract.
    supported_parameters: list[Literal["temperature", "seed", "max_tokens", "stream"]] = Field(default_factory=lambda: ["max_tokens"])


class SLOSpec(Record):
    e2e_s: float = Field(default=2, gt=0)
    first_content_s: float = Field(default=1, gt=0)


class ResourceBudget(Record):
    max_requests: int = Field(default=500, ge=1, le=20000)
    max_total_tokens: int = Field(default=2_000_000, ge=1, le=20_000_000)
    max_output_tokens: int = Field(default=32, ge=1, le=4096)
    max_concurrency: int = Field(default=16, ge=1, le=256)
    max_duration_s: float = Field(default=120, gt=0, le=3600)
    reserved_gpus: int = Field(default=2, ge=0, le=2)
    max_reserved_gpu_seconds: float = Field(default=240, ge=0, le=7200)


class WarmupSpec(Record):
    complete_probe_sequence: bool = False
    samples_per_class: Literal[2, 4] = 2
    max_requests: int = Field(default=24, ge=12, le=96)
    max_duration_s: float = Field(default=45, gt=0, le=300)
    relative_tolerance: float = Field(default=.10, ge=0, le=.5)
    absolute_tolerance_s: float = Field(default=.005, ge=0, le=.1)

    @model_validator(mode='after')
    def fixed_windows(self):
        window_size = 2 * self.samples_per_class
        if self.max_requests < 3 * window_size:
            raise ValueError('Warmup requires at least three complete windows')
        if self.complete_probe_sequence and self.max_requests % window_size:
            raise ValueError('Fixed warmup sequence requires complete windows')
        return self


class WorkloadSpec(Record):
    kind: Literal["long-prefix", "short", "prefix-shift", "mixed-burst"] = "prefix-shift"
    long_prefix_mode: Literal["legacy", "shared", "unique"] = "legacy"
    seed: int = Field(default=101, ge=0)
    split: Literal["calibration", "tuning", "test"] = "test"
    rate_rps: float = Field(default=6, gt=0, le=500)
    injection_s: float = Field(default=30, gt=0, le=1800)
    arrival_model: Literal["open-loop", "scripted-session"] = "open-loop"
    think_s: float = Field(default=.1, ge=0, le=60)


class ExperimentSpec(Record):
    mode: Mode = Mode.SIMULATION
    workload: WorkloadSpec = Field(default_factory=WorkloadSpec)
    slo: SLOSpec = Field(default_factory=SLOSpec)
    budget: ResourceBudget = Field(default_factory=ResourceBudget)
    policy: PolicyName = "StateAware"
    horizon_s: float = Field(default=40, gt=0, le=3600)
    observation_s: float = Field(default=40, gt=0, le=3600)
    drain_s: float = Field(default=10, ge=0, le=120)
    max_dispatch_lag_s: float = Field(default=.1, gt=0, le=10)
    min_practical_gain_requests: float = Field(default=2, ge=0)
    transition_s: float = Field(default=3, ge=0, le=60)
    endpoint_id: str | None = None
    replay_bundle_id: str | None = None
    plan_id: str | None = None
    calibration_id: str | None = None

    @model_validator(mode="after")
    def boundaries(self):
        if self.observation_s < self.workload.injection_s:
            raise ValueError("Observation must include the full injection interval")
        if self.observation_s > self.budget.max_duration_s:
            raise ValueError("Observation exceeds duration budget")
        if self.workload.injection_s + self.drain_s != self.observation_s:
            raise ValueError("observation_s must equal injection_s + drain_s")
        return self


class RequestEvent(Record):
    request_id: str
    workload_class: str = "short"
    prefix_group: str | None = None
    scheduled_s: float = Field(ge=0)
    dispatch_s: float | None = Field(default=None, ge=0)
    first_content_s: float | None = Field(default=None, ge=0)
    first_reasoning_s: float | None = Field(default=None, ge=0)
    final_content_s: float | None = Field(default=None, ge=0)
    completed_s: float | None = Field(default=None, ge=0)
    chunk_times_s: list[float] = Field(default_factory=list)
    termination: Literal["complete", "client_drop", "timeout", "error", "cancelled", "unfinished", "partial_stream", "budget_refusal"] = "unfinished"
    finish_reason: str | None = None
    output_chars: int = Field(default=0, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    input_tokens: int | None = Field(default=None, ge=0)
    token_origin: Literal["server", "synthetic", "unknown"] = "unknown"
    quality_valid: bool = False
    quality_check: str = "not-assessed"
    provider_request_id: str | None = None
    attempt: int = Field(default=1, ge=1)
    status_code: int | None = None
    worker_id: str | None = None
    config_id: str | None = None
    scheduling_lag_s: float | None = Field(default=None, ge=0)
    clock_domain: str = "client-monotonic-relative"
    origin: Origin = "synthetic"

    @model_validator(mode="after")
    def ordered(self):
        values = [self.scheduled_s, self.dispatch_s, self.first_content_s,
                  self.final_content_s, self.completed_s]
        present = [v for v in values if v is not None]
        if present != sorted(present):
            raise ValueError("Impossible request lifecycle order")
        if self.first_content_s is not None and self.dispatch_s is None:
            raise ValueError("Content without dispatch")
        if self.termination == "complete" and (self.completed_s is None or self.dispatch_s is None):
            raise ValueError("Complete request lacks timestamps")
        if any(t < (self.dispatch_s or 0) or (self.completed_s is not None and t > self.completed_s)
               for t in self.chunk_times_s) or self.chunk_times_s != sorted(self.chunk_times_s):
            raise ValueError("Invalid chunk timestamps")
        return self


class WorkerSnapshot(Record):
    worker_id: str
    config_id: str
    generation: int = Field(ge=0)
    ready: bool
    accepting: bool
    in_flight: int = Field(ge=0)
    device_uuid: str | None = None
    device_model: str | None = None
    process_id: int | None = None
    observed_at_unix_s: float
    resource_evidence: Literal["independently-observed", "operator-provided", "synthetic", "unknown"] = "unknown"


class TransitionEvent(Record):
    operation_id: str
    worker_id: str
    state: str
    at_s: float = Field(ge=0)
    from_config: str
    to_config: str
    generation: int | None = Field(default=None, ge=0)
    origin: Origin
    detail: str = ""


class DecisionInput(Record):
    current_goodput_rps: float = Field(ge=0)
    candidate_goodput_rps: float = Field(ge=0)
    transition_deficit_requests: float | None = Field(default=None, ge=0)
    deficit_reference: Literal["candidate-steady"] = "candidate-steady"
    horizon_s: float = Field(gt=0)
    min_gain_requests: float = Field(default=2, ge=0)
    uncertainty_requests: float | None = Field(default=None, ge=0)
    state_known: bool = True
    out_of_distribution: bool = False
    evidence_ids: list[str] = Field(default_factory=list)
    origin: Origin = "synthetic"


class DecisionRecord(Record):
    action: Literal["KEEP", "SWITCH", "WAIT", "INSUFFICIENT_EVIDENCE"]
    horizon_s: float
    gain_requests: float | None
    transition_deficit_requests: float | None
    break_even_s: float | None
    uncertainty_requests: float | None
    reasons: list[str]
    evidence_ids: list[str]
    expires_at_unix_s: float
    origin: Origin
    sensitivity: list[dict[str, float | None]] = Field(default_factory=list)


class ValidationReport(Record):
    valid: bool
    errors: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    integrity_valid: bool | None = None


class RunManifest(Record):
    run_id: str
    mode: Mode
    origin: Origin
    experiment: ExperimentSpec
    offered_ids: list[str]
    created_at_unix_s: float
    versions: dict[str, str]
    hardware: list[dict] = Field(default_factory=list)
    resource_intervals: list[dict] = Field(default_factory=list)
    configurations: dict[str, dict] = Field(default_factory=dict)
    policy_parameters: dict = Field(default_factory=dict)
    limitations: list[str] = Field(default_factory=list)
    original_run_id: str | None = None
    original_mode: Mode | None = None
    omitted_sensitive_data: str = "Prompt and response text omitted; structure and validation outcomes retained. Hashes are not anonymization."
    clock_domain: str = "client-monotonic-relative"
