"""Transparent decisions using only calibration and past observations."""
import time
from collections import deque
from .schemas import DecisionInput, DecisionRecord


def evaluate_decision(value: DecisionInput) -> DecisionRecord:
    delta = value.candidate_goodput_rps - value.current_goodput_rps
    loss = value.transition_deficit_requests
    gain = None if loss is None else delta * value.horizon_s - loss
    break_even = loss / delta if loss is not None and delta > 0 else None
    if delta <= 0:
        action, reasons = "KEEP", ["Candidate has no positive estimated steady-state advantage"]
    elif not value.state_known or value.out_of_distribution or loss is None or not value.evidence_ids or value.uncertainty_requests is None:
        action, reasons = "INSUFFICIENT_EVIDENCE", ["State, in-distribution calibration, cost uncertainty, and evidence are required"]
    elif gain - value.uncertainty_requests >= value.min_gain_requests:
        action, reasons = "SWITCH", ["Conservative integrated gain exceeds the declared practical threshold"]
    else:
        action, reasons = "WAIT", ["Gain does not repay transition cost and uncertainty within the assumed horizon"]
    return DecisionRecord(action=action, horizon_s=value.horizon_s, gain_requests=gain,
                          transition_deficit_requests=loss, break_even_s=break_even,
                          uncertainty_requests=value.uncertainty_requests, reasons=reasons,
                          evidence_ids=value.evidence_ids, expires_at_unix_s=time.time() + 60,
                          origin=value.origin,
                          sensitivity=[{"horizon_s": h, "gain_requests": None if loss is None else delta * h - loss}
                                       for h in [value.horizon_s / 2, value.horizon_s, value.horizon_s * 2]])


class OnlinePolicy:
    """No workload schedule or future phase/answer/length is accepted here."""
    def __init__(self, name, calibration, horizon_s, min_gain=2):
        self.name, self.calibration = name, calibration
        self.horizon_s, self.min_gain = horizon_s, min_gain
        self.past = deque(maxlen=40)
        self.advantage_since = None
        self.last_switch_s = -1e9

    def observe_arrival(self, workload_class, prefix_group, at_s=None):
        self.past.append((workload_class, prefix_group, at_s))

    def choose(self, at_s, current, queue_depth):
        if self.name == "StaticBest" or len(self.past) < 8:
            return current, None
        mix = sum(c == "long" for c, _, _ in self.past) / len(self.past)
        times = [t for _, _, t in self.past if t is not None]
        demand = (len(times) - 1) / (times[-1] - times[0]) if len(times) >= 2 and times[-1] > times[0] else None
        diagnostic = self.calibration.get('capacity_diagnostic')
        if diagnostic is not None:
            # Cohort goodput at one offered load is not universal capacity.
            # Pure-class calibration does not validate harmonic interpolation
            # for mixtures, nor extrapolation to another arrival rate.
            measured_load = diagnostic['target_rate_rps']
            supported = (diagnostic['ready'] and mix in (0, 1) and demand is not None
                         and abs(demand-measured_load) <= .05*measured_load)
            if not supported:
                self.advantage_since = None
                decision = DecisionRecord(action='INSUFFICIENT_EVIDENCE', horizon_s=self.horizon_s,
                    gain_requests=None, transition_deficit_requests=None, break_even_s=None,
                    uncertainty_requests=None, reasons=['Load/class or capacity discrimination is outside measured support'],
                    evidence_ids=[self.calibration['id']], expires_at_unix_s=time.time()+60,
                    origin=self.calibration.get('origin', 'synthetic'), sensitivity=[]) if self.name == 'StateAware' else None
                return current, decision
        def capacity(config):
            rates = self.calibration["rates"][config]
            value = 1 / (mix / rates["long"] + (1 - mix) / rates["short"])
            return min(demand, value) if demand is not None else value
        candidate = max(self.calibration["rates"], key=capacity)
        if candidate == current or abs(capacity(candidate) - capacity(current)) < 1e-9:
            self.advantage_since = None
            decision = evaluate_decision(DecisionInput(current_goodput_rps=capacity(current),
                candidate_goodput_rps=capacity(candidate), horizon_s=self.horizon_s,
                evidence_ids=[self.calibration["id"]], origin=self.calibration.get("origin", "synthetic"))) if self.name == "StateAware" else None
            return current, decision
        a, b = capacity(current), capacity(candidate)
        if self.name == "SteadyStateFirst":
            self.last_switch_s = at_s
            return candidate, None
        tuning = self.calibration["hysteresis"]
        if b / a - 1 < tuning["advantage_fraction"]:
            self.advantage_since = None
            return current, None
        if self.advantage_since is None:
            self.advantage_since = at_s
        if at_s - self.advantage_since < tuning["persistence_s"] or at_s - self.last_switch_s < tuning["dwell_s"]:
            return current, None
        decision = None
        if self.name == "StateAware":
            bucket = "long" if mix >= .5 else "short"
            pair = self.calibration["costs"].get(f"{current}>{candidate}:{bucket}", {})
            state_bucket = "backlogged" if queue_depth > 2 else "idle"
            estimate = pair.get("by_queue", {}).get(state_bucket)
            decision = evaluate_decision(DecisionInput(
                current_goodput_rps=a, candidate_goodput_rps=b, horizon_s=self.horizon_s,
                transition_deficit_requests=estimate["mean"] if estimate else None,
                uncertainty_requests=estimate["spread"] if estimate else None,
                min_gain_requests=self.min_gain, state_known=queue_depth >= 0 and demand is not None,
                evidence_ids=[self.calibration["id"]] if estimate else [], origin=self.calibration.get("origin", "synthetic")))
            if decision.action != "SWITCH":
                return current, decision
        self.last_switch_s, self.advantage_since = at_s, None
        return candidate, decision
