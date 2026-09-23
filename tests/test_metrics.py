import pytest
from transitionbench.schemas import RequestEvent, SLOSpec
from transitionbench.metrics import summarize


def test_offered_denominator_includes_drops_and_horizon_is_inclusive():
    rows = [
        RequestEvent(request_id="a", scheduled_s=0, dispatch_s=0.1,
                     first_content_s=0.2, final_content_s=0.5, completed_s=1,
                     termination="complete", output_chars=2, quality_valid=True),
        RequestEvent(request_id="b", scheduled_s=0.2, termination="client_drop"),
        RequestEvent(request_id="c", scheduled_s=0.3, dispatch_s=0.3,
                     first_content_s=0.5, final_content_s=1.1, completed_s=1.1,
                     termination="complete", output_chars=2, quality_valid=True),
    ]
    result = summarize(rows, SLOSpec(e2e_s=2, first_content_s=1), 1)
    assert result["offered"] == 3
    assert result["qualified"] == 1
    assert result["attainment"] == pytest.approx(1 / 3)
    assert result["goodput_rps"] == 1
    assert result["latencies"][0]["scheduled_e2e_s"] == 1
    assert result["latencies"][0]["api_e2e_s"] == .9

