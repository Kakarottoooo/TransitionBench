import time
import pytest
from transitionbench.rollout import PlanStore, execute_plan
from transitionbench.schemas import WorkerSnapshot, ResourceBudget


class FixtureAdapter:
    """CPU contract fixture; never hardware evidence."""
    def __init__(self):
        self.workers = {str(i): WorkerSnapshot(worker_id=str(i), config_id="A", generation=0,
                         ready=True, accepting=True, in_flight=0, device_uuid=f"synthetic-{i}",
                         device_model="fixture", observed_at_unix_s=time.time(), resource_evidence="synthetic") for i in range(2)}
        self.calls = []
        self.fail_readiness = False

    async def snapshot(self):
        return list(self.workers.values())

    async def operation(self, operation, worker_id, payload, key):
        self.calls.append((operation, worker_id))
        w = self.workers[worker_id]
        if operation == "drain":
            w.accepting = False
        if operation in ("apply", "rollback"):
            assert payload["expected_generation"] == w.generation
            w.config_id = payload["config_id"]
            w.generation += 1
        if operation == "readiness" and self.fail_readiness:
            self.fail_readiness = False
            raise RuntimeError("Injected readiness failure")
        if operation == "observe":
            w.accepting = True
        w.observed_at_unix_s = time.time()
        return w.model_dump(mode="json")


async def test_exact_approval_and_idempotent_execution(tmp_path):
    store = PlanStore(tmp_path / "plans.db")
    adapter = FixtureAdapter()
    plan = store.create("fixture", "B", await adapter.snapshot(), ResourceBudget(), ttl_s=60)
    with pytest.raises(ValueError, match="approval"):
        await execute_plan(store, plan["id"], adapter)
    store.approve(plan["id"], plan["hash"])
    result = await execute_plan(store, plan["id"], adapter)
    assert result["state"] == "COMPLETE"
    calls = len(adapter.calls)
    assert (await execute_plan(store, plan["id"], adapter))["state"] == "COMPLETE"
    assert len(adapter.calls) == calls
    assert adapter.calls.index(("observe", "0")) < adapter.calls.index(("drain", "1"))


async def test_readiness_failure_rolls_back_and_records_cost(tmp_path):
    store, adapter = PlanStore(tmp_path / "plans.db"), FixtureAdapter()
    plan = store.create("fixture", "B", await adapter.snapshot(), ResourceBudget(), ttl_s=60)
    store.approve(plan["id"], plan["hash"])
    adapter.fail_readiness = True
    result = await execute_plan(store, plan["id"], adapter)
    assert result["state"] == "ABORTED"
    assert adapter.workers["0"].config_id == "A"
    assert any(e["state"] == "ROLLING_BACK" for e in result["events"])
    assert result["elapsed_s"] > 0
