"""Official SDK stdio server. Analysis/planning only; no traffic or approval tool."""
import os
from urllib.parse import urlsplit
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from .sdk import Client

mcp = FastMCP("TransitionBench", instructions="Evidence-aware analysis and planning. Simulation is not GPU evidence. No execution authority is exposed.")
READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=False)


def client():
    url = os.environ.get("TRANSITIONBENCH_API", "http://127.0.0.1:8765")
    parsed = urlsplit(url)
    if parsed.hostname not in ("127.0.0.1", "localhost", "::1") or parsed.username or parsed.password:
        raise ValueError("MCP only connects to an operator-configured loopback API")
    return Client(url)


@mcp.tool(annotations=READ_ONLY)
def list_capabilities() -> dict:
    """List available capabilities with evidence levels."""
    with client() as api:
        return api.capabilities()


@mcp.tool(annotations=READ_ONLY)
def validate_experiment(experiment: dict) -> dict:
    """Validate a bounded experiment without executing any request."""
    with client() as api:
        return api.validate(experiment)


@mcp.tool(annotations=READ_ONLY)
def get_run(run_id: str) -> dict:
    """Read the status and provenance of an existing run."""
    with client() as api:
        return api.get_run(run_id)


@mcp.tool(annotations=READ_ONLY)
def analyze_bundle(run_id: str) -> dict:
    """Read independent verification for a run already imported or executed locally."""
    with client() as api:
        return api.request("GET", f"/api/v1/runs/{run_id}/artifacts/verification.json")


@mcp.tool(annotations=READ_ONLY)
def compare_runs(run_ids: list[str]) -> list[dict]:
    """Compare matched run-level results without inventing counterfactuals."""
    with client() as api:
        return api.compare(run_ids)


@mcp.tool(annotations=READ_ONLY)
def explain_decision(assumptions: dict) -> dict:
    """Evaluate explicit horizon and transition-cost assumptions."""
    with client() as api:
        return api.evaluate(assumptions)


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, openWorldHint=False))
def plan_transition(config_id: str, budget: dict) -> dict:
    """Create a plan against a preconfigured experimental hook. Does not approve or execute it."""
    with client() as api:
        return api.request("POST", "/api/v1/transition-plans", {"config_id": config_id, "budget": budget})


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

