from fastapi import APIRouter, HTTPException
from app.models import DataState
from app.engine.runner import Graph, run_graph
from app.workflows import data_quality
import uuid
from typing import Dict

router = APIRouter()

# in-memory stores
_GRAPHS: Dict[str, Dict] = {}
_RUNS: Dict[str, Dict] = {}

@router.post("/graph/create")
def create_graph(payload: Dict):
    graph_id = str(uuid.uuid4())
    _GRAPHS[graph_id] = payload
    return {"graph_id": graph_id}

@router.post("/graph/run")
async def run_graph_endpoint(payload: Dict):
    graph_id = payload.get("graph_id")
    initial_state = payload.get("initial_state", {})
    if graph_id not in _GRAPHS:
        raise HTTPException(status_code=404, detail="graph not found")
    # build node map from available functions in data_quality
    node_map = {
        "profile_data": data_quality.profile_data,
        "identify_anomalies": data_quality.identify_anomalies,
        "generate_rules": data_quality.generate_rules,
        "apply_rules": data_quality.apply_rules,
        "re_evaluate": data_quality.re_evaluate,
    }
    graph = Graph(_GRAPHS[graph_id], node_map)
    state = DataState(**initial_state)
    run_id = str(uuid.uuid4())
    _RUNS[run_id] = {"state": state, "log": [], "status": "running"}
    final_state, log = await run_graph(graph, state, run_id)
    _RUNS[run_id] = {"state": final_state, "log": log, "status": "finished"}
    return {"run_id": run_id, "final_state": final_state.dict(), "log": log}

@router.get("/graph/state/{run_id}")
def get_state(run_id: str):
    if run_id not in _RUNS:
        raise HTTPException(status_code=404, detail="run not found")
    run = _RUNS[run_id]
    state = run["state"]
    # if state is a Pydantic model, convert to dict
    return {"run_id": run_id, "status": run["status"], "state": state.dict() if hasattr(state, "dict") else state, "log": run["log"]}