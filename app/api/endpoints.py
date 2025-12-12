# app/api/endpoints.py
import uuid
import asyncio
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from typing import Dict
from app.models import DataState
from app.engine.runner import Graph, run_graph, stream_graph
from app.registry import TOOLS  # <-- use the registry (dynamic)
from app.workflows import data_quality  # keep import for HTTP endpoint backward compatibility

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

    # Build node_map from registry TOOLS (TOOL values are callables)
    node_map = dict(TOOLS)  # shallow copy

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
    return {"run_id": run_id, "status": run["status"], "state": state.dict() if hasattr(state, "dict") else state, "log": run["log"]}

# -------------------------
# WebSocket streaming endpoint
# -------------------------
@router.websocket("/ws/graph/run")
async def websocket_graph_run(websocket: WebSocket):
    await websocket.accept()
    try:
        init_data = await websocket.receive_json()
        graph_id = init_data.get("graph_id")
        initial_state_dict = init_data.get("initial_state", {})

        if graph_id not in _GRAPHS:
            await websocket.send_json({"error": "graph not found"})
            await websocket.close()
            return

        node_map = dict(TOOLS)
        graph = Graph(_GRAPHS[graph_id], node_map)
        state = DataState(**initial_state_dict)
        run_id = str(uuid.uuid4())

        # stream_graph yields events
        async for event in stream_graph(graph, state, run_id):
            await websocket.send_json(event)
            # make it look nicer for demos; remove or reduce in production
            await asyncio.sleep(0.05)

        await websocket.close()
    except WebSocketDisconnect:
        print("WebSocket client disconnected")
    except Exception as e:
        # send error and close
        try:
            await websocket.send_json({"error": str(e)})
        except Exception:
            pass
        await websocket.close()