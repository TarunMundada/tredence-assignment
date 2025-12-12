from typing import Dict, Any, Optional
import asyncio
import time
from app.models import DataState
from types import SimpleNamespace

class Graph:
    def __init__(self, graph_json: Dict[str, Any], node_map: Dict[str, callable]):
        self.start_node = graph_json.get("start_node")
        self.edges = graph_json.get("edges", {})
        self.node_map = node_map

    def get_node_fn(self, name):
        return self.node_map.get(name)

def eval_condition(cond_obj: Dict[str, Any], state: DataState) -> bool:
    # Very small, explicit condition language:
    # cond_obj = {"lhs": "anomaly_count", "op": ">", "rhs": 0}
    lhs = cond_obj.get("lhs")
    op = cond_obj.get("op")
    rhs = cond_obj.get("rhs")
    lhs_val = getattr(state, lhs) if hasattr(state, lhs) else state.metadata.get(lhs)
    if op == ">":
        return lhs_val > rhs
    if op == "<":
        return lhs_val < rhs
    if op == ">=":
        return lhs_val >= rhs
    if op == "<=":
        return lhs_val <= rhs
    if op == "==":
        return lhs_val == rhs
    return False

async def run_graph(graph: Graph, state: DataState, run_id: Optional[str] = None):
    current = graph.start_node
    log = []
    max_iters = state.metadata.get("max_iterations", 5)
    while current is not None:
        node_fn = graph.get_node_fn(current)
        if node_fn is None:
            break
        start_ts = time.time()
        # support sync or async
        if asyncio.iscoroutinefunction(node_fn):
            state = await node_fn(state)
        else:
            state = node_fn(state)
        end_ts = time.time()
        log.append({"node": current, "start": start_ts, "end": end_ts, "anomaly_count": state.anomaly_count})
        edge = graph.edges.get(current)
        # simple edge handling: string, or object with condition
        next_node = None
        if isinstance(edge, str):
            next_node = edge
        elif isinstance(edge, dict):
            cond = edge.get("condition")
            if cond:
                if eval_condition(cond.get("check"), state):
                    next_node = cond.get("true")
                else:
                    next_node = cond.get("false")
        # safety: iteration cap
        if state.iteration >= max_iters:
            break
        current = next_node
    return state, log

async def stream_graph(graph: Graph, state, run_id: Optional[str] = None):
    """
    Async generator that yields execution events in real-time.
    Yields dicts that can be sent directly over WebSocket.
    """
    current = graph.start_node
    max_iters = state.metadata.get("max_iterations", 5)

    # start event
    yield {
        "type": "start",
        "run_id": run_id,
        "start_node": current,
        "initial_state": state.dict() if hasattr(state, "dict") else state
    }

    while current is not None:
        node_fn = graph.get_node_fn(current)
        if node_fn is None:
            yield {"type": "error", "message": f"node '{current}' not found in node_map"}
            break

        start_ts = time.time()
        # run node (support sync + async)
        try:
            if asyncio.iscoroutinefunction(node_fn):
                state = await node_fn(state)
            else:
                # run sync in executor to avoid blocking the event loop if node is heavy
                state = await asyncio.get_event_loop().run_in_executor(None, node_fn, state)
        except Exception as e:
            yield {"type": "error", "node": current, "message": str(e)}
            break

        end_ts = time.time()
        duration = end_ts - start_ts

        # step event (trim snapshot if huge)
        yield {
            "type": "step",
            "node": current,
            "duration": duration,
            "anomaly_count": getattr(state, "anomaly_count", None),
            "state_snapshot": state.dict() if hasattr(state, "dict") else state
        }

        # next node decision
        edge = graph.edges.get(current)
        next_node = None
        if isinstance(edge, str):
            next_node = edge
        elif isinstance(edge, dict):
            cond = edge.get("condition")
            if cond:
                try:
                    if eval_condition(cond.get("check"), state):
                        next_node = cond.get("true")
                    else:
                        next_node = cond.get("false")
                except Exception as e:
                    yield {"type": "error", "message": f"condition eval error: {e}"}
                    break

        # safety
        if getattr(state, "iteration", 0) >= max_iters:
            yield {"type": "info", "message": "Max iterations reached", "iteration": state.iteration}
            break

        current = next_node

    # completion
    yield {"type": "complete", "final_state": state.dict() if hasattr(state, "dict") else state}