import pytest
from app.engine.runner import Graph, run_graph
from app.models import DataState
from app.workflows import data_quality

graph_json = {
    "start_node": "profile_data",
    "edges": {
        "profile_data": "identify_anomalies",
        "identify_anomalies": "generate_rules",
        "generate_rules": "apply_rules",
        "apply_rules": "re_evaluate",
        "re_evaluate": {
            "condition": {
                "check": {"lhs": "anomaly_count", "op": ">", "rhs": 0},
                "true": "generate_rules",
                "false": None
            }
        }
    }
}

node_map = {
    "profile_data": data_quality.profile_data,
    "identify_anomalies": data_quality.identify_anomalies,
    "generate_rules": data_quality.generate_rules,
    "apply_rules": data_quality.apply_rules,
    "re_evaluate": data_quality.re_evaluate,
}

@pytest.mark.asyncio
async def test_full_graph_run_converges():
    initial_state = DataState(
        data=[
            {"id": 1, "age": 25},
            {"id": 2, "age": -5},
            {"id": 3, "age": None}
        ],
        metadata={
            "threshold": 0,
            "max_iterations": 5,
            "non_negative_columns": ["age"]
        }
    )

    graph = Graph(graph_json, node_map)
    final_state, logs = await run_graph(graph, initial_state)

    assert final_state.anomaly_count == 0, "All anomalies should be resolved"
    assert final_state.iteration <= 5
    assert any("clipped" in a for a in final_state.applied_actions), "Clip rule should apply"