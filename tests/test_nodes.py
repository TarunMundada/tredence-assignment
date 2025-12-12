import pytest
from app.models import DataState
from app.workflows import data_quality

def test_profile_data_basic():
    state = DataState(
        data=[
            {"id": 1, "age": 10},
            {"id": 2, "age": 20},
            {"id": 3, "age": None}
        ]
    )
    new = data_quality.profile_data(state)
    assert "age" in new.profile
    assert new.profile["age"]["null_count"] == 1
    assert new.profile["age"]["mean"] == 15.0

def test_identify_anomalies_negative():
    state = DataState(
        data=[
            {"id": 1, "age": -5},
            {"id": 2, "age": 20}
        ],
        metadata={"non_negative_columns": ["age"]}
    )
    state = data_quality.profile_data(state)
    new = data_quality.identify_anomalies(state)
    assert new.anomaly_count == 1
    assert new.anomalies[0].issue == "negative_value"

def test_generate_rules_produces_clip_to_zero():
    state = DataState(
        data=[
            {"id": 1, "age": -3},
            {"id": 2, "age": 10}
        ],
        metadata={"non_negative_columns": ["age"]}
    )
    state = data_quality.profile_data(state)
    state = data_quality.identify_anomalies(state)
    new = data_quality.generate_rules(state)

    clip_rules = [r for r in new.rules if r.rule_type == "clip" and r.column == "age"]
    assert len(clip_rules) >= 1
    assert clip_rules[0].params["min"] == 0.0

def test_apply_rules_clips_negative():
    state = DataState(
        data=[
            {"id": 1, "age": -5},
            {"id": 2, "age": 20}
        ]
    )
    state.rules = [
        {"column": "age", "rule_type": "clip", "params": {"min": 0.0, "max": 25.0}}
    ]
    new = data_quality.apply_rules(state)
    assert new.data[0]["age"] == 0.0  # clipped