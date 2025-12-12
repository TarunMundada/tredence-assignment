from app.models import DataState, Anomaly, Rule
import pandas as pd
import numpy as np
from typing import List

# Node: profile_data (simple)
def profile_data(state: DataState) -> DataState:
    df = pd.DataFrame(state.data)
    profile = {}
    for col in df.columns:
        series = df[col]
        profile[col] = {
            "dtype": str(series.dtype),
            "null_count": int(series.isnull().sum()),
            "unique": int(series.nunique(dropna=True)),
        }
        if pd.api.types.is_numeric_dtype(series):
            profile[col].update({
                "min": None if series.dropna().empty else float(series.min()),
                "max": None if series.dropna().empty else float(series.max()),
                "mean": None if series.dropna().empty else float(series.mean()),
                "std": None if series.dropna().empty else float(series.std()),
            })
    state.profile = profile
    return state

# Node: identify_anomalies
def identify_anomalies(state: DataState) -> DataState:
    df = pd.DataFrame(state.data)
    anomalies = []
    for col in df.columns:
        series = df[col]
        # nulls
        null_idx = series[series.isnull()].index.tolist()
        for idx in null_idx:
            anomalies.append(Anomaly(row_index=int(idx), column=col, issue="null", value=None))
        # numeric outliers by z-score (>3)
        if pd.api.types.is_numeric_dtype(series):
            vals = series.dropna().astype(float)
            if len(vals) >= 2:
                mean = vals.mean()
                std = vals.std()
                if std > 0:
                    z = (series - mean).abs() / std
                    outlier_idx = z[z > 3].dropna().index.tolist()
                    for idx in outlier_idx:
                        anomalies.append(Anomaly(row_index=int(idx), column=col, issue="z_outlier", value=series.iloc[int(idx)]))
        # negative where metadata says non_negative
        nonneg_cols = state.metadata.get("non_negative_columns", [])
        if col in nonneg_cols:
            neg_idx = series[series < 0].dropna().index.tolist()
            for idx in neg_idx:
                anomalies.append(Anomaly(row_index=int(idx), column=col, issue="negative_value", value=series.iloc[int(idx)]))
    state.anomalies = anomalies
    state.anomaly_count = len(anomalies)
    return state

# Node: generate_rules (simple heuristics)
def generate_rules(state: DataState) -> DataState:
    rules = []
    # if nulls present -> impute_mean for numeric, fill_mode for categorical
    for col, meta in state.profile.items():
        if meta.get("null_count", 0) > 0:
            if meta["dtype"].startswith("float") or meta["dtype"].startswith("int") or "int" in meta["dtype"]:
                rules.append(Rule(column=col, rule_type="impute_mean").dict())
            else:
                rules.append(Rule(column=col, rule_type="impute_mode").dict())
        # if numeric and std is large relative to mean -> clip
        if "std" in meta and meta.get("std") not in (None, 0):
            rules.append(Rule(column=col, rule_type="clip", params={"min": meta.get("min"), "max": meta.get("max")}).dict())
    state.rules = [Rule(**r) if isinstance(r, dict) else r for r in rules]
    return state

# Node: apply_rules (basic)
def apply_rules(state: DataState) -> DataState:
    df = pd.DataFrame(state.data)
    actions = []
    for rule in state.rules:
        col = rule.column
        if rule.rule_type == "impute_mean":
            if col in df.columns:
                mean = df[col].dropna().astype(float).mean()
                count_before = df[col].isnull().sum()
                df[col] = df[col].fillna(mean)
                actions.append({"rule": rule.dict(), "filled": int(count_before)})
        elif rule.rule_type == "impute_mode":
            if col in df.columns:
                mode = df[col].dropna().mode()
                fill = mode.iloc[0] if not mode.empty else None
                count_before = df[col].isnull().sum()
                df[col] = df[col].fillna(fill)
                actions.append({"rule": rule.dict(), "filled": int(count_before)})
        elif rule.rule_type == "clip":
            if col in df.columns:
                params = rule.params or {}
                mn = params.get("min")
                mx = params.get("max")
                before_outliers = ((df[col] < mn) | (df[col] > mx)).sum() if mn is not None and mx is not None else 0
                if mn is not None:
                    df[col] = df[col].apply(lambda x: mn if (pd.notnull(x) and x < mn) else x)
                if mx is not None:
                    df[col] = df[col].apply(lambda x: mx if (pd.notnull(x) and x > mx) else x)
                actions.append({"rule": rule.dict(), "clipped": int(before_outliers)})
    state.data = df.to_dict(orient="records")
    state.applied_actions.extend(actions)
    return state

# Node: re_evaluate (reuse identify_anomalies)
def re_evaluate(state: DataState) -> DataState:
    state.iteration = state.iteration + 1
    # re-profile and re-identify anomalies
    state = profile_data(state)
    state = identify_anomalies(state)
    return state