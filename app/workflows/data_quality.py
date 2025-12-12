from app.models import DataState, Anomaly, Rule
import pandas as pd
import numpy as np
from typing import List
from collections import OrderedDict

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

def identify_anomalies(state: DataState) -> DataState:
    df = pd.DataFrame(state.data)
    anomalies = []
    nonneg_cols = state.metadata.get("non_negative_columns", [])
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
                        anomalies.append(Anomaly(row_index=int(idx), column=col, issue="z_outlier", value=float(series.iloc[int(idx)])))
        # negative where metadata says non_negative
        if col in nonneg_cols:
            # ensure comparison ignores NaN
            neg_idx = df.index[df[col].notnull() & (df[col] < 0)].tolist()
            for idx in neg_idx:
                anomalies.append(Anomaly(row_index=int(idx), column=col, issue="negative_value", value=float(df.loc[idx, col])))
    state.anomalies = anomalies
    state.anomaly_count = len(anomalies)
    return state

# Node: generate_rules (simple heuristics)
def _rule_key(rule: Rule):
    # stable key for deduplication
    return (rule.column, rule.rule_type, tuple(sorted((rule.params or {}).items())))

def generate_rules(state: DataState) -> DataState:
    rules = state.rules or []
    existing_keys = set(_rule_key(r if isinstance(r, Rule) else Rule(**r)) for r in rules)

    new_rules = []
    nonneg_cols = state.metadata.get("non_negative_columns", [])

    for col, meta in state.profile.items():
        # If nulls -> impute
        if meta.get("null_count", 0) > 0:
            dtype = meta.get("dtype", "")
            if "float" in dtype or "int" in dtype:
                r = Rule(column=col, rule_type="impute_mean", params={})
            else:
                r = Rule(column=col, rule_type="impute_mode", params={})
            if _rule_key(r) not in existing_keys:
                new_rules.append(r)
                existing_keys.add(_rule_key(r))

        # If column is declared non-negative, propose clipping to 0 (only if min < 0)
        if col in nonneg_cols:
            col_min = meta.get("min")
            if col_min is not None and col_min < 0:
                # create clip_to_zero rule
                r = Rule(column=col, rule_type="clip", params={"min": 0.0, "max": meta.get("max")})
                if _rule_key(r) not in existing_keys:
                    new_rules.append(r)
                    existing_keys.add(_rule_key(r))
                # Also consider an option to impute negatives to mean or absolute value; clip is least destructive.

        # Generic clip suggestion only if it would change something (min != max)
        if "std" in meta and meta.get("std") not in (None, 0):
            mn = meta.get("min")
            mx = meta.get("max")
            if mn is not None and mx is not None and mn != mx:
                r = Rule(column=col, rule_type="clip", params={"min": mn, "max": mx})
                if _rule_key(r) not in existing_keys:
                    new_rules.append(r)
                    existing_keys.add(_rule_key(r))

    # merge: keep previous rules + new rules (avoid unbounded growth, you might want to replace old rules instead)
    merged_rules = list(state.rules or [])  # preserve existing
    merged_rules.extend(new_rules)
    state.rules = merged_rules
    return state

def apply_rules(state: DataState) -> DataState:
    import math
    df = pd.DataFrame(state.data)
    actions = []
    for rule in state.rules:
        col = rule.column
        if col not in df.columns:
            continue
        if rule.rule_type == "impute_mean":
            non_null = df[col].dropna().astype(float)
            if len(non_null) == 0:
                continue
            mean_val = non_null.mean()
            null_mask = df[col].isnull()
            filled_count = int(null_mask.sum())
            if filled_count > 0:
                df.loc[null_mask, col] = mean_val
                actions.append({"rule": rule.dict(), "filled": filled_count})
        elif rule.rule_type == "impute_mode":
            non_null = df[col].dropna()
            if non_null.empty:
                continue
            mode_series = non_null.mode()
            if mode_series.empty:
                continue
            fill_val = mode_series.iloc[0]
            null_mask = df[col].isnull()
            filled_count = int(null_mask.sum())
            if filled_count > 0:
                df.loc[null_mask, col] = fill_val
                actions.append({"rule": rule.dict(), "filled": filled_count})
        elif rule.rule_type == "clip":
            params = rule.params or {}
            mn = params.get("min", None)
            mx = params.get("max", None)
            # compute how many rows would be changed
            changed_mask = pd.Series([False]*len(df))
            if mn is not None:
                less_mask = df[col].notnull() & (df[col] < mn)
                changed_mask = changed_mask | less_mask
                if less_mask.any():
                    df.loc[less_mask, col] = mn
            if mx is not None:
                greater_mask = df[col].notnull() & (df[col] > mx)
                changed_mask = changed_mask | greater_mask
                if greater_mask.any():
                    df.loc[greater_mask, col] = mx
            changed_count = int(changed_mask.sum())
            if changed_count > 0:
                actions.append({"rule": rule.dict(), "clipped": changed_count})
        # other rule types could be added here...
    # record only non-empty actions
    if actions:
        state.applied_actions.extend(actions)
    state.data = df.to_dict(orient="records")
    return state

# Node: re_evaluate (reuse identify_anomalies)
def re_evaluate(state: DataState) -> DataState:
    state.iteration = state.iteration + 1
    # re-profile and re-identify anomalies
    state = profile_data(state)
    state = identify_anomalies(state)
    return state