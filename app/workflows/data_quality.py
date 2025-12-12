from typing import List, Dict, Any, Tuple
from collections import OrderedDict
import pandas as pd
import numpy as np

from app.models import DataState, Anomaly, Rule

# ----------------------
# Helpers
# ----------------------
def _rule_key(rule: Rule) -> Tuple:
    params = rule.params or {}
    # stable representation of params
    params_items = tuple(sorted(params.items()))
    return (rule.column, rule.rule_type, params_items)

# ----------------------
# Nodes
# ----------------------

def profile_data(state: DataState) -> DataState:
    """
    Simple profiler: for each column compute dtype, null_count, unique, and
    numeric stats (min, max, mean, std) when applicable.
    """
    df = pd.DataFrame(state.data)
    profile: Dict[str, Dict[str, Any]] = {}
    for col in df.columns:
        series = df[col]
        meta: Dict[str, Any] = {
            "dtype": str(series.dtype),
            "null_count": int(series.isnull().sum()),
            "unique": int(series.nunique(dropna=True)),
        }
        if pd.api.types.is_numeric_dtype(series):
            non_null = series.dropna()
            meta.update({
                "min": None if non_null.empty else float(non_null.min()),
                "max": None if non_null.empty else float(non_null.max()),
                "mean": None if non_null.empty else float(non_null.mean()),
                "std": None if non_null.empty else float(non_null.std()),
            })
        profile[col] = meta
    state.profile = profile
    return state

def identify_anomalies(state: DataState) -> DataState:
    """
    Detect anomalies:
    - nulls
    - numeric z-score outliers (|z| > 3)
    - negative values for columns listed in metadata.non_negative_columns
    """
    df = pd.DataFrame(state.data)
    anomalies: List[Anomaly] = []
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
                mean = float(vals.mean())
                std = float(vals.std())
                if std > 0:
                    z = (series - mean).abs() / std
                    outlier_idx = z[z > 3].dropna().index.tolist()
                    for idx in outlier_idx:
                        val = series.iloc[int(idx)]
                        anomalies.append(Anomaly(row_index=int(idx), column=col, issue="z_outlier", value=float(val)))

        # negative values for non-negative columns
        if col in nonneg_cols:
            neg_idx = df.index[df[col].notnull() & (df[col] < 0)].tolist()
            for idx in neg_idx:
                anomalies.append(Anomaly(row_index=int(idx), column=col, issue="negative_value", value=float(df.loc[idx, col])))

    state.anomalies = anomalies
    state.anomaly_count = len(anomalies)
    return state

def generate_rules(state: DataState) -> DataState:
    """
    Heuristic rule generator:
    - impute_mean for numeric columns with nulls
    - impute_mode for categorical columns with nulls
    - clip-to-zero for non_negative_columns if observed min < 0
    - generic clip based on observed min/max only if it would be meaningful
    Dedupes rules and merges with existing state.rules (avoids duplication).
    """
    existing_rules = state.rules or []
    # normalize existing keys
    existing_keys = set()
    normalized_existing: List[Rule] = []
    for r in existing_rules:
        if isinstance(r, Rule):
            rr = r
        elif isinstance(r, dict):
            rr = Rule(**r)
        else:
            # best effort
            rr = Rule(column=r.get("column"), rule_type=r.get("rule_type"), params=r.get("params", {}))
        normalized_existing.append(rr)
        existing_keys.add(_rule_key(rr))

    new_rules: List[Rule] = []
    nonneg_cols = state.metadata.get("non_negative_columns", [])

    for col, meta in state.profile.items():
        # null handling
        if meta.get("null_count", 0) > 0:
            dtype = meta.get("dtype", "")
            if "float" in dtype or "int" in dtype or "numeric" in dtype:
                r = Rule(column=col, rule_type="impute_mean", params={})
            else:
                r = Rule(column=col, rule_type="impute_mode", params={})
            if _rule_key(r) not in existing_keys:
                new_rules.append(r)
                existing_keys.add(_rule_key(r))

        # non-negative columns: propose clip-to-zero if observed min < 0
        if col in nonneg_cols:
            col_min = meta.get("min")
            if col_min is not None and col_min < 0:
                r = Rule(column=col, rule_type="clip", params={"min": 0.0, "max": meta.get("max")})
                if _rule_key(r) not in existing_keys:
                    new_rules.append(r)
                    existing_keys.add(_rule_key(r))

        # generic clip suggestion only if it could change values (min != max and std not 0)
        if "std" in meta and meta.get("std") not in (None, 0):
            mn = meta.get("min")
            mx = meta.get("max")
            if mn is not None and mx is not None and mn != mx:
                r = Rule(column=col, rule_type="clip", params={"min": mn, "max": mx})
                if _rule_key(r) not in existing_keys:
                    new_rules.append(r)
                    existing_keys.add(_rule_key(r))

    # Merge: keep existing rules and append new ones (but dedupe)
    merged: List[Rule] = normalized_existing + new_rules

    # final dedupe preserving order
    seen = set()
    clean_rules: List[Rule] = []
    for rr in merged:
        key = _rule_key(rr)
        if key in seen:
            continue
        seen.add(key)
        # prune no-op clip rules where clip.min equals profile.min (these are derived from observed min and do nothing)
        if rr.rule_type == "clip":
            prof = state.profile.get(rr.column, {})
            prof_min = prof.get("min")
            clip_min = (rr.params or {}).get("min")
            if prof_min is not None and clip_min == prof_min:
                # drop this no-op clip (we expect stricter clips like clip-to-zero for non-negative columns)
                continue
        clean_rules.append(rr)

    # store as list of dicts for JSON friendliness
    state.rules = [r.dict() for r in clean_rules]
    return state

def apply_rules(state: DataState) -> DataState:
    """
    Apply rules to the in-memory dataframe.
    Accepts rules as dicts or Rule instances. Only logs actions that actually changed values.
    Keeps rules that could not be applied or had no effect for future runs.
    """
    df = pd.DataFrame(state.data)
    actions: List[Dict[str, Any]] = []
    remaining_rules: List[Rule] = []

    def _to_rule_obj(raw) -> Rule:
        if isinstance(raw, Rule):
            return raw
        if isinstance(raw, dict):
            return Rule(**raw)
        # fallback
        return Rule(column=raw.get("column"), rule_type=raw.get("rule_type"), params=raw.get("params", {}))

    for raw_rule in (state.rules or []):
        rule = _to_rule_obj(raw_rule)
        col = rule.column
        if col not in df.columns:
            # cannot apply now; keep for later
            remaining_rules.append(rule)
            continue

        if rule.rule_type == "impute_mean":
            non_null = df[col].dropna().astype(float)
            if len(non_null) == 0:
                remaining_rules.append(rule)
                continue
            mean_val = non_null.mean()
            null_mask = df[col].isnull()
            filled_count = int(null_mask.sum())
            if filled_count > 0:
                df.loc[null_mask, col] = mean_val
                actions.append({"rule": rule.dict(), "filled": filled_count})
            else:
                remaining_rules.append(rule)

        elif rule.rule_type == "impute_mode":
            non_null = df[col].dropna()
            if non_null.empty:
                remaining_rules.append(rule)
                continue
            mode_series = non_null.mode()
            if mode_series.empty:
                remaining_rules.append(rule)
                continue
            fill_val = mode_series.iloc[0]
            null_mask = df[col].isnull()
            filled_count = int(null_mask.sum())
            if filled_count > 0:
                df.loc[null_mask, col] = fill_val
                actions.append({"rule": rule.dict(), "filled": filled_count})
            else:
                remaining_rules.append(rule)

        elif rule.rule_type == "clip":
            params = rule.params or {}
            mn = params.get("min", None)
            mx = params.get("max", None)
            changed_mask = pd.Series([False] * len(df))
            if mn is not None:
                less_mask = df[col].notnull() & (df[col] < mn)
                if less_mask.any():
                    df.loc[less_mask, col] = mn
                    changed_mask = changed_mask | less_mask
            if mx is not None:
                greater_mask = df[col].notnull() & (df[col] > mx)
                if greater_mask.any():
                    df.loc[greater_mask, col] = mx
                    changed_mask = changed_mask | greater_mask
            changed_count = int(changed_mask.sum())
            if changed_count > 0:
                actions.append({"rule": rule.dict(), "clipped": changed_count})
            else:
                # keep for future attempts
                remaining_rules.append(rule)
        else:
            # unknown rule type: preserve it for auditing or later application
            remaining_rules.append(rule)

    # only record real actions
    if actions:
        state.applied_actions.extend(actions)

    # remaining_rules -> store as list of dicts
    state.rules = [r.dict() for r in remaining_rules]

    # update the data
    state.data = df.to_dict(orient="records")
    return state

def re_evaluate(state: DataState) -> DataState:
    """
    Increase iteration counter, re-profile and re-run anomaly detection.
    """
    state.iteration = (state.iteration or 0) + 1
    state = profile_data(state)
    state = identify_anomalies(state)
    return state