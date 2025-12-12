from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class Anomaly(BaseModel):
    row_index: int
    column: str
    issue: str
    value: Any = None

class Rule(BaseModel):
    column: str
    rule_type: str  # e.g., "impute_mean", "clip", "cast"
    params: Dict[str, Any] = {}

class DataState(BaseModel):
    data: List[Dict[str, Any]] = []
    profile: Dict[str, Any] = {}
    anomalies: List[Anomaly] = []
    rules: List[Rule] = []
    applied_actions: List[Dict[str, Any]] = []
    anomaly_count: int = 0
    metadata: Dict[str, Any] = {}
    iteration: int = 0