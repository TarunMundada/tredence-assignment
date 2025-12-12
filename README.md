# Data Quality Engine

A flexible, extensible data quality workflow engine built with Python and FastAPI. This engine provides a framework for defining, executing, and monitoring data quality workflows through a graph-based approach.

## Features

- **Graph-Based Workflows**: Define data quality processes as directed graphs with conditional branching
- **Extensible Node System**: Register custom functions as workflow nodes
- **Real-Time Streaming**: WebSocket support for real-time workflow execution monitoring
- **RESTful API**: HTTP endpoints for creating and running workflows
- **Built-In Data Quality Operations**: Profile data, identify anomalies, generate and apply data quality rules
- **Iterative Processing**: Automatically re-evaluate data quality until anomalies are resolved

## Project Structure

```
data-quality-engine/
├── app/
│   ├── api/          # REST API endpoints
│   ├── engine/       # Core workflow execution engine
│   ├── workflows/    # Built-in data quality workflow nodes
│   ├── main.py       # FastAPI application entry point
│   ├── models.py     # Data models (Pydantic)
│   └── registry.py   # Node function registry
├── examples/         # Example workflow definitions
├── tests/            # Unit tests
└── requirements.txt  # Python dependencies
```

## Core Concepts

### DataState Model

The [DataState](file:///c%3A/Users/tarun/OneDrive/Desktop/data-quality-engine/app/models.py#L15-L23) model represents the state of data as it flows through the workflow:

- `data`: List of dictionaries representing tabular data
- `profile`: Column-level statistics and metadata
- `anomalies`: Detected data quality issues
- `rules`: Generated data quality rules
- `applied_actions`: Log of applied transformations
- `anomaly_count`: Count of detected anomalies
- `metadata`: User-defined metadata
- `iteration`: Current iteration count for iterative workflows

### Workflow Graph

Workflows are defined as JSON graphs with:

- `start_node`: Entry point of the workflow
- `edges`: Defines the flow between nodes, supporting conditional routing

### Built-In Nodes

The engine includes several built-in data quality nodes in [data_quality.py](file:///c%3A/Users/tarun/OneDrive/Desktop/data-quality-engine/app/workflows/data_quality.py):

1. **profile_data**: Computes statistics for each column (null counts, unique values, numeric stats)
2. **identify_anomalies**: Detects data quality issues (nulls, outliers, negative values)
3. **generate_rules**: Creates data quality rules based on detected anomalies
4. **apply_rules**: Applies generated rules to clean the data
5. **re_evaluate**: Increments iteration counter and re-runs profiling/anomaly detection

## Installation

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd data-quality-engine
   ```
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Starting the Server

```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`.

### API Endpoints

#### Create a Workflow Graph

```http
POST /graph/create
Content-Type: application/json

{
  "start_node": "profile_data",
  "edges": {
    "profile_data": "identify_anomalies",
    "identify_anomalies": "generate_rules",
    "generate_rules": "apply_rules",
    "apply_rules": "re_evaluate",
    "re_evaluate": {
      "condition": {
        "check": { "lhs": "anomaly_count", "op": ">=", "rhs": 1 },
        "true": "generate_rules",
        "false": null
      }
    }
  }
}
```

Response:

```json
{
  "graph_id": "uuid-string"
}
```

#### Run a Workflow

```http
POST /graph/run
Content-Type: application/json

{
  "graph_id": "uuid-string",
  "initial_state": {
    "data": [
      {"id": 1, "age": -5, "name": "Alice"},
      {"id": 2, "age": 25, "name": "Bob"},
      {"id": 3, "age": null, "name": "Charlie"}
    ],
    "metadata": {
      "non_negative_columns": ["age"]
    }
  }
}
```

#### Get Workflow State

```http
GET /graph/state/{run_id}
```

#### WebSocket Streaming

Connect to `ws://localhost:8000/ws/graph/run` to receive real-time updates during workflow execution.

### Adding Custom Nodes

Register new functions as workflow nodes using the [@register](file:///c%3A/Users/tarun/OneDrive/Desktop/data-quality-engine/app/registry.py#L3-L7) decorator:

```python
from app.registry import register
from app.models import DataState

@register("my_custom_node")
def my_custom_function(state: DataState) -> DataState:
    # Process the data state
    # Return modified state
    return state
```

## Example Workflow

See [examples/graphs_json.json](file:///c%3A/Users/tarun/OneDrive/Desktop/data-quality-engine/examples.py/graphs_json.json) for a complete workflow definition that demonstrates:

- Iterative data cleaning until anomalies are resolved
- Conditional branching based on anomaly count
- Full data quality pipeline from profiling to rule application

## Running Tests

```bash
pytest
```
