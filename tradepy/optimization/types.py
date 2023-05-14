from typing import Any, TypedDict


Number = float | int

ParameterValuesBatch = list[dict[str, Number]]


class TaskRequest(TypedDict):
    workspace_id: str
    id: str
    batch_id: str
    parameters: dict[str, Number]
    strategy: str
    dataset_path: str
    base_context: dict[str, Any]


class TaskResult(TaskRequest):
    metrics: dict[str, Any]
