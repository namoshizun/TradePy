from typing import Any, TypedDict


Number = float | int

ParameterValuesBatch = list[dict[str, Number]]


class TaskRequest(TypedDict):
    workspace_id: str
    id: str
    batch_id: str
    dataset_path: str
    optimizer_class: str
    backtest_conf: dict[str, Any]


class TaskResult(TaskRequest):
    metrics: dict[str, Any]
