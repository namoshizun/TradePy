from typing import Any, TypedDict


Number = float | int

ParameterValuesBatch = list[dict[str, Number]]


class TaskRequest(TypedDict):
    id: str
    batch_id: str
    repetition: int
    dataset_path: str
    backtest_conf: dict[str, Any]


class TaskResult(TaskRequest):
    metrics: dict[str, Any]
