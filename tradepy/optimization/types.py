from typing import Any, TypedDict


Number = float | int


class Parameter(TypedDict):
    name: str
    choices: list[Number]


ParameterValuesBatch = list[dict[str, Number]]


class TaskRequest(TypedDict):
    id: str
    batch: int
    parameters: dict[str, Number]
    strategy: str
    dataset_path: str
    context: dict[str, Any]


class TaskResult(TaskRequest):
    metrics: dict[str, Any]
