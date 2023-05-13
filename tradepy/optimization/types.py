from typing import Any, TypedDict
from typing_extensions import NotRequired


class WorkerPayload(TypedDict):
    strategy: str  # strategy class import path
    workspace_dir: str
    parameters: dict[str, Any]

    dataset_file_path: NotRequired[str]
    dataset_download_url: NotRequired[str]
