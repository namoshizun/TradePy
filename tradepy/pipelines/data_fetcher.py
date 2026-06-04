import traceback
import uuid
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn

from tradepy import config

T = TypeVar("T")


@dataclass
class DataFetchJob(Generic[T]):
    func: Callable[..., T]
    args: dict[str, Any]
    result: T | None = None
    error_message: str | None = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def ok(self) -> bool:
        return self.result is not None and self.error_message is None


class DataFetcher:
    def __init__(self, title: str):
        self.title = title

    def _fetch_data(
        self,
        jobs: dict[str, DataFetchJob[T]],
    ) -> Generator[DataFetchJob[T], None, None]:
        with ThreadPoolExecutor(
            max_workers=config.common.download_concurrency
        ) as executor:
            future_to_job = {
                executor.submit(job.func, **job.args): job
                for job in jobs.values()
            }
            for future in as_completed(future_to_job):
                job = future_to_job[future]
                try:
                    job.result = future.result()
                    job.error_message = None
                except Exception:
                    job.result = None
                    job.error_message = traceback.format_exc()
                yield job

    def submit(
        self,
        jobs: list[DataFetchJob[T]],
        max_retries: int = 3,
    ) -> Generator[DataFetchJob[T], None, None]:
        if not jobs:
            return

        total = len(jobs)
        completed = 0

        progress_columns = (
            TextColumn("[{task.description}]", markup=False),
            BarColumn(),
            TaskProgressColumn(),
        )

        _jobs = {job.id: job for job in jobs}
        retries = 0
        with Progress(*progress_columns) as progress:
            task_id = progress.add_task(self.title, total=total)

            while _jobs and retries <= max_retries:
                for job in self._fetch_data(_jobs):
                    if job.error_message:
                        progress.console.print(
                            f"[red]下载任务 [{job.id}] 失败. 参数: {job.args}. 错误信息[/red]: {job.error_message}"
                        )
                        continue

                    completed += 1
                    progress.update(task_id, completed=completed)
                    yield job
                    _jobs.pop(job.id)

                # If some jobs failed, we will start another round
                retries += 1
