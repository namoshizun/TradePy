import traceback
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
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

    def ok(self) -> bool:
        return self.result is not None and self.error_message is None


class DataFetcher:
    def __init__(self, title: str):
        self.title = title

    def _fetch_data(
        self,
        jobs: list[DataFetchJob[T]],
    ) -> Generator[DataFetchJob[T], None, None]:
        with ThreadPoolExecutor(
            max_workers=config.common.download_concurrency
        ) as executor:
            future_to_job = {
                executor.submit(job.func, **job.args): job for job in jobs
            }
            for future in as_completed(future_to_job):
                job = future_to_job[future]
                try:
                    job.result = future.result()
                except Exception:
                    job.error_message = traceback.format_exc()
                yield job

    def submit(
        self,
        jobs: list[DataFetchJob[T]],
        retry_failed: bool = True,
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
        with Progress(*progress_columns) as progress:
            task_id = progress.add_task(self.title, total=total)
            for job in self._fetch_data(jobs):
                completed += 1
                progress.update(task_id, completed=completed)
                yield job
