import abc
import os
import pandas as pd
import random
from pathlib import Path
from typing import Generator, Type
from datetime import datetime
from dataclasses import dataclass, field, asdict

from tradepy.core.context import Context
from tradepy.optimization.types import Number, ParameterValuesBatch, Parameter, TaskRequest, TaskResult
from tradepy.trade_book.trade_book import TradeBook


def get_default_workspace_dir() -> Path:
    path = os.path.expanduser(f"~/.tradepy/optimizer/{datetime.now().isoformat()[:19]}")
    return Path(path)


def get_default_optimizer_class() -> Type["ParameterOptimizer"]:
    from tradepy.optimization.grid_search import GridSearch
    return GridSearch


def get_random_id() -> str:
    return str(random.randint(0, 999999)).zfill(6)


@dataclass
class Scheduler:

    parameters: list[Parameter]
    context: Context
    dataset_path: str
    strategy: str
    runs_per_parameter: int = 1
    optimizer_class: Type["ParameterOptimizer"] = field(default_factory=get_default_optimizer_class)
    workspace_dir: Path = field(default_factory=get_default_workspace_dir)

    def __post_init__(self):
        self.workspace_dir.mkdir(parents=True, exist_ok=True)

    @property
    def task_log_file_path(self) -> Path:
        return self.workspace_dir / "tasks.csv"

    def _make_task_request(self, batch: int, param_values: dict[str, Number]) -> TaskRequest:
        # total_runs: int = np.prod([len(p["choices"]) for p in self.parameters]) * self.runs_per_parameter  # type: ignore
        return {
            "id": "test",
            "batch": batch,
            "parameters": param_values,
            "strategy": self.strategy,
            "dataset_path": self.dataset_path,
            "context": asdict(self.context)
        }

    def _update_tasks_log(self, batch_df: pd.DataFrame):
        if batch_df.index.name != "id":
            batch_df.set_index("id", inplace=True)

        try:
            tasks_df = pd.read_csv(self.task_log_file_path, index_col="id")
            tasks_df.update(batch_df)

            new_tasks = batch_df[~batch_df.index.isin(tasks_df.index)]
            if not new_tasks.empty:
                tasks_df = pd.concat([tasks_df, new_tasks])

            tasks_df.to_csv(self.task_log_file_path)
        except FileNotFoundError:
            batch_df.to_csv(self.task_log_file_path)

    def _submit_tasks_and_patch_results(self, batch_df: pd.DataFrame) -> list[TaskResult]:
        ...

    def run(self):
        task_batches = []
        optimizer = self.optimizer_class(self.parameters)

        batch_count = 0
        params_batch_generator = optimizer.generate_parameters_batch()
        while True:
            try:
                params_batch = next(params_batch_generator)
            except StopIteration:
                break

            batch_count += 1
            requests = [
                self._make_task_request(batch_count, values)
                for values in params_batch
            ]
            # Make task batch dataframe
            batch_df = pd.DataFrame(requests).set_index("id")
            self._update_tasks_log(batch_df)

            results = self._submit_tasks_and_patch_results(batch_df)
            self._update_tasks_log(batch_df)

            # Gather results
            task_batches.append(batch_df)
            optimizer.consume_batch_result(results)


@dataclass
class ParameterOptimizer:
    parameters: list[Parameter]

    @abc.abstractmethod
    def generate_parameters_batch(self) -> Generator[ParameterValuesBatch, None, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def consume_batch_result(self, results: list[TaskResult]):
        raise NotImplementedError

    @abc.abstractclassmethod
    def evaluate_trades(cls, trade_book: TradeBook):
        raise NotImplementedError
