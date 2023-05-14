import os
import pandas as pd
import random
from loguru import logger
from pathlib import Path
from typing import Type
from datetime import datetime
from dataclasses import dataclass, field, asdict
from dask.distributed import Client as DaskClient

import tradepy
from tradepy.core.context import Context
from tradepy.optimization.parameter import Parameter, ParameterGroup
from tradepy.optimization.types import Number, TaskRequest, TaskResult
from tradepy.optimization.base import ParameterOptimizer
from tradepy.optimization.worker import Worker
from tradepy.utils import import_class


def get_default_optimizer_class() -> Type["ParameterOptimizer"]:
    return import_class(tradepy.config.optimizer_class)


def get_default_workspace_dir() -> Path:
    now = datetime.now()
    path = os.path.expanduser(f"~/.tradepy/optimizer/{now.date()}/{now.isoformat()[11:19]}")
    return Path(path)


def get_random_id() -> str:
    return str(random.randint(0, 999999)).zfill(6)


@dataclass
class Scheduler:

    parameters: list[Parameter | ParameterGroup]
    context: Context
    dataset_path: str
    strategy: str
    optimizer_class: Type["ParameterOptimizer"] = field(default_factory=get_default_optimizer_class)
    workspace_dir: Path = field(default_factory=get_default_workspace_dir)

    def __post_init__(self):
        self.workspace_dir.mkdir(parents=True, exist_ok=True)

    @property
    def task_log_file_path(self) -> Path:
        return self.workspace_dir / "tasks.csv"

    def _make_task_request(self, batch_id: str, param_values: dict[str, Number]) -> TaskRequest:
        ctx = asdict(self.context)

        # Remove context values that are part of parameters.
        for k in list(ctx.keys()):
            if k in param_values:
                del ctx[k]

        return {
            "id": get_random_id(),
            "batch_id": batch_id,
            "workspace_id": self.workspace_dir.name,
            "parameters": param_values,
            "strategy": self.strategy,
            "dataset_path": self.dataset_path,
            "base_context": ctx
        }

    def _update_tasks_log(self, batch_df: pd.DataFrame):
        if batch_df.index.name != "id":
            batch_df.set_index("id", inplace=True)

        try:
            tasks_df = pd.read_csv(self.task_log_file_path, index_col="id", dtype=str)
            tasks_df.update(batch_df)

            new_tasks = batch_df[~batch_df.index.isin(tasks_df.index)]
            if not new_tasks.empty:
                tasks_df = pd.concat([tasks_df, new_tasks])

            tasks_df.to_csv(self.task_log_file_path)
        except FileNotFoundError:
            batch_df.to_csv(self.task_log_file_path)

    def _submit_tasks_and_patch_results(self,
                                        dask_client: DaskClient,
                                        batch_df: pd.DataFrame) -> list[TaskResult]:
        def executor(request: TaskRequest) -> TaskResult:
            now = datetime.now()
            workspace_dir = os.path.expanduser(f'~/.tradepy/worker/{now.date()}/{request["workspace_id"]}')
            return Worker(workspace_dir).run(request)

        logger.info(f'提交{len(batch_df)}个任务')
        futures = dask_client.map(executor, batch_df.reset_index().to_dict(orient="records"))
        results: list[TaskResult] = dask_client.gather(futures)  # type: ignore

        # Update metrics
        metrics_df = pd.DataFrame([
            {
                "id": r["id"],
                "metrics": r["metrics"]
            }
            for r in results
        ]).set_index("id")
        batch_df["metrics"] = metrics_df["metrics"]
        return results

    def _run_once(self, dask_client: DaskClient, run_id: int = 1):
        optimizer = self.optimizer_class(self.parameters)
        params_batch_generator = optimizer.generate_parameters_batch()
        batch_count = 0
        while True:
            try:
                params_batch = next(params_batch_generator)
                batch_count += 1
                logger.info(f'获取第{batch_count}个参数批, 批数量 = {len(params_batch)}')
            except StopIteration:
                break

            batch_id = f'{run_id}-{batch_count}'
            requests = [
                self._make_task_request(batch_id, values)
                for values in params_batch
            ]

            # Make task batch dataframe
            batch_df = pd.DataFrame(requests).set_index("id")
            self._update_tasks_log(batch_df)

            results = self._submit_tasks_and_patch_results(dask_client, batch_df)
            self._update_tasks_log(batch_df)

            # Gather results
            optimizer.consume_batch_result(results)

    def run(self, repetitions: int = 1, dask_args: dict | None = None):
        dask_args = dask_args or dict()
        dask_client = DaskClient(**dask_args)

        try:
            info = dask_client.scheduler_info()
            logger.info(f'启动Dask集群: id={info["id"]}, dashboard port={info["services"]["dashboard"]}, {dask_client}')

            for rep in range(1, repetitions + 1):
                logger.info(f'第{rep}次执行')
                self._run_once(dask_client, rep)
        except Exception as exc:
            logger.exception(exc)
        finally:
            dask_client.close()
            logger.info('关闭Dask集群')
