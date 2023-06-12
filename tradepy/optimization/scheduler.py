import os
import pandas as pd
from uuid import uuid4
from loguru import logger
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from dask.distributed import Client as DaskClient

from tradepy.core.conf import BacktestConf, OptimizationConf, StrategyConf
from tradepy.optimization.parameter import Parameter, ParameterGroup
from tradepy.optimization.types import Number, TaskRequest, TaskResult
from tradepy.optimization.worker import Worker


def get_default_workspace_dir() -> Path:
    now = datetime.now()
    path = os.path.expanduser(
        f"~/.tradepy/optimizer/{now.date()}/{now.isoformat()[11:19]}"
    )
    return Path(path)


def get_random_id() -> str:
    return str(uuid4())


@dataclass
class Scheduler:
    parameters: list[Parameter | ParameterGroup]
    conf: OptimizationConf
    workspace_dir: Path = field(default_factory=get_default_workspace_dir)

    def __post_init__(self):
        self.workspace_dir.mkdir(parents=True, exist_ok=True)

    @property
    def task_log_file_path(self) -> Path:
        return self.workspace_dir / "tasks.csv"

    def _make_task_request(
        self, batch_id: str, param_values: dict[str, Number]
    ) -> TaskRequest:
        backtest_conf: BacktestConf = self.conf.backtest.copy()
        backtest_conf.strategy.update(**param_values)

        return {
            "id": get_random_id(),
            "batch_id": batch_id,
            "workspace_id": self.workspace_dir.name,
            "dataset_path": str(self.conf.dataset_path),
            "optimizer_class": self.conf.optimizer_class,
            "backtest_conf": backtest_conf.dict(),
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

    def _submit_tasks_and_patch_results(
        self, dask_client: DaskClient, batch_df: pd.DataFrame
    ) -> list[TaskResult]:
        def executor(request: TaskRequest) -> TaskResult:
            now = datetime.now()
            workspace_dir = os.path.expanduser(
                f'~/.tradepy/worker/{now.date()}/{request["workspace_id"]}'
            )
            return Worker(workspace_dir).run(request)

        logger.info(f"提交{len(batch_df)}个任务")
        futures = dask_client.map(
            executor, batch_df.reset_index().to_dict(orient="records")
        )
        results: list[TaskResult] = dask_client.gather(futures)  # type: ignore

        # Update metrics
        metrics_df = pd.DataFrame(
            [{"id": r["id"], "metrics": r["metrics"]} for r in results]
        ).set_index("id")
        batch_df["metrics"] = metrics_df["metrics"]
        return results

    def _run_once(self, dask_client: DaskClient, run_id: int = 1):
        optimizer = self.conf.load_optimizer_class()(self.parameters)
        params_batch_generator = optimizer.generate_parameters_batch()
        batch_count = 0
        while True:
            try:
                params_batch = next(params_batch_generator)
                batch_count += 1
                logger.info(f"获取第{batch_count}个参数批, 批数量 = {len(params_batch)}")
            except StopIteration:
                break

            batch_id = f"{run_id}-{batch_count}"
            requests = [
                self._make_task_request(batch_id, values) for values in params_batch
            ]

            # Make task batch dataframe
            batch_df = pd.DataFrame(requests).set_index("id")
            self._update_tasks_log(batch_df)

            results = self._submit_tasks_and_patch_results(dask_client, batch_df)
            self._update_tasks_log(batch_df)

            # Gather results
            optimizer.consume_batch_result(results)

    def run(self, repetitions: int | None = None, dask_args: dict | None = None):
        if not repetitions:
            repetitions = self.conf.repetition

        _dask_args = self.conf.dask.dict().copy()
        if dask_args:
            _dask_args.update(dask_args)

        dask_client = DaskClient(**_dask_args)

        try:
            info = dask_client.scheduler_info()
            logger.info(
                f'启动Dask集群: id={info["id"]}, dashboard port={info["services"]["dashboard"]}, {dask_client}'
            )

            for rep in range(1, repetitions + 1):
                logger.info(f"第{rep}次执行")
                self._run_once(dask_client, rep)
        except Exception as exc:
            logger.exception(exc)
        finally:
            dask_client.close()
            logger.info("关闭Dask集群")
