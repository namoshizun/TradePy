import abc
import os
import pickle
import pandas as pd
from typing import Any, Generic, Type, TypeVar, TypedDict
from uuid import uuid4
from loguru import logger
from pathlib import Path
from datetime import datetime
from dask.distributed import Client as DaskClient
from tradepy.backtest.evaluation import ResultEvaluator

from tradepy.core.conf import BacktestConf, DaskConf, OptimizationConf, TaskConf
from tradepy.optimization.parameter import Parameter, ParameterGroup
from tradepy.optimization.result import OptimizationResult
from tradepy.optimization.types import Number, TaskRequest, TaskResult
from tradepy.optimization.worker import Worker
from tradepy.strategy.base import StrategyBase
from tradepy.utils import optimize_dtype_memory


def get_default_workspace_dir() -> Path:
    now = datetime.now()
    path = os.path.expanduser(
        f"~/.tradepy/optimizer/{now.date()}/{now.isoformat()[11:19]}"
    )
    return Path(path)


def get_random_id() -> str:
    return str(uuid4())


ConfType = TypeVar("ConfType", bound=TaskConf)


class TaskScheduler(Generic[ConfType]):
    def __init__(self, conf: ConfType) -> None:
        self.conf = conf
        self.workspace_dir: Path = get_default_workspace_dir()
        self.workspace_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"任务工作目录: {self.workspace_dir}")

    @property
    def task_log_file_path(self) -> Path:
        return self.workspace_dir / "tasks.csv"

    def _executor(self, request: TaskRequest) -> TaskResult:
        # Run backtesting
        trade_book_path: str = Worker(self.workspace_dir).run(request)

        # Evaluate results
        with open(trade_book_path, "rb") as file:
            trade_book = pickle.load(file)
            evaluator_class: Type[ResultEvaluator] = self.conf.load_evaluator_class()
            metrics = evaluator_class(trade_book).evaluate_trades()  # type: ignore
            return dict(metrics=metrics, **request)  # type: ignore

    def _output_indicators_df(self, df: pd.DataFrame) -> Path:
        strategy = self.conf.backtest.strategy.load_strategy()
        ind_df = strategy.compute_all_indicators_df(df)
        out_path = self.workspace_dir / "dataset.pkl"
        ind_df = optimize_dtype_memory(ind_df)
        ind_df.to_pickle(out_path)
        logger.info(f"回测数据已保存至: {out_path}")
        return out_path

    def make_task_request(
        self,
        repetition: int,
        batch_id: str,
        param_values: dict[str, Number] | None = None,
    ) -> TaskRequest:
        backtest_conf: BacktestConf = self.conf.backtest.model_copy(deep=True)
        if param_values:
            backtest_conf.strategy.update(**param_values)

        if issubclass(kls := backtest_conf.strategy.strategy_class, StrategyBase):
            backtest_conf.strategy.strategy_class = f"{kls.__module__}.{kls.__name__}"

        return {
            "id": get_random_id(),
            "repetition": repetition,
            "batch_id": batch_id,
            "dataset_path": str(self.conf.dataset_path),
            "backtest_conf": backtest_conf.model_dump(),
        }

    def update_tasks_log(self, batch_df: pd.DataFrame):
        if batch_df.index.name != "id":
            batch_df.set_index("id", inplace=True)

        try:
            tasks_df = pd.read_csv(self.task_log_file_path, index_col="id", dtype=str)
            old_df = tasks_df.loc[~tasks_df.index.isin(batch_df.index)].copy()
            tasks_df = pd.concat([old_df, batch_df])
            tasks_df.to_csv(self.task_log_file_path)
        except FileNotFoundError:
            batch_df.to_csv(self.task_log_file_path)

    def submit_tasks_and_patch_results(
        self, dask_client: DaskClient, batch_df: pd.DataFrame
    ) -> list[TaskResult]:
        logger.info(f"提交{len(batch_df)}个任务")
        futures = dask_client.map(
            self._executor, batch_df.reset_index().to_dict(orient="records")
        )
        results: list[TaskResult] = dask_client.gather(futures)  # type: ignore

        # Update metrics
        metrics_df = pd.DataFrame(
            [{"id": r["id"], "metrics": r["metrics"]} for r in results]
        ).set_index("id")
        batch_df["metrics"] = metrics_df["metrics"]
        return results

    @abc.abstractmethod
    def _run_once(self, dask_client: DaskClient, run_id: int = 1):
        raise NotImplementedError

    @abc.abstractmethod
    def _run(self, repetitions: int, dask_client: DaskClient):
        raise NotImplementedError

    def run(
        self,
        repetitions: int | None = None,
        *,
        dask_args: dict | None = None,
        data_df: pd.DataFrame | None = None,
    ) -> Any:
        """
        执行回测计算任务

        :param repetition: 每组参数运行多少次回测
        :param dask_args: ``dask.distributed.Client`` 的参数, e.g., { "n_workers": 4, "threads_per_worker": 1 }
        :param data_df: 回测数据，可以是通过 ``StocksDailyBarsDepot`` 加载的原始日K数据，也可以是已经包含了策略指标的
        """
        # Set args
        if not repetitions:
            repetitions = self.conf.repetition

        _dask_args = DaskConf().model_dump()
        _dask_args.update(dask_args or dict())

        # Pre-compute indicators (if required)
        if not self.conf.dataset_path:
            if data_df is None:
                raise ValueError("如果没有配置回测数据地址，则必须直接提供`data_df`")
            if data_df.empty:
                raise ValueError("`data_df`是空的")
            self.conf.dataset_path = self._output_indicators_df(data_df)

        # Run dask
        dask_client = DaskClient(**_dask_args)

        try:
            info = dask_client.scheduler_info()
            logger.info(
                f'启动Dask集群: id={info["id"]}, dashboard port={info["services"]["dashboard"]}, {dask_client}'
            )
            return self._run(repetitions, dask_client)
        except Exception as exc:
            logger.exception(exc)
        finally:
            dask_client.close()
            logger.info("关闭Dask集群")


def _make_parameter(name: str | list[str], choices) -> Parameter | ParameterGroup:
    if isinstance(name, str):
        assert isinstance(choices, list) and not isinstance(choices[0], list)
        return Parameter(name, tuple(choices))

    assert isinstance(choices, list) and all(len(c) == len(name) for c in choices)
    return ParameterGroup(name=tuple(name), choices=tuple(map(tuple, choices)))


class OptimizationScheduler(TaskScheduler[OptimizationConf]):
    class SearchRangeSpec(TypedDict):
        name: str
        range: Any

    def __init__(
        self, conf: OptimizationConf, search_ranges: list[SearchRangeSpec]
    ) -> None:
        parameters: list[Parameter | ParameterGroup] = [
            _make_parameter(p["name"], p["range"]) for p in search_ranges
        ]
        self.parameters = parameters
        super().__init__(conf)

    def __run_once(self, dask_client: DaskClient, run_id: int = 1):
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
                self.make_task_request(run_id, batch_id, values)
                for values in params_batch
            ]

            # Make task batch dataframe
            batch_df = pd.DataFrame(requests).set_index("id")
            self.update_tasks_log(batch_df)

            results = self.submit_tasks_and_patch_results(dask_client, batch_df)
            self.update_tasks_log(batch_df)

            # Gather results
            optimizer.consume_batch_result(results)

    def _run(self, repetitions: int, dask_client: DaskClient):
        for rep in range(1, repetitions + 1):
            logger.info(f"第{rep}次执行")
            self.__run_once(dask_client, rep)

        return OptimizationResult(self.parameters, self.workspace_dir)


class BacktestRunsScheduler(TaskScheduler):
    def _run(self, repetitions: int, dask_client: DaskClient):
        requests = [
            self.make_task_request(idx + 1, str(idx)) for idx in range(repetitions)
        ]

        # Make task batch dataframe
        batch_df = pd.DataFrame(requests).set_index("id")
        self.update_tasks_log(batch_df)

        self.submit_tasks_and_patch_results(dask_client, batch_df)
        self.update_tasks_log(batch_df)
