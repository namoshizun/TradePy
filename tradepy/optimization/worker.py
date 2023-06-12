import os
import json
from pathlib import Path
from datetime import datetime
import pickle
from typing import Type
from loguru import logger
import pandas as pd

from tradepy.core.conf import BacktestConf
from tradepy.strategy.base import BacktestStrategy
from tradepy.decorators import timeit
from tradepy.trade_book.trade_book import TradeBook
from tradepy.utils import import_class
from tradepy.optimization.base import ParameterOptimizer
from tradepy.optimization.types import TaskRequest, TaskResult


class Worker:
    def __init__(self, workspace_dir: str | Path | None = None) -> None:
        if not workspace_dir:
            now = datetime.now()
            workspace_dir = os.path.expanduser(
                f"~/.tradepy/worker/{now.date()}/{now.isoformat()[11:19]}"
            )

        self.workspace_dir = Path(workspace_dir)
        self.workspace_dir.mkdir(parents=True, exist_ok=True)

    def create_task_dir(self, id: str) -> Path:
        path = self.workspace_dir / id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_task_data(self, task_dir: Path, data: TaskRequest | TaskResult):
        path = task_dir / "task.json"
        with path.open("w+") as f:
            json.dump(data, f)

    def write_trade_book(self, task_dir: Path, trade_book: TradeBook):
        path = task_dir / "trade_book.pkl"
        with path.open("wb") as f:
            pickle.dump(trade_book, f)

    def backtest(self, req: TaskRequest) -> TradeBook:
        # Load dataset
        dataset_path = req["dataset_path"]

        if dataset_path.endswith("csv"):
            df = pd.read_csv(dataset_path)
        elif dataset_path.endswith("pkl"):
            df = pd.read_pickle(dataset_path)
        else:
            raise ValueError(f"不支持的数据格式: {os.path.splitext(dataset_path)[1]}")

        # Run backtest
        bt_conf: BacktestConf = BacktestConf.from_dict(req["backtest_conf"])
        strategy_class: Type[BacktestStrategy] = bt_conf.strategy.load_strategy_class()
        _, trade_book = strategy_class.backtest(df, bt_conf)
        return trade_book

    def run(self, request: TaskRequest) -> TaskResult:
        logger.info(f'开始执行任务: {request["id"]}')

        with timeit() as timer:
            task_dir = self.create_task_dir(request["id"])
            self.write_task_data(task_dir, request)

            trade_book = self.backtest(request)
            self.write_trade_book(task_dir, trade_book)

            optimizer_class: Type["ParameterOptimizer"] = import_class(
                request["optimizer_class"]
            )
            metrics = optimizer_class.evaluate_trades(trade_book)  # type: ignore
            result: TaskResult = dict(metrics=metrics, **request)  # type: ignore
            self.write_task_data(task_dir, result)

        logger.info(f'任务执行完成: {request["id"]}, 耗时: {timer["seconds"]}s')
        return result
