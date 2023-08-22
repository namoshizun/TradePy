import os
import json
import pickle
from pathlib import Path
from datetime import datetime
from typing import Type
from loguru import logger
import pandas as pd

from tradepy.core.conf import BacktestConf
from tradepy.strategy.base import BacktestStrategy
from tradepy.decorators import timeit
from tradepy.trade_book.trade_book import TradeBook
from tradepy.optimization.types import TaskRequest


class Worker:
    def __init__(self, workspace_dir: str | Path) -> None:
        if isinstance(workspace_dir, str):
            workspace_dir = Path(workspace_dir)

        self.workspace_dir = workspace_dir / "workers"
        self.workspace_dir.mkdir(parents=True, exist_ok=True)

    def get_or_create_task_dir(self, id: str) -> Path:
        path = self.workspace_dir / id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_task_request(self, task_dir: Path, data: TaskRequest):
        path = task_dir / "task.json"
        with path.open("w+") as f:
            json.dump(data, f)

    def write_trade_book(self, task_dir: Path, trade_book: TradeBook) -> Path:
        path = task_dir / "trade_book.pkl"
        with path.open("wb") as f:
            pickle.dump(trade_book, f)
        return path

    def backtest(self, request: TaskRequest) -> TradeBook:
        # Load dataset
        dataset_path = request["dataset_path"]

        if dataset_path.endswith("csv"):
            df = pd.read_csv(dataset_path)
        elif dataset_path.endswith("pkl"):
            df = pd.read_pickle(dataset_path)
        else:
            raise ValueError(f"不支持的数据格式: {os.path.splitext(dataset_path)[1]}")

        # Run backtest
        bt_conf: BacktestConf = BacktestConf.from_dict(request["backtest_conf"])
        strategy_class: Type[BacktestStrategy] = bt_conf.strategy.load_strategy_class()
        _, trade_book = strategy_class.backtest(df, bt_conf)
        return trade_book

    def run(self, request: TaskRequest) -> str:
        logger.info(f'开始执行任务: {request["id"]} (第{request["repetition"]}轮)')

        with timeit() as timer:
            task_dir = self.get_or_create_task_dir(request["id"])
            self.write_task_request(task_dir, request)

            trade_book = self.backtest(request)
            trade_book_path = self.write_trade_book(task_dir, trade_book)

        logger.info(f'任务执行完成: {request["id"]}, 耗时: {timer["seconds"]}s')
        return str(trade_book_path.absolute())
