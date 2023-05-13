import os
import json
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
import pickle
from typing import Generator, Type

import pandas as pd
from tradepy.core.context import china_market_context
from tradepy.core.strategy import BacktestStrategy
from tradepy.trade_book.trade_book import TradeBook
from tradepy.utils import import_class
from tradepy.optimization.base import ParameterOptimizer, get_default_optimizer_class
from tradepy.optimization.types import TaskRequest, TaskResult


def get_default_workspace_dir() -> Path:
    path = os.path.expanduser(f"~/.tradepy/worker/{datetime.now().isoformat()[:19]}")
    return Path(path)


@dataclass
class Worker:
    cluster_url: str
    workspace_dir: Path = field(default_factory=get_default_workspace_dir)
    optimizer_class: Type[ParameterOptimizer] = field(default_factory=get_default_optimizer_class)

    def fetch_task(self) -> Generator[TaskRequest, None, None]:
        ...

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

    def backtest(self, task: TaskRequest) -> TradeBook:
        strategy_class: Type[BacktestStrategy] = import_class(task["strategy"])
        ctx = china_market_context(**task["context"])
        dataset_path = task["dataset_path"]

        if dataset_path.endswith("csv"):
            df = pd.read_csv(dataset_path)
        elif dataset_path.endswith("pkl"):
            df = pd.read_pickle(dataset_path)
        else:
            raise ValueError(f"不支持的数据格式: {os.path.splitext(dataset_path)[1]}")

        _, trade_book = strategy_class.backtest(df, ctx)
        return trade_book

    def run(self):
        for request in self.fetch_task():
            task_dir = self.create_task_dir(request["id"])
            self.write_task_data(task_dir, request)

            trade_book = self.backtest(request)
            self.write_trade_book(task_dir, trade_book)

            metrics = self.optimizer_class.evaluate_trades(trade_book)
            self.write_task_data(task_dir, dict(**request, metrics=metrics))
            # TODO: return metrics to cluster
