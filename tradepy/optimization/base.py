import abc
from typing import Generator
from dataclasses import dataclass

from tradepy.optimization.types import ParameterValuesBatch, TaskResult
from tradepy.optimization.parameter import Parameter
from tradepy.trade_book.trade_book import TradeBook


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
