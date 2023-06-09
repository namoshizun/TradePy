from typing import Any, Generator
from itertools import product

from tradepy.backtest.evaluation import ResultEvaluator
from tradepy.optimization.base import ParameterOptimizer
from tradepy.optimization.types import ParameterValuesBatch, TaskResult
from tradepy.trade_book.trade_book import TradeBook


class GridSearch(ParameterOptimizer):
    def generate_parameters_batch(self) -> Generator[ParameterValuesBatch, None, None]:
        def flatten(values):
            for p in list(values.keys()):
                v = values[p]
                if isinstance(v, (list, tuple)):
                    for _p, _v in zip(p, v):
                        values[_p] = _v
                    del values[p]

            return values

        keys = [param.name for param in self.parameters]
        values = [param.choices for param in self.parameters]
        combinations = product(*values)
        yield [flatten(dict(zip(keys, combination))) for combination in combinations]

    def consume_batch_result(self, results: list[TaskResult]):
        pass

    @classmethod
    def evaluate_trades(cls, trade_book: TradeBook) -> dict[str, Any]:
        e = ResultEvaluator(trade_book)
        return {
            "total_returns": e.get_total_returns(),
            "max_drawdown": e.get_max_drawdown(),
            "sharpe_ratio": e.get_sharpe_ratio(),
            "success_rate": e.get_success_rate(),
            "number_of_trades": e.get_number_of_trades(),
            "number_of_stop_loss": e.get_number_of_stop_loss(),
            "number_of_take_profit": e.get_number_of_take_profit(),
            "number_of_close": e.get_number_of_close(),
        }
