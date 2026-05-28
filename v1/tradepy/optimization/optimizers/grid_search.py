from typing import Generator
from itertools import product

from tradepy.optimization.base import ParameterOptimizer
from tradepy.optimization.types import ParameterValuesBatch, TaskResult


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
