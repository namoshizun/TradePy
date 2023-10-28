import abc
from typing import Generator

from tradepy.optimization.types import ParameterValuesBatch, TaskResult
from tradepy.optimization.parameter import Parameter, ParameterGroup


class ParameterOptimizer:
    def __init__(self, parameters: list[Parameter | ParameterGroup]):
        self.parameters = parameters

    @abc.abstractmethod
    def generate_parameters_batch(self) -> Generator[ParameterValuesBatch, None, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def consume_batch_result(self, results: list[TaskResult]):
        raise NotImplementedError
