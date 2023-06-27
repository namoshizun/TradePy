import abc
from typing import Generator
from dataclasses import dataclass

from tradepy.optimization.types import ParameterValuesBatch, TaskResult
from tradepy.optimization.parameter import Parameter, ParameterGroup


@dataclass
class ParameterOptimizer:
    parameters: list[Parameter | ParameterGroup]

    @abc.abstractmethod
    def generate_parameters_batch(self) -> Generator[ParameterValuesBatch, None, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def consume_batch_result(self, results: list[TaskResult]):
        raise NotImplementedError
