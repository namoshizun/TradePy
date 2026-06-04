from abc import ABC, abstractmethod
from typing import Any


class Pipeline(ABC):
    @abstractmethod
    def execute(self) -> Any:
        raise NotImplementedError
