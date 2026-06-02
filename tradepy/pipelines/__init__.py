from abc import ABC, abstractmethod


class Pipeline(ABC):
    @abstractmethod
    def execute(self): ...
