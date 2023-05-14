from dataclasses import dataclass

from tradepy.optimization.types import Number


@dataclass
class Parameter:
    name: str
    choices: tuple[Number, ...]


@dataclass
class ParameterGroup:
    name: tuple[str]
    choices: tuple[tuple[Number, ...]]
