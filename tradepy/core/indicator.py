from dataclasses import dataclass, field


@dataclass
class Indicator:

    name: str

    notna: bool = False
    outputs: list[str] = field(default_factory=list)
    predecessors: list[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.outputs:
            self.outputs = [self.name]

    @property
    def is_multi_output(self) -> bool:
        return len(self.outputs) > 1

    def __hash__(self) -> int:
        return hash(self.name)

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return str(self)
