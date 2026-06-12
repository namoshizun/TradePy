from collections.abc import Iterable
from typing import Callable

from tradepy.core.position import Position


class Holdings:
    PriceLookupFun = Callable[[str], float]

    def __init__(self):
        self.positions: dict[str, Position] = dict()  # code => Position

    @property
    def position_codes(self) -> set[str]:
        return {code for code, _ in self}

    @property
    def avail_codes(self) -> set[str]:
        return {code for code, pos in self if pos.avail_vol > 0}

    def buy(self, positions: Iterable[Position]) -> float:
        total = 0

        for pos in positions:
            if pos.code in self.positions:
                raise ValueError(f"{pos.code} already in position")

            self.positions[pos.code] = pos
            total += pos.cost

        return total

    def sell(self, positions: Iterable[Position]) -> float:
        total = 0

        for pos in positions:
            if pos.code not in self.positions:
                raise ValueError(
                    f"Position not found: {pos}. Current positions: {self.positions}"
                )

            pos = self.positions.pop(pos.code)
            total += pos.latest_price * pos.yesterday_vol

        return total

    def has(self, code: str) -> bool:
        return code in self.positions

    def __iter__(self):
        yield from self.positions.items()

    def __getitem__(self, code: str):
        return self.positions[code]

    def __str__(self):
        return "; ".join(
            f"[{code}]: {pos.pct_chg}%" for code, pos in self.positions.items()
        )

    def __len__(self):
        return len(self.positions)

    def __repr__(self):
        return str(self)
