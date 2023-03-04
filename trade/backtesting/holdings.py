from typing import Callable, Iterable
from contextlib import suppress
from trade.backtesting.position import Position


class Holdings:

    PriceLookupFun = Callable[[str], float]

    def __init__(self):
        self.positions: dict[str, Position] = dict()  # code => Position
    
    @property
    def position_codes(self) -> set[str]:
        return set(code for code, _ in self)

    def tick(self, price_lookup: PriceLookupFun):
        for _, pos in self:
            with suppress(KeyError):
                pos.tick(price_lookup(pos.code))

    def buy(self, positions: Iterable[Position]) -> float:
        total = 0

        for pos in positions:
            if pos.code in self.positions:
                raise ValueError(f'{pos.code} already in position')

            self.positions[pos.code] = pos
            total += pos.cost
        
        return total

    def sell(self, positions: Iterable[Position]) -> float:
        total = 0

        for pos in positions:
            if pos.code not in self.positions:
                raise ValueError(f"Position not found: {pos}. Current positions: {self.positions}")

            pos = self.positions.pop(pos.code)
            total += pos.total_value_at(pos.latest_price)

        return total

    def get_total_worth(self):
        return sum(
            pos.total_value_at(pos.latest_price)
            for _, pos in self
        )

    def has(self, code) -> bool:
        return code in self.positions

    def __iter__(self):
        yield from self.positions.items()

    def __getitem__(self, code: str):
        return self.positions[code]
