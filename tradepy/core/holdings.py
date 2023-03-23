from typing import Callable, Iterable
from contextlib import suppress
from tradepy.core.position import Position
from tradepy.decorators import require_mode


class Holdings:

    PriceLookupFun = Callable[[str], float]

    def __init__(self):
        self.positions: dict[str, Position] = dict()  # code => Position

    @property
    def position_codes(self) -> set[str]:
        return set(code for code, _ in self)

    def as_list(self):
        return [
            pos.as_dict()
            for pos in self.positions.values()
        ]

    @classmethod
    def from_list(cls, data) -> "Holdings":
        positions = {
            pos["code"]: Position(
                id=pos["id"],
                latest_price=pos["latest_price"],
                timestamp=pos["timestamp"],
                code=pos["code"],
                price=pos["price"],
                vol=pos["shares"],
            )
            for pos in data
        }
        instance = cls()
        instance.positions = positions
        return instance

    def update_price(self, price_lookup: PriceLookupFun):
        for _, pos in self:
            with suppress(KeyError):
                pos.update_price(price_lookup(pos.code))

    @require_mode("backtest")
    def buy(self, positions: Iterable[Position]) -> float:
        total = 0

        for pos in positions:
            if pos.code in self.positions:
                raise ValueError(f'{pos.code} already in position')

            self.positions[pos.code] = pos
            total += pos.cost

        return total

    @require_mode("backtest")
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
