from dataclasses import asdict, dataclass, field
from typing import Iterable

from tradepy.core.holdings import Holdings
from tradepy.core.position import Position
from tradepy.utils import round_val


@dataclass
class Account:

    buy_commission_rate: float = 0
    sell_commission_rate: float = 0

    cash_amount: float = 0
    holdings: Holdings = field(default_factory=Holdings)

    def update(self, price_lookup: Holdings.PriceLookupFun):
        if any(self.holdings):
            self.holdings.tick(price_lookup)

    def buy(self, positions: Iterable[Position]):
        if cost_total := self.holdings.buy(positions):
            self.cash_amount -= self.add_buy_commissions(cost_total)

    def sell(self, positions: Iterable[Position]):
        if close_total := self.holdings.sell(positions):
            self.cash_amount += self.take_sell_commissions(close_total)

    def clear(self):
        all_positions = [
            pos
            for _, pos in self.holdings
        ]
        self.sell(all_positions)

    def clone(self):
        return self.__class__(**{
            k: v
            for k, v in asdict(self).items()
        })

    @round_val
    def add_buy_commissions(self, amount: float) -> float:
        return amount * (1 + self.buy_commission_rate * 1e-2)

    @round_val
    def take_sell_commissions(self, amount: float) -> float:
        return amount * (1 - self.sell_commission_rate * 1e-2)

    def get_total_asset_value(self) -> float:
        return self.holdings.get_total_worth() + self.cash_amount

    def get_positions_value(self) -> float:
        return self.holdings.get_total_worth()
