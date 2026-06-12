import abc
import dataclasses
from typing import Callable

from tradepy.core.holdings import Holdings
from tradepy.core.position import Position
from tradepy.decors import round_val


@dataclasses.dataclass
class Account(abc.ABC):
    free_cash_amount: float
    frozen_cash_amount: float

    def unfreeze_cash(self, amount: float):
        self.free_cash_amount += amount
        self.frozen_cash_amount -= amount

    def freeze_cash(self, amount: float):
        self.free_cash_amount -= amount
        self.frozen_cash_amount += amount

    @abc.abstractmethod
    def get_market_value(self) -> float:
        raise NotImplementedError

    def get_total_capital(self) -> float:
        return (
            self.get_market_value()
            + self.free_cash_amount
            + self.frozen_cash_amount
        )


PriceLookupFun = Callable[[str], float]


@dataclasses.dataclass
class BacktestAccount(Account):
    broker_commission_rate: float
    min_broker_commission_fee: float
    stamp_duty_rate: float

    holdings: Holdings = dataclasses.field(default_factory=Holdings)

    def get_market_value(self) -> float:
        return sum(
            pos.total_value_at(pos.latest_price) for _, pos in self.holdings
        )

    def pre_open(self):
        self.unfreeze_cash(self.frozen_cash_amount)
        for _, pos in self.holdings:
            pos.avail_vol = pos.vol
            pos.yesterday_vol = pos.vol

    def buy(self, *positions: Position):
        if cost_total := self.holdings.buy(positions):
            self.free_cash_amount -= self.add_buy_commissions(cost_total)

    def sell(self, *positions: Position):
        if close_total := self.holdings.sell(positions):
            self.frozen_cash_amount += self.take_sell_commissions(close_total)

    def clear(self):
        all_positions = [pos for _, pos in self.holdings]
        self.sell(*all_positions)

    @round_val
    def get_broker_commission_fee(self, amount: float) -> float:
        fee = amount * (self.broker_commission_rate * 1e-2)
        return max(fee, self.min_broker_commission_fee)

    @round_val
    def get_stamp_duty_fee(self, amount: float) -> float:
        return amount * (self.stamp_duty_rate * 1e-2)

    @round_val
    def add_buy_commissions(self, amount: float) -> float:
        fee = self.get_broker_commission_fee(amount)
        return amount + fee

    @round_val
    def take_sell_commissions(self, amount: float) -> float:
        broker_commission_fee = self.get_broker_commission_fee(amount)
        stamp_duty_fee = self.get_stamp_duty_fee(amount)
        return amount - broker_commission_fee - stamp_duty_fee

    def get_position_net_pct_chg(self, position: Position) -> float:
        gross_return = position.profit_or_loss_at(position.latest_price)
        buy_commission_fee = self.get_broker_commission_fee(position.cost)
        sell_commission_fee = self.get_broker_commission_fee(
            position.total_value
        )
        stamp_duty_fee = self.get_stamp_duty_fee(position.total_value)
        net_return = (
            gross_return
            - buy_commission_fee
            - sell_commission_fee
            - stamp_duty_fee
        )
        return net_return / position.cost
