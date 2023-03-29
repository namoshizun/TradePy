from pydantic import BaseModel, Field
from typing import Iterable
from tradepy.core.holdings import Holdings
from tradepy.core.position import Position
from tradepy.decorators import require_mode
from tradepy.utils import round_val


class Account(BaseModel):

    free_cash_amount: float
    frozen_cash_amount: float
    market_value: float

    @property
    def total_asset_value(self) -> float:
        return self.market_value + self.free_cash_amount + self.frozen_cash_amount


class BacktestAccount(BaseModel):

    free_cash_amount: float
    broker_commission_rate: float
    stamp_duty_rate: float

    holdings: Holdings = Field(default_factory=Holdings)
    frozen_cash_amount: float = 0  # noop
    market_value: float = 0  # noop

    class Config:
        arbitrary_types_allowed = True

    @require_mode("backtest")
    def update_holdings(self, price_lookup: Holdings.PriceLookupFun):
        if any(self.holdings):
            self.holdings.update_price(price_lookup)

    @require_mode("backtest")
    def buy(self, positions: Iterable[Position]):
        if cost_total := self.holdings.buy(positions):
            self.free_cash_amount -= self.add_buy_commissions(cost_total)

    @require_mode("backtest")
    def sell(self, positions: Iterable[Position]):
        if close_total := self.holdings.sell(positions):
            self.free_cash_amount += self.take_sell_commissions(close_total)

    @require_mode("backtest")
    def clear(self):
        all_positions = [
            pos
            for _, pos in self.holdings
        ]
        self.sell(all_positions)

    @round_val
    def add_buy_commissions(self, amount: float) -> float:
        return amount * (1 + self.broker_commission_rate * 1e-2)

    @round_val
    def take_sell_commissions(self, amount: float) -> float:
        rate = self.broker_commission_rate + self.stamp_duty_rate
        return amount * (1 - rate * 1e-2)

    @property
    def total_asset_value(self) -> float:
        return self.holdings.get_total_worth() + self.free_cash_amount
