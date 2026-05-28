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

    def free_cash(self, amount: float):
        self.free_cash_amount += amount
        self.frozen_cash_amount -= amount

    def freeze_cash(self, amount: float):
        self.free_cash_amount -= amount
        self.frozen_cash_amount += amount

    @property
    def total_asset_value(self) -> float:
        return self.market_value + self.free_cash_amount + self.frozen_cash_amount


class BacktestAccount(BaseModel):
    free_cash_amount: float
    broker_commission_rate: float
    min_broker_commission_fee: float
    stamp_duty_rate: float

    holdings: Holdings = Field(default_factory=Holdings)
    frozen_cash_amount: float = 0  # noset in backtesting

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
        all_positions = [pos for _, pos in self.holdings]
        self.sell(all_positions)

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
        sell_commission_fee = self.get_broker_commission_fee(position.total_value)
        stamp_duty_fee = self.get_stamp_duty_fee(position.total_value)
        net_return = (
            gross_return - buy_commission_fee - sell_commission_fee - stamp_duty_fee
        )
        return net_return / position.cost

    @property
    def total_asset_value(self) -> float:
        return self.market_value + self.free_cash_amount

    @property
    def market_value(self) -> float:
        return self.holdings.get_total_market_value()
