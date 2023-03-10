import abc
import inspect
import talib
import random
import pandas as pd
from collections import defaultdict
from typing import Any, TypedDict, Generic, TypeVar

from tradepy.backtesting.context import Context
from tradepy.backtesting.backtester import Backtester
from tradepy.backtesting.account import TradeBook
from tradepy.backtesting.position import Position
from tradepy.decorators import requirement
from tradepy.types import IndSeries
from tradepy.utils import calc_pct_chg


class TickData(TypedDict):
    code: str
    timestamp: str
    open: float
    close: float
    high: float
    low: float
    vol: int

    chg: float | None
    pct_chg: float | None


class IndicatorRequirement(TypedDict):
    notna: bool


TickDataType = TypeVar("TickDataType", bound=TickData)


class StrategyBase(Generic[TickDataType]):

    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self.buy_indicators: list[str] = inspect.getfullargspec(self.should_buy).args[1:]
        self.close_indicators: list[str] = inspect.getfullargspec(self.should_close).args[1:]
        self.indicator_requirements: dict[str, IndicatorRequirement] = defaultdict(dict)  # type: ignore

    def __getattr__(self, name: str):
        return getattr(self.ctx, name)

    def pre_process(self, bars_df: pd.DataFrame):
        return bars_df

    def post_process(self, bars_df: pd.DataFrame):
        notna_indicators = [
            ind
            for ind, req in self.indicator_requirements.items()
            if req.get("notna", False)
        ]

        if notna_indicators:
            bars_df.dropna(subset=notna_indicators, inplace=True)

        return bars_df

    @abc.abstractmethod
    def should_buy(self, *indicators) -> bool | float:
        raise NotImplementedError

    def should_close(self, *indicators) -> bool:
        return False

    def should_stop_loss(self, tick: TickDataType, position: Position) -> float | None:
        # During opening
        open_pct_chg = calc_pct_chg(position.price, tick["open"])
        if open_pct_chg <= -self.stop_loss:
            return tick["open"]

        # During exchange
        low_pct_chg = calc_pct_chg(position.price, tick["low"])
        if low_pct_chg <= -self.stop_loss:
            return position.price_at_pct_change(-self.stop_loss)

    def should_take_profit(self, tick: TickDataType, position: Position) -> float | None:
        # During opening
        open_pct_chg = calc_pct_chg(position.price, tick["open"])
        if open_pct_chg >= self.take_profit:
            return tick["open"]

        # During exchange
        high_pct_chg = calc_pct_chg(position.price, tick["high"])
        if high_pct_chg >= self.take_profit:
            return position.price_at_pct_change(self.take_profit)

    def get_portfolio_and_budget(self,
                                 bar_df: pd.DataFrame,
                                 buy_options: list[Any],
                                 budget: float) -> tuple[pd.DataFrame, float]:
        # Reject this bar if signal ratio is abnormal
        min_sig, max_sig = self.signals_percent_range
        signal_ratio = 100 * len(buy_options) / len(bar_df)
        if (signal_ratio < min_sig) or (signal_ratio > max_sig):
            return pd.DataFrame(), 0

        # Limit number of new opens
        if len(buy_options) > self.max_position_opens:
            buy_options = random.sample(buy_options, self.max_position_opens)

        # Limit position budget allocation
        min_position_allocation = budget // len(buy_options)
        max_position_value = self.max_position_size * self.account.get_total_asset_value()

        if min_position_allocation > max_position_value:
            budget = len(buy_options) * max_position_value

        # Generate the order frame and annotate the actual price to price
        indices, prices = zip(*buy_options)
        port_df = bar_df.loc[list(indices), ["company"]].copy()
        port_df["order_price"] = prices
        return port_df, budget

    def allocate_positions(self, port_df: pd.DataFrame, budget: float) -> list[Position]:
        if port_df.empty or budget <= 0:
            return []

        # Calculate the minimum number of shares to buy per stock
        num_stocks = len(port_df)

        port_df["trade_unit_price"] = port_df["order_price"] * self.trading_unit
        port_df["trade_units"] = budget / num_stocks // port_df["trade_unit_price"]

        # Gather the remaining budget
        remaining_budget = budget - (port_df["trade_unit_price"] * port_df["trade_units"]).sum()

        # Distribute that budget again, starting from the big guys
        port_df.sort_values("trade_unit_price", inplace=True, ascending=False)
        for row in port_df.itertuples():
            if residual_shares := remaining_budget // row.trade_unit_price:
                port_df.at[row.Index, "trade_units"] += residual_shares
                remaining_budget -= residual_shares * row.trade_unit_price

        return [
            Position(
                timestamp=row.Index[0],
                code=row.Index[1],
                company=row.company,
                price=row.order_price,
                shares=row.trade_units * self.trading_unit,
            )
            for row in port_df.itertuples()
            if row.trade_units > 0
        ]

    @classmethod
    def backtest(cls, ticks_data: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, TradeBook]:
        instance = cls(ctx)
        bt = Backtester(ctx)
        return bt.run(ticks_data.copy(), instance)

    @classmethod
    def get_indicators_df(cls, ticks_data: pd.DataFrame, ctx: Context) -> pd.DataFrame:
        bt = Backtester(ctx)
        strategy = cls(ctx)
        return bt.get_indicators_df(ticks_data.copy(), strategy)


class Strategy(StrategyBase[TickData]):

    def chg(self, close: IndSeries):
        return (close - close.shift(1)).fillna(0).round(2)

    def pct_chg(self, chg: IndSeries, close: IndSeries):
        return (100 * chg / close.shift(1)).fillna(0).round(2)

    @requirement(notna=True)
    def ma5(self, close):
        return talib.SMA(close, 5).round(2)

    @requirement(notna=True)
    def ma20(self, close):
        return talib.SMA(close, 20).round(2)

    # @requirement(notna=True)
    def ma60(self, close):
        return talib.SMA(close, 60).round(2)

    # @requirement(notna=True)
    def ma250(self, close):
        return talib.SMA(close, 250).round(2)
