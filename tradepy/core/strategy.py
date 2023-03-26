import abc
import sys
import inspect
import talib
import random
import pandas as pd
from functools import cache, cached_property
from itertools import chain
from collections import defaultdict
from typing import Any, TypedDict, Generic, TypeVar
from tqdm import tqdm

from tradepy import LOG
from tradepy.core.context import Context
from tradepy.backtest.backtester import Backtester
from tradepy.core.order import Order
from tradepy.core.trade_book import TradeBook
from tradepy.core.position import Position
from tradepy.decorators import tag
from tradepy.utils import calc_pct_chg
from tradepy.core import Indicator, IndicatorSet, adjust_factors


class BarData(TypedDict):
    code: str
    timestamp: str
    open: float
    close: float
    high: float
    low: float
    vol: int

    chg: float | None
    pct_chg: float | None


BarDataType = TypeVar("BarDataType", bound=BarData)


class IndicatorsRegistry:

    def __init__(self) -> None:
        self.registry: dict[str, IndicatorSet] = defaultdict(IndicatorSet)

    def register(self, strategy_class_name: str, indicator: Indicator):
        self.registry[strategy_class_name].add(indicator)

    @cache
    def get_specs(self, strategy: "StrategyBase") -> list[Indicator]:
        ind_iter = chain.from_iterable(
            self.registry[kls.__name__]
            for kls in strategy.__class__.__mro__
        )
        return list(ind_iter)

    @cache
    def resolve_execute_order(self, strategy: "StrategyBase") -> list[Indicator]:
        indicator_set = IndicatorSet(*strategy.all_indicators)
        return indicator_set.sort_by_execute_order(strategy._required_indicators)

    def __str__(self) -> str:
        return str(self.registry)

    def __repr__(self) -> str:
        return str(self)


class StrategyBase(Generic[BarDataType]):

    indicators_registry: IndicatorsRegistry = IndicatorsRegistry()

    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self.buy_indicators: list[str] = inspect.getfullargspec(self.should_buy).args[1:]
        self.close_indicators: list[str] = inspect.getfullargspec(self.should_close).args[1:]

        self._required_indicators: list[str] = list(self.buy_indicators + self.close_indicators)

    def __getattr__(self, name: str):
        return getattr(self.ctx, name)

    def pre_process(self, bars_df: pd.DataFrame):
        return bars_df

    def post_process(self, bars_df: pd.DataFrame):
        notna_indicators: list[str] = [
            ind.name
            for ind in self.all_indicators
            if ind.name in self._required_indicators and ind.notna
        ]

        if notna_indicators:
            bars_df.dropna(subset=notna_indicators, inplace=True)

        return bars_df

    @cached_property
    def all_indicators(self) -> list[Indicator]:
        return self.indicators_registry.get_specs(self)

    @abc.abstractmethod
    def should_stop_loss(self, tick: BarDataType, position: Position) -> float | None:
        raise NotImplementedError

    @abc.abstractmethod
    def should_take_profit(self, tick: BarDataType, position: Position) -> float | None:
        raise NotImplementedError

    @abc.abstractmethod
    def should_buy(self, *indicators) -> bool | float:
        raise NotImplementedError

    def should_close(self, *indicators) -> bool:
        return False

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

        # FIXME: This is a hack to get the dataframe look right...
        if "timestamp" in bar_df.index.names:
            port_df = bar_df.loc[list(indices), ["company"]].copy()
        else:
            port_df = bar_df.loc[list(indices), ["company", "timestamp"]].copy()

        port_df["order_price"] = prices
        return port_df, budget

    def generate_buy_orders(self, port_df: pd.DataFrame, budget: float) -> list[Order]:
        """
        port_df: portfolio dataframe
        budget: total budget to allocate
        """
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
            Order(
                id=Order.make_id(row.code),
                timestamp=row.timestamp,
                code=row.code,
                price=row.order_price,
                vol=row.trade_units * self.trading_unit,
                direction="buy",
                status="pending"
            )
            for row in port_df.reset_index().itertuples()
            if row.trade_units > 0
        ]

    def adjust_stock_history_prices(self, code: str, bars_df: pd.DataFrame):
        assert isinstance(self.hfq_adjust_factors, pd.DataFrame)
        adj = adjust_factors.AdjustFactors(self.hfq_adjust_factors.loc[code])
        return adj.backward_adjust_history_prices(bars_df)

    def adjust_stocks_latest_prices(self, bars_df: pd.DataFrame):
        assert isinstance(self.hfq_adjust_factors, pd.DataFrame)
        adj = adjust_factors.AdjustFactors(self.hfq_adjust_factors)
        return adj.backward_adjust_stocks_latest_prices(bars_df)

    def _adjust_then_compute(self, bars_df: pd.DataFrame, indicators: list[Indicator]):
        code: str = bars_df.index[0]  # type: ignore
        bars_df.sort_values("timestamp", inplace=True)
        bars_df = self.pre_process(bars_df)
        # Pre-process before computing indicators
        if bars_df.empty:
            # Won't trade this stock
            return bars_df

        if self.hfq_adjust_factors is not None:
            adj_df = self.hfq_adjust_factors.copy()
            assert isinstance(adj_df, pd.DataFrame)
            # Adjust prices
            try:
                bars_df = self.adjust_stock_history_prices(code, bars_df)
                if bars_df["pct_chg"].abs().max() > 21:
                    # Either adjust factor is missing or incorrect...
                    return pd.DataFrame()
            except KeyError:
                return pd.DataFrame()

        # Compute indicators
        for ind in indicators:
            if ind.name in bars_df:
                # double check because a multi-output indicator might yield other indicators
                continue

            method = getattr(self, ind.name)
            result = method(*[bars_df[col] for col in ind.predecessors])

            if ind.is_multi_output:
                for idx, out_col in enumerate(ind.outputs):
                    bars_df[out_col] = result[idx]
            else:
                bars_df[ind.outputs[0]] = result

        # Post-process and done
        return self.post_process(bars_df)

    def compute_all_indicators_df(self, df: pd.DataFrame) -> pd.DataFrame:
        LOG.info('>>> 获取待计算因子')
        indicators = [
            ind
            for ind in self.indicators_registry.resolve_execute_order(self)
            if not set(ind.outputs).issubset(set(df.columns))
        ]

        if not indicators:
            LOG.info('- 所有因子已存在, 不用再计算')
            return df
        LOG.info(f'- 待计算: {indicators}')

        LOG.info('>>> 重建索引')
        if df.index.name != "code":
            df.reset_index(inplace=True)
            df.set_index("code", inplace=True)

        LOG.info('>>> 计算每支个股的技术因子')
        n_codes = df.index.nunique()
        miniters = n_codes // 20  # print progress every 5%
        return pd.concat(
            self._adjust_then_compute(bars_df.copy(), indicators)
            for _, bars_df in tqdm(df.groupby(level="code"), file=sys.stdout, miniters=miniters)
        )


class BacktestStrategy(StrategyBase[BarData]):

    def should_stop_loss(self, tick: BarData, position: Position) -> float | None:
        # During opening
        open_pct_chg = calc_pct_chg(position.price, tick["open"])
        if open_pct_chg <= -self.stop_loss:
            return tick["open"]

        # During exchange
        low_pct_chg = calc_pct_chg(position.price, tick["low"])
        if low_pct_chg <= -self.stop_loss:
            return position.price_at_pct_change(-self.stop_loss)

    def should_take_profit(self, tick: BarData, position: Position) -> float | None:
        # During opening
        open_pct_chg = calc_pct_chg(position.price, tick["open"])
        if open_pct_chg >= self.take_profit:
            return tick["open"]

        # During exchange
        high_pct_chg = calc_pct_chg(position.price, tick["high"])
        if high_pct_chg >= self.take_profit:
            return position.price_at_pct_change(self.take_profit)

    @tag(notna=True)
    def ma5(self, close):
        return talib.SMA(close, 5).round(2)

    @tag(notna=True)
    def ma20(self, close):
        return talib.SMA(close, 20).round(2)

    @tag(notna=True)
    def ma60(self, close):
        return talib.SMA(close, 60).round(2)

    @tag(notna=True)
    def ma250(self, close):
        return talib.SMA(close, 250).round(2)

    @classmethod
    def backtest(cls, bars_df: pd.DataFrame, ctx: Context) -> tuple[pd.DataFrame, TradeBook]:
        instance = cls(ctx)
        bt = Backtester(ctx)
        return bt.run(bars_df.copy(), instance)


class LiveStrategy(StrategyBase[BarDataType]):

    def should_stop_loss(self, tick: BarDataType, position: Position) -> float | None:
        pct_chg = calc_pct_chg(position.price, tick["close"])
        if pct_chg <= -self.stop_loss:
            return tick["close"]  # TODO: may be slightly lower to improve the chance of selling

    def should_take_profit(self, tick: BarDataType, position: Position) -> float | None:
        pct_chg = calc_pct_chg(position.price, tick["close"])
        if pct_chg >= self.take_profit:
            return tick["close"]  # TODO: may be slightly higher to improve the chance of buying

    @abc.abstractmethod
    def compute_open_indicators(self, quote_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    @abc.abstractmethod
    def compute_close_indicators(self, quote_df: pd.DataFrame, ind_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    @abc.abstractmethod
    def compute_intraday_indicators(self, quote_df: pd.DataFrame, ind_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()
