import abc
import sys
import inspect
import talib
import pandas as pd
from functools import cache, cached_property
from itertools import chain
from collections import defaultdict
from typing import TypedDict, Generic, TypeVar
from tqdm import tqdm

import tradepy
from tradepy import LOG
from tradepy.core.conf import BacktestConf, StrategyConf
from tradepy.depot.misc import AdjustFactorDepot
from tradepy.trade_book import TradeBook
from tradepy.core.order import Order
from tradepy.core.position import Position
from tradepy.decorators import tag
from tradepy.utils import calc_pct_chg
from tradepy.core import Indicator, IndicatorSet
from tradepy.core.adjust_factors import AdjustFactors
from tradepy.core.budget_allocator import evenly_distribute


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


Price = float
Weight = float
BuyOption = tuple[Price, Weight]


class IndicatorsRegistry:
    def __init__(self) -> None:
        self.registry: dict[str, IndicatorSet] = defaultdict(IndicatorSet)

    def register(self, strategy_class_name: str, indicator: Indicator):
        self.registry[strategy_class_name].add(indicator)

    @cache
    def get_specs(self, strategy: "StrategyBase") -> list[Indicator]:
        ind_iter = chain.from_iterable(
            self.registry[kls.__name__] for kls in strategy.__class__.__mro__
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

    def __init__(self, conf: StrategyConf) -> None:
        self.conf = conf
        self.adjust_factors: AdjustFactors | None = None
        if conf.adjust_prices_before_compute:
            self.adjust_factors = AdjustFactorDepot.load()

        self.buy_indicators: list[str] = inspect.getfullargspec(self.should_buy).args[
            1:
        ]
        self.close_indicators: list[str] = inspect.getfullargspec(
            self.should_close
        ).args[1:]
        self.stop_loss_indicators: list[str] = inspect.getfullargspec(
            self.should_stop_loss
        ).args[3:]
        self.take_profit_indicators: list[str] = inspect.getfullargspec(
            self.should_take_profit
        ).args[3:]

        self._required_indicators: list[str] = list(
            self.buy_indicators
            + self.close_indicators
            + self.stop_loss_indicators
            + self.take_profit_indicators
        )

    def __getattr__(self, name: str):
        # Lookup custom strategy parameters from the conf object
        return getattr(self.conf, name)

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
    def should_stop_loss(
        self, tick: BarDataType, position: Position, *indicators
    ) -> float | None:
        raise NotImplementedError

    @abc.abstractmethod
    def should_take_profit(
        self, tick: BarDataType, position: Position, *indicators
    ) -> float | None:
        raise NotImplementedError

    @abc.abstractmethod
    def should_buy(self, *indicators) -> BuyOption | None:
        raise NotImplementedError

    def should_close(self, *indicators) -> bool:
        return False

    def adjust_portfolio_and_budget(
        self,  # THE ABSOLUTELY WORST INTERFACE IN THIS PROJECT!
        port_df: pd.DataFrame,
        budget: float,
        n_stocks: int,
        total_asset_value: float,
        max_position_opens: int | None = None,
        max_position_size: float | None = None,
    ) -> tuple[pd.DataFrame, float]:
        # Reject this bar if signal ratio is abnormal
        min_sig, max_sig = self.signals_percent_range
        n_options = len(port_df)
        signal_ratio = 100 * n_options / n_stocks
        if (signal_ratio < min_sig) or (signal_ratio > max_sig):
            return pd.DataFrame(), 0

        if max_position_opens is None:
            max_position_opens = self.max_position_opens

        if max_position_size is None:
            max_position_size = self.max_position_size

        # Limit number of new opens
        if n_options > max_position_opens:
            port_df = port_df.sample(n=max_position_opens, weights=port_df["weight"])

        # Limit position budget allocation
        min_position_allocation = budget // n_options
        max_position_value = max_position_size * total_asset_value

        if min_position_allocation > max_position_value:
            budget = n_options * max_position_value

        return port_df, budget

    def generate_buy_orders(self, port_df: pd.DataFrame, budget: float) -> list[Order]:
        """
        port_df: portfolio dataframe
        budget: total budget to allocate
        """
        if port_df.empty or budget <= 0:
            return []

        _port_df = port_df.reset_index()
        _port_df["temp_index"] = _port_df.index.values
        allocations = evenly_distribute(
            _port_df[["temp_index", "order_price"]].values,
            budget=budget,
            min_trade_cost=self.min_trade_amount,
            trade_lot_vol=tradepy.config.common.trade_lot_vol,
        )
        _port_df["total_lots"] = pd.Series(allocations[:, 1], index=allocations[:, 0])

        return [
            Order(  # type: ignore
                id=Order.make_id(row.code),
                timestamp=row.timestamp,
                code=row.code,
                price=row.order_price,
                vol=row.total_lots * tradepy.config.common.trade_lot_vol,
                direction="buy",
            )
            for row in _port_df.itertuples()
            if row.total_lots > 0
        ]

    def adjust_stock_history_prices(self, code: str, bars_df: pd.DataFrame):
        assert isinstance(self.adjust_factors, AdjustFactors)
        bars_df["orig_open"] = bars_df["open"].copy()
        return self.adjust_factors.backward_adjust_history_prices(code, bars_df)

    def adjust_stocks_latest_prices(self, bars_df: pd.DataFrame):
        assert isinstance(self.adjust_factors, AdjustFactors)
        return self.adjust_factors.backward_adjust_stocks_latest_prices(bars_df)

    def _adjust_then_compute(self, bars_df: pd.DataFrame, indicators: list[Indicator]):
        code: str = bars_df.index[0]  # type: ignore
        bars_df.sort_values("timestamp", inplace=True)
        bars_df = self.pre_process(bars_df)
        # Pre-processing
        if bars_df.empty:
            # Won't trade this stock
            return bars_df

        if self.adjust_factors is not None:
            # Adjust prices before computing indicators
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
        LOG.info(">>> 获取待计算因子")
        indicators = [
            ind
            for ind in self.indicators_registry.resolve_execute_order(self)
            if not set(ind.outputs).issubset(set(df.columns))
        ]

        if not indicators:
            LOG.info("- 所有因子已存在, 不用再计算")
            return df
        LOG.info(f"- 待计算: {indicators}")

        if df.index.name != "code":
            LOG.info(">>> 重建索引")
            df.reset_index(inplace=True)
            df.set_index("code", inplace=True, drop=False)

        LOG.info(">>> 计算每支个股的技术因子")
        n_codes = df.index.nunique()
        miniters = n_codes // 20  # print progress every 5%
        return pd.concat(
            self._adjust_then_compute(bars_df.copy(), indicators)
            for _, bars_df in tqdm(
                df.groupby(level="code"), file=sys.stdout, miniters=miniters
            )
        )


class BacktestStrategy(StrategyBase[BarData]):
    def should_stop_loss(self, bar: BarData, position: Position) -> float | None:
        # During opening
        open_pct_chg = calc_pct_chg(position.price, bar["open"])
        if open_pct_chg <= -self.stop_loss:
            return bar["open"]

        # During exchange
        low_pct_chg = calc_pct_chg(position.price, bar["low"])
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
    def backtest(
        cls, bars_df: pd.DataFrame, conf: BacktestConf
    ) -> tuple[pd.DataFrame, TradeBook]:
        from tradepy.backtest.backtester import Backtester

        instance = cls(conf.strategy)
        bt = Backtester(conf)
        return bt.run(bars_df.copy(), instance)


class LiveStrategy(StrategyBase[BarDataType]):
    def should_stop_loss(self, tick: BarDataType, position: Position) -> float | None:
        pct_chg = calc_pct_chg(position.price, tick["close"])
        if pct_chg <= -self.stop_loss:
            return tick[
                "close"
            ]  # TODO: may be slightly lower to improve the chance of selling

    def should_take_profit(self, tick: BarDataType, position: Position) -> float | None:
        pct_chg = calc_pct_chg(position.price, tick["close"])
        if pct_chg >= self.take_profit:
            return tick[
                "close"
            ]  # TODO: may be slightly higher to improve the chance of buying

    @abc.abstractmethod
    def compute_open_indicators(self, quote_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    @abc.abstractmethod
    def compute_close_indicators(
        self, quote_df: pd.DataFrame, ind_df: pd.DataFrame
    ) -> pd.DataFrame:
        raise NotImplementedError()

    @abc.abstractmethod
    def compute_intraday_indicators(
        self, quote_df: pd.DataFrame, ind_df: pd.DataFrame
    ) -> pd.DataFrame:
        raise NotImplementedError()
