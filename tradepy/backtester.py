import sys
import random
import numba as nb
import numpy as np
import pandas as pd
from typing import TYPE_CHECKING, Any
from tqdm import tqdm

from tradepy import LOG
from tradepy.core.position import Position
from tradepy.core.trade_book import TradeBook
from tradepy.core.context import Context
from tradepy.core.indicator import Indicator

if TYPE_CHECKING:
    from tradepy.core.strategy import StrategyBase, BarDataType


@nb.njit
def assign_factor_value_to_day(fac_ts, fac_vals, timestamps):
    i = 0

    factors = []
    for ts in timestamps:
        while True:
            if fac_ts[i + 1] > ts:
                break
            i += 1
        factors.append(fac_vals[i])
    return factors


class Backtester:

    def __init__(self, ctx: Context) -> None:
        self.account = ctx.account

        if ctx.hfq_adjust_factors is not None:
            _adf = ctx.hfq_adjust_factors.copy()
            _adf.reset_index(inplace=True)
            _adf.set_index("code", inplace=True)
            _adf.sort_values(["code", "timestamp"], inplace=True)
            self.adjust_factors_df = _adf
        else:
            self.adjust_factors_df = None

    def _adjust_prices(self, bars_df: pd.DataFrame, adj_df: pd.DataFrame) -> pd.DataFrame:
        # Find each day's adjust factor
        factor_vals = assign_factor_value_to_day(
            nb.typed.List(adj_df["timestamp"].tolist()),
            nb.typed.List(adj_df["hfq_factor"].tolist()),
            nb.typed.List(bars_df["timestamp"].tolist())
        )

        # Adjust prices accordingly
        bars_df[["open", "close", "high", "low"]] *= np.array(factor_vals).reshape(-1, 1)
        bars_df["chg"] = (bars_df["close"] - bars_df["close"].shift(1)).fillna(0)
        bars_df["pct_chg"] = (100 * (bars_df['chg'] / bars_df['close'].shift(1))).fillna(0)
        return bars_df.round(2)

    def _adjust_then_compute(self, bars_df, indicators: list[Indicator], strategy: "StrategyBase"):
        code = bars_df.index[0]
        bars_df.sort_values("timestamp", inplace=True)
        bars_df = strategy.pre_process(bars_df)
        # Pre-process before computing indicators
        if bars_df.empty:
            # Won't trade this stock
            return bars_df

        if self.adjust_factors_df is not None:
            # Adjust prices
            try:
                adjust_factors_df = self.adjust_factors_df.loc[code].sort_values("timestamp")
                bars_df = self._adjust_prices(bars_df, adjust_factors_df)
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

            method = getattr(strategy, ind.name)
            result = method(*[bars_df[col] for col in ind.predecessors])

            if ind.is_multi_output:
                for idx, out_col in enumerate(ind.outputs):
                    bars_df[out_col] = result[idx]
            else:
                bars_df[ind.outputs[0]] = result

        # Post-process and done
        return strategy.post_process(bars_df)

    def get_indicators_df(self, df: pd.DataFrame, strategy: "StrategyBase") -> pd.DataFrame:
        LOG.info('>>> 获取待计算因子')
        indicators = [
            ind
            for ind in strategy.indicators_registry.resolve_execute_order(strategy)
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
        bars_df = pd.concat(
            self._adjust_then_compute(bars_df.copy(), indicators, strategy)
            for _, bars_df in tqdm(df.groupby(level="code"), file=sys.stdout)
        )

        return bars_df

    def get_buy_options(self,
                        df: pd.DataFrame,
                        strategy: "StrategyBase",
                        sell_positions: list[Position]) -> list[tuple[Any, float]]:
        sold_or_holding_codes = self.account.holdings.position_codes | \
            set(pos.code for pos in sell_positions)
        jitter_price = lambda p: p * random.uniform(1 - 1e-4 * 5, 1 + 1e-4 * 5)  # 0.05% slip

        return [
            (index, jitter_price(price))
            for index, *indicators in df[strategy.buy_indicators].itertuples(name=None)  # twice faster than the default .itertuples options
            if (index[1] not in sold_or_holding_codes) and (price := strategy.should_buy(*indicators))
        ]

    def get_close_signals(self, df: pd.DataFrame, strategy: "StrategyBase") -> list[Any]:
        if not strategy.close_indicators:
            return []

        curr_positions = self.account.holdings.position_codes
        if not curr_positions:
            return []

        return [
            index
            for index, *indicators in df[strategy.close_indicators].itertuples(name=None)
            if (index[1] in curr_positions) and strategy.should_close(*indicators)
        ]

    def trade(self, df: pd.DataFrame, strategy: "StrategyBase") -> TradeBook:
        if list(getattr(df.index, "names", [])) != ["timestamp", "code"]:
            LOG.info('>>> 重建索引 [timestamp, code]')
            # Index by timestamp: trade in the day order
            #          code: look up the current price for positions in holding
            df.reset_index(inplace=True)
            df.set_index(["timestamp", "code"], inplace=True)
            df.sort_index(inplace=True)

        LOG.info('>>> 交易中 ...')
        trade_book = TradeBook()

        # Per day
        for timestamp, sub_df in tqdm(df.groupby(level="timestamp"), file=sys.stdout):
            assert isinstance(timestamp, str)

            # Opening
            price_lookup = lambda code: sub_df.loc[(timestamp, code), "close"]
            self.account.update_holdings(price_lookup)

            # Sell
            close_indices = self.get_close_signals(sub_df, strategy)
            sell_positions = []
            for code, pos in self.account.holdings:
                index = (timestamp, code)
                if index not in sub_df.index:
                    # Not a tradable day, so nothing to do
                    continue

                bar: BarDataType = sub_df.loc[index].to_dict()  # type: ignore

                # [1] Take profit
                if take_profit_price := strategy.should_take_profit(bar, pos):
                    pos.close(take_profit_price)
                    trade_book.take_profit(timestamp, pos)
                    sell_positions.append(pos)

                # [2] Stop loss
                elif stop_loss_price := strategy.should_stop_loss(bar, pos):
                    pos.close(stop_loss_price)
                    trade_book.stop_loss(timestamp, pos)
                    sell_positions.append(pos)

                # [3] Close
                elif index in close_indices:
                    pos.close(bar["close"])
                    trade_book.close_position(timestamp, pos)
                    sell_positions.append(pos)

            if sell_positions:
                self.account.sell(sell_positions)

            # Buy
            buy_options = self.get_buy_options(sub_df, strategy, sell_positions)  # list[DF_Index, BuyPrice]
            if buy_options:
                port_df, budget = strategy.get_portfolio_and_budget(sub_df, buy_options, self.account.cash_amount)
                buy_positions = strategy.allocate_positions(port_df, budget)

                self.account.buy(buy_positions)
                for pos in buy_positions:
                    trade_book.buy(timestamp, pos)

            # Log this action day
            if buy_options or close_indices:
                trade_book.log_capitals(
                    timestamp,
                    self.account.cash_amount,
                    self.account.holdings.get_total_worth()
                )

        # That was quite a long story :D
        return trade_book

    def run(self, bars_df: pd.DataFrame, strategy: "StrategyBase") -> tuple[pd.DataFrame, TradeBook]:
        ind_df = self.get_indicators_df(bars_df, strategy)
        trade_book = self.trade(ind_df, strategy)
        return ind_df, trade_book
