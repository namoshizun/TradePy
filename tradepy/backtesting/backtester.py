import sys
import numpy as np
import pandas as pd
from contextlib import suppress
from typing import TYPE_CHECKING, Any
from tqdm import tqdm

from tradepy.backtesting.account import TradeBook
from tradepy.backtesting.context import Context
from tradepy.backtesting.dag import IndicatorsResolver

if TYPE_CHECKING:
    from tradepy.backtesting.strategy import StrategyBase, TickDataType


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
        adj_df = adj_df.reset_index(drop=True).set_index("timestamp")
        bars_df = bars_df.join(adj_df, on="timestamp")
        factor_vals = bars_df["hfq_factor"].values

        if np.isnan(factor_vals[0]):
            # Patch the furtherest factor value, whose timestamp might be outside the ticks range
            min_date = bars_df.iloc[0]["timestamp"]
            for w in adj_df.rolling(2):
                if len(w) == 2:
                    _, until = w.iloc[0].name, w.iloc[1].name
                    if until > min_date:
                        factor_vals[0] = w.iloc[0]["hfq_factor"]
                        break

        # Assign each day a adjust factor
        bars_df["hfq_factor"] = factor_vals
        factor_vals = bars_df["hfq_factor"].reset_index(drop=True).interpolate(method="pad").values

        # Adjust prices accordingly
        bars_df[["open", "close", "high", "low"]] *= factor_vals.reshape(-1, 1)
        bars_df["chg"] = bars_df["close"] - bars_df["close"].shift(1)
        bars_df["pct_chg"] = 100 * (bars_df['chg'] / bars_df['close'].shift(1))

        bars_df.drop("hfq_factor", axis=1, inplace=True)
        bars_df.dropna(inplace=True)
        return bars_df.round(2)

    def get_indicators_df(self, df: pd.DataFrame, strategy: "StrategyBase") -> pd.DataFrame:
        def adjust_then_compute(bars_df, code, indicators, **kws):
            bars_df.sort_values("timestamp", inplace=True)
            bars_df = strategy.pre_process(bars_df)
            # Pre-process before computing indicators
            if bars_df.empty:
                # Won't trade this stock
                return bars_df

            if self.adjust_factors_df is not None:
                # Adjust prices
                with suppress(KeyError):
                    adjust_factors_df = self.adjust_factors_df.loc[code].sort_values("timestamp")
                    bars_df = self._adjust_prices(bars_df, adjust_factors_df)

            # Compute indicators
            for ind, predecessors in indicators.items():
                if ind not in bars_df:  # double checked because preproc might add the needed indicators
                    method = getattr(strategy, ind)
                    bars_df[ind] = method(*[bars_df[col] for col in predecessors])

            # Post-process and done
            return strategy.post_process(bars_df)

        print('>>> 获取待计算因子')
        ind_resolv = {
            indicator: predecessors
            for indicator, predecessors in IndicatorsResolver(strategy).get_compute_order().items()
            if indicator not in df
        }

        if not ind_resolv:
            print('- 所有因子已存在, 不用再计算')
            return df
        print(f'- 待计算: {list(ind for ind in ind_resolv.keys())}')

        print('>>> 重建索引')
        if df.index.name != "code":
            df.reset_index(inplace=True)
            df.set_index("code", inplace=True)

        print('>>> 计算每支个股的技术因子')
        bars_df = pd.concat(
            adjust_then_compute(bars_df, code, ind_resolv)
            for code, bars_df in tqdm(df.groupby(level="code"), file=sys.stdout)
        )

        return bars_df

    def get_buy_signals(self, df: pd.DataFrame, strategy: "StrategyBase") -> list[Any]:
        curr_positions = self.account.holdings.position_codes

        return [
            row.Index
            for row in df.itertuples()
            # won't increase existing positions
            if (row.Index[1] not in curr_positions) and strategy.should_buy(*[
                getattr(row, ind)
                for ind in strategy.buy_indicators
            ])
        ]

    def get_close_signals(self, df: pd.DataFrame, strategy: "StrategyBase") -> list[Any]:
        if not strategy.close_indicators:
            return []

        curr_positions = self.account.holdings.position_codes
        if not curr_positions:
            return []

        return [
            row.Index
            for row in df.itertuples()
            # only evaluate existing positions
            if (row.Index[1] in curr_positions) and strategy.should_close(*[
                getattr(row, ind)
                for ind in strategy.close_indicators
            ])
        ]

    def trade(self, df: pd.DataFrame, strategy: "StrategyBase") -> TradeBook:
        if list(getattr(df.index, "names", [])) != ["timestamp", "code"]:
            print('>>> 重建索引 [timestamp, code]')
            # Index by timestamp: trade in the day order
            #          code: look up the current price for positions in holding
            df.reset_index(inplace=True)
            df.set_index(["timestamp", "code"], inplace=True)
            df.sort_index(inplace=True)

        print('>>> 交易中 ...')
        trade_book = TradeBook()

        # Per day
        for timestamp, sub_df in tqdm(df.groupby(level="timestamp"), file=sys.stdout):
            assert isinstance(timestamp, str)

            # Opening
            price_lookup = lambda code: sub_df.loc[(timestamp, code), "close"]
            self.account.tick(price_lookup)

            # Signals
            buy_indices = self.get_buy_signals(sub_df, strategy)
            close_indices = self.get_close_signals(sub_df, strategy)

            # Sell
            sell_positions = []
            for code, pos in self.account.holdings:
                index = (timestamp, code)
                if index not in sub_df.index:
                    # Not a tradable day, so nothing to do
                    continue

                bar: TickDataType = sub_df.loc[index].to_dict()  # type: ignore

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
            if buy_indices:
                pool_df, budget = strategy.get_pool_and_budget(sub_df, buy_indices, self.account.cash_amount)
                buy_positions = strategy.generate_positions(pool_df, budget)

                self.account.buy(buy_positions)
                for pos in buy_positions:
                    trade_book.buy(timestamp, pos)

            # Log this action day
            if buy_indices or close_indices:
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
