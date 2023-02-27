import inspect
import operator
import numpy as np
import pandas as pd
from typing import TYPE_CHECKING
from tqdm import tqdm

from trade.backtesting.account import Account, TradeBook
from trade.backtesting.context import Context

if TYPE_CHECKING:
    from trade.backtesting.strategy import StrategyBase, TickDataType


class Backtester:

    def __init__(self, ctx: Context) -> None:
        self.account = Account(
            cash_amount=ctx.cash_amount,
            buy_commission_rate=ctx.buy_commission_rate,
            sell_commission_rate=ctx.sell_commission_rate,
        )

        if ctx.hfq_adjust_factors:
            _adf = ctx.hfq_adjust_factors.copy()
            _adf.reset_index(inplace=True)
            _adf.set_index("code", inplace=True)
            _adf.sort_values(["code", "date"], inplace=True)
            self.adjust_factors_df = _adf
        else:
            self.adjust_factors_df = None

    def __filter_in_holdings(self, selector: pd.Series) -> pd.Series:
        for index, _ in selector.iteritems():
            _, code = index
            if self.account.holdings.has(code):
                selector.at[index] = False
        return selector

    def __get_buy_indicators(self, strategy: "StrategyBase") -> list[str]:
        spec = inspect.getfullargspec(strategy.should_buy)
        return spec.args[1:]

    def get_indicators_df(self, df: pd.DataFrame, strategy: "StrategyBase") -> pd.DataFrame:
        def adjust_then_compute(code, ticks_df):
            ticks_df.sort_values("timestamp", inplace=True)

            if self.adjust_factors_df:
                adjust_factors_df = self.adjust_factors_df.loc[code]
                ticks_df = self.adjust_prices(ticks_df, adjust_factors_df)

            return strategy.compute_indicators(ticks_df)

        print(f'>>> Skip if indicators already exist')
        indicator_names = self.__get_buy_indicators(strategy)
        if set(indicator_names).issubset(set(df.columns)):
            print(f'> found {indicator_names}')
            return df

        print('>>> Index by code ...')
        if df.index.name != "code":
            df.reset_index(inplace=True)
            df.set_index("code", inplace=True)
        
        print('>>> Computing indicators ...')
        return pd.concat(
            adjust_then_compute(code, ticks_df)
            for code, ticks_df in tqdm(df.groupby(level="code"))
        )

    def adjust_prices(self, ticks_df: pd.DataFrame, adj_df: pd.DataFrame) -> pd.DataFrame:
        # Rolling to get the first bonus window
        tick_min_ts = ticks_df.iloc[0]["timestamp"]
        factor_window_iter = iter(adj_df.rolling(window=2, step=1))
        curr_window = None
        for w in factor_window_iter:
            if len(w) == 2:
                if w.iloc[1]["timestamp"] >= tick_min_ts:
                    curr_window = w
                    break
        assert curr_window is not None

        # Assign each day an adjust factor
        adjust_factors = []
        for _, tick in ticks_df.iterrows():
            if tick["timestamp"] >= curr_window.iloc[1]["timestamp"]:
                curr_window = next(factor_window_iter)

            adjust_factors.append(curr_window.iloc[0]["hfq_factor"])
        
        ticks_df["adjust_factors"] = adjust_factors

        # Adjust prices accordingly
        ticks_df[["open", "close", "high", "low"]] *= np.array(adjust_factors).reshape(-1, 1)
        ticks_df["chg"] = ticks_df["close"] - ticks_df["close"].shift(1)
        ticks_df["pct_chg"] = 100 * (ticks_df['chg'] / ticks_df['close'].shift(1))

        ticks_df.dropna(inplace=True)
        return ticks_df.round(2)

    def trade(self, df: pd.DataFrame, strategy: "StrategyBase") -> TradeBook:
        if list(getattr(df.index, "names", [])) != ["timestamp", "code"]:
            print('>>> Resetting indices ...')
            # Index by timestamp: trade in the day order
            #          code: look up the current price for positions in holding
            df.reset_index(inplace=True)
            df.set_index(["timestamp", "code"], inplace=True)
            df.sort_index(inplace=True)

        print('>>> Trading ...')
        trade_book = TradeBook()

        # Per day
        indicator_names = self.__get_buy_indicators(strategy)
        for timestamp, sub_df in tqdm(df.groupby(level="timestamp")):
            assert isinstance(timestamp, str)

            # Opening
            price_lookup = lambda code: sub_df.loc[(timestamp, code), "close"]
            self.account.tick(price_lookup)

            # Sell
            sell_positions = []
            for code, pos in self.account.holdings:
                index = (timestamp, code)
                if index not in sub_df.index:
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

            if sell_positions:
                self.account.sell(sell_positions)

            # Buy
            stocks_selector = strategy.should_buy(*[
                operator.getitem(sub_df, ind)
                for ind in indicator_names
            ])
            stocks_selector = self.__filter_in_holdings(stocks_selector)

            buy_positions = []
            if stocks_selector.any():
                buy_positions = strategy.generate_positions(
                    sub_df[stocks_selector].copy(),
                    self.account.cash_amount
                )

                self.account.buy(buy_positions)
                for pos in buy_positions:
                    trade_book.buy(timestamp, pos)

            # Log closing capitals
            if buy_positions or sell_positions:
                trade_book.log_capitals(
                    timestamp,
                    self.account.cash_amount,
                    self.account.holdings.get_total_worth()
                )

        return trade_book

    def run(self, ticks_df: pd.DataFrame, strategy: "StrategyBase") -> TradeBook:
        ind_df = self.get_indicators_df(ticks_df, strategy)
        trade_book = self.trade(ind_df, strategy)
        return trade_book
