import abc
import inspect
import operator
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import TypedDict, Generic, TypeVar


from trade.backtesting.account import Account, TradeBook
from trade.backtesting.position import Position
from trade.utils import calc_pct_chg


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



TickDataType = TypeVar("TickDataType", bound=TickData)


class StrategyBase(Generic[TickDataType]):

    buy_commission_rate = 0
    sell_commission_rate = 0

    trading_unit = 1
    stop_loss = 0
    take_profit = 0
    expiry = np.inf  # position never expires unless stop loss or take profit

    def __init__(self, cash_amount: float) -> None:
        self.reset(cash_amount)
        assert self.account

    def reset(self, cash_amount: float):
        self.account = Account(
            cash_amount=cash_amount,
            buy_commission_rate=self.buy_commission_rate,
            sell_commission_rate=self.sell_commission_rate,
        )

    @abc.abstractmethod
    def should_buy(self, *indicators) -> pd.Series:
        raise NotImplementedError

    @abc.abstractmethod
    def compute_indicators(self, ticks_df: pd.DataFrame):
        raise NotImplementedError

    def should_exit(self, tick: TickDataType, position: Position) -> float | None:
        if position.days_held >= self.expiry:
            return tick["close"]

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

    def generate_positions(self, df: pd.DataFrame) -> list[Position]:
        # Calculate the minimum number of shares to buy per stock
        num_stocks = len(df)
        cash_amount = self.account.cash_amount

        df["trade_price"] = df["close"] * self.trading_unit
        df["trade_shares"] = cash_amount / num_stocks // df["trade_price"]

        # Gather the remaining budget
        remaining_budget = cash_amount - (df["trade_price"] * df["trade_shares"]).sum()

        # Distribute that budget again, starting from the big guys
        df.sort_values("trade_price", inplace=True, ascending=False)
        for idx, stock in df.iterrows():
            if residual_shares := remaining_budget // stock["trade_price"]:
                df.at[idx, "trade_shares"] += residual_shares
                remaining_budget -= residual_shares * stock["trade_price"]

        return [
            Position(
                timestamp=timestamp,
                code=code,
                price=stock["close"],
                shares=stock["trade_shares"] * self.trading_unit,
            )
            for (timestamp, code), stock in df.iterrows()
            if stock["trade_shares"] > 0
        ]

    def __get_buy_indicators(self) -> list[str]:
        spec = inspect.getfullargspec(self.should_buy)
        return spec.args[1:]

    def __filter_in_holdings(self, selector: pd.Series) -> pd.Series:
        for index, _ in selector.iteritems():
            _, code = index
            if self.account.holdings.has(code):
                selector.at[index] = False
        return selector

    # ----------
    # Main steps
    # ----------
    def get_indicators_df(self, df: pd.DataFrame) -> pd.DataFrame:
        indicator_names = self.__get_buy_indicators()
        if set(indicator_names).issubset(set(df.columns)):
            print(f'{indicator_names} already exists. Skip computing indicators')
            return df

        if df.index.name != "code":
            print('>>> Resetting indices ...')
            df.reset_index(inplace=True)
            df.set_index("code", inplace=True)

        print('>>> Computing indicators ...')
        return pd.concat(
            self.compute_indicators(ticks_df.sort_values("timestamp"))
            for _, ticks_df in tqdm(df.groupby(level="code"))
        )

    def trade(self, df: pd.DataFrame) -> TradeBook:
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
        indicator_names = self.__get_buy_indicators()
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
                if take_profit_price := self.should_take_profit(bar, pos):
                    pos.close(take_profit_price)
                    trade_book.take_profit(timestamp, pos)
                    sell_positions.append(pos)

                # [2] Stop loss
                elif stop_loss_price := self.should_stop_loss(bar, pos):
                    pos.close(stop_loss_price)
                    trade_book.stop_loss(timestamp, pos)
                    sell_positions.append(pos)

                # [3] expired
                elif exit_price := self.should_exit(bar, pos):
                    pos.close(exit_price)
                    trade_book.exit(timestamp, pos)
                    sell_positions.append(pos)

            if sell_positions:
                self.account.sell(sell_positions)

            # Buy
            stocks_selector = self.should_buy(*[
                operator.getitem(sub_df, ind)
                for ind in indicator_names
            ])
            stocks_selector = self.__filter_in_holdings(stocks_selector)

            buy_positions = []
            if stocks_selector.any():
                buy_positions = self.generate_positions(sub_df[stocks_selector].copy())

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

    def evaluate(self, trade_book: TradeBook):
        ...

    def backtest(self, df: pd.DataFrame) -> TradeBook:
        df = self.get_indicators_df(df)
        trade_book = self.trade(df)
        self.evaluate(trade_book)
        return trade_book



class Strategy(StrategyBase[TickData]):

    def compute_indicators(self, df: pd.DataFrame):
        if "chg" not in df:
            df["chg"] = (df["close"] - df["close"].shift(1)).fillna(0).round(2)

        if "pct_chg" not in df:
            df["pct_chg"] = (100 * df["chg"] / df["close"].shift(1)).round(2)

        return super().compute_indicators(df)



class ChinaMarketStrategy(Strategy):
    trading_unit = 100
    buy_commission_rate = 1e-3
    sell_commission_rate = 1e-3

