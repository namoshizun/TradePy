import pandas as pd
from functools import cached_property

import tradepy
from tradepy.core.account import Account
from tradepy.core.models import Position
from tradepy.types import TradeActions
from tradepy.core.trade_book.types import CapitalsLog
from tradepy.core.trade_book.storage import TradeBookStorage, SQLiteTradeBookStorage, InMemoryTradeBookStorage


class TradeBook:

    def __init__(self, storage: TradeBookStorage) -> None:
        self.storage = storage

    @cached_property
    def trade_logs_df(self) -> pd.DataFrame:
        df = pd.DataFrame(self.storage.fetch_trade_logs())
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)

        codes = df["code"].unique()
        code_to_company = tradepy.listing.df.loc[codes, "name"]
        df = df.join(code_to_company, on="code")
        df.rename(columns={"name": "company"}, inplace=True)
        return df

    @cached_property
    def cap_logs_df(self) -> pd.DataFrame:
        cap_df = pd.DataFrame(self.storage.fetch_capital_logs())
        cap_df["timestamp"] = pd.to_datetime(cap_df["timestamp"])
        cap_df["capital"] = cap_df["market_value"] + cap_df["free_cash_amount"] + cap_df["frozen_cash_amount"]
        cap_df["pct_chg"] = cap_df["capital"].pct_change()
        cap_df.dropna(inplace=True)
        cap_df.set_index("timestamp", inplace=True)
        return cap_df

    def buy(self, timestamp: str, pos: Position):
        self.storage.buy(timestamp, pos)

    def close(self, *args, **kwargs):
        kwargs["action"] = TradeActions.CLOSE
        self.storage.sell(*args, **kwargs)

    def stop_loss(self, *args, **kwargs):
        kwargs["action"] = TradeActions.STOP_LOSS
        self.storage.sell(*args, **kwargs)

    def take_profit(self, *args, **kwargs):
        kwargs["action"] = TradeActions.TAKE_PROFIT
        self.storage.sell(*args, **kwargs)

    def log_opening_capitals(self, timestamp: str, account: Account):
        self.storage.log_opening_capitals(timestamp, account)

    def log_closing_capitals(self, timestamp: str, account: Account):
        self.storage.log_closing_capitals(timestamp, account)

    def get_opening(self, date: str) -> CapitalsLog | None:
        return self.storage.get_opening(date)

    @classmethod
    def backtest(cls) -> "TradeBook":
        return cls(InMemoryTradeBookStorage())

    @classmethod
    def live_trading(cls) -> "TradeBook":
        return cls(SQLiteTradeBookStorage())
