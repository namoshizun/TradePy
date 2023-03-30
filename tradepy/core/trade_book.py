import abc
import pandas as pd
from functools import cached_property
from typing import TypedDict, Union
from typing_extensions import NotRequired

import tradepy
from tradepy.core.account import Account, BacktestAccount
from tradepy.core.position import Position
from tradepy.types import TradeActions, TradeActionType


class TradeLog(TypedDict):
    timestamp: str
    action: str

    pos_id: str
    code: str
    vol: int
    price: float
    total_value: float
    chg: NotRequired[float | None]
    pct_chg: NotRequired[float | None]
    total_return: NotRequired[float | None]


class CapitalsLog(TypedDict):
    timestamp: str
    market_value: float
    free_cash_amount: float
    frozen_cash_amount: float


AnyAccount = Union[Account, BacktestAccount]  # FIXME: not so cool ...


class TradeBookStorage:

    @abc.abstractmethod
    def sell(self, timestamp: str, pos: Position, action: TradeActionType):
        raise NotImplementedError

    @abc.abstractmethod
    def buy(self, timestamp: str, pos: Position):
        raise NotImplementedError

    @abc.abstractmethod
    def log_opening_capitals(self, timestamp: str, account: AnyAccount):
        raise NotImplementedError

    @abc.abstractmethod
    def log_closing_capitals(self, timestamp: str, account: AnyAccount):
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_trade_logs(self) -> list[TradeLog]:
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_capital_logs(self) -> list[CapitalsLog]:
        raise NotImplementedError


class InMemoryTradeBookStorage(TradeBookStorage):

    def __init__(self) -> None:
        self.trade_logs: list[TradeLog] = list()
        self.capital_logs: list[CapitalsLog] = list()

    def sell(self, timestamp: str, pos: Position, action: TradeActionType):
        chg = pos.chg_at(pos.latest_price)
        pct_chg = pos.pct_chg_at(pos.latest_price)
        assert pos.latest_price

        self.trade_logs.append({
            "timestamp": timestamp,
            "action": action,
            "pos_id": pos.id,
            "code": pos.code,
            "vol": pos.vol,
            "price": pos.latest_price,
            "total_value": pos.latest_price * pos.vol,
            "chg": chg,
            "pct_chg": pct_chg,
            "total_return": (pos.price * pct_chg * 1e-2) * pos.vol
        })

    def buy(self, timestamp: str, pos: Position):
        self.trade_logs.append({
            "timestamp": timestamp,
            "action": TradeActions.OPEN,
            "pos_id": pos.id,
            "code": pos.code,
            "vol": pos.vol,
            "price": pos.price,
            "total_value": pos.price * pos.vol,
        })

    def log_closing_capitals(self, timestamp, account: AnyAccount):
        self.capital_logs.append({
            "frozen_cash_amount": account.frozen_cash_amount,
            "timestamp": timestamp,
            "market_value": account.market_value,
            "free_cash_amount": account.free_cash_amount,
        })

    def fetch_trade_logs(self) -> list[TradeLog]:
        return self.trade_logs

    def fetch_capital_logs(self) -> list[CapitalsLog]:
        return self.capital_logs


class SQLiteTradeBookStorage(TradeBookStorage):
    ...


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

    def exit(self, *args, **kwargs):
        kwargs["action"] = TradeActions.CLOSE
        self.storage.sell(*args, **kwargs)

    def stop_loss(self, *args, **kwargs):
        kwargs["action"] = TradeActions.STOP_LOSS
        self.storage.sell(*args, **kwargs)

    def take_profit(self, *args, **kwargs):
        kwargs["action"] = TradeActions.TAKE_PROFIT
        self.storage.sell(*args, **kwargs)

    def close_position(self, *args, **kwargs):
        kwargs["action"] = TradeActions.CLOSE
        self.storage.sell(*args, **kwargs)

    def log_opening_capitals(self, timestamp: str, account: Account):
        self.storage.log_opening_capitals(timestamp, account)

    def log_closing_capitals(self, timestamp: str, account: Account):
        self.storage.log_closing_capitals(timestamp, account)

    @classmethod
    def backtest(cls) -> "TradeBook":
        return cls(InMemoryTradeBookStorage())

    @classmethod
    def live_trading(cls) -> "TradeBook":
        return cls(SQLiteTradeBookStorage())
