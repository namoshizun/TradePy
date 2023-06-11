import pandas as pd
from loguru import logger
from functools import cached_property

import tradepy
from tradepy.core.account import Account
from tradepy.core.models import Position
from tradepy.types import TradeActions, TradeActionType
from tradepy.trade_book.types import CapitalsLog, TradeLog, AnyAccount
from tradepy.trade_book.storage import (
    TradeBookStorage,
    SQLiteTradeBookStorage,
    InMemoryTradeBookStorage,
)


class TradeBook:
    def __init__(self, storage: TradeBookStorage) -> None:
        self.storage = storage

    @cached_property
    def trade_logs_df(self) -> pd.DataFrame:
        df = pd.DataFrame(self.storage.fetch_trade_logs())
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)

        try:
            codes = df["code"].unique()
            code_to_company = tradepy.listing.df.loc[codes, "name"]
            df = df.join(code_to_company, on="code")
            df.rename(columns={"name": "company"}, inplace=True)
        except FileNotFoundError:
            logger.debug("未找到股票列表数据, 无法在交易历史中添加公司名称")
        except KeyError:
            logger.debug("股票列表数据中没有找到某些股票的公司名称")
        return df

    @cached_property
    def cap_logs_df(self) -> pd.DataFrame:
        cap_df = pd.DataFrame(self.storage.fetch_capital_logs())
        cap_df["timestamp"] = pd.to_datetime(cap_df["timestamp"])
        cap_df["capital"] = (
            cap_df["market_value"]
            + cap_df["free_cash_amount"]
            + cap_df["frozen_cash_amount"]
        )
        cap_df["pct_chg"] = cap_df["capital"].pct_change()
        cap_df.dropna(inplace=True)
        cap_df.set_index("timestamp", inplace=True)
        return cap_df

    def clone(self) -> "TradeBook":
        storage = self.storage.clone()
        return TradeBook(storage)

    def make_open_position_log(self, timestamp: str, pos: Position) -> TradeLog:
        chg = pos.chg_at(pos.latest_price)
        pct_chg = pos.pct_chg_at(pos.latest_price)

        return {
            "timestamp": timestamp,
            "action": TradeActions.OPEN,
            "id": pos.id,
            "code": pos.code,
            "vol": pos.vol,
            "price": pos.price,
            "total_value": pos.price * pos.vol,
            "chg": chg,
            "pct_chg": pct_chg,
            "total_return": (pos.price * pct_chg * 1e-2) * pos.vol,
        }

    def make_close_position_log(
        self, timestamp: str, pos: Position, action: TradeActionType
    ) -> TradeLog:
        assert pos.is_closed
        chg = pos.chg_at(pos.latest_price)
        pct_chg = pos.pct_chg_at(pos.latest_price)
        sold_vol = pos.yesterday_vol

        return {
            "timestamp": timestamp,
            "action": action,
            "id": pos.id,
            "code": pos.code,
            "vol": sold_vol,
            "price": pos.latest_price,
            "total_value": pos.latest_price * sold_vol,
            "chg": chg,
            "pct_chg": pct_chg,
            "total_return": (pos.price * pct_chg * 1e-2) * sold_vol,
        }

    def make_capital_log(self, timestamp, account: AnyAccount) -> CapitalsLog:
        return {
            "frozen_cash_amount": account.frozen_cash_amount,
            "timestamp": timestamp,
            "market_value": account.market_value,
            "free_cash_amount": account.free_cash_amount,
        }

    def buy(self, timestamp: str, pos: Position):
        log = self.make_open_position_log(timestamp, pos)
        try:
            self.storage.buy(log)
        except Exception as exc:
            logger.error(f"导出开仓日志错误, {log}")
            raise exc

    def sell(self, timestamp: str, pos: Position, action: TradeActionType):
        log = self.make_close_position_log(timestamp, pos, action)
        try:
            self.storage.sell(log)
        except Exception as exc:
            logger.error(f"导出开仓日志错误, {log}")
            raise exc

    def close(self, *args, **kwargs):
        kwargs["action"] = TradeActions.CLOSE
        self.sell(*args, **kwargs)

    def stop_loss(self, *args, **kwargs):
        kwargs["action"] = TradeActions.STOP_LOSS
        self.sell(*args, **kwargs)

    def take_profit(self, *args, **kwargs):
        kwargs["action"] = TradeActions.TAKE_PROFIT
        self.sell(*args, **kwargs)

    def log_opening_capitals(self, date: str, account: Account):
        log = self.make_capital_log(date, account)
        self.storage.log_opening_capitals(log)

    def log_closing_capitals(self, date: str, account: Account):
        log = self.make_capital_log(date, account)
        self.storage.log_closing_capitals(log)

    def get_opening(self, date: str) -> CapitalsLog | None:
        return self.storage.get_opening(date)

    @classmethod
    def backtest(cls) -> "TradeBook":
        return cls(InMemoryTradeBookStorage())

    @classmethod
    def live_trading(cls) -> "TradeBook":
        return cls(SQLiteTradeBookStorage())
