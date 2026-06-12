import pickle
from functools import cached_property
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from tradepy.core.account import Account
from tradepy.core.position import Position
from tradepy.core.types import TradeActionType
from tradepy.trade_book.storage import (
    InMemoryTradeBookStorage,
    SQLiteTradeBookStorage,
    TradeBookStorage,
)
from tradepy.trade_book.types import CapitalsLog, TradeLog


class TradeBook:
    def __init__(self, storage: TradeBookStorage) -> None:
        self.storage = storage

    @cached_property
    def trade_logs_df(self) -> pl.DataFrame:
        return (
            pl.DataFrame(self.storage.fetch_trade_logs())
            .sort("timestamp")
            .with_columns(
                pl.col(
                    "price", "total_value", "chg", "pct_chg", "total_return"
                ).round(2)
            )
        )

    @cached_property
    def cap_logs_df(self) -> pl.DataFrame:
        return (
            pl.DataFrame(self.storage.fetch_capital_logs())
            .with_columns(pl.col("timestamp").str.to_datetime())
            .with_columns(
                (
                    pl.col("market_value")
                    + pl.col("free_cash_amount")
                    + pl.col("frozen_cash_amount")
                ).alias("capital")
            )
            .with_columns(
                pl.col("capital").pct_change().fill_null(0).alias("pct_chg")
            )
            .drop_nulls()
            .sort("timestamp")
            .with_columns(
                pl.col(
                    "frozen_cash_amount",
                    "market_value",
                    "free_cash_amount",
                    "capital",
                    "pct_chg",
                ).round(2)
            )
        )

    def save(self, path: str | Path):
        with open(path, "wb") as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls, path: str | Path) -> "TradeBook":
        with open(path, "rb") as f:
            return pickle.load(f)

    def clone(self) -> "TradeBook":
        storage = self.storage.clone()
        return TradeBook(storage)

    def make_open_position_log(self, timestamp: str, pos: Position) -> TradeLog:
        chg = pos.chg_at(pos.latest_price)
        pct_chg = pos.pct_chg_at(pos.latest_price)

        return {
            "timestamp": timestamp,
            "action": "开仓",
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

    def make_capital_log(self, timestamp: str, account: Account) -> CapitalsLog:
        return {
            "frozen_cash_amount": account.frozen_cash_amount,
            "timestamp": timestamp,
            "market_value": account.get_market_value(),
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

    def log_opening_capitals(self, date: str, account: Account):
        log = self.make_capital_log(date, account)
        self.storage.log_opening_capitals(log)

    def log_closing_capitals(self, date: str, account: Account):
        log = self.make_capital_log(date, account)
        self.storage.log_closing_capitals(log)

    def get_opening(self, date: str) -> CapitalsLog | None:
        return self.storage.get_opening(date)

    @classmethod
    def backtest(cls, *storage_args: Any) -> "TradeBook":
        return cls(InMemoryTradeBookStorage(*storage_args))

    @classmethod
    def live_trading(cls, *storage_args: Any) -> "TradeBook":
        return cls(SQLiteTradeBookStorage(*storage_args))
