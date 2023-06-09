import os
import sqlite3
from loguru import logger

from tradepy.trade_book.types import TradeLog, CapitalsLog
from tradepy.trade_book.storage import TradeBookStorage
from tradepy.trade_book.storage.sqlite_orm import Table


class SQLiteTradeBookStorage(TradeBookStorage):
    def __init__(self) -> None:
        db_path = os.path.expanduser("~/.tradepy/trade_book.db")
        self.conn = sqlite3.connect(db_path)

        self.trade_logs_tbl: Table[TradeLog] = Table.from_typed_dict(TradeLog)
        self.capital_logs_tbl: Table[CapitalsLog] = Table.from_typed_dict(CapitalsLog)

        self.trade_logs_tbl.create_table(self.conn)
        self.capital_logs_tbl.create_table(self.conn)

    def __del__(self):
        self.conn.close()

    def buy(self, log: TradeLog):
        self.trade_logs_tbl.insert(self.conn, log)

    def sell(self, log: TradeLog):
        self.trade_logs_tbl.insert(self.conn, log)

    def log_opening_capitals(self, log: CapitalsLog):
        date = log["timestamp"]
        if self.capital_logs_tbl.select(self.conn, timestamp=date):
            logger.warning(f"{date}已存在开盘资金记录，将不再记录。")
            return

        self.capital_logs_tbl.insert(self.conn, log)

    def log_closing_capitals(self, log: CapitalsLog):
        date = log["timestamp"]
        self.capital_logs_tbl.update(self.conn, where={"timestamp": date}, update=log)

    def fetch_trade_logs(self) -> list[TradeLog]:
        return self.trade_logs_tbl.select(self.conn)

    def fetch_capital_logs(self) -> list[CapitalsLog]:
        return self.capital_logs_tbl.select(self.conn)

    def get_opening(self, date: str) -> CapitalsLog | None:
        logs = self.capital_logs_tbl.select(self.conn, timestamp=date)
        if not logs:
            return None

        if len(logs) > 1:
            logger.warning(f"找到多个{date}的开盘资金记录，将返回第一个。")

        return logs[0]
