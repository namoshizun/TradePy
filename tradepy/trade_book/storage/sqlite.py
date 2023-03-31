import os
import sqlite3
from loguru import logger

from tradepy.core.models import Position
from tradepy.types import TradeActionType
from tradepy.trade_book.types import TradeLog, CapitalsLog, AnyAccount
from tradepy.trade_book.storage import TradeBookStorage
from tradepy.trade_book.storage.sqlite_orm import Table


class SQLiteTradeBookStorage(TradeBookStorage):

    def __init__(self) -> None:
        db_path = os.path.expanduser('~/.tradepy/trade_book.db')
        self.conn = sqlite3.connect(db_path)

        self.trade_logs_tbl: Table[TradeLog] = Table.from_typed_dict(TradeLog)
        self.capital_logs_tbl: Table[CapitalsLog] = Table.from_typed_dict(CapitalsLog)

        self.trade_logs_tbl.create_table(self.conn)
        self.capital_logs_tbl.create_table(self.conn)

    def __del__(self):
        self.conn.close()

    def buy(self, timestamp: str, pos: Position):
        log = self.make_open_position_log(timestamp, pos)
        self.trade_logs_tbl.insert(self.conn, log)

    def sell(self, timestamp: str, pos: Position, action: TradeActionType):
        log = self.make_close_position_log(timestamp, pos, action)
        self.trade_logs_tbl.insert(self.conn, log)

    def log_opening_capitals(self, date: str, account: AnyAccount):
        log = self.make_capital_log(date, account)
        self.capital_logs_tbl.insert(self.conn, log)

    def log_closing_capitals(self, date: str, account: AnyAccount):
        log = self.make_capital_log(date, account)
        self.capital_logs_tbl.update(
            self.conn,
            where={'timestamp': date},
            update=log
        )

    def fetch_trade_logs(self) -> list[TradeLog]:
        return self.trade_logs_tbl.select(self.conn)

    def fetch_capital_logs(self) -> list[CapitalsLog]:
        return self.capital_logs_tbl.select(self.conn)

    def get_opening(self, date: str) -> CapitalsLog | None:
        logs = self.capital_logs_tbl.select(self.conn, timestamp=date)
        if not logs:
            return None

        if len(logs) > 1:
            logger.warning(f'Found more than one opening capital log on {date}. Will return the first one.')

        return logs[0]
