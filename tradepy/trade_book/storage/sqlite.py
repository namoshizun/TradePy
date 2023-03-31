import os
import sqlite3

from tradepy.core.models import Position
from tradepy.types import TradeActionType
from tradepy.trade_book.types import TradeLog, CapitalsLog, AnyAccount
from tradepy.trade_book.storage import TradeBookStorage
from tradepy.trade_book.storage.sqlite_orm import Table


class SQLiteTradeBookStorage(TradeBookStorage):

    def __init__(self) -> None:
        db_path = os.path.expanduser('~/.tradepy/trade_book.db')
        self.conn = sqlite3.connect(db_path)

        self.trade_logs_ent: Table[TradeLog] = Table.from_typed_dict(TradeLog)
        self.capital_logs_ent: Table[CapitalsLog] = Table.from_typed_dict(CapitalsLog)

        self.trade_logs_ent.create_table(self.conn)
        self.capital_logs_ent.create_table(self.conn)

    def __del__(self):
        self.conn.close()

    def buy(self, timestamp: str, pos: Position):
        return super().buy(timestamp, pos)

    def sell(self, timestamp: str, pos: Position, action: TradeActionType):
        return super().sell(timestamp, pos, action)

    def log_opening_capitals(self, timestamp: str, account: AnyAccount):
        return super().log_opening_capitals(timestamp, account)

    def log_closing_capitals(self, timestamp: str, account: AnyAccount):
        return super().log_closing_capitals(timestamp, account)

    def fetch_trade_logs(self) -> list[TradeLog]:
        return super().fetch_trade_logs()

    def fetch_capital_logs(self) -> list[CapitalsLog]:
        return super().fetch_capital_logs()

    def get_opening(self, date: str) -> CapitalsLog | None:
        return super().get_opening(date)
