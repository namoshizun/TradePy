from tradepy.core.models import Position
from tradepy.types import TradeActionType
from tradepy.trade_book.types import TradeLog, CapitalsLog, AnyAccount
from tradepy.trade_book.storage import TradeBookStorage


class InMemoryTradeBookStorage(TradeBookStorage):

    def __init__(self) -> None:
        self.trade_logs: list[TradeLog] = list()
        self.capital_logs: list[CapitalsLog] = list()

    def sell(self, timestamp: str, pos: Position, action: TradeActionType):
        log = self.make_close_position_log(timestamp, pos, action)
        self.trade_logs.append(log)

    def buy(self, timestamp: str, pos: Position):
        log = self.make_open_position_log(timestamp, pos)
        self.trade_logs.append(log)

    def log_closing_capitals(self, timestamp, account: AnyAccount):
        log = self.make_capital_log(timestamp, account)
        self.capital_logs.append(log)

    def fetch_trade_logs(self) -> list[TradeLog]:
        return self.trade_logs

    def fetch_capital_logs(self) -> list[CapitalsLog]:
        return self.capital_logs
