from copy import deepcopy
from tradepy.trade_book.types import TradeLog, CapitalsLog
from tradepy.trade_book.storage import TradeBookStorage


class InMemoryTradeBookStorage(TradeBookStorage):
    def __init__(self) -> None:
        self.trade_logs: list[TradeLog] = list()
        self.capital_logs: list[CapitalsLog] = list()

    def sell(self, log: TradeLog):
        self.trade_logs.append(log)

    def buy(self, log: TradeLog):
        self.trade_logs.append(log)

    def log_closing_capitals(self, log: CapitalsLog):
        self.capital_logs.append(log)

    def fetch_trade_logs(self) -> list[TradeLog]:
        return self.trade_logs

    def fetch_capital_logs(self) -> list[CapitalsLog]:
        return self.capital_logs

    def clone(self) -> "InMemoryTradeBookStorage":
        instance = InMemoryTradeBookStorage()
        instance.trade_logs = deepcopy(self.trade_logs)
        instance.capital_logs = deepcopy(self.capital_logs)
        return instance
