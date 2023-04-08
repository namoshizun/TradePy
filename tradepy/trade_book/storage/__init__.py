import abc
from tradepy.trade_book.types import TradeLog, CapitalsLog


class TradeBookStorage:

    @abc.abstractmethod
    def sell(self, log: TradeLog):
        raise NotImplementedError

    @abc.abstractmethod
    def buy(self, log: TradeLog):
        raise NotImplementedError

    @abc.abstractmethod
    def log_opening_capitals(self, log: CapitalsLog):
        raise NotImplementedError

    @abc.abstractmethod
    def log_closing_capitals(self, log: CapitalsLog):
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_trade_logs(self) -> list[TradeLog]:
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_capital_logs(self) -> list[CapitalsLog]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_opening(self, date: str) -> CapitalsLog | None:
        raise NotImplementedError

    @abc.abstractmethod
    def clone(self):
        raise NotImplementedError



from .sqlite import SQLiteTradeBookStorage  # noqa
from .memory import InMemoryTradeBookStorage  # noqa
