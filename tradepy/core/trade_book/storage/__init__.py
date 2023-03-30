import abc
from tradepy.core.models import Position
from tradepy.types import TradeActions, TradeActionType
from tradepy.core.trade_book.types import TradeLog, CapitalsLog, AnyAccount


class TradeBookStorage:

    def make_open_position_log(self, timestamp: str, pos: Position) -> TradeLog:
        return {
            "timestamp": timestamp,
            "action": TradeActions.OPEN,
            "id": pos.id,
            "code": pos.code,
            "vol": pos.vol,
            "price": pos.price,
            "total_value": pos.price * pos.vol,
        }

    def make_close_position_log(self, timestamp: str, pos: Position, action: TradeActionType) -> TradeLog:
        chg = pos.chg_at(pos.latest_price)
        pct_chg = pos.pct_chg_at(pos.latest_price)
        assert pos.latest_price

        return {
            "timestamp": timestamp,
            "action": action,
            "id": pos.id,
            "code": pos.code,
            "vol": pos.vol,
            "price": pos.latest_price,
            "total_value": pos.latest_price * pos.vol,
            "chg": chg,
            "pct_chg": pct_chg,
            "total_return": (pos.price * pct_chg * 1e-2) * pos.vol
        }

    def make_capital_log(self, timestamp, account: AnyAccount) -> CapitalsLog:
        return {
            "frozen_cash_amount": account.frozen_cash_amount,
            "timestamp": timestamp,
            "market_value": account.market_value,
            "free_cash_amount": account.free_cash_amount,
        }

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

    @abc.abstractmethod
    def get_opening(self, date: str) -> CapitalsLog | None:
        raise NotImplementedError


from .sqlite import SQLiteTradeBookStorage  # noqa
from .memory import InMemoryTradeBookStorage  # noqa
