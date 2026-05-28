from typing import TypedDict, Union
from typing_extensions import NotRequired

from tradepy.core.account import Account, BacktestAccount


class TradeLog(TypedDict):
    timestamp: str
    action: str

    id: str
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
