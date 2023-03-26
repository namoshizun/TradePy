import pandas as pd
from functools import cached_property
from typing import TypedDict
from typing_extensions import NotRequired

from tradepy.core.position import Position
from tradepy.types import TradeActions, TradeActionType


class TradeLog(TypedDict):
    timestamp: str
    tag: str

    pos_id: str
    code: str
    shares: int
    price: float
    total_value: float
    chg: NotRequired[float | None]
    pct_chg: NotRequired[float | None]
    total_return: NotRequired[float | None]


class CapitalsLog(TypedDict):
    timestamp: str
    positions_value: float
    free_cash_amount: float


class TradeBook:

    def __init__(self) -> None:
        self.trade_logs: list[TradeLog] = list()
        self.capital_logs: list[CapitalsLog] = list()

    @cached_property
    def trade_logs_df(self) -> pd.DataFrame:
        df = pd.DataFrame(self.trade_logs)
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
        return df

    @cached_property
    def cap_logs_df(self) -> pd.DataFrame:
        cap_df = pd.DataFrame(self.capital_logs)
        cap_df["timestamp"] = pd.to_datetime(cap_df["timestamp"])
        cap_df["capital"] = cap_df["positions_value"] + cap_df["free_cash_amount"]
        cap_df["pct_chg"] = cap_df["capital"].pct_change()
        cap_df.dropna(inplace=True)
        cap_df.set_index("timestamp", inplace=True)
        return cap_df

    def reset(self):
        self.trade_logs = list()
        self.capital_logs = list()

    def __sell(self, timestamp: str, pos: Position, action: TradeActionType):
        chg = pos.chg_at(pos.latest_price)
        pct_chg = pos.pct_chg_at(pos.latest_price)
        assert pos.latest_price

        self.trade_logs.append({
            "timestamp": timestamp,
            "tag": action,
            "pos_id": pos.id,
            "code": pos.code,
            "shares": pos.vol,
            "price": pos.latest_price,
            "total_value": pos.latest_price * pos.vol,
            "chg": chg,
            "pct_chg": pct_chg,
            "total_return": (pos.price * pct_chg * 1e-2) * pos.vol
        })

    def buy(self, timestamp: str, pos: Position):
        self.trade_logs.append({
            "timestamp": timestamp,
            "tag": TradeActions.OPEN,
            "pos_id": pos.id,
            "code": pos.code,
            "shares": pos.vol,
            "price": pos.price,
            "total_value": pos.price * pos.vol,
        })

    def exit(self, *args, **kwargs):
        kwargs["action"] = TradeActions.CLOSE
        self.__sell(*args, **kwargs)

    def stop_loss(self, *args, **kwargs):
        kwargs["action"] = TradeActions.STOP_LOSS
        self.__sell(*args, **kwargs)

    def take_profit(self, *args, **kwargs):
        kwargs["action"] = TradeActions.TAKE_PROFIT
        self.__sell(*args, **kwargs)

    def close_position(self, *args, **kwargs):
        kwargs["action"] = TradeActions.CLOSE
        self.__sell(*args, **kwargs)

    def log_capitals(self, timestamp, cash_amount: float, positions_value: float):
        self.capital_logs.append({
            "timestamp": timestamp,
            "positions_value": positions_value,
            "free_cash_amount": cash_amount,
        })
