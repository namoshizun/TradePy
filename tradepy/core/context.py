import pandas as pd
import numpy as np
from datetime import date
from copy import deepcopy
from dataclasses import dataclass, field

from tradepy.core.account import Account
from tradepy.utils import get_latest_trade_date


@dataclass
class Context:
    # Trade params
    stop_loss: float
    take_profit: float

    # Account settings
    cash_amount: float
    trading_unit: int
    buy_commission_rate: float = 0
    sell_commission_rate: float = 0
    account: Account = field(init=False)

    # Position sizing
    max_position_size: float = np.inf
    max_position_opens: float = np.inf

    # Misc
    signals_percent_range = (0, 100)  # in percentage
    hfq_adjust_factors: pd.DataFrame | None = None

    def __post_init__(self):
        if self.hfq_adjust_factors is not None:
            adj_fac_cols = ["code", "timestamp", "hfq_factor"]
            assert set(cols := self.hfq_adjust_factors.columns).issubset(set(adj_fac_cols)), cols

            _adf = self.hfq_adjust_factors.copy()
            _adf.reset_index(inplace=True)
            _adf.set_index("code", inplace=True)
            _adf.sort_values(["code", "timestamp"], inplace=True)
            self.hfq_adjust_factors = _adf

        self.account = Account(
            cash_amount=self.cash_amount,
            buy_commission_rate=self.buy_commission_rate,
            sell_commission_rate=self.sell_commission_rate
        )

    def get_trade_date(self) -> date:
        return get_latest_trade_date()
        # return date(2023, 3, 10)

    @classmethod
    def build(cls, **kwargs):
        # Pluck args not defined in the vanilia context definition
        extra_args = dict()
        for key, value in kwargs.items():
            if key not in Context.__annotations__:
                extra_args[key] = deepcopy(value)

        # Build the vanilia context object
        for key in extra_args.keys():
            kwargs.pop(key)
        ctx = cls(**kwargs)

        # Monkey patch extra args to it
        for attr, value in extra_args.items():
            setattr(ctx, attr, value)

        return ctx


def china_market_context(**kwargs) -> Context:
    data_args = {
        "trading_unit": 100,
        "buy_commission_rate": 0,
        "sell_commission_rate": 1e-3,
    }
    data_args.update(kwargs)
    return Context.build(**data_args)
