import pandas as pd
import numpy as np
from datetime import date
from copy import deepcopy
from dataclasses import dataclass

from tradepy.utils import get_latest_trade_date


@dataclass
class Context:
    # Trade params
    stop_loss: float
    take_profit: float

    # Account settings
    cash_amount: float
    trading_unit: int
    broker_commission_rate: float = 0
    stamp_duty_rate: float = 0

    # Position sizing
    max_position_size: float = np.inf
    max_position_opens: float = np.inf

    # Misc
    signals_percent_range = (0, 100)  # in percentage
    hfq_adjust_factors: pd.DataFrame | None = None  # TODO: should be instance of AdjustFactos or None

    def __post_init__(self):
        if self.hfq_adjust_factors is not None:
            adj_fac_cols = ["code", "timestamp", "hfq_factor"]
            assert set(cols := self.hfq_adjust_factors.columns).issubset(set(adj_fac_cols)), cols

            _adf = self.hfq_adjust_factors.copy()
            _adf.reset_index(inplace=True)
            _adf.set_index("code", inplace=True)
            _adf.sort_values(["code", "timestamp"], inplace=True)
            self.hfq_adjust_factors = _adf

    def get_trade_date(self) -> date:
        return get_latest_trade_date()

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
        "broker_commission_rate": 0.05,  # percent
        "stamp_duty_rate": 0.1,  # percent
    }
    data_args.update(kwargs)
    return Context.build(**data_args)
