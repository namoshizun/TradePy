import numpy as np
from datetime import date
from copy import deepcopy
from dataclasses import dataclass
from tradepy.core.adjust_factors import AdjustFactors

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

    # Position control
    max_position_size: float = np.inf
    max_position_opens: float = np.inf
    min_trade_amount: float = 0

    # Misc
    signals_percent_range = (0, 100)  # in percentage
    adjust_factors: AdjustFactors | None = None

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
