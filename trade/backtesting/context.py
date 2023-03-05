import pandas as pd
import numpy as np
from copy import deepcopy
from dataclasses import dataclass, field

from trade.backtesting.account import Account


@dataclass
class Context:
    # Trade params
    stop_loss: float
    take_profit: float

    # Account settings
    cash_amount: float
    trading_unit: int
    buy_commission_rate: float
    sell_commission_rate: float
    account: Account = field(init=False)

    # Position sizing
    max_position_size: float = np.inf
    max_position_opens: float = np.inf

    # Misc
    hfq_adjust_factors: pd.DataFrame | None = None

    def __post_init__(self):
        if self.hfq_adjust_factors is not None:
            adj_fac_cols = ["code", "timestamp", "hfq_factor"]
            assert set(cols := self.hfq_adjust_factors.columns).issubset(set(adj_fac_cols)), cols
        self.account = Account(
            cash_amount=self.cash_amount,
            buy_commission_rate=self.buy_commission_rate,
            sell_commission_rate=self.sell_commission_rate
        )

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
