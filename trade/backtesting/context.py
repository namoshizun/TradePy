import pandas as pd
from copy import deepcopy
from dataclasses import dataclass


@dataclass
class Context:
    trading_unit: int
    stop_loss: float
    take_profit: float

    cash_amount: float
    buy_commission_rate: float
    sell_commission_rate: float

    hfq_adjust_factors: pd.DataFrame | None = None

    def __post_init__(self):
        if self.hfq_adjust_factors is None:
            return

        adj_fac_cols = ["code", "timestamp", "hfq_factor"]
        assert set(cols := self.hfq_adjust_factors.columns).issubset(set(adj_fac_cols)), cols
    
    @classmethod
    def build(cls, **kwargs):
        # Pluck args not defined in the vanilia context definition
        extra_args = dict()
        for field, value in kwargs.items():
            if field not in Context.__annotations__:
                extra_args[field] = deepcopy(value)

        # Build the vanilia context object
        for field in extra_args.keys():
            kwargs.pop(field)
        ctx = cls(**kwargs)

        # Monkey patch extra args to it
        for field, value in extra_args.items():
            setattr(ctx, field, value)

        return ctx


def china_market_context(**kwargs) -> Context:
    data_args = {
        "trading_unit": 100,
        "buy_commission_rate": 0,
        "sell_commission_rate": 1e-3,
    }
    data_args.update(kwargs)
    return Context.build(**data_args)
