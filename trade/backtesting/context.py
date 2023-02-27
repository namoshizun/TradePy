import pandas as pd
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
        if not self.hfq_adjust_factors:
            return

        adj_fac_cols = ["code", "timestamp", "hfq_factor"]
        assert set(cols := self.hfq_adjust_factors.columns).issubset(set(adj_fac_cols)), cols


def china_market_context(**kwargs) -> Context:
    data_args = {
        "buy_commission_rate": 0,
        "sell_commission_rate": 1e-3,
    }
    data_args.update(data_args)
    return Context(**data_args)
