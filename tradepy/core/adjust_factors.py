import pandas as pd
import numpy as np
import numba as nb
from functools import cached_property


@nb.njit(cache=True)
def _assign_factor_value_to_day(fac_ts, fac_vals, timestamps):
    i = 0

    factors = []
    for ts in timestamps:
        while True:
            if fac_ts[i + 1] > ts:
                break
            i += 1
        factors.append(fac_vals[i])
    return factors


class AdjustFactors:
    def __init__(self, factors_df: pd.DataFrame):
        adj_fac_cols = set(["code", "timestamp", "hfq_factor"])
        assert set(cols := factors_df.columns).issubset(adj_fac_cols), cols

        if factors_df.index.name != "code":
            factors_df.reset_index(inplace=True)
            factors_df.set_index("code", inplace=True)

        self.factors_df = factors_df.copy()
        self.factors_df.sort_values(["code", "timestamp"], inplace=True)

    @cached_property
    def latest_factors(self) -> pd.DataFrame:
        return self.factors_df.groupby("code").tail(2).dropna()  # drop the end padding

    def to_real_price(self, code: str, price: float) -> float:
        factor: float = self.latest_factors.loc[code, "hfq_factor"]  # type: ignore
        return round(price / factor, 2)

    def backward_adjust_history_prices(self, code: str, bars_df: pd.DataFrame):
        """
        bars_df: an individual stock's day bars
        """
        factors_df = self.factors_df.loc[code]

        # Find each day's adjust factor
        factor_vals = _assign_factor_value_to_day(
            nb.typed.List(factors_df["timestamp"].tolist()),
            nb.typed.List(factors_df["hfq_factor"].tolist()),
            nb.typed.List(bars_df["timestamp"].tolist()),
        )

        # Adjust prices accordingly
        bars_df[["open", "close", "high", "low"]] *= np.array(factor_vals).reshape(
            -1, 1
        )
        bars_df["chg"] = (bars_df["close"] - bars_df["close"].shift(1)).fillna(0)
        bars_df["pct_chg"] = (
            100 * (bars_df["chg"] / bars_df["close"].shift(1))
        ).fillna(0)
        return bars_df.round(2)

    def backward_adjust_stocks_latest_prices(
        self, bars_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        bars_df: stocks' candlestick bars (one per stock).
        """
        bars_df["close"] *= self.latest_factors["hfq_factor"]
        bars_df["low"] *= self.latest_factors["hfq_factor"]
        bars_df["high"] *= self.latest_factors["hfq_factor"]
        bars_df["open"] *= self.latest_factors["hfq_factor"]
        return bars_df
