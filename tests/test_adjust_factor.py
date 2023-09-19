import pytest
import pandas as pd
import numpy as np
from pandas.testing import assert_series_equal
from tradepy.core.adjust_factors import AdjustFactors, _assign_factor_value_to_day


@pytest.fixture
def sample_stock_day_k() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ["000333", "2020-10-10", 100.0, 105.0, 108.0, 98.0],
            ["000333", "2020-10-11", 110.0, 115.0, 118.0, 108.0],
            ["000333", "2020-10-12", 105.0, 110.0, 113.0, 103.0],
            ["600519", "2020-10-10", 2000.0, 2050.0, 2070.0, 1980.0],
            ["600519", "2020-10-11", 2050.0, 2100.0, 2120.0, 2030.0],
            ["600519", "2020-10-12", 2030.0, 2080.0, 2100.0, 2010.0],
        ],
        columns=["code", "timestamp", "open", "close", "high", "low"],
    ).set_index("code")


@pytest.fixture
def adjust_factors():
    return AdjustFactors(
        pd.DataFrame(
            [
                ["000333", "1900-01-01", 1.0],
                ["000333", "2019-01-01", 2.0],
                ["000333", "2020-10-11", 2.2],
                ["000333", "3000-01-01", np.nan],
                ["600519", "1900-01-01", 1],
                ["600519", "2019-01-01", 1.5],
                ["600519", "2020-10-10", 1.7],
                ["600519", "3000-01-01", np.nan],
            ],
            columns=["code", "timestamp", "hfq_factor"],
        ).set_index("code")
    )


@pytest.mark.parametrize(
    "stock_code,date,hfq_factor",
    [
        ("000333", "2020-10-10", 2.0),
        ("000333", "2020-10-11", 2.2),
        ("000333", "2020-10-12", 2.2),
        ("600519", "2020-10-10", 1.7),
        ("600519", "2020-10-11", 1.7),
        ("600519", "2020-10-12", 1.7),
    ],
)
def test_adjust_factors_history_prices(
    adjust_factors: AdjustFactors,
    sample_stock_day_k: pd.DataFrame,
    stock_code: str,
    date: str,
    hfq_factor: float,
):
    orig_df = sample_stock_day_k.loc[stock_code].query("timestamp == @date")
    adj_df = adjust_factors.backward_adjust_history_prices(stock_code, orig_df.copy())
    assert_series_equal(adj_df["close"], orig_df["close"] * hfq_factor)


def test_assign_factor_value_to_day():
    timestamps = [1, 2, 3]
    fac_ts = [1, 2, 3, 4, 5]
    fac_vals = [2.0, 2.2, 2.5, 1.5, 1.7]

    factors = _assign_factor_value_to_day(fac_ts, fac_vals, timestamps)
    assert factors == [2.0, 2.2, 2.5]  # Expected adjusted factors for timestamps


@pytest.mark.parametrize(
    "stock_code,latest_hfq_factor",
    [
        ("000333", 2.2),
        ("600519", 1.7),
    ],
)
def test_adjust_factors_backward_adjust_stocks_latest_prices(
    adjust_factors: AdjustFactors,
    sample_stock_day_k: pd.DataFrame,
    stock_code: str,
    latest_hfq_factor: float,
):
    max_day = sample_stock_day_k["timestamp"].max()
    bars_df = sample_stock_day_k.query("timestamp == @max_day")

    adjusted_bars = adjust_factors.backward_adjust_stocks_latest_prices(bars_df.copy())

    assert (
        pytest.approx(bars_df.loc[stock_code, "close"] * latest_hfq_factor)
        == adjusted_bars.loc[stock_code, "close"]
    )
