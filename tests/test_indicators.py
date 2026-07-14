from datetime import date

import numpy as np
import polars as pl
import pytest
import talib
from talib._ta_lib import MA_Type

from tradepy.strategy.indicators import (
    BIAS,
    KDJ,
    SMA,
    CrossSectionIndicator,
    IndicatorValue,
    Lag,
    Percentile,
    Rank,
    Take,
)


def eval_indicator(value: IndicatorValue, df: pl.DataFrame) -> np.ndarray:
    assert isinstance(value, pl.Expr)
    return df.select(value.alias("_"))["_"].to_numpy().astype(np.float64)


def eval_cross_section(
    indicator: CrossSectionIndicator, df: pl.DataFrame
) -> pl.DataFrame:
    expr = indicator.resolve()
    assert isinstance(expr, pl.Expr)
    return df.with_columns(expr.over(*indicator.partition_by).alias("_out"))


def talib_lag(values: np.ndarray, periods: int) -> np.ndarray:
    out = np.full_like(values, np.nan, dtype=np.float64)
    if periods > 0:
        out[periods:] = values[:-periods]
    return out


def assert_tail_allclose(
    actual: np.ndarray,
    expected: np.ndarray,
    *,
    tail: int = 50,
    rtol: float = 1e-5,
    atol: float = 1e-8,
) -> None:
    valid = ~np.isnan(actual) & ~np.isnan(expected)
    np.testing.assert_allclose(
        actual[valid][-tail:],
        expected[valid][-tail:],
        rtol=rtol,
        atol=atol,
    )


@pytest.mark.parametrize("period", [5, 20, 60])
def test_sma(fake_klines_convergence: pl.DataFrame, period: int) -> None:
    close = fake_klines_convergence["close"].to_numpy().astype(np.float64)
    expected = talib.SMA(close, timeperiod=period)
    actual = eval_indicator(SMA(period).resolve(), fake_klines_convergence)
    assert_tail_allclose(actual, expected)


def test_sma_warmup(
    fake_klines_short: pl.DataFrame,
) -> None:
    period = 3
    close = fake_klines_short["close"].to_numpy().astype(np.float64)
    expected = talib.SMA(close, timeperiod=period)
    actual = eval_indicator(SMA(period).resolve(), fake_klines_short)
    np.testing.assert_allclose(actual, expected, equal_nan=True)


def test_sma_custom_column(
    fake_klines_convergence: pl.DataFrame,
) -> None:
    period = 20
    open_ = fake_klines_convergence["open"].to_numpy().astype(np.float64)
    expected = talib.SMA(open_, timeperiod=period)
    actual = eval_indicator(
        SMA(period, column="open").resolve(),
        fake_klines_convergence,
    )
    assert_tail_allclose(actual, expected)


def test_sma_ref_sma_pipeline(
    fake_klines_convergence: pl.DataFrame,
) -> None:
    lag_period = 2
    sma_period = 5
    base_period = 10
    close = fake_klines_convergence["close"].to_numpy().astype(np.float64)
    expected = talib.SMA(
        talib_lag(talib.SMA(close, timeperiod=base_period), lag_period),
        timeperiod=sma_period,
    )
    actual = eval_indicator(
        (SMA(base_period) | Lag(lag_period) | SMA(sma_period)).resolve(),
        fake_klines_convergence,
    )
    assert_tail_allclose(actual, expected)


@pytest.mark.parametrize(
    ("name", "period"),
    [("fast", 6), ("mid", 12), ("slow", 24)],
)
def test_bias_converges_with_talib_sma(
    fake_klines_convergence: pl.DataFrame,
    name: str,
    period: int,
) -> None:
    close = fake_klines_convergence["close"].to_numpy().astype(np.float64)
    ma = talib.SMA(close, timeperiod=period)
    expected = 100.0 * (close - ma) / ma
    actual = eval_indicator(
        (BIAS() | Take(name)).resolve(),
        fake_klines_convergence,
    )
    assert_tail_allclose(actual, expected)


@pytest.mark.parametrize("period", [5, 20])
def test_bias_pipeline(
    fake_klines_convergence: pl.DataFrame,
    period: int,
) -> None:
    close = fake_klines_convergence["close"].to_numpy().astype(np.float64)
    base = talib.SMA(close, timeperiod=period)
    ma = talib.SMA(base, timeperiod=12)
    expected = 100.0 * (base - ma) / ma
    actual = eval_indicator(
        (SMA(period) | BIAS() | Take("mid")).resolve(),
        fake_klines_convergence,
    )
    assert_tail_allclose(actual, expected)


@pytest.mark.parametrize("name", ["k", "d", "j"])
def test_kdj_converges_with_talib_stoch(
    fake_klines_convergence: pl.DataFrame,
    name: str,
) -> None:
    high = fake_klines_convergence["high"].to_numpy().astype(np.float64)
    low = fake_klines_convergence["low"].to_numpy().astype(np.float64)
    close = fake_klines_convergence["close"].to_numpy().astype(np.float64)
    fastk, _ = talib.STOCHF(
        high,
        low,
        close,
        fastk_period=9,
        fastd_period=1,
        fastd_matype=MA_Type.SMA,
    )
    k = talib.EMA(fastk, timeperiod=5)
    d = talib.EMA(k, timeperiod=5)
    expected_by_name = {
        "k": k,
        "d": d,
        "j": 3.0 * k - 2.0 * d,
    }
    actual = eval_indicator(
        (KDJ() | Take(name)).resolve(),
        fake_klines_convergence,
    )
    assert_tail_allclose(actual, expected_by_name[name], rtol=1e-4, atol=1e-6)


@pytest.mark.parametrize("lag_period", [1, 2, 5])
def test_ref(fake_klines_convergence: pl.DataFrame, lag_period: int) -> None:
    sma_period = 20
    close = fake_klines_convergence["close"].to_numpy().astype(np.float64)
    expected = talib_lag(talib.SMA(close, timeperiod=sma_period), lag_period)
    actual = eval_indicator(
        (SMA(sma_period) | Lag(lag_period)).resolve(),
        fake_klines_convergence,
    )
    assert_tail_allclose(actual, expected)


def test_ref_requires_upstream() -> None:
    with pytest.raises(ValueError, match="requires an upstream indicator"):
        Lag().resolve()


def test_cross_section_indicators_are_not_composable() -> None:
    with pytest.raises(TypeError, match="not composable"):
        _ = SMA(2) | Rank()

    with pytest.raises(TypeError, match="not composable"):
        _ = Rank() | Lag(1)

    with pytest.raises(TypeError, match="not composable"):
        _ = SMA(2) | Percentile()


def test_rank_partitions_by_date() -> None:
    df = pl.DataFrame(
        {
            "code": ["A", "B", "C", "A", "B", "C"],
            "date": [date(2024, 1, 1)] * 3 + [date(2024, 1, 2)] * 3,
            "pe": [30.0, 10.0, 20.0, 5.0, 15.0, 25.0],
        }
    )
    out = eval_cross_section(Rank(column="pe"), df)
    day1 = out.filter(pl.col("date") == date(2024, 1, 1)).sort("code")
    day2 = out.filter(pl.col("date") == date(2024, 1, 2)).sort("code")

    assert day1["_out"].to_list() == [3.0, 1.0, 2.0]
    assert day2["_out"].to_list() == [1.0, 2.0, 3.0]


def test_rank_partitions_by_date_and_industry() -> None:
    df = pl.DataFrame(
        {
            "code": ["A", "B", "C", "D"],
            "date": [date(2024, 1, 1)] * 4,
            "industry_code": ["X", "X", "Y", "Y"],
            "pe": [30.0, 10.0, 5.0, 50.0],
        }
    )
    out = eval_cross_section(Rank(column="pe", over="industry_code"), df)
    by_code = {row["code"]: row["_out"] for row in out.to_dicts()}

    assert by_code == {"A": 2.0, "B": 1.0, "C": 1.0, "D": 2.0}


def test_percentile_partitions_by_date() -> None:
    codes = [f"{i:03d}" for i in range(100)]
    df = pl.DataFrame(
        {
            "code": codes,
            "date": [date(2024, 1, 1)] * 100,
            "pe": [float(i) for i in range(1, 101)],
        }
    )
    out = eval_cross_section(Percentile(column="pe"), df)
    assert out.sort("pe")["_out"].to_list() == [float(i) for i in range(1, 101)]


def test_percentile_step() -> None:
    df = pl.DataFrame(
        {
            "code": ["A", "B", "C", "D"],
            "date": [date(2024, 1, 1)] * 4,
            "pe": [1.0, 2.0, 3.0, 4.0],
        }
    )
    out = eval_cross_section(Percentile(column="pe", step=25), df)
    by_code = {row["code"]: row["_out"] for row in out.to_dicts()}
    assert by_code == {"A": 25.0, "B": 50.0, "C": 75.0, "D": 100.0}


def test_percentile_partitions_by_date_and_industry() -> None:
    df = pl.DataFrame(
        {
            "code": ["A", "B", "C", "D"],
            "date": [date(2024, 1, 1)] * 4,
            "industry_code": ["X", "X", "Y", "Y"],
            "pe": [30.0, 10.0, 5.0, 50.0],
        }
    )
    out = eval_cross_section(
        Percentile(column="pe", step=50, over="industry_code"), df
    )
    by_code = {row["code"]: row["_out"] for row in out.to_dicts()}
    assert by_code == {"A": 100.0, "B": 50.0, "C": 50.0, "D": 100.0}
