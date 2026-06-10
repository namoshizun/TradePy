import numpy as np
import polars as pl
import pytest
import talib
from talib._ta_lib import MA_Type

from tradepy.strategy.indicators import (
    BIAS,
    KDJ,
    SMA,
    IndicatorValue,
    Lag,
    Take,
)


def eval_indicator(value: IndicatorValue, df: pl.DataFrame) -> np.ndarray:
    assert isinstance(value, pl.Expr)
    return df.select(value.alias("_"))["_"].to_numpy().astype(np.float64)


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
