import talib
import numpy as np
import pandas as pd
import numba as nb


Series = pd.Series


def skdj(close: Series, low: Series, high: Series, n: int = 15, m: int = 5):
    lowv = low.rolling(n).min()
    highv = high.rolling(n).max()
    try:
        rsv = talib.EMA((close - lowv) / (highv - lowv) * 100, m)
        K = talib.EMA(rsv, m)
        D = talib.SMA(K, m)
    except Exception:
        return np.array([np.nan] * len(close))
    return K, D


@nb.njit
def _window_sum(X, dx):
    cumsum = np.cumsum(X)
    s = cumsum[dx - 1:]
    s[1:] -= cumsum[:-dx]
    return s


@nb.njit
def _linear_regression(X, y):
    dx = len(X)
    sx = _window_sum(X, dx)
    sy = _window_sum(y, dx)
    sx2 = _window_sum(X**2, dx)
    sxy = _window_sum(X * y, dx)  # type: ignore
    slope = (dx * sxy - sx * sy) / (dx * sx2 - sx**2)
    intercept = (sy - slope * sx) / dx
    return slope[0], intercept[0]


def lin_reg_slope(values: Series, dx: int):
    # NOTE: magnitudes faster than applying `sklearn.linear_model.LinearRegression` to a rolling window
    def fit(y):
        if np.isnan(y).all():
            return np.nan
        X = np.arange(len(y))
        slope, _ = _linear_regression(X, y)
        return slope

    return values.rolling(dx).apply(fit, raw=True)
