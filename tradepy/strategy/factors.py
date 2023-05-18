import talib
import numpy as np
import pandas as pd
import numba as nb
from typing import Literal


Series = pd.Series


# ----
# SDKJ
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


# -----------------
# Linear Regression
@nb.njit
def _window_sum(X, dx):
    cumsum = np.cumsum(X)
    s = cumsum[dx - 1 :]
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


def linear_regression_slope(values: Series, dx: int):
    # NOTE: magnitudes faster than applying `sklearn.linear_model.LinearRegression` to a rolling window
    def fit(y):
        if np.isnan(y).all():
            return np.nan
        X = np.arange(len(y))
        slope, _ = _linear_regression(X, y)
        return slope

    return values.rolling(dx).apply(fit, raw=True)


# ----------------------------
# Release of restricted shares
@nb.njit(cache=True)
def _get_nearest_release_date_indexes(
    direction: str, query_dates: list[str], release_dates: list[str]
) -> list[int]:
    assert direction in ("last", "next")
    indices = [-1 for i in range(len(query_dates))]

    if direction == "next":
        j = 0
    else:
        j = len(release_dates) - 1

    for i, date in enumerate(query_dates):
        while 0 <= j <= len(release_dates) - 1:
            if direction == "next":
                if release_dates[j] >= date:
                    indices[i] = j
                    break
                j += 1
            elif direction == "last":
                if release_dates[j] <= date:
                    indices[i] = j
                    break
                j -= 1

    return [i for i in indices if i != -1]


def nearest_release_of_restricted_shares(
    direction: Literal["last", "next"], query_dates: Series, releases_df: pd.DataFrame
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    indexes = _get_nearest_release_date_indexes(
        direction,
        nb.typed.List(query_dates.tolist()),
        nb.typed.List(releases_df["timestamp"].tolist()),
    )

    release_dates = releases_df.loc[indexes, "timestamp"].values
    shares_category = releases_df.loc[indexes, "shares_category"].values
    pct_shares = releases_df.loc[indexes, "pct_shares"].values

    pad_len = len(query_dates) - len(indexes)
    if pad_len > 0:
        # FIXME: pad direction will be different based on the query direction
        pad_arr = np.array([np.nan] * pad_len)
        release_dates = np.concatenate([release_dates, pad_arr])
        shares_category = np.concatenate([shares_category, pad_arr])
        pct_shares = np.concatenate([pct_shares, pad_arr])

    return release_dates, shares_category, pct_shares
