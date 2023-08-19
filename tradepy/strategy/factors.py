import talib
import numpy as np
import pandas as pd
import numba as nb
from typing import Literal
from tradepy.decorators import tag


Series = pd.Series


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


class FactorsMixin:
    def __custom_params(self, name, default):
        try:
            return getattr(self, name)
        except KeyError:
            return default

    @tag(notna=True)
    def sma5(self, close: Series):
        """
        简单移动5均
        """
        return talib.SMA(close, 5)

    @tag(notna=True)
    def sma10(self, close: Series):
        """
        简单移动10均
        """
        return talib.SMA(close, 10)

    @tag(notna=True)
    def sma20(self, close: Series):
        """
        简单移动20均
        """
        return talib.SMA(close, 20)

    @tag(notna=True)
    def sma30(self, close: Series):
        """
        简单移动30均
        """
        return talib.SMA(close, 30)

    @tag(notna=True)
    def sma60(self, close: Series):
        """
        简单移动60均
        """
        return talib.SMA(close, 60)

    @tag(notna=True)
    def sma120(self, close: Series):
        """
        简单移动120均
        """
        return talib.SMA(close, 120)

    @tag(notna=True)
    def sma250(self, close: Series):
        """
        简单移动250均
        """
        return talib.SMA(close, 250)

    @tag(notna=True)
    def ema5(self, close: Series):
        """
        指数移动5均
        """
        return talib.EMA(close, 5)

    @tag(notna=True)
    def ema10(self, close: Series):
        """
        指数移动10均
        """
        return talib.EMA(close, 10)

    @tag(notna=True)
    def ema20(self, close: Series):
        """
        指数移动20均
        """
        return talib.EMA(close, 20)

    @tag(notna=True)
    def ema30(self, close: Series):
        """
        指数移动30均
        """
        return talib.EMA(close, 30)

    @tag(notna=True)
    def ema60(self, close: Series):
        """
        指数移动60均
        """
        return talib.EMA(close, 60)

    @tag(notna=True)
    def ema120(self, close: Series):
        """
        指数移动120均
        """
        return talib.EMA(close, 120)

    @tag(notna=True)
    def ema250(self, close: Series):
        """
        指数移动250均
        """
        return talib.EMA(close, 250)

    @tag(outputs=["sdj_k", "sdj_d"], notna=True)
    def skdj(self, close: Series, low: Series, high: Series):
        """
        SKDJ - 慢速随机指标

        输出指标: sdj_k, sdj_d
        """
        n: int = 15
        m: int = 5
        lowv = low.rolling(n).min()
        highv = high.rolling(n).max()

        try:
            rsv = talib.EMA((close - lowv) / (highv - lowv) * 100, m)
            K = talib.EMA(rsv, m)
            D = talib.SMA(K, m)
        except Exception:
            nans = np.array([np.nan] * len(close))
            return nans, nans
        return K, D

    @tag(notna=True)
    def macd(self, close: Series):
        """
        MACD指标
        """
        _, __, hist = talib.MACD(close)
        return hist

    @tag(outputs=["boll_lower", "boll_middle", "boll_upper"], notna=True)
    def boll(self, close):
        """
        布林带, 自定义参数:

        - boll_period: 均值周期, 默认20
        - boll_dev_up: 上轨标准差, 默认2
        - boll_dev_down: 下轨标准差, 默认2

        输出指标: boll_lower, boll_middle, boll_upper
        """
        return talib.BBANDS(
            close,
            timeperiod=self.__custom_params("boll_period", 20),
            nbdevup=self.__custom_params("boll_dev_up", 2),
            nbdevdn=self.__custom_params("boll_dev_down", 2),
        )

    @tag(outputs=["rsi_fast", "rsi_mid", "rsi_slow"], notna=True)
    def rsi(self, close):
        """
        RSI, 自定义参数:

        - rsi_fast_period: 快周期, 默认6
        - rsi_mid_period: 中周期, 默认12
        - rsi_slow_period: 慢周期, 默认24

        输出指标: rsi_fast, rsi_mid, rsi_slow
        """
        fast = talib.RSI(close, timeperiod=self.__custom_params("rsi_fast_period", 6))
        mid = talib.RSI(close, timeperiod=self.__custom_params("rsi_mid_period", 12))
        slow = talib.RSI(close, timeperiod=self.__custom_params("rsi_slow_period", 24))
        return fast, mid, slow
