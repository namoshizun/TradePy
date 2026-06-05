from abc import ABC

import polars as pl

from tradepy.decors import indicator


def _row_index() -> pl.Expr:
    return pl.int_range(pl.len())


def _mask_warmup(value: pl.Expr, warmup: int) -> pl.Expr:
    return pl.when(_row_index() < warmup).then(None).otherwise(value)


def _fast_ewm(value: pl.Expr, *, alpha: float, warmup: int) -> pl.Expr:
    """Recursive EMA (``adjust=False``) with the unreliable warm-up masked off.

    Unlike TA-Lib we seed from the first observation instead of a simple moving
    average, which keeps this to a single native polars kernel (no rolling-mean
    seed, no per-row masking). The seed discrepancy decays geometrically by a
    factor of ``1 - alpha`` per bar, so ``warmup`` nulls out the leading region
    before the result is indistinguishable from TA-Lib.
    """
    ewm = value.ewm_mean(alpha=alpha, adjust=False, ignore_nulls=True)
    return _mask_warmup(ewm, warmup)


WARMUP_FACTORS = {
    "macd": 3,
    "atr": 2,
    "rsi": 2,
}


class StrategyBase(ABC):
    @indicator(not_na=True)
    def sma5(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=5)

    @indicator(not_na=True)
    def sma10(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=10)

    @indicator(not_na=True)
    def sma20(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=20)

    @indicator(not_na=True)
    def sma30(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=30)

    @indicator(not_na=True)
    def sma60(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=60)

    @indicator(not_na=True)
    def sma120(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=120)

    @indicator(not_na=True)
    def sma250(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=250)

    @indicator(not_na=True)
    def macd(self) -> tuple[pl.Expr, pl.Expr, pl.Expr]:
        fast_period = 12
        slow_period = 26
        signal_period = 9
        warmup = WARMUP_FACTORS["macd"] * slow_period

        close = pl.col("close")
        fast_ema = close.ewm_mean(alpha=2 / (fast_period + 1), adjust=False)
        slow_ema = close.ewm_mean(alpha=2 / (slow_period + 1), adjust=False)
        macd_line = fast_ema - slow_ema
        signal = macd_line.ewm_mean(alpha=2 / (signal_period + 1), adjust=False)
        return (
            _mask_warmup(macd_line, warmup).alias("macd"),
            _mask_warmup(signal, warmup).alias("macd_signal"),
            _mask_warmup(macd_line - signal, warmup).alias("macd_hist"),
        )

    @indicator(not_na=True)
    def typical_price(self) -> pl.Expr:
        return (pl.col("high") + pl.col("low") + pl.col("close")) / 3

    @indicator(not_na=True)
    def atr(self) -> pl.Expr:
        period = 14
        prev_close = pl.col("close").shift(1)
        true_range = pl.max_horizontal(
            pl.col("high") - pl.col("low"),
            (pl.col("high") - prev_close).abs(),
            (pl.col("low") - prev_close).abs(),
        )
        return _fast_ewm(
            true_range, alpha=1 / period, warmup=WARMUP_FACTORS["atr"] * period
        )

    @indicator(not_na=True)
    def rsi(self) -> tuple[pl.Expr, ...]:
        def _rsi(period: int) -> pl.Expr:
            warmup = WARMUP_FACTORS["rsi"] * period
            delta = pl.col("close").diff()
            gain = delta.clip(lower_bound=0.0)
            loss = (-delta).clip(lower_bound=0.0)
            avg_gain = _fast_ewm(gain, alpha=1 / period, warmup=warmup)
            avg_loss = _fast_ewm(loss, alpha=1 / period, warmup=warmup)
            total = avg_gain + avg_loss
            return (
                pl.when(total == 0)
                .then(0.0)
                .otherwise(100.0 * avg_gain / total)
            )

        return (
            _rsi(6).alias("rsi_fast"),
            _rsi(12).alias("rsi_mid"),
            _rsi(24).alias("rsi_slow"),
        )

    @indicator(not_na=True)
    def boll(self) -> tuple[pl.Expr, pl.Expr, pl.Expr]:
        boll_window = 20
        k = 2.0
        mid = pl.col("close").rolling_mean(window_size=boll_window)
        boll_std = pl.col("close").rolling_std(
            window_size=boll_window, ddof=0
        )  # population σ
        upper = mid + k * boll_std
        lower = mid - k * boll_std
        return (
            mid.alias("boll_mid"),
            upper.alias("boll_upper"),
            lower.alias("boll_lower"),
        )
