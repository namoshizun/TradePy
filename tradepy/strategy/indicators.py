import abc
from dataclasses import dataclass
from typing import Self, TypeAlias

import polars as pl

IndicatorValue: TypeAlias = pl.Expr | dict[str, pl.Expr]


def _row_index() -> pl.Expr:
    return pl.int_range(pl.len())


def _mask_warmup(value: pl.Expr, warmup: int) -> pl.Expr:
    return pl.when(_row_index() < warmup).then(None).otherwise(value)


def _fast_ewm(value: pl.Expr, *, alpha: float, warmup: int) -> pl.Expr:
    """Recursive EMA (``adjust=False``) with the unreliable warm-up masked off."""
    ewm = value.ewm_mean(alpha=alpha, adjust=False, ignore_nulls=True)
    return _mask_warmup(ewm, warmup)


WARMUP_FACTORS = {
    "macd": 3,
    "atr": 2,
    "rsi": 2,
    "kdj": 4,
}


class Indicator(float, abc.ABC):
    def __new__(cls, *_args: object, **_kwargs: object) -> Self:
        return float.__new__(cls, float("nan"))

    def __ne__(self, other: object) -> bool:
        return not self == other

    @property
    def not_na(self) -> bool:
        return True

    def __or__(self, step: object) -> "IndicatorPipeline":
        if not isinstance(step, Indicator):
            raise TypeError(
                f"Cannot compose Indicator with {type(step).__name__}"
            )
        return IndicatorPipeline((self, step))

    def resolve(self) -> IndicatorValue:
        return self._eval(None)

    @abc.abstractmethod
    def _eval(self, value: IndicatorValue | None) -> IndicatorValue:
        raise NotImplementedError


@dataclass(frozen=True)
class IndicatorPipeline(Indicator):
    steps: tuple[Indicator, ...]

    @property
    def not_na(self) -> bool:
        return all(step.not_na for step in self.steps)

    def __or__(self, step: object) -> "IndicatorPipeline":
        if not isinstance(step, Indicator):
            raise TypeError(
                f"Cannot compose Indicator with {type(step).__name__}"
            )
        return IndicatorPipeline((*self.steps, step))

    def _eval(self, value: IndicatorValue | None) -> IndicatorValue:
        current: IndicatorValue | None = value
        for step in self.steps:
            current = step._eval(current)
        if current is None:
            raise ValueError("Indicator pipeline requires at least one step")
        return current


def _single_input(
    value: IndicatorValue | None,
    *,
    default_column: str | None,
    operator: str,
) -> pl.Expr:
    if value is None:
        if default_column is None:
            raise ValueError(f"{operator} requires an upstream indicator")
        return pl.col(default_column)

    if isinstance(value, dict):
        raise ValueError(
            f"{operator} requires a single input; use Take(...) first"
        )

    return value


def _root_only(value: IndicatorValue | None, operator: str) -> None:
    if value is not None:
        raise ValueError(f"{operator} cannot consume an upstream indicator")


@dataclass(frozen=True)
class Take(Indicator):
    name: str

    def _eval(self, value: IndicatorValue | None) -> pl.Expr:
        if value is None:
            raise ValueError("Take requires an upstream indicator")
        if not isinstance(value, dict):
            raise ValueError(
                "Take can only select from a multi-output indicator"
            )
        return value[self.name]


@dataclass(frozen=True)
class Lag(Indicator):
    periods: int = 1

    def _eval(self, value: IndicatorValue | None) -> pl.Expr:
        expr = _single_input(
            value,
            default_column=None,
            operator=type(self).__name__,
        )
        return expr.shift(self.periods)


@dataclass(frozen=True)
class SMA(Indicator):
    period: int
    column: str = "close"

    def _eval(self, value: IndicatorValue | None) -> pl.Expr:
        expr = _single_input(
            value,
            default_column=self.column,
            operator=type(self).__name__,
        )
        return expr.rolling_mean(window_size=self.period)


@dataclass(frozen=True)
class MACD(Indicator):
    fast: int = 12
    slow: int = 26
    signal: int = 9
    column: str = "close"

    def _eval(self, value: IndicatorValue | None) -> dict[str, pl.Expr]:
        _root_only(value, type(self).__name__)

        warmup = WARMUP_FACTORS["macd"] * self.slow
        close = pl.col(self.column)
        fast_ema = close.ewm_mean(alpha=2 / (self.fast + 1), adjust=False)
        slow_ema = close.ewm_mean(alpha=2 / (self.slow + 1), adjust=False)
        macd_line = fast_ema - slow_ema
        signal = macd_line.ewm_mean(alpha=2 / (self.signal + 1), adjust=False)
        return {
            "dif": _mask_warmup(macd_line, warmup),
            "dea": _mask_warmup(signal, warmup),
            "hist": _mask_warmup(macd_line - signal, warmup),
        }


@dataclass(frozen=True)
class RSI(Indicator):
    fast: int = 6
    mid: int = 12
    slow: int = 24
    column: str = "close"

    def _eval(self, value: IndicatorValue | None) -> dict[str, pl.Expr]:
        _root_only(value, type(self).__name__)
        close = pl.col(self.column)

        def rsi(period: int) -> pl.Expr:
            warmup = WARMUP_FACTORS["rsi"] * period
            delta = close.diff()
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

        return {
            "fast": rsi(self.fast),
            "mid": rsi(self.mid),
            "slow": rsi(self.slow),
        }


@dataclass(frozen=True)
class BIAS(Indicator):
    fast: int = 6
    mid: int = 12
    slow: int = 24
    column: str = "close"

    def _eval(self, value: IndicatorValue | None) -> dict[str, pl.Expr]:
        expr = _single_input(
            value,
            default_column=self.column,
            operator=type(self).__name__,
        )

        def bias(period: int) -> pl.Expr:
            ma = expr.rolling_mean(window_size=period)
            return 100.0 * (expr - ma) / ma

        return {
            "fast": bias(self.fast),
            "mid": bias(self.mid),
            "slow": bias(self.slow),
        }


@dataclass(frozen=True)
class BOLL(Indicator):
    window: int = 20
    k: float = 2.0
    column: str = "close"

    def _eval(self, value: IndicatorValue | None) -> dict[str, pl.Expr]:
        _root_only(value, type(self).__name__)

        close = pl.col(self.column)
        mid = close.rolling_mean(window_size=self.window)
        std = close.rolling_std(window_size=self.window, ddof=0)
        return {
            "mid": mid,
            "upper": mid + self.k * std,
            "lower": mid - self.k * std,
        }


@dataclass(frozen=True)
class KDJ(Indicator):
    period: int = 9
    k_period: int = 3
    d_period: int = 3

    def _eval(self, value: IndicatorValue | None) -> dict[str, pl.Expr]:
        _root_only(value, type(self).__name__)

        low = pl.col("low").rolling_min(window_size=self.period)
        high = pl.col("high").rolling_max(window_size=self.period)
        rsv = 100.0 * (pl.col("close") - low) / (high - low)
        warmup = self.period + WARMUP_FACTORS["kdj"] * max(
            self.k_period, self.d_period
        )
        k = _fast_ewm(rsv, alpha=1 / self.k_period, warmup=warmup)
        d = _fast_ewm(k, alpha=1 / self.d_period, warmup=warmup)

        return {
            "k": k,
            "d": d,
            "j": 3.0 * k - 2.0 * d,
        }


@dataclass(frozen=True)
class ATR(Indicator):
    period: int = 14

    def _eval(self, value: IndicatorValue | None) -> pl.Expr:
        _root_only(value, type(self).__name__)

        prev_close = pl.col("close").shift(1)
        true_range = pl.max_horizontal(
            pl.col("high") - pl.col("low"),
            (pl.col("high") - prev_close).abs(),
            (pl.col("low") - prev_close).abs(),
        )
        return _fast_ewm(
            true_range,
            alpha=1 / self.period,
            warmup=WARMUP_FACTORS["atr"] * self.period,
        )
