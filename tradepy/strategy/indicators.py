import abc
import math
from dataclasses import dataclass, field
from typing import ClassVar, Literal, Self, TypeAlias

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


@dataclass(frozen=True)
class Indicator(float, abc.ABC):
    """A composable indicator step.

    Steps are chained with ``|`` into an :class:`IndicatorPipeline` and
    evaluated by :meth:`resolve`. Each step receives the upstream value
    (``None`` at the pipeline root) and produces either a single series
    or a named multi-output mapping, which must be narrowed with
    :class:`Take` before feeding a single-input step.

    Subclasses ``float`` so instances can serve as typed default values
    of strategy ``buy``/``sell`` parameters.
    """

    column: str = field(default="close", kw_only=True)
    not_na: bool = field(default=True, kw_only=True)

    def __new__(cls, *_args: object, **_kwargs: object) -> Self:
        return float.__new__(cls, float("nan"))

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __or__(self, step: object) -> "IndicatorPipeline":
        if not isinstance(step, Indicator):
            raise TypeError(
                f"Cannot compose Indicator with {type(step).__name__}"
            )
        if isinstance(step, CrossSectionIndicator) or any(
            isinstance(s, CrossSectionIndicator) for s in step._flatten()
        ):
            raise TypeError("Cross-sectional indicators are not composable")
        return IndicatorPipeline((*self._flatten(), *step._flatten()))

    def _flatten(self) -> tuple["Indicator", ...]:
        return (self,)

    @property
    def partition_by(self) -> tuple[str, ...]:
        """Window partition used when evaluating this indicator."""
        return ("code",)

    def resolve(self) -> IndicatorValue:
        return self._pipe(None)

    @abc.abstractmethod
    def _pipe(self, value: IndicatorValue | None) -> IndicatorValue:
        """Evaluate this step against the upstream value."""
        raise NotImplementedError


@dataclass(frozen=True)
class IndicatorPipeline(Indicator):
    steps: tuple[Indicator, ...]

    def __post_init__(self) -> None:
        if not all(step.not_na == self.steps[0].not_na for step in self.steps):
            raise ValueError("All steps must have the same not_na")

    def _flatten(self) -> tuple[Indicator, ...]:
        return self.steps

    @property
    def partition_by(self) -> tuple[str, ...]:
        return self.steps[-1].partition_by

    def _pipe(self, value: IndicatorValue | None) -> IndicatorValue:
        for step in self.steps:
            value = step._pipe(value)
        if value is None:
            raise ValueError("Indicator pipeline requires at least one step")
        return value


@dataclass(frozen=True)
class SeriesIndicator(Indicator):
    """An indicator computed from a single input series.

    At the pipeline root the input defaults to the adjusted ``column``
    price. ``requires_upstream`` marks transforms that are meaningless
    without a piped input (e.g. :class:`Lag`).
    """

    requires_upstream: ClassVar[bool] = False

    def _pipe(self, value: IndicatorValue | None) -> IndicatorValue:
        if isinstance(value, dict):
            raise ValueError(
                f"{type(self).__name__} expects a single series; "
                "pipe Take(...) first"
            )
        if value is None:
            if self.requires_upstream:
                raise ValueError(
                    f"{type(self).__name__} requires an upstream indicator"
                )
            value = pl.col(self.column)
        return self.compute(value)

    @abc.abstractmethod
    def compute(self, value: pl.Expr) -> IndicatorValue:
        raise NotImplementedError


@dataclass(frozen=True)
class CrossSectionIndicator(SeriesIndicator):
    """An indicator computed across stocks within each date.

    Evaluated with ``.over("date", *over)``. Use ``over`` to add grouping
    columns (e.g. ``over="industry_code"`` for within-industry ranks).

    Not composable with ``|`` (series and cross-section windows cannot be
    nested in a single Polars expression).
    """

    over: str | tuple[str, ...] = field(default=(), kw_only=True)

    def __or__(self, step: object) -> "IndicatorPipeline":
        raise TypeError("Cross-sectional indicators are not composable")

    @property
    def partition_by(self) -> tuple[str, ...]:
        extra = (self.over,) if isinstance(self.over, str) else tuple(self.over)
        return ("date", *extra)


# -- Composable indicators ---------------------------------------------------
@dataclass(frozen=True)
class Take(Indicator):
    """Select one output of a multi-output indicator."""

    name: str

    def _pipe(self, value: IndicatorValue | None) -> pl.Expr:
        if value is None:
            raise ValueError("Take requires an upstream indicator")
        if not isinstance(value, dict):
            raise ValueError(
                "Take can only select from a multi-output indicator"
            )
        return value[self.name]


@dataclass(frozen=True)
class Lag(SeriesIndicator):
    periods: int = 1

    requires_upstream: ClassVar[bool] = True

    def compute(self, value: pl.Expr) -> pl.Expr:
        return value.shift(self.periods)


@dataclass(frozen=True)
class Rank(CrossSectionIndicator):
    method: Literal["average", "min", "max", "dense", "ordinal"] = "average"
    descending: bool = False

    def compute(self, value: pl.Expr) -> pl.Expr:
        return value.rank(method=self.method, descending=self.descending)


@dataclass(frozen=True)
class Percentile(CrossSectionIndicator):
    step: int = 1
    descending: bool = False

    def compute(self, value: pl.Expr) -> pl.Expr:
        rank = value.rank(method="ordinal", descending=self.descending)
        bins = 100 // self.step
        return ((rank * bins + pl.len() - 1) // pl.len()) * self.step


# -- Single series indicators ---------------------------------------------------


@dataclass(frozen=True)
class OriginalPrice(SeriesIndicator):
    def compute(self, value: pl.Expr) -> pl.Expr:
        return value / pl.col("adj_factor")


@dataclass(frozen=True)
class SMA(SeriesIndicator):
    period: int

    def compute(self, value: pl.Expr) -> pl.Expr:
        return value.rolling_mean(window_size=self.period)


@dataclass(frozen=True)
class MACD(SeriesIndicator):
    fast: int = 12
    slow: int = 26
    signal: int = 9

    WARMUP_FACTOR: ClassVar[int] = 3

    def compute(self, value: pl.Expr) -> dict[str, pl.Expr]:
        def ema(expr: pl.Expr, period: int) -> pl.Expr:
            return expr.ewm_mean(alpha=2 / (period + 1), adjust=False)

        warmup = self.WARMUP_FACTOR * self.slow
        dif = ema(value, self.fast) - ema(value, self.slow)
        dea = ema(dif, self.signal)
        return {
            "dif": _mask_warmup(dif, warmup),
            "dea": _mask_warmup(dea, warmup),
            "hist": _mask_warmup(dif - dea, warmup),
        }


@dataclass(frozen=True)
class RSI(SeriesIndicator):
    fast: int = 6
    mid: int = 12
    slow: int = 24

    WARMUP_FACTOR: ClassVar[int] = 2

    def compute(self, value: pl.Expr) -> dict[str, pl.Expr]:
        def _rsi(period: int) -> pl.Expr:
            warmup = self.WARMUP_FACTOR * period
            delta = value.diff()
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
            "fast": _rsi(self.fast),
            "mid": _rsi(self.mid),
            "slow": _rsi(self.slow),
        }


@dataclass(frozen=True)
class BIAS(SeriesIndicator):
    fast: int = 6
    mid: int = 12
    slow: int = 24

    def compute(self, value: pl.Expr) -> dict[str, pl.Expr]:
        def _bias(period: int) -> pl.Expr:
            ma = value.rolling_mean(window_size=period)
            return 100.0 * (value - ma) / ma

        return {
            "fast": _bias(self.fast),
            "mid": _bias(self.mid),
            "slow": _bias(self.slow),
        }


@dataclass(frozen=True)
class BOLL(SeriesIndicator):
    window: int = 20
    k: float = 2.0

    def compute(self, value: pl.Expr) -> dict[str, pl.Expr]:
        mid = value.rolling_mean(window_size=self.window)
        std = value.rolling_std(window_size=self.window, ddof=0)
        return {
            "mid": mid,
            "upper": mid + self.k * std,
            "lower": mid - self.k * std,
        }


@dataclass(frozen=True)
class KDJ(SeriesIndicator):
    period: int = 9
    k_period: int = 3
    d_period: int = 3

    WARMUP_FACTOR: ClassVar[int] = 4

    def compute(self, value: pl.Expr) -> dict[str, pl.Expr]:
        low = pl.col("low").rolling_min(window_size=self.period)
        high = pl.col("high").rolling_max(window_size=self.period)
        rsv = 100.0 * (value - low) / (high - low)
        warmup = self.period + self.WARMUP_FACTOR * max(
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
class ATR(SeriesIndicator):
    period: int = 14

    WARMUP_FACTOR: ClassVar[int] = 2

    def compute(self, value: pl.Expr) -> pl.Expr:
        prev_close = value.shift(1)
        true_range = pl.max_horizontal(
            pl.col("high") - pl.col("low"),
            (pl.col("high") - prev_close).abs(),
            (pl.col("low") - prev_close).abs(),
        )
        return _fast_ewm(
            true_range,
            alpha=1 / self.period,
            warmup=self.WARMUP_FACTOR * self.period,
        )


@dataclass(frozen=True)
class TypicalPrice(SeriesIndicator):
    def compute(self, value: pl.Expr) -> pl.Expr:
        return (pl.col("high") + pl.col("low") + pl.col("close")) / 3


@dataclass(frozen=True)
class Volatility(SeriesIndicator):
    method: Literal["std", "atr"] = "atr"
    atr_period: int = 14
    std_period: int = 20

    def compute(self, value: pl.Expr) -> pl.Expr:
        if self.method == "atr":
            atr = ATR(period=self.atr_period).compute(value)
            return 100.0 * atr / TypicalPrice().compute(value)

        if self.method == "std":
            log_ret = (value / value.shift(1)).log()
            return log_ret.rolling_std(
                window_size=self.std_period, ddof=0
            ) * math.sqrt(252)

        raise ValueError(f"Invalid volatility method: {self.method}")
