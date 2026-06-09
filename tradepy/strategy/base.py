import abc
import inspect
import random
import sys
from dataclasses import dataclass
from typing import Callable, Generic

import numpy as np

from tradepy.core.position import Position

if sys.version_info >= (3, 13):
    from typing import TypeVar  # pyright: ignore[reportUnreachable]
else:
    from typing_extensions import TypeVar  # pyright: ignore[reportUnreachable]

import polars as pl
from pandera.typing.polars import DataFrame

from tradepy.core.config import StrategyConf
from tradepy.core.types import (
    BarData,
    LazyStockDailyMetricsDataFrame,
    StockDailyMetricsDataFrame,
)
from tradepy.decors import indicator
from tradepy.strategy.transpiler import PolarsExprTranspiler
from tradepy.utils import calc_pct_chg, ensure_laziness


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


@dataclass
class IndicatorExpression:
    name: str
    expr: pl.Expr
    not_na: bool = True


ConfigT = TypeVar("ConfigT", bound=StrategyConf, default=StrategyConf)


class StrategyBase(abc.ABC, Generic[ConfigT]):
    _tradepy_strategy: bool = True

    buy: Callable[..., float | None]
    sell: Callable[..., float | None] = lambda self: None

    def __init__(self, config: ConfigT) -> None:
        self.config = config

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        if cls.__name__ in ("BacktestStrategyBase",):
            return

        for klass in cls.__mro__:
            if klass is StrategyBase:
                break
            if "buy" in klass.__dict__:
                return
        raise TypeError(f"{cls.__name__} must define a buy method")

    @abc.abstractmethod
    def should_stop_loss(
        self, bar: BarData, position: Position
    ) -> float | None:
        raise NotImplementedError

    @abc.abstractmethod
    def should_take_profit(
        self, bar: BarData, position: Position
    ) -> float | None:
        raise NotImplementedError

    @indicator()
    def sma5(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=5)

    @indicator()
    def sma10(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=10)

    @indicator()
    def sma20(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=20)

    @indicator()
    def sma30(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=30)

    @indicator()
    def sma60(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=60)

    @indicator()
    def sma120(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=120)

    @indicator()
    def sma250(self) -> pl.Expr:
        return pl.col("close").rolling_mean(window_size=250)

    @indicator()
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
            _mask_warmup(macd_line, warmup).alias("macd_dif"),
            _mask_warmup(signal, warmup).alias("macd_dea"),
            _mask_warmup(macd_line - signal, warmup).alias("macd_hist"),
        )

    @indicator()
    def typical_price(self) -> pl.Expr:
        return (pl.col("high") + pl.col("low") + pl.col("close")) / 3

    @indicator()
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

    @indicator()
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

    @indicator()
    def boll(self) -> tuple[pl.Expr, ...]:
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

    def apply_slippage(
        self,
        price: float,
        ref_price: float,
    ) -> float:

        method, params = (
            self.config.slippage.method,
            self.config.slippage.params,
        )

        if method == "max_jump":
            max_num_jumps = int(params)
            one_jump_pct_chg = 0.01 / ref_price
            pct_chgs = [
                one_jump_pct_chg * i for i in range(1, max_num_jumps + 1)
            ]
            slip_pct = random.choice(pct_chgs + [0])
            return price * (1 - slip_pct)

        if method == "max_pct":
            max_pct_chg = float(params)
            jitter = random.uniform(0, max_pct_chg * 1e-2)
            return price * (1 - jitter)

        if method == "weibull":
            slip_pct_chg = (
                np.random.weibull(params["shape"]) * params["scale"]
                + params["shift"]
            )
            return price * (1 - slip_pct_chg * 1e-2)

        raise ValueError(f"无效的滑点配置: {self.config.slippage}")

    def infer_required_indicators(self) -> list[str]:
        required: set[str] = set()
        for method_name in ("buy", "sell"):
            method = getattr(self.__class__, method_name)
            for name, param in inspect.signature(method).parameters.items():
                if name == "self":
                    continue
                if param.kind in (
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                ):
                    continue
                required.add(name)
        return list(required)

    def collect_indicator_expressions(self) -> tuple[IndicatorExpression, ...]:
        exprs: list[IndicatorExpression] = list()

        for cls in self.__class__.__mro__:
            if not getattr(cls, "_tradepy_strategy", False):
                continue

            for name, attr in cls.__dict__.items():
                if not getattr(attr, "_tradepy_indicator_compute", False):
                    continue

                # Get the actual polars expression
                results = attr(self)
                if not isinstance(results, tuple):
                    results = (results,)

                _expr: pl.Expr
                for _expr in results:
                    output_name = _expr.meta.output_name()
                    if (
                        output_name in _expr.meta.root_names()
                        or output_name == "literal"
                    ):
                        # Column-rooted or anonymous polars names → method name
                        _expr = _expr.alias(name)

                    exprs.append(
                        IndicatorExpression(
                            name=_expr.meta.output_name(),
                            expr=_expr,
                            not_na=getattr(
                                attr, "_tradepy_indicator_not_na", True
                            ),
                        )
                    )

        return tuple(exprs)

    def compute_indicators(
        self, df: StockDailyMetricsDataFrame | LazyStockDailyMetricsDataFrame
    ) -> DataFrame:
        required_indicators = self.infer_required_indicators()
        indicator_expressions = tuple(
            expr
            for expr in self.collect_indicator_expressions()
            if expr.name in required_indicators
        )

        _df = df.with_columns(
            *(expr.expr.over("code") for expr in indicator_expressions)
        )

        not_null_columns = [
            expr.name for expr in indicator_expressions if expr.not_na
        ]
        if not_null_columns:
            _df = _df.drop_nulls(subset=not_null_columns)

        _df = ensure_laziness(_df, False)

        return _df  # pyright: ignore[reportReturnType]

    def build_buy_expr(self) -> pl.Expr:
        return PolarsExprTranspiler(self).transpile("buy") / pl.col(
            "adj_factor"
        )

    def build_sell_expr(self) -> pl.Expr:
        return PolarsExprTranspiler(self).transpile("sell") / pl.col(
            "adj_factor"
        )


class BacktestStrategyBase(StrategyBase[ConfigT]):
    def should_stop_loss(
        self, bar: BarData, position: Position
    ) -> float | None:
        # During opening
        open_pct_chg = calc_pct_chg(position.price, bar.orig_open)
        if open_pct_chg <= -self.config.stop_loss:
            return bar.orig_open

        # During exchange
        low_pct_chg = calc_pct_chg(position.price, bar.orig_low)
        if low_pct_chg <= -self.config.stop_loss:
            return position.price_at_pct_change(-self.config.stop_loss)

    def should_take_profit(
        self, bar: BarData, position: Position
    ) -> float | None:
        # During opening
        open_pct_chg = calc_pct_chg(position.price, bar.orig_open)
        if open_pct_chg >= self.config.take_profit:
            return bar.orig_open

        # During exchange
        high_pct_chg = calc_pct_chg(position.price, bar.orig_high)
        if high_pct_chg >= self.config.take_profit:
            return position.price_at_pct_change(self.config.take_profit)
