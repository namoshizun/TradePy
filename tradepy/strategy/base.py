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
from tradepy.strategy.indicators import Indicator
from tradepy.strategy.transpiler import PolarsExprTranspiler
from tradepy.utils import calc_pct_chg, ensure_laziness


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

    def _strategy_parameters(
        self,
    ) -> tuple[inspect.Parameter, ...]:
        params: list[inspect.Parameter] = []
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
                params.append(param)
        return tuple(params)

    def infer_required_indicators(self) -> list[str]:
        required: set[str] = set(
            param.name for param in self._strategy_parameters()
        )
        return list(required)

    def collect_indicator_expressions(self) -> tuple[IndicatorExpression, ...]:
        exprs: dict[str, IndicatorExpression] = {}
        indicators: dict[str, Indicator] = {}

        for param in self._strategy_parameters():
            name, default = param.name, param.default
            if not isinstance(default, Indicator):
                continue

            existing = indicators.get(name)
            if existing is not None:
                if existing != default:
                    raise ValueError(
                        f"Conflicting indicator defaults for parameter {name!r}"
                    )
                continue

            result = default.resolve()
            if isinstance(result, dict):
                outputs = ", ".join(sorted(result))
                raise ValueError(
                    f"Indicator parameter {name!r} resolves to multiple "
                    f"outputs ({outputs}); pipe Take(...) before using it"
                )

            indicators[name] = default
            exprs[name] = IndicatorExpression(
                name=name,
                expr=result.alias(name),
                not_na=default.not_na,
            )

        return tuple(exprs.values())

    def compute_indicators(
        self, df: StockDailyMetricsDataFrame | LazyStockDailyMetricsDataFrame
    ) -> DataFrame:
        indicator_expressions = self.collect_indicator_expressions()

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
