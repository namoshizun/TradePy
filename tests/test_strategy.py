from datetime import date, timedelta
from typing import Annotated, Any

import polars as pl
import pytest

from tradepy.core.config import SlippageConf, StrategyConf
from tradepy.strategy import (
    ATR,
    BOLL,
    KDJ,
    MACD,
    RSI,
    SMA,
    DatePart,
    Lag,
    Percentile,
    Rank,
    StrategyBase,
    Take,
)
from tradepy.strategy.indicators import Indicator
from tradepy.strategy.transpiler import PolarsExprTranspiler


def _config() -> StrategyConf:
    slippage = SlippageConf(method="max_pct", params=0.02)
    return StrategyConf(
        strategy_class="UnusedStrategy",
        stop_loss=0,
        take_profit=0,
        slippage=slippage,
        max_position_size=1,
        max_position_opens=10000,
        min_trade_amount=0,
    )


class _RisklessStrategy(StrategyBase):
    def buy(self, *args: Any, **kwargs: Any) -> float | None:
        return None

    def should_stop_loss(self, bar: Any, position: Any) -> float | None:
        return None

    def should_take_profit(self, bar: Any, position: Any) -> float | None:
        return None


def test_subclass_without_buy_raises() -> None:
    with pytest.raises(TypeError, match="must define a buy method"):

        class NoBuyStrategy(StrategyBase):
            pass


def test_subclass_may_inherit_buy_from_parent() -> None:
    class ParentStrategy(_RisklessStrategy):
        def buy(self, sma5: Annotated[float, SMA(5)]) -> float | None:
            return sma5

    class ChildStrategy(ParentStrategy):
        pass

    assert ChildStrategy(_config()).infer_required_indicators() == ["sma5"]


def test_collect_indicator_expressions_names_atr_not_literal() -> None:
    class AtrStrategy(_RisklessStrategy):
        def buy(self, atr: Annotated[float, ATR()]) -> float | None:
            return atr

    names = {
        expr.name
        for expr in AtrStrategy(_config()).collect_indicator_expressions()
    }
    assert "atr" in names
    assert "literal" not in names


def test_infer_required_indicators_unions_buy_and_sell_params() -> None:
    class ExampleStrategy(_RisklessStrategy):
        def buy(
            self,
            sma20: Annotated[float, SMA(20)],
            macd_hist: Annotated[float, MACD() | Take("hist")],
        ) -> float | None:
            return sma20 if macd_hist > 0 else None

        def sell(
            self,
            sma20: Annotated[float, SMA(20)],
            macd_hist: Annotated[float, MACD() | Take("hist")],
            atr: Annotated[float, ATR()],
        ) -> float | None:
            return sma20 + atr if macd_hist < 0 else None

    assert set(ExampleStrategy(_config()).infer_required_indicators()) == {
        "sma20",
        "macd_hist",
        "atr",
    }


def test_collect_indicator_expressions_reuses_same_buy_sell_indicator() -> None:
    class SharedIndicatorStrategy(_RisklessStrategy):
        MA10 = SMA(10)

        def buy(self, ma10: Annotated[float, MA10]) -> float | None:
            return ma10

        def sell(self, ma10: Annotated[float, MA10]) -> float | None:
            return ma10

    exprs = SharedIndicatorStrategy(_config()).collect_indicator_expressions()

    assert [expr.name for expr in exprs] == ["ma10"]


def test_collect_indicator_expressions_rejects_conflicting_defaults() -> None:
    class ConflictingIndicatorStrategy(_RisklessStrategy):
        def buy(self, ma10: Annotated[float, SMA(10)]) -> float | None:
            return ma10

        def sell(self, ma10: Annotated[float, SMA(20)]) -> float | None:
            return ma10

    with pytest.raises(ValueError, match="Conflicting indicator defaults"):
        ConflictingIndicatorStrategy(_config()).collect_indicator_expressions()


def test_infer_required_indicators_ignores_var_positional() -> None:
    class VarArgStrategy(_RisklessStrategy):
        def buy(
            self, sma5: Annotated[float, SMA(5)], *args: Any
        ) -> float | None:
            return sma5

    assert VarArgStrategy(_config()).infer_required_indicators() == ["sma5"]


def test_default_sell_transpiles_to_null_price() -> None:
    class BuyOnlyStrategy(_RisklessStrategy):
        def buy(self, close: float) -> float | None:
            return close

    strategy = BuyOnlyStrategy(_config())
    df = pl.DataFrame({"close": [1.0, 2.0]})

    transpiler = PolarsExprTranspiler(strategy)
    out = df.with_columns(transpiler.transpile("sell").alias("sell_price"))

    assert strategy.sell() is None
    assert out["sell_price"].to_list() == [None, None]


def test_compute_indicators_partitions_by_code() -> None:
    class Sma5Strategy(_RisklessStrategy):
        def buy(self, sma5: Annotated[float, SMA(5)]) -> float | None:
            return sma5

    dates = [date(2024, 1, day) for day in range(1, 6)]
    df = pl.DataFrame(
        {
            "code": ["A"] * 5 + ["B"] * 5,
            "date": dates + dates,
            "close": [1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0],
            "adj_factor": [1.0] * 10,
        }
    )

    out = Sma5Strategy(_config()).compute_indicators(df)  # pyright: ignore[reportArgumentType]

    last_a = out.filter(pl.col("code") == "A").tail(1)["sma5"].item()
    last_b = out.filter(pl.col("code") == "B").tail(1)["sma5"].item()
    assert last_a == pytest.approx(3.0)
    assert last_b == pytest.approx(30.0)


def test_multi_output_indicators_are_selected_by_take() -> None:
    class MultiOutputStrategy(_RisklessStrategy):
        def buy(
            self,
            macd_hist: Annotated[float, MACD() | Take("hist")],
            rsi_fast: Annotated[float, RSI() | Take("fast")],
            boll_upper: Annotated[float, BOLL() | Take("upper")],
        ) -> float | None:
            return macd_hist + rsi_fast + boll_upper

    names = {
        expr.name
        for expr in MultiOutputStrategy(
            _config()
        ).collect_indicator_expressions()
    }

    assert names == {"macd_hist", "rsi_fast", "boll_upper"}


def test_indicator_composition_with_ref() -> None:
    class SmaLagStrategy(_RisklessStrategy):
        def buy(
            self, sma5_ref1: Annotated[float, SMA(5) | Lag(1)]
        ) -> float | None:
            return sma5_ref1

    dates = [date(2024, 1, day) for day in range(1, 7)]
    df = pl.DataFrame(
        {
            "code": ["A"] * 6,
            "date": dates,
            "close": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "adj_factor": [1.0] * 6,
        }
    )

    out = SmaLagStrategy(_config()).compute_indicators(df)  # pyright: ignore[reportArgumentType]

    assert out.tail(1)["sma5_ref1"].item() == pytest.approx(3.0)


def test_indicator_composition_with_custom_rsi_period() -> None:
    class SmoothedRsiStrategy(_RisklessStrategy):
        def buy(
            self,
            smoothed_rsi: Annotated[float, RSI(fast=7) | Take("fast") | SMA(5)],
        ) -> float | None:
            return smoothed_rsi

    dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(30)]
    df = pl.DataFrame(
        {
            "code": ["A"] * 30,
            "date": dates,
            "close": [float(i) for i in range(1, 31)],
            "adj_factor": [1.0] * 30,
        }
    )

    out = SmoothedRsiStrategy(_config()).compute_indicators(df)  # pyright: ignore[reportArgumentType]

    assert "smoothed_rsi" in out.columns
    assert out.height > 0
    assert out["smoothed_rsi"].null_count() == 0


def test_multi_output_indicator_requires_take() -> None:
    class BadStrategy(_RisklessStrategy):
        def buy(self, macd: Annotated[float, MACD()]) -> float | None:
            return macd

    with pytest.raises(ValueError, match=r"pipe Take\(\.\.\.\)"):
        BadStrategy(_config()).collect_indicator_expressions()


def test_collect_cross_section_expression_uses_date_partition() -> None:
    class PeRankStrategy(_RisklessStrategy):
        def buy(
            self,
            pe_rank: Annotated[float, Rank(column="pe", over="industry_code")],
        ) -> float | None:
            return pe_rank

    exprs = PeRankStrategy(_config()).collect_indicator_expressions()
    assert len(exprs) == 1
    assert exprs[0].over == ("date", "industry_code")


def test_compute_indicators_respects_dtype() -> None:
    class DtypeStrategy(_RisklessStrategy):
        def buy(
            self,
            sma5: Annotated[float, SMA(5)],
            pe_rank: Annotated[
                float,
                Rank(column="pe", method="ordinal", dtype=pl.UInt32),
            ],
            hist: Annotated[float, MACD() | Take("hist", dtype=pl.Float32)],
        ) -> float | None:
            return sma5

    dates = [date(2024, 1, day) for day in range(1, 31)]
    df = pl.DataFrame(
        {
            "code": ["A"] * 30 + ["B"] * 30,
            "date": dates + dates,
            "close": [float(i) for i in range(1, 61)],
            "pe": [float(i) for i in range(1, 61)],
            "high": [float(i) + 1 for i in range(1, 61)],
            "low": [float(i) - 1 for i in range(1, 61)],
            "adj_factor": [1.0] * 60,
        }
    )

    out = DtypeStrategy(_config()).compute_indicators(df)  # pyright: ignore[reportArgumentType]

    assert out["sma5"].dtype == pl.Float64
    assert out["pe_rank"].dtype == pl.UInt32
    assert out["hist"].dtype == pl.Float32


def test_compute_indicators_builtin_default_dtypes() -> None:
    class BuiltinDtypeStrategy(_RisklessStrategy):
        def buy(
            self,
            pe_rank: Annotated[float, Rank(column="pe", method="ordinal")],
            pe_pct: Annotated[float, Percentile(column="pe")],
            rsi_fast: Annotated[float, RSI() | Take("fast")],
            kdj_k: Annotated[float, KDJ() | Take("k")],
        ) -> float | None:
            return pe_rank

    dates = [date(2024, 1, day) for day in range(1, 31)]
    df = pl.DataFrame(
        {
            "code": ["A"] * 30 + ["B"] * 30,
            "date": dates + dates,
            "close": [float(i) for i in range(1, 61)],
            "pe": [float(i) for i in range(1, 61)],
            "high": [float(i) + 1 for i in range(1, 61)],
            "low": [float(i) - 1 for i in range(1, 61)],
            "adj_factor": [1.0] * 60,
        }
    )

    out = BuiltinDtypeStrategy(_config()).compute_indicators(df)  # pyright: ignore[reportArgumentType]

    assert out["pe_rank"].dtype == pl.Float32
    assert out["pe_pct"].dtype == pl.UInt8
    assert out["rsi_fast"].dtype == pl.Float16
    assert out["kdj_k"].dtype == pl.Float16


def test_annotated_int_indicator_is_not_a_float_subclass() -> None:
    class CalendarStrategy(_RisklessStrategy):
        def buy(
            self,
            month: Annotated[int, DatePart(part="month")],
            day: Annotated[int, DatePart(part="day")],
        ) -> float | None:
            return None if (month, day) != (1, 2) else 1.0

    strategy = CalendarStrategy(_config())
    params = {
        param.name: param.indicator for param in strategy._strategy_parameters()
    }
    assert isinstance(params["month"], DatePart)
    assert isinstance(params["day"], DatePart)
    assert not isinstance(params["month"], float)
    assert not issubclass(DatePart, float)
    assert not issubclass(Indicator, float)

    dates = [date(2024, 1, day) for day in range(1, 4)]
    df = pl.DataFrame(
        {
            "code": ["A"] * 3,
            "date": dates,
            "close": [1.0, 2.0, 3.0],
            "adj_factor": [1.0] * 3,
        }
    )
    out = strategy.compute_indicators(df)  # pyright: ignore[reportArgumentType]
    assert out["month"].dtype == pl.UInt8
    assert out["day"].dtype == pl.UInt8
    assert out["month"].to_list() == [1, 1, 1]
    assert out["day"].to_list() == [1, 2, 3]


def test_raw_column_params_have_no_indicator_metadata() -> None:
    class RawColumnStrategy(_RisklessStrategy):
        def buy(
            self,
            close: float,
            sma5: Annotated[float, SMA(5)],
        ) -> float | None:
            return close if sma5 > 0 else None

    params = {
        param.name: param.indicator
        for param in RawColumnStrategy(_config())._strategy_parameters()
    }
    assert params["close"] is None
    assert isinstance(params["sma5"], SMA)
