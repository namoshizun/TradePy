from datetime import date

import polars as pl
import pytest

from tradepy.strategy import StrategyBase


def test_subclass_without_buy_raises() -> None:
    with pytest.raises(TypeError, match="must define a buy method"):

        class NoBuyStrategy(StrategyBase):
            pass


def test_subclass_may_inherit_buy_from_parent() -> None:
    class ParentStrategy(StrategyBase):
        def buy(self, sma5: float):
            return pl.lit(True)

    class ChildStrategy(ParentStrategy):
        pass

    assert ChildStrategy().infer_required_indicators() == ["sma5"]


def test_collect_indicator_expressions_names_atr_not_literal() -> None:
    names = {expr.name for expr in StrategyBase().collect_indicator_expressions()}
    assert "atr" in names
    assert "literal" not in names


def test_infer_required_indicators_unions_buy_and_sell_params() -> None:
    class ExampleStrategy(StrategyBase):
        def buy(self, sma20: float, macd_hist: float):
            return pl.lit(True)

        def sell(self, sma20: float, macd_hist: float, atr: float):
            return pl.lit(False)

    assert set(ExampleStrategy().infer_required_indicators()) == {
        "sma20",
        "macd_hist",
        "atr",
    }


def test_infer_required_indicators_ignores_var_positional() -> None:
    class VarArgStrategy(StrategyBase):
        def buy(self, sma5: float):
            return pl.lit(True)

    assert VarArgStrategy().infer_required_indicators() == ["sma5"]


def test_compute_indicators_partitions_by_code() -> None:
    class Sma5Strategy(StrategyBase):
        def buy(self, sma5: float):
            return pl.lit(True)

    dates = [date(2024, 1, day) for day in range(1, 6)]
    df = pl.DataFrame(
        {
            "code": ["A"] * 5 + ["B"] * 5,
            "date": dates + dates,
            "close": [1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0],
        }
    )

    out = Sma5Strategy().compute_indicators(df)  # pyright: ignore[reportArgumentType]

    last_a = out.filter(pl.col("code") == "A").tail(1)["sma5"].item()
    last_b = out.filter(pl.col("code") == "B").tail(1)["sma5"].item()
    assert last_a == pytest.approx(3.0)
    assert last_b == pytest.approx(30.0)
