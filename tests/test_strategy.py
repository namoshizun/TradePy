from datetime import date

import polars as pl
import pytest

from tradepy.core.config import SlippageConf, StrategyConf
from tradepy.strategy import StrategyBase


def _config() -> StrategyConf:
    slippage = SlippageConf(method="max_pct", params=0.02)
    return StrategyConf(
        strategy_class="UnusedStrategy",
        stop_loss=0,
        take_profit=0,
        take_profit_slip=slippage,
        stop_loss_slip=slippage,
        max_position_size=1,
        max_position_opens=10000,
        min_trade_amount=0,
    )


def test_subclass_without_buy_raises() -> None:
    with pytest.raises(TypeError, match="must define a buy method"):

        class NoBuyStrategy(StrategyBase):
            pass


def test_subclass_may_inherit_buy_from_parent() -> None:
    class ParentStrategy(StrategyBase):
        def buy(self, sma5: float):
            return True

    class ChildStrategy(ParentStrategy):
        pass

    assert ChildStrategy(_config()).infer_required_indicators() == ["sma5"]


def test_collect_indicator_expressions_names_atr_not_literal() -> None:
    names = {
        expr.name
        for expr in StrategyBase(_config()).collect_indicator_expressions()
    }
    assert "atr" in names
    assert "literal" not in names


def test_infer_required_indicators_unions_buy_and_sell_params() -> None:
    class ExampleStrategy(StrategyBase):
        def buy(self, sma20: float, macd_hist: float):
            return True

        def sell(self, sma20: float, macd_hist: float, atr: float):
            return False

    assert set(ExampleStrategy(_config()).infer_required_indicators()) == {
        "sma20",
        "macd_hist",
        "atr",
    }


def test_infer_required_indicators_ignores_var_positional() -> None:
    class VarArgStrategy(StrategyBase):
        def buy(self, sma5: float):
            return True

    assert VarArgStrategy(_config()).infer_required_indicators() == ["sma5"]


def test_compute_indicators_partitions_by_code() -> None:
    class Sma5Strategy(StrategyBase):
        def buy(self, sma5: float):
            return True

    dates = [date(2024, 1, day) for day in range(1, 6)]
    df = pl.DataFrame(
        {
            "code": ["A"] * 5 + ["B"] * 5,
            "date": dates + dates,
            "close": [1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0],
        }
    )

    out = Sma5Strategy(_config()).compute_indicators(df)  # pyright: ignore[reportArgumentType]

    last_a = out.filter(pl.col("code") == "A").tail(1)["sma5"].item()
    last_b = out.filter(pl.col("code") == "B").tail(1)["sma5"].item()
    assert last_a == pytest.approx(3.0)
    assert last_b == pytest.approx(30.0)
