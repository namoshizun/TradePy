from dataclasses import dataclass
from typing import Any

import polars as pl
import pytest

from tradepy.strategy.transpiler import PolarsExprTranspiler


@dataclass
class _Ma60Conf:
    ma60_support_thres: float


class _Ma60SupportStrategy:
    """Mirrors real strategy shape: config, mangled helper, layered should_buy."""

    def __init__(self, conf: _Ma60Conf) -> None:
        self.conf = conf

    def __ma60_support_level(self, ma60: float) -> float:
        return round(ma60 * (1 + self.conf.ma60_support_thres * 1e-2), 2)

    def should_buy(
        self,
        close: float,
        sma5: float,
        sma20: float,
        rsi_fast: float,
        sma60: float,
        n_below_ma60_support_past_20: bool,
    ) -> bool:
        if rsi_fast < 15:
            return True

        if sma20 > sma60:
            return True

        if close > self.__ma60_support_level(sma60):
            return True

        if n_below_ma60_support_past_20:
            if rsi_fast < 30:
                return True
            if sma5 > sma20:
                return True

        return False

    def should_sell(self, close: float, sma20: float) -> bool:
        if close < sma20 * (1 - self.conf.ma60_support_thres * 1e-2):
            return True
        return False


def _ma60_support_level(conf: _Ma60Conf, ma60: float) -> float:
    return round(ma60 * (1 + conf.ma60_support_thres * 1e-2), 2)


def _ref_should_buy(row: dict[str, Any], conf: _Ma60Conf) -> bool:
    if row["rsi_fast"] < 15:
        return True
    if row["sma20"] > row["sma60"]:
        return True
    if row["close"] > _ma60_support_level(conf, row["sma60"]):
        return True
    if row["n_below_ma60_support_past_20"]:
        if row["rsi_fast"] < 30:
            return True
        if row["sma5"] > row["sma20"]:
            return True
    return False


def _ref_should_sell(row: dict[str, Any], conf: _Ma60Conf) -> bool:
    return row["close"] < row["sma20"] * (1 - conf.ma60_support_thres * 1e-2)


def _eval_mask(strategy: Any, method_name: str, df: pl.DataFrame) -> list[bool]:
    expr = PolarsExprTranspiler(strategy).transpile(method_name)
    return df.with_columns(expr.alias("_mask"))["_mask"].to_list()


def _assert_matches_reference(
    strategy: Any,
    method_name: str,
    df: pl.DataFrame,
    ref: Any,
) -> None:
    got = _eval_mask(strategy, method_name, df)
    expected = [ref(row) for row in df.iter_rows(named=True)]
    assert got == expected


MA60_FIXTURE_DF = pl.DataFrame(
    {
        "close": [100.0, 50.0, 200.0, 80.0, 90.0],
        "sma5": [10.0, 10.0, 5.0, 12.0, 8.0],
        "sma20": [20.0, 25.0, 30.0, 15.0, 20.0],
        "rsi_fast": [10.0, 40.0, 20.0, 25.0, 50.0],
        "sma60": [30.0, 30.0, 40.0, 20.0, 25.0],
        "n_below_ma60_support_past_20": [False, True, False, True, True],
    }
)


def test_ma60_should_buy_matches_python_reference() -> None:
    conf = _Ma60Conf(ma60_support_thres=2.0)
    strategy = _Ma60SupportStrategy(conf)
    _assert_matches_reference(
        strategy,
        "should_buy",
        MA60_FIXTURE_DF,
        lambda row: _ref_should_buy(row, conf),
    )


def test_ma60_should_sell_matches_python_reference() -> None:
    conf = _Ma60Conf(ma60_support_thres=5.0)
    strategy = _Ma60SupportStrategy(conf)
    sell_df = MA60_FIXTURE_DF.select("close", "sma20")
    _assert_matches_reference(
        strategy,
        "should_sell",
        sell_df,
        lambda row: _ref_should_sell(row, conf),
    )


def test_config_change_retranspile_changes_buy_mask() -> None:
    # Only the close vs ma60-support branch can fire on this row.
    df = pl.DataFrame(
        {
            "close": [105.0],
            "sma5": [0.0],
            "sma20": [10.0],
            "rsi_fast": [20.0],
            "sma60": [100.0],
            "n_below_ma60_support_past_20": [False],
        }
    )
    low = _Ma60SupportStrategy(_Ma60Conf(ma60_support_thres=0.0))
    high = _Ma60SupportStrategy(_Ma60Conf(ma60_support_thres=10.0))

    assert _eval_mask(low, "should_buy", df) == [True]
    assert _eval_mask(high, "should_buy", df) == [False]


class _UnaryLogicStrategy:
    def signal(self, x: float, y: float) -> bool:
        if x < 1 and y > 2:
            return True
        if not (x == 0):
            return True
        return False


def test_boolean_ops_and_not() -> None:
    df = pl.DataFrame({"x": [0.0, 1.0, 0.5, 3.0], "y": [3.0, 3.0, 1.0, 0.0]})
    _assert_matches_reference(
        _UnaryLogicStrategy(),
        "signal",
        df,
        lambda row: (row["x"] < 1 and row["y"] > 2) or not (row["x"] == 0),
    )


class _ChainedCompareStrategy:
    def signal(self, a: float, b: float, c: float) -> bool:
        if 1 < a < 10:
            return True
        if b <= c <= 100:
            return True
        return False


def test_chained_comparisons() -> None:
    df = pl.DataFrame(
        {
            "a": [5.0, 0.5, 15.0],
            "b": [10.0, 50.0, 200.0],
            "c": [20.0, 50.0, 150.0],
        }
    )
    _assert_matches_reference(
        _ChainedCompareStrategy(),
        "signal",
        df,
        lambda row: (1 < row["a"] < 10) or (row["b"] <= row["c"] <= 100),
    )


@dataclass
class _ScaleConf:
    k: float


class _ArithmeticStrategy:
    def __init__(self) -> None:
        self.conf = _ScaleConf(k=2.0)

    def level(self, base: float) -> float:
        return base * (1 + self.conf.k * 1e-2)

    def signal(self, price: float, base: float) -> bool:
        if price > self.level(base):
            return True
        return False


def test_arithmetic_and_self_attr_in_helper() -> None:
    df = pl.DataFrame({"price": [102.0, 98.0], "base": [100.0, 100.0]})
    _assert_matches_reference(
        _ArithmeticStrategy(),
        "signal",
        df,
        lambda row: row["price"] > round(row["base"] * 1.02, 2),
    )


class _TruthyFlagStrategy:
    def signal(self, flag: bool, x: float) -> bool:
        if flag:
            if x > 0:
                return True
        return False


def test_truthy_column_parameter() -> None:
    df = pl.DataFrame(
        {"flag": [True, True, False, False], "x": [1.0, -1.0, 5.0, 5.0]}
    )
    _assert_matches_reference(
        _TruthyFlagStrategy(),
        "signal",
        df,
        lambda row: bool(row["flag"]) and row["x"] > 0,
    )


class _IfElseStrategy:
    def signal(self, flag: bool, x: float) -> bool:
        if flag:
            if x > 0:
                return True
        else:
            if x < 0:
                return True
        return False


def test_else_branch_is_guarded_by_negated_if_condition() -> None:
    df = pl.DataFrame(
        {
            "flag": [True, True, False, False],
            "x": [-1.0, 1.0, -1.0, 1.0],
        }
    )
    _assert_matches_reference(
        _IfElseStrategy(),
        "signal",
        df,
        lambda row: (
            (row["flag"] and row["x"] > 0) or (not row["flag"] and row["x"] < 0)
        ),
    )


class _ReturnFalseElifStrategy:
    def signal(self, x: float) -> bool:
        if x > 10:
            return False
        elif x > 5:
            return True
        return False


def test_elif_after_return_false_respects_python_reachability() -> None:
    df = pl.DataFrame({"x": [11.0, 7.0, 4.0]})
    _assert_matches_reference(
        _ReturnFalseElifStrategy(),
        "signal",
        df,
        lambda row: False if row["x"] > 10 else row["x"] > 5,
    )


class _ReturnExpressionStrategy:
    def signal(self, x: float, y: float) -> bool:
        if x > 10:
            return False
        return y > 0


def test_bool_return_expression_is_transpiled() -> None:
    df = pl.DataFrame({"x": [11.0, 7.0, 4.0], "y": [1.0, 1.0, -1.0]})
    _assert_matches_reference(
        _ReturnExpressionStrategy(),
        "signal",
        df,
        lambda row: False if row["x"] > 10 else row["y"] > 0,
    )


class _LocalVariableStrategy:
    def signal(
        self, atr: float, typical_price: float, threshold: float
    ) -> bool:
        volatility = 100 * atr / typical_price
        is_hot: bool = volatility > threshold
        return is_hot


def test_local_assignments_can_feed_later_conditions_and_returns() -> None:
    df = pl.DataFrame(
        {
            "atr": [3.0, 1.0, 5.0],
            "typical_price": [100.0, 100.0, 50.0],
            "threshold": [2.0, 2.0, 20.0],
        }
    )
    _assert_matches_reference(
        _LocalVariableStrategy(),
        "signal",
        df,
        lambda row: 100 * row["atr"] / row["typical_price"] > row["threshold"],
    )


class _PiecewiseHelperStrategy:
    def __level(self, base: float) -> float:
        scaled = base * 2
        if base > 0:
            return scaled
        return -base * 3

    def signal(self, price: float, base: float) -> bool:
        return price > self.__level(base)


def test_piecewise_private_helper_is_inlined_with_control_flow() -> None:
    df = pl.DataFrame(
        {
            "price": [3.0, 5.0, 7.0, 8.0],
            "base": [1.0, -1.0, 4.0, -3.0],
        }
    )
    _assert_matches_reference(
        _PiecewiseHelperStrategy(),
        "signal",
        df,
        lambda row: (
            row["price"]
            > (row["base"] * 2 if row["base"] > 0 else -row["base"] * 3)
        ),
    )


class _UnaryValueStrategy:
    def signal(self, x: float, y: float) -> bool:
        return -x > 2 and +y > 0


def test_unary_value_operators() -> None:
    df = pl.DataFrame({"x": [-3.0, -1.0, -4.0], "y": [1.0, 1.0, -1.0]})
    _assert_matches_reference(
        _UnaryValueStrategy(),
        "signal",
        df,
        lambda row: -row["x"] > 2 and +row["y"] > 0,
    )


class _KeywordOnlyParamStrategy:
    def signal(self, *, x: float) -> bool:
        return x > 0


def test_keyword_only_parameters_are_columns() -> None:
    df = pl.DataFrame({"x": [1.0, -1.0]})
    _assert_matches_reference(
        _KeywordOnlyParamStrategy(),
        "signal",
        df,
        lambda row: row["x"] > 0,
    )


class _AlwaysFalseStrategy:
    def signal(self, x: float) -> bool:
        if x > 0:
            return False
        return False


def test_no_true_return_paths_yields_false() -> None:
    df = pl.DataFrame({"x": [1.0, -1.0, 0.0]})
    got = _eval_mask(_AlwaysFalseStrategy(), "signal", df)
    assert got == [False, False, False]


class _AliasStrategy:
    def go(self, x: float) -> bool:
        if x > 0:
            return True
        return False


def test_transpile_alias() -> None:
    expr = PolarsExprTranspiler(_AliasStrategy()).transpile(
        "go", alias="buy_signal"
    )
    assert expr.meta.output_name() == "buy_signal"


class _OrInConditionStrategy:
    def signal(self, x: float, y: float) -> bool:
        if x < 0 or y > 10:
            return True
        return False


def test_or_inside_if_condition() -> None:
    df = pl.DataFrame({"x": [-1.0, 1.0, 2.0], "y": [0.0, 11.0, 5.0]})
    _assert_matches_reference(
        _OrInConditionStrategy(),
        "signal",
        df,
        lambda row: row["x"] < 0 or row["y"] > 10,
    )


class _RoundStrategy:
    def scaled(self, v: float) -> float:
        return round(v * 1.3333, 2)

    def signal(self, a: float, b: float) -> bool:
        if a > self.scaled(b):
            return True
        return False


def test_round_builtin_in_inlined_helper() -> None:
    df = pl.DataFrame({"a": [2.0, 1.0], "b": [1.5, 1.5]})
    _assert_matches_reference(
        _RoundStrategy(),
        "signal",
        df,
        lambda row: row["a"] > round(row["b"] * 1.3333, 2),
    )


class _BadNameStrategy:
    def signal(self, x: float) -> bool:
        if mystery > 1:  # noqa: F821
            return True
        return False


def test_unknown_name_in_condition_raises() -> None:
    with pytest.raises(NameError, match="mystery"):
        PolarsExprTranspiler(_BadNameStrategy()).transpile("signal")


class _BadCallStrategy:
    def signal(self, x: float) -> bool:
        if len(x) > 1:  # pyright: ignore[reportArgumentType]
            return True
        return False


def test_unsupported_call_raises() -> None:
    with pytest.raises(NotImplementedError, match="len"):
        PolarsExprTranspiler(_BadCallStrategy()).transpile("signal")


class _BadReturnStrategy:
    def signal(self, x: float) -> bool:
        return 1  # pyright: ignore[reportReturnType]


def test_non_bool_constant_return_raises() -> None:
    with pytest.raises(NotImplementedError, match="Unsupported bool return"):
        PolarsExprTranspiler(_BadReturnStrategy()).transpile("signal")


class _UnsupportedStatementStrategy:
    def signal(self, x: float) -> bool:
        for threshold in (1, 2):
            if x > threshold:
                return True
        return False


def test_unsupported_statement_raises() -> None:
    with pytest.raises(NotImplementedError, match="Unsupported statement"):
        PolarsExprTranspiler(_UnsupportedStatementStrategy()).transpile(
            "signal"
        )


class _NonExhaustiveHelperStrategy:
    def level(self, x: float) -> float:
        if x > 0:
            return x

    def signal(self, price: float, x: float) -> bool:
        return price > self.level(x)


def test_helper_with_missing_return_path_raises() -> None:
    with pytest.raises(ValueError, match="does not return"):
        PolarsExprTranspiler(_NonExhaustiveHelperStrategy()).transpile("signal")


class _HelperScopeLeakStrategy:
    def helper(self, x: float) -> float:
        return x + close  # noqa: F821

    def signal(self, close: float, rsi_fast: float) -> bool:
        return close > self.helper(rsi_fast)


def test_helper_unknown_name_does_not_resolve_from_outer_method_params() -> None:
    with pytest.raises(NameError, match="close"):
        PolarsExprTranspiler(_HelperScopeLeakStrategy()).transpile("signal")


class _ElifStrategy:
    def signal(self, bucket: float, x: float) -> bool:
        if bucket == 1:
            if x > 10:
                return True
        elif bucket == 2:
            if x < 5:
                return True
        return False


def test_elif_collects_or_branches() -> None:
    df = pl.DataFrame(
        {"bucket": [1.0, 1.0, 2.0, 2.0, 3.0], "x": [11.0, 9.0, 3.0, 7.0, 100.0]}
    )
    _assert_matches_reference(
        _ElifStrategy(),
        "signal",
        df,
        lambda row: (
            (row["bucket"] == 1 and row["x"] > 10)
            or (row["bucket"] == 2 and row["x"] < 5)
        ),
    )


class _PowerStrategy:
    def signal(self, base: float, exp: float, limit: float) -> bool:
        if base**exp > limit:
            return True
        return False


def test_power_operator() -> None:
    df = pl.DataFrame(
        {"base": [2.0, 3.0], "exp": [3.0, 2.0], "limit": [7.0, 10.0]}
    )
    _assert_matches_reference(
        _PowerStrategy(),
        "signal",
        df,
        lambda row: row["base"] ** row["exp"] > row["limit"],
    )


def test_ma60_support_level_inlined_matches_round_formula() -> None:
    conf = _Ma60Conf(ma60_support_thres=2.5)
    strategy = _Ma60SupportStrategy(conf)
    df = pl.DataFrame(
        {
            "close": [103.0, 101.0],
            "sma5": [0.0, 0.0],
            "sma20": [0.0, 0.0],
            "rsi_fast": [50.0, 50.0],
            "sma60": [100.0, 100.0],
            "n_below_ma60_support_past_20": [False, False],
        }
    )
    support = _ma60_support_level(conf, 100.0)
    assert support == pytest.approx(102.5)
    _assert_matches_reference(
        strategy,
        "should_buy",
        df,
        lambda row: row["close"] > support,
    )
