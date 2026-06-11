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
    ) -> float | None:
        if rsi_fast < 15:
            return close

        temp = (sma20 + 1) > sma60
        if temp:
            return close

        if close > self.__ma60_support_level(sma60):
            return close

        if n_below_ma60_support_past_20:
            if rsi_fast < 30:
                return close
            if sma5 > sma20:
                return close

        return None

    def should_sell(self, close: float, sma20: float) -> float | None:
        if close < sma20 * (1 - self.conf.ma60_support_thres * 1e-2):
            return close
        return None


def _ma60_support_level(conf: _Ma60Conf, ma60: float) -> float:
    return round(ma60 * (1 + conf.ma60_support_thres * 1e-2), 2)


def _ref_should_buy(row: dict[str, Any], conf: _Ma60Conf) -> float | None:
    if row["rsi_fast"] < 15:
        return row["close"]
    if row["sma20"] > row["sma60"]:
        return row["close"]
    if row["close"] > _ma60_support_level(conf, row["sma60"]):
        return row["close"]
    if row["n_below_ma60_support_past_20"]:
        if row["rsi_fast"] < 30:
            return row["close"]
        if row["sma5"] > row["sma20"]:
            return row["close"]
    return None


def _ref_should_sell(row: dict[str, Any], conf: _Ma60Conf) -> float | None:
    should_sell = row["close"] < row["sma20"] * (
        1 - conf.ma60_support_thres * 1e-2
    )
    return row["close"] if should_sell else None


def _eval_values(
    strategy: Any, method_name: str, df: pl.DataFrame
) -> list[float | None]:
    expr = PolarsExprTranspiler(strategy).transpile(method_name)
    return df.with_columns(expr.alias("_value"))["_value"].to_list()


def _assert_matches_reference(
    strategy: Any,
    method_name: str,
    df: pl.DataFrame,
    ref: Any,
) -> None:
    got = _eval_values(strategy, method_name, df)
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


def test_config_change_retranspile_changes_buy_price() -> None:
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

    assert _eval_values(low, "should_buy", df) == [105.0]
    assert _eval_values(high, "should_buy", df) == [None]


class _UnaryLogicStrategy:
    def signal(self, x: float, y: float) -> float | None:
        if x < 1 and y > 2:
            return 1.0
        if not (x == 0):
            return 1.0
        return None


def test_boolean_ops_and_not() -> None:
    df = pl.DataFrame({"x": [0.0, 1.0, 0.5, 3.0], "y": [3.0, 3.0, 1.0, 0.0]})
    _assert_matches_reference(
        _UnaryLogicStrategy(),
        "signal",
        df,
        lambda row: (
            1.0
            if (row["x"] < 1 and row["y"] > 2) or not (row["x"] == 0)
            else None
        ),
    )


class _ChainedCompareStrategy:
    def signal(self, a: float, b: float, c: float) -> float | None:
        if 1 < a < 10:
            return 1.0
        if b <= c <= 100:
            return 1.0
        return None


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
        lambda row: (
            1.0
            if (1 < row["a"] < 10) or (row["b"] <= row["c"] <= 100)
            else None
        ),
    )


@dataclass
class _ScaleConf:
    k: float


class _ArithmeticStrategy:
    def __init__(self) -> None:
        self.conf = _ScaleConf(k=2.0)

    def level(self, base: float) -> float:
        return base * (1 + self.conf.k * 1e-2)

    def signal(self, price: float, base: float) -> float | None:
        if price > self.level(base):
            return price
        return None


def test_arithmetic_and_self_attr_in_helper() -> None:
    df = pl.DataFrame({"price": [102.0, 98.0], "base": [100.0, 100.0]})
    _assert_matches_reference(
        _ArithmeticStrategy(),
        "signal",
        df,
        lambda row: (
            row["price"]
            if row["price"] > round(row["base"] * 1.02, 2)
            else None
        ),
    )


class _TruthyFlagStrategy:
    def signal(self, flag: bool, x: float) -> float | None:
        if flag:
            if x > 0:
                return x
        return None


def test_truthy_column_parameter() -> None:
    df = pl.DataFrame(
        {"flag": [True, True, False, False], "x": [1.0, -1.0, 5.0, 5.0]}
    )
    _assert_matches_reference(
        _TruthyFlagStrategy(),
        "signal",
        df,
        lambda row: row["x"] if bool(row["flag"]) and row["x"] > 0 else None,
    )


class _IfElseStrategy:
    def signal(self, flag: bool, x: float) -> float | None:
        if flag:
            if x > 0:
                return x
        else:
            if x < 0:
                return x
        return None


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
            row["x"]
            if (row["flag"] and row["x"] > 0)
            or (not row["flag"] and row["x"] < 0)
            else None
        ),
    )


class _ReturnFalseElifStrategy:
    def signal(self, x: float) -> float | None:
        if x > 10:
            return None
        elif x > 5:
            return x
        return None


def test_elif_after_return_false_respects_python_reachability() -> None:
    df = pl.DataFrame({"x": [11.0, 7.0, 4.0]})
    _assert_matches_reference(
        _ReturnFalseElifStrategy(),
        "signal",
        df,
        lambda row: row["x"] if 5 < row["x"] <= 10 else None,
    )


class _ReturnExpressionStrategy:
    def signal(self, x: float, y: float) -> float | None:
        if x > 10:
            return None
        return y


def test_value_return_expression_is_transpiled() -> None:
    df = pl.DataFrame({"x": [11.0, 7.0, 4.0], "y": [1.0, 1.0, -1.0]})
    _assert_matches_reference(
        _ReturnExpressionStrategy(),
        "signal",
        df,
        lambda row: None if row["x"] > 10 else row["y"],
    )


class _LocalVariableStrategy:
    def signal(
        self, atr: float, typical_price: float, threshold: float
    ) -> float | None:
        volatility = 100 * atr / typical_price
        is_hot: bool = volatility > threshold
        if is_hot:
            return volatility
        return None


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
        lambda row: (
            100 * row["atr"] / row["typical_price"]
            if 100 * row["atr"] / row["typical_price"] > row["threshold"]
            else None
        ),
    )


class _PiecewiseHelperStrategy:
    def __level(self, base: float) -> float:
        scaled = base * 2
        if base > 0:
            return scaled
        return -base * 3

    def signal(self, price: float, base: float) -> float | None:
        if price > self.__level(base):
            return price
        return None


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
            if row["price"]
            > (row["base"] * 2 if row["base"] > 0 else -row["base"] * 3)
            else None
        ),
    )


class _UnaryValueStrategy:
    def signal(self, x: float, y: float) -> float | None:
        if -x > 2 and +y > 0:
            return y
        return None


def test_unary_value_operators() -> None:
    df = pl.DataFrame({"x": [-3.0, -1.0, -4.0], "y": [1.0, 1.0, -1.0]})
    _assert_matches_reference(
        _UnaryValueStrategy(),
        "signal",
        df,
        lambda row: row["y"] if -row["x"] > 2 and +row["y"] > 0 else None,
    )


class _KeywordOnlyParamStrategy:
    def signal(self, *, x: float) -> float | None:
        if x > 0:
            return x
        return None


def test_keyword_only_parameters_are_columns() -> None:
    df = pl.DataFrame({"x": [1.0, -1.0]})
    _assert_matches_reference(
        _KeywordOnlyParamStrategy(),
        "signal",
        df,
        lambda row: row["x"] if row["x"] > 0 else None,
    )


class _AlwaysFalseStrategy:
    def signal(self, x: float) -> float | None:
        if x > 0:
            return None
        return None


def test_no_price_return_paths_yields_null() -> None:
    df = pl.DataFrame({"x": [1.0, -1.0, 0.0]})
    got = _eval_values(_AlwaysFalseStrategy(), "signal", df)
    assert got == [None, None, None]


class _AliasStrategy:
    def go(self, x: float) -> float | None:
        if x > 0:
            return x
        return None


def test_transpile_alias() -> None:
    expr = PolarsExprTranspiler(_AliasStrategy()).transpile(
        "go", alias="buy_price"
    )
    assert expr.meta.output_name() == "buy_price"


class _OrInConditionStrategy:
    def signal(self, x: float, y: float) -> float | None:
        if x < 0 or y > 10:
            return y
        return None


def test_or_inside_if_condition() -> None:
    df = pl.DataFrame({"x": [-1.0, 1.0, 2.0], "y": [0.0, 11.0, 5.0]})
    _assert_matches_reference(
        _OrInConditionStrategy(),
        "signal",
        df,
        lambda row: row["y"] if row["x"] < 0 or row["y"] > 10 else None,
    )


class _MembershipStrategy:
    def signal(
        self, name: str, code: str, bucket: float, price: float
    ) -> float | None:
        if "ST" not in name and code in ("AAA", "BBB"):
            return price
        if "ETF" in name and bucket not in {3.0, 4.0}:
            return price
        return None


def test_membership_conditions() -> None:
    df = pl.DataFrame(
        {
            "name": ["Good Co", "ST Bad", "ETF Fund", "ETF Halted"],
            "code": ["AAA", "AAA", "CCC", "BBB"],
            "bucket": [1.0, 1.0, 3.0, 2.0],
            "price": [10.0, 20.0, 30.0, 40.0],
        }
    ).with_columns(pl.col("name", "code").cast(pl.Categorical))
    _assert_matches_reference(
        _MembershipStrategy(),
        "signal",
        df,
        lambda row: (
            row["price"]
            if ("ST" not in row["name"] and row["code"] in ("AAA", "BBB"))
            or ("ETF" in row["name"] and row["bucket"] not in {3.0, 4.0})
            else None
        ),
    )


class _RoundStrategy:
    def scaled(self, v: float) -> float:
        return round(v * 1.3333, 2)

    def signal(self, a: float, b: float) -> float | None:
        if a > self.scaled(b):
            return a
        return None


def test_round_builtin_in_inlined_helper() -> None:
    df = pl.DataFrame({"a": [2.0, 1.0], "b": [1.5, 1.5]})
    _assert_matches_reference(
        _RoundStrategy(),
        "signal",
        df,
        lambda row: (
            row["a"] if row["a"] > round(row["b"] * 1.3333, 2) else None
        ),
    )


class _BadNameStrategy:
    def signal(self, x: float) -> float | None:
        if mystery > 1:  # pyright: ignore[reportUndefinedVariable]  # noqa: F821
            return x
        return None


def test_unknown_name_in_condition_raises() -> None:
    with pytest.raises(NameError, match="mystery"):
        PolarsExprTranspiler(_BadNameStrategy()).transpile("signal")


class _BadCallStrategy:
    def signal(self, x: float) -> float | None:
        if len(x) > 1:  # pyright: ignore[reportArgumentType]
            return x
        return None


def test_unsupported_call_raises() -> None:
    with pytest.raises(NotImplementedError, match="len"):
        PolarsExprTranspiler(_BadCallStrategy()).transpile("signal")


class _ConstantPriceStrategy:
    def signal(self, x: float) -> float | None:
        if x > 0:
            return 1.5
        return None


def test_constant_price_return_is_transpiled() -> None:
    df = pl.DataFrame({"x": [1.0, -1.0]})
    _assert_matches_reference(
        _ConstantPriceStrategy(),
        "signal",
        df,
        lambda row: 1.5 if row["x"] > 0 else None,
    )


class _UnsupportedStatementStrategy:
    def signal(self, x: float) -> float | None:
        for threshold in (1, 2):
            if x > threshold:
                return x
        return None


def test_unsupported_statement_raises() -> None:
    with pytest.raises(NotImplementedError, match="Unsupported statement"):
        PolarsExprTranspiler(_UnsupportedStatementStrategy()).transpile(
            "signal"
        )


class _NonExhaustiveHelperStrategy:
    def level(self, x: float) -> float:  # pyright: ignore[reportReturnType]
        if x > 0:
            return x

    def signal(self, price: float, x: float) -> float | None:
        if price > self.level(x):
            return price
        return None


def test_helper_with_missing_return_path_raises() -> None:
    with pytest.raises(ValueError, match="does not return"):
        PolarsExprTranspiler(_NonExhaustiveHelperStrategy()).transpile("signal")


class _HelperScopeLeakStrategy:
    def helper(self, x: float) -> float:
        return x + close  # pyright: ignore[reportUndefinedVariable]  # noqa: F821

    def signal(self, close: float, rsi_fast: float) -> float | None:
        if close > self.helper(rsi_fast):
            return close
        return None


def test_helper_unknown_name_does_not_resolve_from_outer_method_params() -> (
    None
):
    with pytest.raises(NameError, match="close"):
        PolarsExprTranspiler(_HelperScopeLeakStrategy()).transpile("signal")


class _ElifStrategy:
    def signal(self, bucket: float, x: float) -> float | None:
        if bucket == 1:
            if x > 10:
                return x
        elif bucket == 2:
            if x < 5:
                return x
        return None


def test_elif_collects_or_branches() -> None:
    df = pl.DataFrame(
        {"bucket": [1.0, 1.0, 2.0, 2.0, 3.0], "x": [11.0, 9.0, 3.0, 7.0, 100.0]}
    )
    _assert_matches_reference(
        _ElifStrategy(),
        "signal",
        df,
        lambda row: (
            row["x"]
            if (row["bucket"] == 1 and row["x"] > 10)
            or (row["bucket"] == 2 and row["x"] < 5)
            else None
        ),
    )


class _PowerStrategy:
    def signal(self, base: float, exp: float, limit: float) -> float | None:
        if base**exp > limit:
            return base
        return None


def test_power_operator() -> None:
    df = pl.DataFrame(
        {"base": [2.0, 3.0], "exp": [3.0, 2.0], "limit": [7.0, 10.0]}
    )
    _assert_matches_reference(
        _PowerStrategy(),
        "signal",
        df,
        lambda row: (
            row["base"] if row["base"] ** row["exp"] > row["limit"] else None
        ),
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
        lambda row: row["close"] if row["close"] > support else None,
    )
