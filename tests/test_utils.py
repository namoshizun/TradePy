import numpy as np
import pandas as pd
from datetime import date
from unittest import mock
from tradepy.utils import (
    gt,
    lt,
    eq,
    gte,
    lte,
    between,
    get_latest_trade_date,
    chunks,
    calc_pct_chg,
    calc_days_diff,
    round_val,
    optimize_dtype_memory,
    import_class,
)
from tradepy.trade_cal import trade_cal


def test_gt():
    assert gt(2.0, 1.0)
    assert not gt(1.0, 2.0)
    assert gt(pd.Series([2.0, 3.0]), pd.Series([1.0, 2.0])).all()
    assert not gt(pd.Series([1.0, 2.0]), pd.Series([2.0, 3.0])).any()


def test_lt():
    assert lt(1.0, 2.0)
    assert not lt(2.0, 1.0)
    assert lt(pd.Series([1.0, 2.0]), pd.Series([2.0, 3.0])).all()
    assert not lt(pd.Series([2.0, 3.0]), pd.Series([1.0, 2.0])).any()


def test_eq():
    assert eq(1.0, 1.00001)
    assert not eq(1.0, 1.1)
    assert eq(pd.Series([1.0, 2.0]), pd.Series([1.00001, 2.00001])).all()
    assert not eq(pd.Series([1.0, 2.0]), pd.Series([1.1, 2.1])).any()


def test_gte():
    assert gte(2.0, 1.9999)
    assert gte(2.0, 2.0)
    assert not gte(1.0, 2.0)
    assert gte(pd.Series([2.0, 3.0]), pd.Series([1.9999, 2.0001])).all()
    assert gte(pd.Series([2.0, 3.0]), pd.Series([2.0, 3.0])).all()
    assert not gte(pd.Series([1.0, 2.0]), pd.Series([2.0, 3.0])).any()


def test_lte():
    assert lte(1.9999, 2.0)
    assert lte(2.0, 2.0)
    assert not lte(2.0, 1.0)
    assert lte(pd.Series([1.9999, 2.0001]), pd.Series([2.0, 3.0])).all()
    assert lte(pd.Series([2.0, 3.0]), pd.Series([2.0, 3.0])).all()
    assert not lte(pd.Series([2.0, 3.0]), pd.Series([1.0, 2.0])).any()


def test_between():
    assert between(2.0, 1.9999, 2.0001)
    assert between(2.0, 2.0, 2.0)
    assert not between(1.0, 2.0, 3.0)
    assert between(
        pd.Series([2.0, 3.0, 4.0]),
        pd.Series([1.9999, 2.0, 3.9999]),
        pd.Series([2.0001, 3.0, 4.0001]),
    ).all()
    assert between(
        pd.Series([2.0, 3.0]), pd.Series([2.0, 3.0]), pd.Series([2.0, 3.0])
    ).all()
    assert not between(
        pd.Series([1.0, 2.0]), pd.Series([2.0, 3.0]), pd.Series([3.0, 4.0])
    ).any()


def test_get_latest_trade_date():
    latest_trade_date = get_latest_trade_date()
    assert isinstance(latest_trade_date, date)

    # Return today if it is already a trade day
    with mock.patch("tradepy.utils.date") as mock_date:
        mock_date.today.return_value = date(2023, 9, 15)
        latest_trade_date = get_latest_trade_date()
        assert str(latest_trade_date) == "2023-09-15"

    # Return the latest known trade date if today is outside of the trade cal
    with mock.patch("tradepy.utils.date") as mock_date:
        mock_date.today.return_value = date(2030, 9, 18)
        latest_trade_date = get_latest_trade_date()
        assert str(latest_trade_date) == trade_cal[0]


def test_chunks():
    data = list(range(10))
    batch_size = 3
    chunked_data = list(chunks(data, batch_size))
    assert len(chunked_data) == 4
    assert chunked_data[0] == [0, 1, 2]
    assert chunked_data[-1] == [9]


def test_calc_pct_chg():
    base_price = 100.0
    then_price = 110.0
    pct_chg = calc_pct_chg(base_price, then_price)
    assert pct_chg == 10.0


def test_calc_days_diff():
    d1 = date(2023, 9, 17)
    d2 = date(2023, 9, 10)
    days_diff = calc_days_diff(d1, d2)
    assert days_diff == 7


def test_round_val():
    @round_val
    def add_two_numbers(a, b):
        return a + b

    result = add_two_numbers(1.1111, 2.2222)
    assert result == 3.33


def test_optimize_dtype_memory():
    sample_data = pd.DataFrame(
        {
            "small_floats": [1.0, 2.0, 3.0],
            "large_floats": [1e5, 2e6, 3e7],
            "small_integers": [1, 2, 3],
            "large_integers": [int(1e5), int(2e6), int(3e7)],
            "enums": pd.Categorical(["a", "b", "c"]),
        }
    )
    optimized_df = optimize_dtype_memory(sample_data)
    assert optimized_df["small_floats"].dtype == np.float16
    assert optimized_df["large_floats"].dtype == np.float32
    assert optimized_df["small_integers"].dtype == np.int8
    assert optimized_df["large_integers"].dtype == np.int32
    assert optimized_df["enums"].dtype == "category"


def test_import_class():
    class_name = "datetime.date"
    imported_class = import_class(class_name)
    assert imported_class == date
