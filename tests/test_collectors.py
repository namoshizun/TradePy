import contextlib
from unittest import mock
import pytest
import tempfile
import pandas as pd
from pathlib import Path
from pandas.testing import assert_frame_equal

from tradepy.stocks import StocksPool
from tradepy.trade_cal import trade_cal
from tradepy.collectors.market_index import (
    EastMoneySectorIndexCollector,
    BroadBasedIndexCollector,
    BroadBasedIndexBarsDepot,
    SectorIndexBarsDepot,
)
from tradepy.collectors.stock_listing import StockListingDepot, StocksListingCollector
from tradepy.collectors.stock_day_bars import (
    StockDayBarsCollector,
    StocksDailyBarsDepot,
)
from tradepy.conversion import broad_index_code_name_mapping


@pytest.fixture
def local_listing_df():
    n_selected = 5
    df = StockListingDepot.load()
    if len(df) > n_selected:
        df = df.iloc[:n_selected].copy()
    return df


@pytest.fixture
def local_stocks_day_k_df():
    return StocksDailyBarsDepot.load()


@contextlib.contextmanager
def _use_alternative_stock_listing(listing_df: pd.DataFrame):
    with tempfile.TemporaryDirectory() as tempdir:
        with mock.patch("tradepy.config.common.database_dir", Path(tempdir)):
            StockListingDepot.save(listing_df)
            with mock.patch("tradepy.listing", StocksPool()):
                yield


def test_collect_stock_listing(local_listing_df: pd.DataFrame):
    selected_stocks = local_listing_df.index.tolist()
    fetched_listing_df = StocksListingCollector().run(
        selected_stocks=selected_stocks, write_file=False
    )
    fetched_listing_df.sort_index(inplace=True)
    local_listing_df.sort_index(inplace=True)

    static_cols = ["name", "sector", "listdate"]
    assert fetched_listing_df.columns.equals(local_listing_df.columns)
    assert_frame_equal(
        fetched_listing_df[static_cols],
        local_listing_df[static_cols],
        check_exact=False,
    )


def test_update_existing_stocks_day_k(local_stocks_day_k_df: pd.DataFrame):
    selected_stocks = local_stocks_day_k_df["code"].unique().tolist()
    stocks_max_ts = local_stocks_day_k_df["timestamp"].max()
    n_bars_per_stock = local_stocks_day_k_df["timestamp"].nunique()

    end_date_idx = (
        trade_cal.index(stocks_max_ts) - 10
    )  # set the ending date up to which we want the day bars to be updated
    end_date = trade_cal[end_date_idx]

    fetched_day_k_df = StockDayBarsCollector(
        since_date=stocks_max_ts, end_date=end_date
    ).run(selected_stocks=selected_stocks, write_file=False)
    assert isinstance(fetched_day_k_df, pd.DataFrame)

    for code, df in fetched_day_k_df.groupby("code"):
        assert code in selected_stocks
        # Check that the day bars between the previous max date and the `end_date` are fetched
        _df = df.query("@stocks_max_ts < timestamp <= @end_date")
        assert _df["timestamp"].nunique() == 10

        # And the existing day bars are not lost
        _df = df.query("timestamp <= @stocks_max_ts")
        assert _df["timestamp"].nunique() == n_bars_per_stock


def test_collect_new_stocks_day_k(
    local_stocks_day_k_df: pd.DataFrame, local_listing_df: pd.DataFrame
):
    selected_stocks = local_stocks_day_k_df["code"].unique().tolist()
    min_ts: str = local_stocks_day_k_df["timestamp"].min()
    max_ts: str = local_stocks_day_k_df["timestamp"].max()

    with _use_alternative_stock_listing(local_listing_df):
        fetched_day_k_df = StockDayBarsCollector(
            since_date=min_ts, end_date=max_ts
        ).run(selected_stocks=selected_stocks, write_file=False)
        assert isinstance(fetched_day_k_df, pd.DataFrame)
        assert fetched_day_k_df.columns.sort_values().equals(
            local_stocks_day_k_df.columns.sort_values()
        )

        main_cols = ["pct_chg"]
        _sort = lambda df: df.set_index(["code", "timestamp"]).sort_index()
        fetched_day_k_df = _sort(fetched_day_k_df)
        local_stocks_day_k_df = _sort(local_stocks_day_k_df)
        assert_frame_equal(
            fetched_day_k_df[main_cols],
            local_stocks_day_k_df[main_cols],
            check_exact=False,
        )


def test_collect_stocks_with_name_changes():
    stocks_had_name_changes = [
        ("600115", "中国东航", "20000101", "不重要の板块", 1e6),
        ("000768", "中航西飞", "20000101", "不重要の板块", 1e6),
        ("002037", "保利联合", "20000101", "不重要の板块", 1e6),
    ]
    listing_df = pd.DataFrame(
        stocks_had_name_changes,
        columns=["code", "name", "listdate", "sector", "total_share"],
    ).set_index("code")

    with _use_alternative_stock_listing(listing_df):
        fetched_day_k_df = StockDayBarsCollector(
            since_date="2019-01-02", end_date="2030-01-01"
        ).run(selected_stocks=listing_df.index.tolist(), write_file=True)
        assert isinstance(fetched_day_k_df, pd.DataFrame)

        for code in listing_df.index:
            assert fetched_day_k_df.query("code == @code")["company"].nunique() > 1


def test_collect_broadcast_based_index():
    BroadBasedIndexCollector().run()
    df = BroadBasedIndexBarsDepot.load()
    assert set(df.index.unique()) == set(broad_index_code_name_mapping.values())


def test_collect_sector_index():
    EastMoneySectorIndexCollector().run(start_date="2020-01-01")
    df = SectorIndexBarsDepot.load()
    assert len(df) > 0
