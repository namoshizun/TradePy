from pathlib import Path
import pytest
import tempfile
import pandas as pd

import tradepy
from tradepy.collectors.etf_listing import ETFListingCollector
from tradepy.collectors.stock_listing import StockListingDepot, StocksListingCollector
from tradepy.collectors.stock_day_bars import (
    StockDayBarsCollector,
    StocksDailyBarsDepot,
)


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


@pytest.yield_fixture
def reset_database_dir(local_listing_df: pd.DataFrame):
    # Temporary change the database dir to a temp dir, but restore it after the test
    with tempfile.TemporaryDirectory() as tempdir:
        curr_dir_backup = Path(tradepy.config.common.database_dir)
        tradepy.config.common.database_dir = Path(tempdir)
        StockListingDepot.save(local_listing_df)
        yield
        tradepy.config.common.database_dir = Path(curr_dir_backup)


def test_collect_stock_listing(local_listing_df: pd.DataFrame):
    selected_stocks = local_listing_df.index.tolist()
    fetched_listing_df = StocksListingCollector().run(
        selected_stocks=selected_stocks, write_file=False
    )
    fetched_listing_df.sort_index(inplace=True)
    local_listing_df.sort_index(inplace=True)

    static_cols = ["name", "sector", "listdate"]
    assert fetched_listing_df.columns.equals(local_listing_df.columns)
    assert fetched_listing_df[static_cols].equals(local_listing_df[static_cols])


def test_collect_stock_day_k(local_stocks_day_k_df: pd.DataFrame, reset_database_dir):
    selected_stocks = local_stocks_day_k_df["code"].unique().tolist()
    min_ts: str = local_stocks_day_k_df["timestamp"].min()
    max_ts: str = local_stocks_day_k_df["timestamp"].max()

    fetched_day_k_df = StockDayBarsCollector(since_date=min_ts, end_date=max_ts).run(
        selected_stocks=selected_stocks, write_file=False
    )
    assert isinstance(fetched_day_k_df, pd.DataFrame)
    assert fetched_day_k_df.columns.sort_values().equals(
        local_stocks_day_k_df.columns.sort_values()
    )

    main_cols = ["open", "high", "low", "close", "vol", "chg"]
    _sort = lambda df: df.reset_index(drop=True).sort_values(["code", "timestamp"])
    fetched_day_k_df = _sort(fetched_day_k_df)
    local_stocks_day_k_df = _sort(local_stocks_day_k_df)
    assert fetched_day_k_df[main_cols].equals(local_stocks_day_k_df[main_cols])
