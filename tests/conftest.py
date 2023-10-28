import os
import pytest
import tempfile
import talib
import pandas as pd
from pathlib import Path
from loguru import logger

import tradepy
from tradepy.core.conf import (
    TradePyConf,
    CommonConf,
    SchedulesConf,
    TradingConf,
    BrokerConf,
    TimeoutsConf,
)
from tradepy.depot.stocks import StockListingDepot, StocksDailyBarsDepot
from tradepy.depot.misc import AdjustFactorDepot
from tradepy.strategy.base import BacktestStrategy, BuyOption
from tradepy.strategy.factors import FactorsMixin
from tradepy.decorators import tag
from .fixtures_data.load import load_dataset


@pytest.yield_fixture(scope="session", autouse=True)
def init_tradepy_config():
    if getattr(tradepy, "config", None) is not None:
        yield tradepy.config
        return

    working_dir = tempfile.TemporaryDirectory()
    working_dir_path = Path(working_dir.name)
    logger.info(f"Initializing tradepy config... Temporary database dir: {working_dir}")
    tradepy.config = TradePyConf(
        common=CommonConf(
            mode="backtest",
            database_dir=working_dir_path,
            trade_lot_vol=100,
            blacklist_path=None,
            redis=None,
        ),
        trading=TradingConf(
            broker=BrokerConf(
                host="localhost",
                port=8001,
            ),
            timeouts=TimeoutsConf(
                download_quote=30,
                download_ask_bid=30,
            ),  # type: ignore
        ),  # type: ignore
        schedules=SchedulesConf(),  # type: ignore
        notifications=None,
    )

    tradepy.config.save_to_config_file(
        temp_config_path := working_dir_path / "config.yaml"
    )
    os.environ["TRADEPY_CONFIG_FILE"] = str(temp_config_path)
    yield tradepy.config
    os.environ.pop("TRADEPY_CONFIG_FILE")
    working_dir.cleanup()


@pytest.fixture(scope="session", autouse=True)
def download_dataset(init_tradepy_config: TradePyConf):
    for name in ["adjust-factors", "daily-k", "listing"]:
        load_dataset(name, init_tradepy_config.common.database_dir)
    yield


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


@pytest.fixture
def local_adjust_factors_df():
    return AdjustFactorDepot.load()


class SampleBacktestStrategy(BacktestStrategy, FactorsMixin):
    @tag(notna=True)
    def vol_ref1(self, vol):
        return vol.shift(1)

    @tag(notna=True, outputs=["boll_upper", "boll_middle", "boll_lower"])
    def boll_21(self, close):
        return talib.BBANDS(close, 21, 2, 2)

    def should_buy(self, sma5, boll_lower, close, vol, vol_ref1) -> BuyOption | None:
        if close <= boll_lower:
            return close, 1

        if close >= sma5 and vol > vol_ref1:
            return close, 1

    def should_sell(self, close, boll_upper) -> bool:
        return close >= boll_upper

    def pre_process(self, bars_df: pd.DataFrame) -> pd.DataFrame:
        return bars_df.query('market != "科创板"').copy()
