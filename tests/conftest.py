import pytest
import tempfile
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
from .fixtures.load import load_dataset


@pytest.yield_fixture(scope="session", autouse=True)
def init_tradepy_config():
    if getattr(tradepy, "config", None) is not None:
        yield tradepy.config
        return

    working_dir = tempfile.TemporaryDirectory()
    logger.info(f"Initializing tradepy config... Temporary database dir: {working_dir}")
    tradepy.config = TradePyConf(
        common=CommonConf(
            mode="backtest",
            database_dir=Path(working_dir.name),
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
    yield tradepy.config
    working_dir.cleanup()


@pytest.fixture(scope="session", autouse=True)
def download_dataset(init_tradepy_config: TradePyConf):
    for name in ["adjust-factors", "daily-k", "listing"]:
        load_dataset(name, init_tradepy_config.common.database_dir)
    yield
