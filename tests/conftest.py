import pytest
import tempfile
from pathlib import Path
from loguru import logger

import tradepy
from tradepy.core.conf import TradePyConf, CommonConf, SchedulesConf


@pytest.fixture(scope="session", autouse=True)
def init_tradepy_config():
    if getattr(tradepy, "config", None) is not None:
        yield
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
        trading=None,
        schedules=SchedulesConf(),
        notifications=None,
    )
    yield tradepy.config
    working_dir.cleanup()


@pytest.fixture(scope="session", autouse=True)
def download_dataset(init_tradepy_config):
    from tradepy.collectors.stock_listing import (
        StocksListingCollector,
        StockListingDepot,
    )
    from tradepy.collectors.adjust_factor import (
        AdjustFactorCollector,
        AdjustFactorDepot,
    )

    if not StockListingDepot.file_path().exists():
        selected_stocks = ["000333", "000001", "600519"]
        logger.info(f"Downloading stock listing for {selected_stocks}")
        StocksListingCollector().run(selected_stocks=selected_stocks)

    if not AdjustFactorDepot.file_path().exists():
        logger.info(f"Downloading adjust factor")
        AdjustFactorCollector().run()

    yield
