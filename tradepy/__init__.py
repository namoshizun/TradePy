import sys
import random
from loguru import logger
from tqdm import tqdm

from tradepy.stocks import StocksPool
from tradepy.vendors import akshare, tushare
from tradepy.core.conf import TradePyConf
from tradepy.hacks import inject_hacks
from tradepy.logging import LOG  # noqa


def is_bootstrapping():
    return sys.orig_argv[-1] == "tradepy.cli.bootstrap"


def is_running_tests():
    return "pytest" in sys.modules


def is_building_docs():
    return "sphinx" in sys.modules


tqdm.pandas()

random.seed()

ak_api = akshare.AkShareClient()
ts_api = tushare

environment_status = {
    "bootstrapping": is_bootstrapping(),
    "running_tests": is_running_tests(),
    "building_docs": is_building_docs(),
}

try:
    config: TradePyConf = TradePyConf.load_from_config_file()
except FileNotFoundError:
    if (
        environment_status["bootstrapping"]
        or environment_status["building_docs"]
        or environment_status["running_tests"]
    ):
        logger.debug(f"TradePy配置项无法从配置文件中加载。当前环境状态{environment_status}")
    else:
        raise


listing: StocksPool = StocksPool()

inject_hacks()
