import sys
import random
from tqdm import tqdm

from tradepy.stocks import StocksPool
from tradepy.vendors import akshare, tushare
from tradepy.core.conf import TradePyConf


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

try:
    config: TradePyConf = TradePyConf.load_from_config_file()
except FileNotFoundError:
    if is_bootstrapping() or is_running_tests() or is_building_docs():
        pass
    else:
        raise


listing: StocksPool = StocksPool()

from tradepy.logging import LOG  # noqa
