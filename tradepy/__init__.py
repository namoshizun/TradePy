import sys
import random
from tqdm import tqdm

from tradepy.stocks import StocksPool
from tradepy.vendors.akshare import AkShareClient
from tradepy.core.conf import TradePyConf


def is_bootstrapping():
    return sys.orig_argv[-1] == "tradepy.cli.bootstrap"


tqdm.pandas()

random.seed()

ak_api = AkShareClient()

try:
    config: TradePyConf = TradePyConf.load_from_config_file()
except FileNotFoundError:
    if is_bootstrapping():
        pass
    else:
        raise


listing: StocksPool = StocksPool()

from tradepy.logging import LOG  # noqa
