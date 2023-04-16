import pathlib
import warnings
from redis import Redis
from tqdm import tqdm

from tradepy.stocks import StocksPool
from tradepy.vendors.akshare import AkShareClient
from tradepy.core.conf import Config
from tradepy.warehouse import ListingDepot


tqdm.pandas()

ak_api = AkShareClient()

config: Config = Config()

listing: StocksPool = StocksPool()

redis_client: Redis | None = None


def _check_conf():
    if config.mode == "backtest":
        if not ListingDepot.file_path().exists():
            warnings.warn('Stock listing data file is not found. Please download the listing data using'
                        'StocksListingCollector first or tradepy might not work.')


def initialize(dataset_dir: str | pathlib.Path | None = None):
    if dataset_dir:
        config.set_database_dir(dataset_dir)

    _check_conf()


from tradepy.logging import LOG  # noqa
