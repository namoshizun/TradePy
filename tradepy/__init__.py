import pathlib
import warnings
from tqdm import tqdm

from tradepy.stocks import StocksPool
from tradepy.vendors.akshare import AkShareClient
from tradepy.core.conf import Config, ModeType
from tradepy.warehouse import ListingDepot


tqdm.pandas()

ak_api = AkShareClient()

config: Config = Config()

listing: StocksPool = StocksPool()


def _init_dataset_dir(path: str | pathlib.Path):
    if isinstance(path, str):
        _path = pathlib.Path(path)
    else:
        _path = path

    if not _path.exists():
        raise FileNotFoundError(f'Dataset directory {path} does not exist')
    config.database_dir = _path


def _check_conf():
    if config.mode == "trading":
        assert config.redis_password, "Please provide the redis password by setting environment variable REDIS_PASSWORD"

    if not ListingDepot.file_path().exists():
        warnings.warn('Stock listing data file is not found. Please download the listing data using'
                      'StocksListingCollector first or tradepy might not work.')


def initialize(dataset_dir: str | pathlib.Path, mode: ModeType):
    if not config.initialized:
        _init_dataset_dir(dataset_dir)
        _check_conf()
        config.initialized = True


from tradepy.logging import LOG  # noqa
