import pathlib

from tradepy.stocks import StocksPool
from tradepy.client import AkShareClient
from tradepy.conf import Config


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
    config.dataset_dir = _path


def initialize(dataset_dir: str | pathlib.Path):
    _init_dataset_dir(dataset_dir)
