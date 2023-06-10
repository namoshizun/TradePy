import os
import pathlib
from functools import cached_property
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Type, get_args
from dotenv import load_dotenv
from redis import Redis, ConnectionPool

from tradepy.utils import import_class

if TYPE_CHECKING:
    from tradepy.strategy.base import LiveStrategy


ModeType = Literal["optimization", "backtest", "paper-trading", "live-trading"]

load_dotenv()
getenv = os.environ.get


@dataclass
class Config:
    # Common Conf
    database_dir: pathlib.Path = pathlib.Path(os.getcwd()) / "database"
    mode: ModeType = getenv("MODE", "backtest")  # type: ignore

    # Trading Mode Conf
    tick_fetch_interval = int(getenv("TICK_FETCH_INTERVAL", "5"))
    assets_sync_interval = int(getenv("ASSETS_SYNC_INTERVAL", "3"))
    redis_host: str = getenv("REDIS_HOST", "localhost")
    redis_port: int = int(getenv("REDIS_PORT", 6379))
    redis_password: str = getenv("REDIS_PASSWORD", "")
    redis_db: int = int(getenv("REDIS_DB", 0))
    strategy_class: str = getenv("TRADE_STRATEGY_CLASS", "")

    # Global states
    redis_connection_pool: ConnectionPool | None = None

    # Optimizer conf
    optimizer_class: str = getenv(
        "OPTIMIZER_CLASS", "tradepy.optimization.optimizers.grid_search.GridSearch"
    )

    @cached_property
    def blacklist_path(self) -> pathlib.Path | None:
        path = getenv("BLACKLIST_PATH")
        if not path:
            return None
        return pathlib.Path(path)

    def __post_init__(self):
        if self.mode not in get_args(ModeType):
            raise ValueError(f"无效的MODE参数: {self.mode}")

    def get_strategy_class(self) -> Type["LiveStrategy"]:
        return import_class(self.strategy_class)

    def get_redis_client(self) -> Redis:
        if self.redis_connection_pool is None:
            self.redis_connection_pool = ConnectionPool(
                host=self.redis_host,
                port=self.redis_port,
                password=self.redis_password,
                db=self.redis_db,
                decode_responses=True,
            )
        return Redis(connection_pool=self.redis_connection_pool)

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def set_database_dir(self, path: str | pathlib.Path):
        if isinstance(path, str):
            _path = pathlib.Path(path)
        else:
            _path = path

        if not _path.exists():
            raise FileNotFoundError(f"Dataset directory {path} does not exist")
        self.database_dir = _path

    def set_mode(self, mode: ModeType):
        if mode == "trading":
            assert (
                self.redis_password
            ), "Please provide the redis password by setting environment variable REDIS_PASSWORD"
        self.mode = mode

    def exit(self):
        if self.redis_connection_pool:
            self.redis_connection_pool.disconnect()
