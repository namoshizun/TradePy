import os
import pathlib
import importlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Type
from dotenv import load_dotenv
from redis import Redis, ConnectionPool

from tradepy.types import MarketType, Markets

if TYPE_CHECKING:
    from tradepy.core.strategy import LiveStrategy


ModeType = Literal["backtest", "trading"]

load_dotenv()
getenv = os.environ.get


@dataclass
class Config:
    # Common Conf
    database_dir: pathlib.Path = pathlib.Path(os.getcwd()) / "database"
    mode: ModeType = "backtest"
    market_types: list[MarketType] = field(default_factory=lambda: ([
        Markets.SH_MAIN,
        Markets.SZ_MAIN,
        Markets.SME,
        Markets.CHI_NEXT,
    ]))

    # Trading Mode Conf
    tick_fetch_interval = int(getenv("TICK_FETCH_INTERVAL", "5"))
    assets_sync_interval = int(getenv("ASSETS_SYNC_INTERVAL", "3"))
    redis_host: str = getenv("REDIS_HOST", "localhost")
    redis_port: int = int(getenv("REDIS_PORT", 6379))
    redis_db: int = int(getenv("REDIS_DB", 0))
    redis_password: str = getenv("REDIS_PASSWORD", "")
    strategy_class: str = getenv("TRADE_STRATEGY_CLASS", "")

    # global states
    redis_connection_pool: ConnectionPool | None = None

    def get_strategy_class(self) -> Type["LiveStrategy"]:
        assert self.strategy_class
        *module_path, class_name = self.strategy_class.split('.')
        module_path = '.'.join(module_path)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)

    def get_redis_client(self) -> Redis:
        if self.redis_connection_pool is None:
            self.redis_connection_pool = ConnectionPool(
                host=self.redis_host,
                port=self.redis_port,
                password=self.redis_password,
                db=self.redis_db,
                decode_responses=True
            )
        return Redis(connection_pool=self.redis_connection_pool)

    def set_database_dir(self, path: str | pathlib.Path):
        if isinstance(path, str):
            _path = pathlib.Path(path)
        else:
            _path = path

        if not _path.exists():
            raise FileNotFoundError(f'Dataset directory {path} does not exist')
        self.database_dir = _path

    def set_mode(self, mode: ModeType):
        if mode == "trading":
            assert self.redis_password, "Please provide the redis password by setting environment variable REDIS_PASSWORD"
        self.mode = mode

    def exit(self):
        if self.redis_connection_pool:
            self.redis_connection_pool.disconnect()
