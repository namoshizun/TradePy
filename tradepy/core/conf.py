import os
import pathlib
from dataclasses import dataclass, field
from typing import Literal

from tradepy.types import MarketType, Markets

ModeType = Literal["backtest", "trading"]

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
    redis_host: str = getenv("REDIS_HOST", "localhost")
    redis_port: int = int(getenv("REDIS_PORT", 6379))
    redis_db: int = int(getenv("REDIS_DB", 0))
    redis_password: str = getenv("REDIS_PASSWORD", "password")

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
