import os
import pathlib
from dataclasses import dataclass
from typing import Literal

ModeType = Literal["backtest", "trading"]

getenv = os.environ.get


@dataclass
class Config:
    database_dir: pathlib.Path = pathlib.Path(os.getcwd()) / "database"
    mode: ModeType = "backtest"
    initialized: bool = False

    redis_host: str = getenv("REDIS_HOST", "localhost")
    redis_port: int = int(getenv("REDIS_PORT", 6379))
    redis_db: int = int(getenv("REDIS_DB", 0))
    redis_password: str = getenv("REDIS_PASSWORD", "")
