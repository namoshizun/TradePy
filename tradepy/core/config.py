import os
from contextlib import suppress
from pathlib import Path
from typing import Annotated, Literal

import yaml
from dotenv import load_dotenv
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
)

load_dotenv()


ModeType = Literal["backtest", "paper-trading", "live-trading"]
SL_TP_Order = Literal["stop loss first", "take profit first", "random"]


def _existing_file(path: Path | None) -> Path | None:
    if path is None:
        return None
    if not path.is_file():
        raise ValueError(f"文件不存在或不是普通文件: {path}")
    return path


ExistingFilePath = Annotated[Path | None, AfterValidator(_existing_file)]


def _ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


AutoCreateDirPath = Annotated[Path, AfterValidator(_ensure_directory)]


# ----
# Base
# ----
class ConfBase(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True, validate_assignment=True
    )

    @classmethod
    def from_file(cls, file_path: Path):
        if os.environ.get("BUILD_DOC", "no") == "yes":
            return

        if not file_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {file_path}")

        with file_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            return cls(**config)


# ------
# Common
# ------
class CommonConf(ConfBase):
    mode: ModeType = Field(..., description="运行模式, 回测/模拟盘/实盘")
    trade_lot_vol: Annotated[int, Field(gt=0)] = Field(
        100, description="每手交易量"
    )
    database_dir: AutoCreateDirPath = Field(
        default_factory=lambda: Path.cwd() / "database",
        description="本地数据存放目录",
    )
    blacklist_path: ExistingFilePath = Field(
        default=None, description="股票黑名单文件路径"
    )
    download_concurrency: Annotated[int, Field(gt=0)] = Field(
        4, description="数据下载并发数"
    )

    tushare_token: SecretStr = Field(..., description="Tushare API Token")

    def _get_stocks_dir(self) -> Path:
        p = self.database_dir / "stocks"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def get_stock_listing_path(self) -> Path:
        return self._get_stocks_dir() / "listing.parquet"

    def get_stock_industry_class_path(self) -> Path:
        return self._get_stocks_dir() / "industry_class.parquet"

    def get_stock_day_klines_path(self) -> Path:
        p = self._get_stocks_dir() / "day" / "klines"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def get_stock_day_basics_path(self) -> Path:
        p = self._get_stocks_dir() / "day" / "basics"
        p.mkdir(parents=True, exist_ok=True)
        return p


# ----
# Main
# ----
class TradePyConf(ConfBase):
    common: CommonConf

    @staticmethod
    def get_default_config_file_path() -> Path:
        return Path.home() / ".tradepy" / "config.yaml"

    @classmethod
    def load_from_config_file(cls) -> "TradePyConf":
        if _path := os.environ.get("TRADEPY_CONFIG_FILE"):
            config_file_path = Path(_path)
        else:
            config_file_path = cls.get_default_config_file_path()

        return cls.from_file(config_file_path)

    def save_to_config_file(self, path: str | Path | None = None):
        if path is None:
            path = self.get_default_config_file_path()

        elif isinstance(path, str):
            path = Path(path)

        with path.open("w") as f:
            conf_dict = self.model_dump()
            with suppress(KeyError):
                # Ad-hoc converting `database_path` to str
                conf_dict["common"]["database_dir"] = (
                    conf_dict["common"]["database_dir"].absolute().as_posix()
                )

            yaml.safe_dump(conf_dict, f, allow_unicode=True)
