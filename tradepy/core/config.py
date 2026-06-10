import os
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal

import yaml
from dotenv import load_dotenv
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
)

from tradepy.utils import import_class

if TYPE_CHECKING:
    from tradepy.strategy import StrategyBase

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


# --------
# Strategy
# --------
class SlippageConf(ConfBase):
    method: Annotated[
        Literal["max_pct", "max_jump", "weibull"],
        Field(
            description="滑点计算方法, max_pct=最大随机百分比, max_jump=最大随机跳点",
        ),
    ]
    params: Annotated[
        Any,
        Field(
            description="滑点计算方法的参数, 如: method=max_jump, params=2, 即为最大可随机出两跳价位的滑点",
        ),
    ]


def _default_slippage_conf() -> SlippageConf:
    return SlippageConf(method="max_pct", params=0.02)


class StrategyConf(ConfBase):
    strategy_class: Annotated[
        str,
        Field(
            description='策略类的module导入路径, 比如"my_strategy.SampleStrategy"',
        ),
    ]
    stop_loss: Annotated[
        float,
        Field(
            default=0,
            description="静态止损百分比， 如果不需要静态止盈止损， 可设置为一个任意大数",
        ),
    ]
    take_profit: Annotated[
        float,
        Field(default=0, description="静态止盈百分比"),
    ]
    slippage: Annotated[
        SlippageConf,
        Field(default_factory=_default_slippage_conf, description="卖出滑点"),
    ]
    max_position_size: Annotated[
        float,
        Field(
            description="最大持仓百分比(0-1), 1 表示允许满仓单股",
        ),
    ] = 1
    max_position_opens: Annotated[
        int,
        Field(
            description="每日最大开仓数量, 如果触发买入信号的标的数量大于此值, 则按照买入信号的权重值顺序买入，权重一致则随机选择",
        ),
    ] = 10000
    min_trade_amount: Annotated[
        int,
        Field(description="每次开仓的最小买入金额, 0 表示不限制"),
    ] = 0

    def load_strategy(self) -> "StrategyBase":
        assert (kls_repr := self.strategy_class)

        if "." in kls_repr:
            kls = import_class(kls_repr)
        else:
            kls = eval(kls_repr)

        return kls(self)


# --------
# Backtest
# --------
class BacktestConf(ConfBase):
    initial_capital: Annotated[
        float,
        Field(description="回测初始资金"),
    ]
    stamp_duty_rate: Annotated[
        float,
        Field(default=0.1, description="印花税率%, 千分之一是0.1"),
    ]
    broker_commission_rate: Annotated[
        float,
        Field(default=0.05, description="佣金费率%, 万五是0.05"),
    ]
    min_broker_commission_fee: Annotated[
        float,
        Field(default=5, description="佣金最低收取金额"),
    ]
    sl_tf_order: Annotated[
        SL_TP_Order,
        Field(
            default="stop loss first",
            description="日K线同时满足止盈和止损条件时, 止盈止损单的触发顺序, random 表示随机选择",
        ),
    ]


# ------
# Common
# ------
class CommonConf(ConfBase):
    mode: ModeType = Field(..., description="运行模式, 回测/模拟盘/实盘")
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

    def get_adjust_factors_path(self) -> Path:
        p = self._get_stocks_dir() / "adjust_factors"
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
