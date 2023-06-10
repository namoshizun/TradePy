from contextlib import suppress
import os
from typing import Any, Literal, Type
from operator import getitem
from pydantic import BaseModel, Field, root_validator

from tradepy.optimization.base import ParameterOptimizer
from tradepy.strategy.base import StrategyBase
from tradepy.utils import import_class


ModeType = Literal["optimization", "backtest", "paper-trading", "live-trading"]


class ConfBase(BaseModel):
    @classmethod
    def from_dict(cls, data: dict[str, Any]):
        def process_value(key: str, value: Any):
            if isinstance(value, str) and value.startswith("$"):
                # Substitute environment variables
                return os.path.expandvars(value)
            elif isinstance(value, dict):
                field_def = cls.__fields__[key]
                field_type = field_def.outer_type_

                with suppress(TypeError):
                    if issubclass(field_type, ConfBase):
                        # Recursively process nested configuration sections
                        return field_type.from_dict(value)

            return value

        return cls(**{k: process_value(k, v) for k, v in data.items()})


class CommonConf(ConfBase):
    mode: ModeType
    trade_lot_vol: int = 100
    blacklist_path: str | None = None


# -----------------------
# Backtest & Optimization
# -----------------------
class BacktestConf(ConfBase):
    cash_amount: float
    broker_commission_rate: float = 0.05
    stamp_duty_rate: float = 0.1


class OptimizationConf(ConfBase):
    optimizer_class: str
    cash_amount: float
    broker_commission_rate: float = 0.05
    stamp_duty_rate: float = 0.1

    def load_optimizer_class(self) -> Type[ParameterOptimizer]:
        return import_class(self.optimizer_class)


# -------
# Trading
# -------
class PeriodicTasksConf(ConfBase):
    tick_fetch_interval: int = 5
    assets_sync_interval: int = 3


class TimeoutsConf(ConfBase):
    download_quote: int = 3
    handle_pre_market_open_call: int = 240  # 4 mins
    compute_open_indicators: int = 228  # < 3.8 mins
    handle_cont_trade: int = 2
    handle_cont_trade_pre_close: int = 180  # 3 mins
    compute_close_indicators: int = 120  # 2mins

    @root_validator(pre=True)
    def check_settings(cls, values):
        if values["handle_pre_market_open_call"] < values["compute_open_indicators"]:
            raise ValueError("handle_pre_market_open_call 应该大于 compute_open_indicators")

        if values["handle_cont_trade_pre_close"] < values["compute_close_indicators"]:
            raise ValueError(
                "handle_cont_trade_pre_close 应该大于 compute_close_indicators"
            )
        return values


class RedisConf(ConfBase):
    host: str
    port: int
    db: int
    password: str


class BrokerConf(ConfBase):
    host: str
    port: int


class XtQuantConf(ConfBase):
    account_id: str
    qmt_data_path: str
    price_type: str


class StrategyConf(ConfBase):
    strategy_class: str
    stop_loss: float
    take_profit: float
    max_position_size: int = 1
    max_position_opens: int = 10000
    min_trade_amount: int = 0
    signals_percent_range: list[int] = [0, 100]

    custom_params: dict[str, Any] = Field(default_factory=dict)

    def __getattr__(self, name: str):
        return getitem(self.custom_params, name)

    @classmethod
    def from_dict(cls, data: dict[str, Any]):
        known_keys = cls.__fields__.keys()
        known_args = {k: v for k, v in data.items() if k in known_keys}
        custom_params = {k: v for k, v in data.items() if k not in known_keys}

        # FIXME: won't be able to expand env vars...
        # return cls(**known_args, custom_params=custom_params)
        return super(cls, cls).from_dict(
            dict(**known_args, custom_params=custom_params)
        )

    def load_strategy_class(self) -> Type[StrategyBase]:
        return import_class(self.strategy_class)


class TradingConf(ConfBase):
    strategy: StrategyConf
    periodic_tasks: PeriodicTasksConf = Field(default_factory=PeriodicTasksConf)
    timeouts: TimeoutsConf
    redis: RedisConf
    broker: BrokerConf
    xtquant: XtQuantConf


# ---------
# Schedules
# ---------


class SchedulesConf(ConfBase):
    update_datasets: str
    warm_broker_db: str
    flush_broker_cache: str


class TradePyConf(ConfBase):
    common: CommonConf
    backtest: BacktestConf | None
    optimization: OptimizationConf | None
    trading: TradingConf | None
    schedules: SchedulesConf | None
