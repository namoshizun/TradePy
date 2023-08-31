import os
import yaml
import multiprocessing
from pathlib import Path
from contextlib import suppress
from typing import Any, Literal, Type, TYPE_CHECKING
from operator import getitem
from pydantic import BaseModel, Field, root_validator, validator
from redis import Redis, ConnectionPool
from dotenv import load_dotenv

from tradepy.optimization.base import ParameterOptimizer
from tradepy.types import MarketType
from tradepy.utils import import_class

if TYPE_CHECKING:
    from tradepy.strategy.base import StrategyBase
    from tradepy.backtest.evaluation import ResultEvaluator


load_dotenv()
ModeType = Literal["optimization", "backtest", "paper-trading", "live-trading"]


# ----
# Base
# ----
class ConfBase(BaseModel):
    class Config:
        validate_assignment = True

    @classmethod
    def from_dict(cls, data: dict[str, Any]):
        def process_value(key: str, value: Any):
            if isinstance(value, str) and value.startswith("$"):
                # Substitute environment variables
                return os.path.expandvars(value)
            elif isinstance(value, dict):
                field_def = cls.__fields__[key]
                field_type = field_def.annotation

                with suppress(TypeError):
                    if issubclass(field_type, ConfBase):
                        # Recursively process nested configuration sections
                        return field_type.from_dict(value)

            return value

        return cls(**{k: process_value(k, v) for k, v in data.items()})

    @classmethod
    def from_file(cls, file_path: str | Path):
        if os.environ.get("BUILD_DOC", "no") == "yes":
            return

        if isinstance(file_path, str):
            file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {file_path}")

        with file_path.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            return cls.from_dict(config)


# -------
# Trading
# -------
class PeriodicTasksConf(ConfBase):
    tick_fetch_interval: int = 5
    assets_sync_interval: int = 3
    cancel_expired_orders_interval: int = 5


class TimeoutsConf(ConfBase):
    download_quote: int = 3
    download_ask_bid: int = 2
    compute_open_indicators: int = 60 * 4  # <= 4 mins
    handle_cont_trade: int = 2
    compute_close_indicators: int = 60 * 4  # <= 4 mins

    @root_validator(pre=True)
    def check_settings(cls, values):
        if not values:
            return values

        if values["compute_open_indicators"] > 60 * 4:
            raise ValueError("compute_open_indicators 应该小于 240 (4 分钟)")

        if values["compute_close_indicators"] > 60 * 4:
            raise ValueError("compute_close_indicators 应该小于 240 (4 分钟)")

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
    price_type: str = "FIXED_PRICE"  # not yet supported


class SlippageConf(ConfBase):
    method: Literal["max_pct", "max_jump", "weibull"]
    params: Any


_default_slippage_conf = lambda: SlippageConf(method="max_pct", params=0.02)


class StrategyConf(ConfBase):
    strategy_class: str | None = None
    stop_loss: float = 0
    take_profit: float = 0
    take_profit_slip: SlippageConf = Field(default_factory=_default_slippage_conf)
    stop_loss_slip: SlippageConf = Field(default_factory=_default_slippage_conf)

    max_position_size: float = 1
    max_position_opens: int = 10000
    min_trade_amount: int = 0

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
        conf_dict = dict(**known_args)
        conf_dict.update(custom_params)
        return super(cls, cls).from_dict(conf_dict)

    def update(self, **kv_pairs):
        predefined_params = self.__fields__.keys()
        for key, value in kv_pairs.items():
            if key not in predefined_params:
                self.custom_params[key] = value
            else:
                setattr(self, key, value)

    def load_strategy(self) -> "StrategyBase":
        kls: Type["StrategyBase"] = self.load_strategy_class()
        return kls(self)

    def load_strategy_class(self) -> Type["StrategyBase"]:
        assert self.strategy_class
        return import_class(self.strategy_class)


class TradingConf(ConfBase):
    broker: BrokerConf
    indicators_window_size: int = 20
    pending_order_expiry: float = 0  # seconds, 0 is no expiry
    cache_retention: int = 7  # days
    xtquant: XtQuantConf | None = None
    markets: tuple[MarketType, ...] = (
        "上证主板",
        "深证主板",
        "创业板",
    )
    strategy: StrategyConf = Field(default_factory=StrategyConf)
    periodic_tasks: PeriodicTasksConf = Field(default_factory=PeriodicTasksConf)
    timeouts: TimeoutsConf = Field(default_factory=TimeoutsConf)


# ---------
# Schedules
# ---------
class SchedulesConf(ConfBase):
    update_datasets: str = "0 20 * * *"
    warm_database: str = "0 9 * * *"
    flush_broker_cache: str = "5 15 * * *"
    vacuum: str = "0 23 * * *"

    @staticmethod
    def parse_cron(cron: str):
        # E.g, parse "0 20 * * *" to dict of hour, minute, second etc
        CRON_FIELDS = ["minute", "hour", "day_of_month", "month_of_year", "day_of_week"]
        parse_value = lambda v: v if v == "*" else int(v)
        return {k: parse_value(v) for k, v in zip(CRON_FIELDS, cron.split())}

    @validator("update_datasets", "flush_broker_cache", "vacuum")
    def ensure_after_market_close(cls, value):
        time_specs = cls.parse_cron(value)
        if time_specs["hour"] < 15:
            raise ValueError("导出当日交易记录、更新本地数据、清理过期数据，应该在收盘后进行")
        return value

    @validator("warm_database")
    def ensure_before_market_open(cls, value):
        time_specs = cls.parse_cron(value)
        if time_specs["hour"] > 9 or time_specs["minute"] > 15:
            raise ValueError("预热数据库应该在09:15前进行")
        return value

    @root_validator(skip_on_failure=True)
    def ensure_order(cls, values):
        def to_iso_time(value):
            time_specs = cls.parse_cron(value)
            return f"{time_specs['hour']:02}:{time_specs['minute']:02}"

        flush_cache_time = to_iso_time(values["flush_broker_cache"])
        update_db_time = to_iso_time(values["update_datasets"])
        vacuum_time = to_iso_time(values["vacuum"])

        if flush_cache_time > update_db_time or flush_cache_time > vacuum_time:
            raise ValueError("导出数据库应该在更新本地数据或清理过期数据之前")

        if update_db_time > vacuum_time:
            raise ValueError("更新本地数据应该在清理过期数据之前")

        return values


# --------
# Backtest
# --------
class BacktestConf(ConfBase):
    cash_amount: float
    broker_commission_rate: float = 0.05
    min_broker_commission_fee: float = 5
    stamp_duty_rate: float = 0.1
    use_minute_k: bool = False
    strategy: StrategyConf
    sl_tf_order: Literal[
        "stop loss first", "take profit first", "random"
    ] = "stop loss first"


# ------------
# Optimization
# ------------
class DaskConf(ConfBase):
    n_workers: int = multiprocessing.cpu_count() * 3 // 4
    threads_per_worker: int = 1


class TaskConf(ConfBase):
    backtest: BacktestConf
    dataset_path: Path | None = None
    repetition: int = 1
    evaluator_class: str = "tradepy.backtest.evaluation.BasicEvaluator"

    def load_evaluator_class(self) -> Type["ResultEvaluator"]:
        return import_class(self.evaluator_class)


class OptimizationConf(TaskConf):
    optimizer_class: str = "tradepy.optimization.optimizers.grid_search.GridSearch"

    def load_optimizer_class(self) -> Type[ParameterOptimizer]:
        return import_class(self.optimizer_class)


# ------------
# Notification
# ------------
class PushPlusWechatNotificationConf(ConfBase):
    enabled: bool = True
    token: str
    topic: str
    daily_limit: int = 100


class NotificationConf(ConfBase):
    wechat: PushPlusWechatNotificationConf | None


# ------
# Common
# ------
class CommonConf(ConfBase):
    mode: ModeType
    database_dir: Path = Field(Path.cwd() / "database")
    trade_lot_vol: int = 100
    blacklist_path: Path | None = None
    redis: RedisConf | None
    redis_connection_pool: ConnectionPool | None = None

    class Config(ConfBase.Config):
        arbitrary_types_allowed = True

    def get_redis_client(self) -> Redis:
        assert self.redis, "未设置Redis配置"

        if self.redis_connection_pool is None:
            self.redis_connection_pool = ConnectionPool(
                host=self.redis.host,
                port=self.redis.port,
                password=self.redis.password,
                db=self.redis.db,
                decode_responses=True,
            )
        return Redis(connection_pool=self.redis_connection_pool)

    def __del__(self):
        if self.redis_connection_pool is not None:
            self.redis_connection_pool.disconnect()

    @validator("blacklist_path", pre=True)
    def check_blacklist_path(cls, value):
        if value is None:
            return value
        p = Path(value)
        assert p.exists(), f"黑名单文件不存在: {p}"
        return p

    @validator("database_dir", pre=True)
    def check_database_dir(cls, value):
        p = Path(value)
        p.mkdir(parents=True, exist_ok=True)
        return p


# ----
# Main
# ----
class TradePyConf(ConfBase):
    common: CommonConf
    trading: TradingConf | None
    schedules: SchedulesConf | None
    notifications: NotificationConf | None

    @classmethod
    def load_from_config_file(cls) -> "TradePyConf":
        if config_file_path := os.environ.get("TRADEPY_CONFIG_FILE"):
            config_file_path = Path(config_file_path)
        else:
            default_path = os.path.expanduser("~/.tradepy/config.yaml")
            config_file_path = Path(default_path)

        return cls.from_file(config_file_path)

    @root_validator(skip_on_failure=True)
    def check_settings(cls, values):
        mode: ModeType = values["common"].mode

        if mode in ("paper-trading", "live-trading"):
            assert values["trading"] is not None, "交易模式下, trading 配置项不能为空"

        return values
