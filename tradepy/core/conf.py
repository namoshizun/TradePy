import os
import yaml
import psutil
from pathlib import Path
from contextlib import suppress
from typing import Any, Literal, Type, TYPE_CHECKING
from operator import getitem
from pydantic import BaseModel, ConfigDict, Field, model_validator, field_validator
from redis import Redis, ConnectionPool
from dotenv import load_dotenv

from tradepy.optimization.base import ParameterOptimizer
from tradepy.types import MarketType
from tradepy.utils import import_class

if TYPE_CHECKING:
    from tradepy.strategy.base import StrategyBase
    from tradepy.backtest.evaluation import ResultEvaluator


load_dotenv()
ModeType = Literal["backtest", "paper-trading", "live-trading"]


# ----
# Base
# ----
class ConfBase(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]):
        def process_value(key: str, value: Any):
            if isinstance(value, str) and value.startswith("$"):
                # Substitute environment variables
                return os.path.expandvars(value)
            elif isinstance(value, dict):
                field_def = cls.model_fields[key]
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
    tick_fetch_interval: int = Field(5, description="行情更新间隔, 单位秒")
    assets_sync_interval: int = Field(3, description="资产与持仓更新间隔, 单位秒")
    cancel_expired_orders_interval: int = Field(5, description="过期未成交委托单清理间隔, 单位秒")


class TimeoutsConf(ConfBase):
    download_quote: int = Field(3, description="下载行情超时时间, 单位秒")
    download_ask_bid: int = Field(2, description="下载买卖盘超时时间, 单位秒")
    compute_open_indicators: int = Field(60 * 4, description="盘前计算当日指标超时时间, 单位秒, 最长4分钟")
    compute_close_indicators: int = Field(
        60 * 4, description="尾盘计算平仓指标超时时间, 单位秒, 最长4分钟"
    )
    handle_cont_trade: int = Field(2, description="处理每个行情推送的超时时间, 包含了策略给出买卖信号和下单, 单位秒")

    @model_validator(mode="after")
    def check_settings(self):
        if self.compute_open_indicators > 60 * 4:
            raise ValueError("compute_open_indicators 应该小于 240 (4 分钟)")

        if self.compute_close_indicators > 60 * 4:
            raise ValueError("compute_close_indicators 应该小于 240 (4 分钟)")

        return self


class RedisConf(ConfBase):
    host: str
    port: int
    db: int
    password: str


class BrokerConf(ConfBase):
    host: str = Field(..., description="交易端服务地址")
    port: int = Field(..., description="交易端服务端口")


class XtQuantConf(ConfBase):
    account_id: str = Field(..., description="您的券商账户ID")
    qmt_data_path: str = Field(..., description="QMT的Userdata_mini目录地址")
    price_type: str = "FIXED_PRICE"  # not yet supported


class SlippageConf(ConfBase):
    method: Literal["max_pct", "max_jump", "weibull"] = Field(
        ..., description="滑点计算方法, max_pct=最大随机百分比, max_jump=最大随机跳点"
    )
    params: Any = Field(
        ..., description="滑点计算方法的参数, 如: method=max_jump, params=2, 即为最大可随机出两跳价位的滑点"
    )


_default_slippage_conf = lambda: SlippageConf(method="max_pct", params=0.02)


class StrategyConf(ConfBase):
    strategy_class: str | None = Field(
        None, description="策略类名, 如: my_strategy.SampleStrategy"
    )
    stop_loss: float = Field(0, description="静态止损百分比， 如果不需要静态止盈止损， 可设置为一个任意大数")
    take_profit: float = Field(0, description="静态止盈百分比")
    take_profit_slip: SlippageConf = Field(
        default_factory=_default_slippage_conf, description="止盈滑点"
    )
    stop_loss_slip: SlippageConf = Field(
        default_factory=_default_slippage_conf, description="止损滑点"
    )
    max_position_size: float = Field(1, description="最大持仓百分比, 1 表示允许满仓单股")
    max_position_opens: int = Field(
        10000, description="每日最大开仓数量, 如果触发买入信号的标的数量大于此值, 则按照买入信号的权重值顺序买入，权重一致则随机选择"
    )
    min_trade_amount: int = Field(0, description="每次开仓的最小买入金额, 0 表示不限制")
    custom_params: dict[str, Any] = Field(
        default_factory=dict, description="自定义参数, 策略类内可在self上直接访问"
    )

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
    broker: BrokerConf = Field(..., description="交易端服务连接配置")
    xtquant: XtQuantConf | None = Field(None, description="XtQuant配置 (策略端不需要)")
    strategy: StrategyConf = Field(default_factory=StrategyConf, description="策略配置")
    periodic_tasks: PeriodicTasksConf = Field(
        default_factory=PeriodicTasksConf, description="定时任启动时间务配置"
    )
    timeouts: TimeoutsConf = Field(default_factory=TimeoutsConf, description="超时时间配置")
    indicators_window_size: int = Field(60, description="计算指标需要至少多少根K线")
    pending_order_expiry: float = Field(0, description="未成交委托单过期时间, 单位秒, 0 表示不过期")
    cache_retention: int = Field(7, description="缓存数据保留天数")
    markets: tuple[MarketType, ...] = Field(
        (
            "上证主板",
            "深证主板",
            "创业板",
        ),
        description="只交易这些主板的股票",
    )


# ---------
# Schedules
# ---------
class SchedulesConf(ConfBase):
    update_datasets: str = Field("0 20 * * *", description="更新本地数据, 默认每天20:00")
    warm_database: str = Field("0 9 * * *", description="预热数据库, 默认每天09:00")
    flush_broker_cache: str = Field("5 15 * * *", description="导出当日交易记录, 默认每天15:05")
    vacuum: str = Field("0 23 * * *", description="清理过期数据, 默认每天23:00")

    @staticmethod
    def parse_cron(cron: str):
        # E.g, parse "0 20 * * *" to dict of hour, minute, second etc
        CRON_FIELDS = ["minute", "hour", "day_of_month", "month_of_year", "day_of_week"]
        parse_value = lambda v: v if v == "*" else int(v)
        return {k: parse_value(v) for k, v in zip(CRON_FIELDS, cron.split())}

    @field_validator("update_datasets", "flush_broker_cache", "vacuum")
    def ensure_after_market_close(cls, value):
        time_specs = cls.parse_cron(value)
        if time_specs["hour"] < 15:
            raise ValueError("导出当日交易记录、更新本地数据、清理过期数据，应该在收盘后进行")
        return value

    @field_validator("warm_database")
    def ensure_before_market_open(cls, value):
        time_specs = cls.parse_cron(value)
        if time_specs["hour"] > 9 or time_specs["minute"] > 15:
            raise ValueError("预热数据库应该在09:15前进行")
        return value

    @model_validator(mode="after")
    def ensure_order(self):
        def to_iso_time(value):
            time_specs = self.parse_cron(value)
            return f"{time_specs['hour']:02}:{time_specs['minute']:02}"

        flush_cache_time = to_iso_time(self.flush_broker_cache)
        update_db_time = to_iso_time(self.update_datasets)
        vacuum_time = to_iso_time(self.vacuum)

        if flush_cache_time > update_db_time or flush_cache_time > vacuum_time:
            raise ValueError("导出数据库应该在更新本地数据或清理过期数据之前")

        if update_db_time > vacuum_time:
            raise ValueError("更新本地数据应该在清理过期数据之前")

        return self


# --------
# Backtest
# --------
class BacktestConf(ConfBase):
    cash_amount: float = Field(..., description="回测初始资金")
    broker_commission_rate: float = Field(0.05, description="佣金费率%, 万五是0.05")
    min_broker_commission_fee: float = Field(5, description="佣金最低收取金额")
    stamp_duty_rate: float = Field(0.1, description="印花税率%, 千分之一是0.1")
    use_minute_k: bool = Field(False, description="是否使用分钟级K线进行回测")
    strategy: StrategyConf
    sl_tf_order: Literal["stop loss first", "take profit first", "random"] = Field(
        "stop loss first", description="止盈止损单的触发顺序, random 表示随机选择"
    )


# ------------
# Optimization
# ------------
class DaskConf(ConfBase):
    n_workers: int = psutil.cpu_count(logical=False) * 3 // 4
    threads_per_worker: int = 1


class TaskConf(ConfBase):
    backtest: BacktestConf
    dataset_path: Path | None = None
    repetition: int = Field(1, description="同一批参数组合重复运行次数")
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
    mode: ModeType = Field(..., description="运行模式, 回测/模拟盘/实盘")
    database_dir: Path = Field(
        default_factory=lambda: Path.cwd() / "database", description="本地数据存放目录"
    )
    trade_lot_vol: int = Field(100, description="每手交易量")
    blacklist_path: Path | None = Field(None, description="股票黑名单文件路径,")
    redis: RedisConf | None = Field(None, description="Redis 配置")
    redis_connection_pool: ConnectionPool | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True) | ConfBase.model_config

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

    @field_validator("blacklist_path", mode="before")
    def check_blacklist_path(cls, value):
        if value is None:
            return value
        p = Path(value)
        assert p.exists(), f"黑名单文件不存在: {p}"
        return p

    @field_validator("database_dir", mode="before")
    def check_database_dir(cls, value):
        p = Path(value)
        p.mkdir(parents=True, exist_ok=True)
        return p


# ----
# Main
# ----
class TradePyConf(ConfBase):
    common: CommonConf
    trading: TradingConf | None = Field(None, description="实盘/模拟盘交易配置")
    schedules: SchedulesConf | None = Field(None, description="定时任务配置")
    notifications: NotificationConf | None = Field(
        None, description="微信通知配置，用于推送异常状态，未完成"
    )

    @classmethod
    def load_from_config_file(cls) -> "TradePyConf":
        if config_file_path := os.environ.get("TRADEPY_CONFIG_FILE"):
            config_file_path = Path(config_file_path)
        else:
            default_path = os.path.expanduser("~/.tradepy/config.yaml")
            config_file_path = Path(default_path)

        return cls.from_file(config_file_path)

    @model_validator(mode="after")
    def check_settings(self):
        mode: ModeType = self.common.mode

        if mode in ("paper-trading", "live-trading"):
            assert self.trading is not None, "交易模式下, trading 配置项不能为空"
            assert self.schedules is not None, "交易模式下, schedules 配置项不能为空"

        return self
