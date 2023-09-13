import json
import os
import sys
from pydantic import BaseModel
import redis
import random
import yaml
import subprocess
from pathlib import Path

import tradepy
from tradepy.core.conf import (
    BrokerConf,
    CommonConf,
    RedisConf,
    SchedulesConf,
    TradePyConf,
    TradingConf,
    XtQuantConf,
)
from tradepy.collectors.stock_listing import StockListingDepot, StocksListingCollector
from tradepy.collectors.adjust_factor import AdjustFactorCollector, AdjustFactorDepot


def check_host_reachable(host):
    """
    Check if a host is reachable using the ping command.
    Returns True if the host is reachable, False otherwise.
    """
    # Execute the ping command
    ping_cmd = ["ping", "-c", "1", host]
    result = subprocess.run(ping_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Check the output of the ping command
    if result.returncode == 0:
        return True
    else:
        return False


def conf_to_dict(conf: BaseModel):
    return json.loads(conf.json(exclude_none=True))


class ConfigInitializer:
    conf_dir = Path.home() / ".tradepy"
    conf_file: Path = conf_dir / "config.yaml"

    def __init__(self):
        if not os.path.exists(self.conf_dir):
            os.makedirs(self.conf_dir)
        self.config_items = dict()

    @property
    def is_trading_mode(self):
        return self.config_items["mode"] in ("paper-trading", "live-trading")

    def file_exists(self) -> bool:
        return self.conf_file.exists()

    def set_mode(self, mode: str):
        if mode not in ["backtest", "paper-trading", "live-trading"]:
            print(f"运行模式不正确!")
            sys.exit(1)

        self.config_items["mode"] = mode

    def set_database_dir(self, path: str):
        if not path:
            print(f"K线数据的下载目录不能为空!")
            sys.exit(1)

        path = os.path.expanduser(path)
        os.makedirs(path, exist_ok=True)
        self.config_items["database_dir"] = path

    def set_redis_connection(self, host: str, port: str, password: str):
        if not password:
            print(f"Redis密码不能为空!")
            sys.exit(1)

        r = redis.Redis(host=host, port=int(port), password=password)
        try:
            r.ping()
        except Exception as e:
            print(f"Redis连接失败: {e}")
            sys.exit(1)

        self.config_items["redis"] = {
            "host": host,
            "port": port,
            "password": password,
            "db": 0,
        }

    def set_xt_quant_connection(self, userdata_path: str, account_id: str):
        from xtquant.xttrader import XtQuantTrader
        from xtquant.xttype import StockAccount

        session_id = random.randint(100000, 999999)
        XtQuantTrader(userdata_path, session_id)
        StockAccount(account_id)

        if not os.path.exists(userdata_path):
            print(f"XtQuant Userdata_Mini目录不存在: {userdata_path}")
            sys.exit(1)

        self.config_items["xtquant"] = {
            "qmt_data_path": userdata_path,
            "account_id": account_id,
        }

    def set_broker_connection(self, host: str, port: str):
        print(" ~ 检查交易端服务地址是否可达 ...", end="")
        if not check_host_reachable(host):
            print(f"🚨 交易端服务地址不可达: {host}")
            sys.exit(1)

        print("ok!")
        self.config_items["broker"] = {
            "host": host,
            "port": port,
        }

    def fetch_prerequisite_dataset(self):
        print("检查是否已下载基础数据 ...")
        print("[1] 股票列表 ")
        if not StockListingDepot.file_path().exists():
            print(" ~ 股票列表不存在，开始下载")
            StocksListingCollector().run(batch_size=25)

        print("[2] 复权因子 ")
        if not AdjustFactorDepot.file_path().exists():
            print(" ~ 复权因子不存在，开始下载")
            AdjustFactorCollector().run(batch_size=25)

    def write(self, is_broker: bool) -> Path:
        # Assemble configurations
        conf = dict()
        conf["common"] = self._get_common_conf()
        if self.is_trading_mode:
            conf["trading"] = self._get_trading_conf()
            if not is_broker:
                conf["schedules"] = self._get_schedules_conf()
        # Write the config file
        with open(self.conf_file, "w") as f:
            yaml.dump(conf, f)
        return self.conf_file

    def _get_common_conf(self) -> dict:
        conf = CommonConf(mode=self.config_items["mode"])
        if self.config_items.get("redis"):
            conf.redis = RedisConf(**self.config_items["redis"])

        if self.config_items.get("database_dir"):
            conf.database_dir = self.config_items["database_dir"]

        return conf_to_dict(conf)

    def _get_trading_conf(self) -> dict:
        conf = TradingConf(broker=BrokerConf(**self.config_items["broker"]))
        if self.config_items.get("xtquant"):
            conf.xtquant = XtQuantConf(**self.config_items["xtquant"])
        return conf_to_dict(conf)

    def _get_schedules_conf(self) -> dict:
        conf = SchedulesConf()
        return conf_to_dict(conf)


def main():
    print("[TradePy初始化程序]")
    initializer = ConfigInitializer()
    if initializer.file_exists():
        ans = input("👀 已存在配置文件，无需初始化, 确定重写? (y/n): ")
        if ans.lower() == "n":
            sys.exit(0)

    # Mode
    mode = input("> 请输入运行模式 (backtest=回测, paper-trading=模拟交易, live-trading=实盘交易) : ")
    initializer.set_mode(mode)

    # Database directory
    need_database_dir, is_broker = False, False
    if initializer.is_trading_mode:
        ans = input("> 是否为交易端? (y/n): ")
        is_broker = ans.lower() == "y"
        need_database_dir = not is_broker
    else:
        need_database_dir = True

    if need_database_dir:
        database_dir = input("> 请输入K线数据的下载目录（完整地址）: ")
        initializer.set_database_dir(database_dir)

    if initializer.is_trading_mode:
        # Redis
        redis_host = input("> 请输入Redis地址（默认localhost）: ") or "localhost"
        redis_port = input("> 请输入Redis端口（默认6379）: ") or "6379"
        redis_password = input("> 请输入Redis密码: ")
        initializer.set_redis_connection(redis_host, redis_port, redis_password)

        # Broker
        broker_host = input("> 请输入交易端服务地址: ")
        broker_port = input("> 请输入交易端服务端口（默认8000）: ") or "8000"
        initializer.set_broker_connection(broker_host, broker_port)

        if is_broker:
            # XtQuant
            xt_quant_dir = input("> 请输入XtQuant Userdata_Mini目录（完整地址）: ")
            xt_quant_account_id = input("> 请输入XtQuant账户ID: ")
            initializer.set_xt_quant_connection(xt_quant_dir, xt_quant_account_id)

    settings_file_path = initializer.write(is_broker)
    print(f"👌 已创建配置文件: {settings_file_path}")

    if initializer.is_trading_mode:
        if not is_broker:
            print("🚨 策略端的TradePy配置文件内，还需要手动填入您的交易策略的配置项")
        tradepy.config = TradePyConf.load_from_config_file()
        initializer.fetch_prerequisite_dataset()


if __name__ == "__main__":
    main()
