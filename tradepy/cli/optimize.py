from typing import Any
import yaml
import argparse

from tradepy.optimization.schedulers import OptimizationScheduler
from tradepy.core.conf import DaskConf, OptimizationConf
from tradepy import LOG


def _read_config_yaml_file(path):
    with open(path, "r") as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def start(conf: dict[str, Any]):
    opt_conf = OptimizationConf.from_dict(conf["config"])
    dask_conf = DaskConf.from_dict(conf["dask"])

    try:
        opt_conf.backtest.strategy.load_strategy_class()
    except (AttributeError, ImportError):
        LOG.error("策略类加载失败, 请检查配置文件中的strategy_class字段, 确保策略类可加载")
        return

    if not opt_conf.dataset_path or not opt_conf.dataset_path.exists():
        LOG.error("数据集不存在, 请检查配置文件中的dataset_path字段, 回测数据(已添加指标)已保存至本地")
        return

    scheduler = OptimizationScheduler(opt_conf, conf["parameters"])
    scheduler.run(dask_args=dask_conf.dict())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="运行模型寻参, 当前仅支持Grid Search")
    parser.add_argument("--config", type=str, help="参数文件地址")
    # TODO: allow using custom parameter search algorithm
    args = parser.parse_args()

    conf = _read_config_yaml_file(args.config)
    start(conf)
