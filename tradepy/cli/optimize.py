from typing import Any
import yaml
import argparse

from tradepy.optimization.schedulers import OptimizationScheduler
from tradepy.core.conf import DaskConf, OptimizationConf


def _read_config_yaml_file(path):
    with open(path, "r") as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def start(conf: dict[str, Any]):
    opt_conf = OptimizationConf.from_dict(conf["config"])
    dask_conf = DaskConf.from_dict(conf["dask"])
    scheduler = OptimizationScheduler(opt_conf, conf["parameters"])
    scheduler.run(dask_args=dask_conf.dict())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="运行模型寻参, 当前仅支持Grid Search")
    parser.add_argument("--config", type=str, help="参数文件地址")
    # TODO: allow using custom parameter search algorithm
    args = parser.parse_args()

    conf = _read_config_yaml_file(args.config)
    start(conf)
