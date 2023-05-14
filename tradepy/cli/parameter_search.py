import yaml
import argparse

from tradepy.optimization.scheduler import Scheduler
from tradepy.optimization.parameter import Parameter, ParameterGroup
from tradepy.core.context import china_market_context


def _read_config_yaml_file(path):
    with open(path, 'r') as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def _make_parameter(name: str | list[str], choices) -> Parameter | ParameterGroup:
    if isinstance(name, str):
        assert isinstance(choices, list) and not isinstance(choices[0], list)
        return Parameter(name, tuple(choices))

    assert isinstance(choices, list) and all(len(c) == len(name) for c in choices)
    return ParameterGroup(
        name=tuple(name),
        choices=tuple(map(tuple, choices))
    )


def start(config):
    base_ctx = china_market_context(**config['base_context'])
    parameters = [
        _make_parameter(p["name"], p["choices"])
        for p in config['parameters']
    ]

    scheduler = Scheduler(
        parameters,
        base_ctx,
        dataset_path=config["dataset"],
        strategy=config["strategy"],
    )

    scheduler.run(
        repetitions=config["repetition"],
        dask_args={
            "n_workers": 11,
            "threads_per_worker": 1
        }
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='运行模型寻参, 当前仅支持Grid Search')
    parser.add_argument('--config', type=str, help='Path to the configuration file')
    # TODO: allow using custom parameter search algorithm
    args = parser.parse_args()

    conf = _read_config_yaml_file(args.config)
    start(conf)
