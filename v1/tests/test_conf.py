import os
import sys
import tempfile
import pytest
import inspect

from tradepy.core.conf import StrategyConf
from tradepy.strategy.base import StrategyBase
from .conftest import SampleBacktestStrategy


@pytest.yield_fixture
def strategy_class_import_path():
    # Save the source code of SampleBacktestStrategy to a temporary file, and add the file
    # directory to the current system path
    source = inspect.getsource(SampleBacktestStrategy)

    with tempfile.NamedTemporaryFile(suffix=".py", mode="w+") as f:
        f.write("import talib\n")
        f.write("import pandas as pd\n")
        f.write("from tradepy.strategy.base import BacktestStrategy, BuyOption\n")
        f.write("from tradepy.decorators import tag\n")
        f.write("from tradepy.strategy.factors import FactorsMixin\n\n")
        f.write(source)
        f.flush()
        temp_module_dir = os.path.dirname(f.name)
        sys.path.append(temp_module_dir)

        try:
            yield f"{os.path.basename(f.name)[:-3]}.SampleBacktestStrategy"
        finally:
            sys.path.remove(temp_module_dir)


@pytest.fixture
def strategy_class():
    return SampleBacktestStrategy


@pytest.mark.parametrize(
    "strategy_class_value",
    ["strategy_class_import_path", "strategy_class"],
)
def test_load_strategy_class(strategy_class_value, request):
    conf = StrategyConf(strategy_class=request.getfixturevalue(strategy_class_value))  # type: ignore
    strategy_class = conf.load_strategy_class()
    assert issubclass(strategy_class, StrategyBase)

    strategy_instance = conf.load_strategy()
    assert isinstance(strategy_instance, StrategyBase)
