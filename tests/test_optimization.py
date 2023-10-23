import math
import itertools
import pytest
import pandas as pd
from tradepy.core.conf import BacktestConf, OptimizationConf, SlippageConf, StrategyConf
from tradepy.strategy.base import BacktestStrategy, BuyOption
from tradepy.strategy.factors import FactorsMixin
from tradepy.decorators import tag
from tradepy.optimization.parameter import Parameter, ParameterGroup
from tradepy.optimization.optimizers.grid_search import GridSearch


class MovingAverageCrossoverStrategy(BacktestStrategy, FactorsMixin):
    @tag(outputs=["ema10_ref1", "sma30_ref1"], notna=True)
    def moving_averages_ref1(self, ema10, sma30) -> pd.Series:
        return ema10.shift(1), sma30.shift(1)

    def should_buy(
        self,
        orig_open,
        sma120,
        ema10,
        sma30,
        typical_price,
        atr,
        ema10_ref1,
        sma30_ref1,
        close,
        company,
    ) -> BuyOption | None:
        if "ST" in company:
            return

        if orig_open < self.min_stock_price:
            return

        volatility = 100 * atr / typical_price
        if volatility < self.min_volatility:
            return

        if (ema10 > sma120) and (ema10_ref1 < sma30_ref1) and (ema10 > sma30):
            return close, 1

    def should_sell(self, ema10, sma30, ema10_ref1, sma30_ref1):
        return (ema10_ref1 > sma30_ref1) and (ema10 < sma30)


@pytest.fixture
def parameters() -> list[Parameter | ParameterGroup]:
    return [
        Parameter("min_stop_price", (3,)),
        Parameter("min_volatility", (2,)),
        ParameterGroup(
            ("stop_loss", "take_profit"),
            (
                (1.8, 4),
                (10, 20),
            ),
        ),
    ]


@pytest.fixture
def optimization_conf() -> OptimizationConf:
    return OptimizationConf(
        repetition=1,
        backtest=BacktestConf(
            cash_amount=1e6,
            broker_commission_rate=0.01,
            min_broker_commission_fee=0,
            strategy=StrategyConf(
                strategy_class=MovingAverageCrossoverStrategy,
                take_profit_slip=SlippageConf(method="max_jump", params=1),
                stop_loss_slip=SlippageConf(method="max_pct", params=0.1),
                max_position_opens=10,
                max_position_size=0.25,
                min_trade_amount=8000,  # type: ignore
            ),
        ),
    )


def test_grid_search_parameter_generation(parameters):
    optimizer = GridSearch(parameters)

    # Check that the batches cover all the parameter combinations.
    batches = list(optimizer.generate_parameters_batch())
    assert len(batches) == math.prod(len(param.choices) for param in parameters)

    # Check that each batch contains all the parameters.
    param_names = itertools.chain.from_iterable(
        [param.name] if isinstance(param.name, str) else param.name
        for param in parameters
    )
    for batch in batches:
        assert all(param in batch for param in param_names)


def test_optimization_scheduler(
    parameters, optimization_conf: OptimizationConf, local_stocks_day_k_df: pd.DataFrame
):
    ...
