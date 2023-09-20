import pandas as pd
import pytest
import talib
from typing import Any
from unittest import mock

from tradepy.strategy.base import BacktestStrategy, BuyOption
from tradepy.strategy.factors import FactorsMixin
from tradepy.decorators import tag
from tradepy.core.conf import BacktestConf, StrategyConf, SlippageConf
from tradepy.core.position import Position


class SampleBacktestStrategy(BacktestStrategy, FactorsMixin):
    @tag(notna=True)
    def vol_ref1(self, vol):
        return vol.shift(1)

    @tag(notna=True, outputs=["boll_upper", "boll_middle", "boll_lower"])
    def boll_21(self, close):
        return talib.BBANDS(close, 21, 2, 2)

    def should_buy(self, sma5, boll_lower, close, vol, vol_ref1) -> BuyOption | None:
        if close <= boll_lower:
            return close, 1

        if close >= sma5 and vol > vol_ref1:
            return close, 1

    def should_sell(self, close, boll_upper) -> bool:
        return close >= boll_upper

    def pre_process(self, bars_df: pd.DataFrame) -> pd.DataFrame:
        return bars_df.query('market != "科创板"').copy()


backtest_conf = BacktestConf(
    cash_amount=1e6,
    broker_commission_rate=0.01,
    min_broker_commission_fee=0,
    use_minute_k=False,
    sl_tf_order="stop loss first",
    strategy=StrategyConf(
        stop_loss=3,
        take_profit=4,
        take_profit_slip=SlippageConf(method="max_jump", params=1),
        stop_loss_slip=SlippageConf(method="max_pct", params=0.1),
        max_position_opens=10,
        max_position_size=0.25,
        min_trade_amount=8000,
    ),  # type: ignore
)

strategy_conf = backtest_conf.strategy


@pytest.fixture
def sample_strategy():
    return SampleBacktestStrategy(strategy_conf)


@pytest.fixture
def sample_position():
    return Position(
        id="1",
        code="000001",
        timestamp="2021-01-01",
        price=10,
        vol=100,
        latest_price=10,
        avail_vol=100,
        yesterday_vol=100,
    )


@pytest.fixture
def sample_portfolio():
    return pd.DataFrame(
        [(30, 1), (100, 1)],
        index=pd.Index(["000001", "000002"], name="code"),
        columns=["order_price", "weight"],
    )


def test_indicators_discovery(sample_strategy: SampleBacktestStrategy):
    assert set(sample_strategy.buy_indicators) == set(
        ["sma5", "boll_lower", "vol", "vol_ref1", "close"]
    )
    assert set(sample_strategy.sell_indicators) == set(["close", "boll_upper"])
    assert sample_strategy.take_profit_indicators == []
    assert sample_strategy.stop_loss_indicators == []


@pytest.mark.parametrize(
    "open_pct_chg,low_pct_chg,should_stop_loss",
    [
        (-(stop_loss := strategy_conf.stop_loss) * 0.2, -stop_loss * 0.8, False),
        (0, 0, False),
        (-stop_loss, -stop_loss, True),
        (-stop_loss * 0.2, -stop_loss * 3, True),
    ],
)
def test_stop_loss(
    sample_strategy: SampleBacktestStrategy,
    sample_position: Position,
    open_pct_chg: float,
    low_pct_chg: float,
    should_stop_loss: bool,
):
    bar: Any = {
        "open": sample_position.price_at_pct_change(open_pct_chg),
        "low": sample_position.price_at_pct_change(low_pct_chg),
    }
    assert (
        bool(sample_strategy.should_stop_loss(bar, sample_position)) == should_stop_loss
    )


@pytest.mark.parametrize(
    "open_pct_chg,high_pct_chg,should_take_profit",
    [
        ((take_profit := strategy_conf.take_profit) * 0.2, take_profit * 0.8, False),
        (0, 0, False),
        (take_profit, take_profit, True),
        (take_profit * 0.2, take_profit * 3, True),
    ],
)
def test_take_profit(
    sample_strategy: SampleBacktestStrategy,
    sample_position: Position,
    open_pct_chg: float,
    high_pct_chg: float,
    should_take_profit: bool,
):
    bar: Any = {
        "open": sample_position.price_at_pct_change(open_pct_chg),
        "high": sample_position.price_at_pct_change(high_pct_chg),
    }
    assert (
        bool(sample_strategy.should_take_profit(bar, sample_position))
        == should_take_profit
    )


def test_compute_indicators(
    sample_strategy: SampleBacktestStrategy,
    local_stocks_day_k_df: pd.DataFrame,
):
    df = sample_strategy.compute_all_indicators_df(local_stocks_day_k_df.copy())

    # All indicators should have been computed
    for ind in sample_strategy._required_indicators:
        assert ind in df.columns
        assert df[ind].notna().all()

    # The original open price is preserved after the prices are adjusted
    assert "orig_open" in df.columns

    # Indicators are not re-computed if they are already present
    with mock.patch(
        "tradepy.strategy.base.StrategyBase._adjust_then_compute"
    ) as compute_method:
        sample_strategy.compute_all_indicators_df(df.copy())
        assert compute_method.call_count == 0


def test_remove_stocks_without_adjust_factors(
    sample_strategy: SampleBacktestStrategy,
    local_stocks_day_k_df: pd.DataFrame,
):
    sample_stock = local_stocks_day_k_df.index[0]
    strange_stock_df = local_stocks_day_k_df.loc[sample_stock].copy()
    strange_stock_df["code"] = "999999"
    strange_stock_df["company"] = "6翻了的公司"
    strange_stock_df.reset_index(drop=True, inplace=True)
    strange_stock_df.set_index("code", drop=False, inplace=True)
    local_stocks_day_k_df = pd.concat([local_stocks_day_k_df, strange_stock_df])

    df = sample_strategy.compute_all_indicators_df(local_stocks_day_k_df.copy())
    assert "999999" not in df.index


def test_generate_valid_buy_orders(
    sample_strategy: SampleBacktestStrategy,
    sample_portfolio: pd.DataFrame,
):
    orders = sample_strategy.generate_buy_orders(
        sample_portfolio, "2023-03-03", budget=1e6
    )
    assert len(orders) == len(sample_portfolio)
    for order in orders:
        assert order.code in sample_portfolio.index
        assert order.timestamp == "2023-03-03"
        assert order.price == sample_portfolio.loc[order.code, "order_price"]
        assert order.vol >= 100
        assert order.direction == "buy"
        trade_amount = order.price * order.vol
        assert trade_amount >= strategy_conf.min_trade_amount


@pytest.mark.parametrize("budget", [-100, 0, 100])
def test_generate_no_orders_if_insufficient_budget(
    sample_strategy: SampleBacktestStrategy,
    sample_portfolio: pd.DataFrame,
    budget: float,
):
    orders = sample_strategy.generate_buy_orders(
        sample_portfolio, "2023-03-03", budget=budget
    )
    assert len(orders) == 0


def test_limit_position_opens(
    sample_strategy: SampleBacktestStrategy,
    sample_portfolio: pd.DataFrame,
):
    sample_strategy.conf.max_position_opens = 1
    modified_portfolio, budget = sample_strategy.adjust_portfolio_and_budget(
        sample_portfolio,
        budget=5e5,
        total_asset_value=1e6,
    )

    assert len(modified_portfolio) == 1
    assert budget / 1e6 <= strategy_conf.max_position_size
