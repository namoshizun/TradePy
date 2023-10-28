import io
import pytest
import pandas as pd
from unittest import mock

from tradepy.trade_book.trade_book import TradeBook
from tradepy.core.conf import BacktestConf, StrategyConf, SlippageConf, SL_TP_Order
from tradepy.backtest.backtester import Backtester
from .conftest import SampleBacktestStrategy


@pytest.fixture
def backtest_conf():
    return BacktestConf(
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


@pytest.fixture
def sample_backtester(backtest_conf: BacktestConf):
    return Backtester(backtest_conf)


@pytest.fixture
def sample_strategy(backtest_conf: BacktestConf):
    return SampleBacktestStrategy(backtest_conf.strategy)


@pytest.fixture
def sample_computed_day_k_df(
    local_stocks_day_k_df: pd.DataFrame,
    sample_strategy: SampleBacktestStrategy,
    request: pytest.FixtureRequest,
):
    cache_key = "cached_computed_day_k"
    if raw_csv_str := request.config.cache.get(cache_key, None):
        return pd.read_csv(io.StringIO(raw_csv_str), dtype={"code": str})

    result_df: pd.DataFrame = sample_strategy.compute_all_indicators_df(
        local_stocks_day_k_df
    )
    buf = io.StringIO()
    result_df.reset_index(drop=True).to_csv(buf)
    buf.seek(0)
    request.config.cache.set(cache_key, buf.read())
    return result_df


@pytest.fixture
def sample_stock_data():
    _ = None
    return pd.DataFrame(
        [
            # code, close, sma5, boll_lower, boll_upper, vol, vol_ref1, comment
            ["000001", 10, 10, 11, _, _, _, "buy: close <= boll_lower"],
            ["000002", 11, 10, _, _, 100, 90, "buy: sma5 breakthrough"],
            ["000003", 12, 15, 10, _, _, _, "noop"],
            ["000004", 13, _, _, 10, _, _, "sell: close >= boll_upper"],
        ],
        columns=[
            "code",
            "close",
            "sma5",
            "boll_lower",
            "boll_upper",
            "vol",
            "vol_ref1",
            "comment",
        ],
    ).set_index("code")


def test_get_buy_options(
    sample_backtester: Backtester,
    sample_strategy: SampleBacktestStrategy,
    sample_stock_data: pd.DataFrame,
):
    selector = sample_stock_data["comment"].str.contains("buy")
    expect_buy_stocks = sample_stock_data[selector].index

    # Should buy the stocks that trigger the buy signals
    buy_options = sample_backtester.get_buy_options(sample_stock_data, sample_strategy)
    assert set(buy_options.index) == set(expect_buy_stocks)

    # Check the returned order prices
    for code, option in buy_options.iterrows():
        assert option["order_price"] == sample_stock_data.loc[code, "close"]


def test_get_close_signals(
    sample_backtester: Backtester,
    sample_strategy: SampleBacktestStrategy,
    sample_stock_data: pd.DataFrame,
):
    selector = sample_stock_data["comment"].str.contains("sell")
    expect_sell_stocks = sample_stock_data[selector].index

    # Should buy the stocks that trigger the buy signals
    with mock.patch(
        "tradepy.core.holdings.Holdings.position_codes",
    ) as mock_position_codes:
        mock_position_codes.__get__ = mock.Mock(
            return_value=sample_stock_data.index.tolist()
        )
        sell_codes = sample_backtester.get_close_signals(
            sample_stock_data, sample_strategy
        )
        assert set(sell_codes) == set(expect_sell_stocks)


@pytest.mark.parametrize(
    "sl_tf_order",
    ["stop loss first", "take profit first", "random"],
)
def test_day_k_trading(
    sl_tf_order: SL_TP_Order,
    sample_computed_day_k_df: pd.DataFrame,
    sample_strategy: SampleBacktestStrategy,
    backtest_conf: BacktestConf,
):
    backtest_conf.sl_tf_order = sl_tf_order
    backtester = Backtester(backtest_conf)
    trade_book = backtester.trade(sample_computed_day_k_df, sample_strategy)

    assert isinstance(trade_book, TradeBook)
