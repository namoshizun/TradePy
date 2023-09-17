import pytest
from unittest import mock
from datetime import date, datetime
from tradepy import trade_cal
from tradepy.core.exchange import AStockExchange
from tradepy.types import MarketPhase


@pytest.fixture(autouse=True)
def mock_trade_cal(monkeypatch):
    # Monkeypatch the trade_cal module to use the custom trade_cal
    monkeypatch.setattr(
        trade_cal, "trade_cal", ["2023-09-15", "2023-09-16", "2023-09-17"]
    )


def test_is_today_trade_day_today():
    # When today's date is in the trade_cal
    today = date(2023, 9, 15)
    # with mock.patch("tradepy.core.exchange.date.today", return_value=today):
    with mock.patch("tradepy.core.exchange.date") as mock_date:
        mock_date.today.return_value = today
        assert AStockExchange.is_today_trade_day() is True


def test_is_today_trade_day_not_today():
    # When today's date is not in the trade_cal
    not_today = date(2023, 9, 18)
    with mock.patch("tradepy.core.exchange.date") as mock_date:
        mock_date.today.return_value = not_today
        assert AStockExchange.is_today_trade_day() is False


def test_market_phase_now_pre_open():
    # When it's a trading day and the time is in the pre-open phase
    pre_open_time = datetime(2023, 9, 15, 9, 10, 0)  # 9:10 AM
    with mock.patch("tradepy.core.exchange.datetime") as mock_datetime:
        mock_datetime.now.return_value = pre_open_time
        assert AStockExchange.market_phase_now() == MarketPhase.PRE_OPEN


def test_market_phase_now_pre_open_call_p1():
    # When it's a trading day and the time is in the pre-open call phase (P1)
    pre_open_call_p1_time = datetime(2023, 9, 15, 9, 25, 0)  # 9:25 AM
    with mock.patch("tradepy.core.exchange.datetime") as mock_datetime:
        mock_datetime.now.return_value = pre_open_call_p1_time
        assert AStockExchange.market_phase_now() == MarketPhase.PRE_OPEN_CALL_P1


def test_market_phase_now_pre_open_call_p2():
    # When it's a trading day and the time is in the pre-open call phase (P2)
    pre_open_call_p2_time = datetime(2023, 9, 15, 9, 29, 0)  # 9:29 AM
    with mock.patch("tradepy.core.exchange.datetime") as mock_datetime:
        mock_datetime.now.return_value = pre_open_call_p2_time
        assert AStockExchange.market_phase_now() == MarketPhase.PRE_OPEN_CALL_P2


def test_market_phase_now_continuous_trade():
    # When it's a trading day and the time is in the continuous trade phase
    continuous_trade_time = datetime(2023, 9, 15, 9, 35, 0)  # 9:35 AM
    with mock.patch("tradepy.core.exchange.datetime") as mock_datetime:
        mock_datetime.now.return_value = continuous_trade_time
        assert AStockExchange.market_phase_now() == MarketPhase.CONT_TRADE


def test_market_phase_now_lunch_break():
    # When it's a trading day and the time is during the lunch break
    for lunch_break_time in [
        datetime(2023, 9, 15, 11, 31, 0),
        datetime(2023, 9, 15, 12, 0, 0),
    ]:
        with mock.patch("tradepy.core.exchange.datetime") as mock_datetime:
            mock_datetime.now.return_value = lunch_break_time
            assert AStockExchange.market_phase_now() == MarketPhase.LUNCHBREAK


def test_market_phase_now_continuous_trade_pre_close():
    # When it's a trading day and the time is in the continuous trade pre-close phase
    pre_close_time = datetime(2023, 9, 15, 14, 56, 0)  # 2:56 PM
    with mock.patch("tradepy.core.exchange.datetime") as mock_datetime:
        mock_datetime.now.return_value = pre_close_time
        assert AStockExchange.market_phase_now() == MarketPhase.CONT_TRADE_PRE_CLOSE


def test_market_phase_now_pre_close_call():
    # When it's a trading day and the time is in the pre-close call phase
    pre_close_call_time = datetime(2023, 9, 15, 14, 57, 0)  # 2:57 PM
    with mock.patch("tradepy.core.exchange.datetime") as mock_datetime:
        mock_datetime.now.return_value = pre_close_call_time
        assert AStockExchange.market_phase_now() == MarketPhase.PRE_CLOSE_CALL


def test_market_phase_now_closed_not_trade_day():
    # When it's not a trading day
    not_trade_day = datetime(2023, 9, 18, 10, 0, 0)  # A non-trading day
    with (
        mock.patch("tradepy.core.exchange.date") as mock_date,
        mock.patch("tradepy.core.exchange.datetime") as mock_datetime,
    ):
        mock_date.today.return_value = not_trade_day.date()
        mock_datetime.now.return_value = not_trade_day
        assert AStockExchange.market_phase_now() == MarketPhase.CLOSED


def test_market_phase_now_closed_not_trade_time():
    # When it's a trading day but not a trading time
    for not_trade_time in [
        datetime(2023, 9, 15, 8, 59, 0),
        datetime(2023, 9, 15, 15, 59, 0),
    ]:
        with mock.patch("tradepy.core.exchange.datetime") as mock_datetime:
            mock_datetime.now.return_value = not_trade_time
            assert AStockExchange.market_phase_now() == MarketPhase.CLOSED


def test_fetch_quote():
    df = AStockExchange.get_quote()
    assert set(df.columns).issubset(
        set(
            [
                "company",
                "pct_chg",
                "chg",
                "vol",
                "close",
                "high",
                "low",
                "open",
                "turnover",
            ]
        )
    )
    assert df.index.name == "code"
    assert df.shape[0] > 0


def test_fetch_bid_ask():
    result = AStockExchange.get_bid_ask("000333")
    assert len(result["buy"]) == 5
    assert len(result["sell"]) == 5
