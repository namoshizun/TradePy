import pytest
from pytest import approx
from tradepy.core.order import SellRemark
from tradepy.core.position import Position


@pytest.fixture
def position():
    return Position(
        id="123",
        timestamp="2023-09-16",
        code="000333",
        price=100,
        vol=100,
        latest_price=110.0,
        avail_vol=100,
        yesterday_vol=100,
    )


@pytest.fixture
def closed_position():
    return Position(
        id="123",
        timestamp="2023-09-16",
        code="000333",
        price=100,
        vol=0,
        latest_price=110.0,
        avail_vol=0,
        yesterday_vol=100,
    )


def test_total_value_at(position: Position):
    price = 120.0
    expected_total_value = 120.0 * position.vol
    assert position.total_value_at(price) == expected_total_value


def test_profit_or_loss_at(position: Position):
    for price_chg in (10, -10, 0, -0.5):
        new_price = position.price * (1 + price_chg * 1e-2)
        expected_profit_loss = (new_price - position.price) * position.vol
        assert position.profit_or_loss_at(new_price) == approx(expected_profit_loss)


def test_pct_chg(position: Position):
    for price_chg in (5, -3, 0):
        new_price = position.price * (1 + price_chg * 1e-2)
        expected_pct_chg = price_chg
        assert position.pct_chg_at(new_price) == approx(expected_pct_chg)


def test_to_sell_order(position: Position, closed_position: Position):
    # Not closed position cannot be converted to a sell order
    timestamp = "2023-09-18"
    with pytest.raises(AssertionError):
        closed_position.to_sell_order(timestamp, "止盈")

    # Test converting a closed position to a sell order
    closing_price = position.price_at_pct_change(5)
    position.update_price(closing_price)
    order = position.to_sell_order(timestamp, "止盈")
    assert order.timestamp == timestamp
    assert order.code == position.code
    assert order.price == closing_price
    assert order.vol == position.vol
    assert order.direction == "sell"
    assert order.status == "pending"
    assert order.filled_vol == 0
    assert order.filled_value == 0
    assert order.cancelled_vol == 0
    assert order.cancelled_value == 0
    assert order.placed_value == closing_price * position.vol

    # The sell remark should ensemble the open price and pct change
    sell_remark: SellRemark = order.get_sell_remark(raw=False)
    assert sell_remark["action"] == "止盈"
    assert sell_remark["price"] == closing_price
    assert sell_remark["vol"] == order.vol
    assert sell_remark["pct_chg"] == approx(5)
