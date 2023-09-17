import pytest
from unittest.mock import MagicMock
from tradepy.core.position import Position
from tradepy.core.account import BacktestAccount
from tradepy.core.holdings import Holdings


@pytest.fixture
def sample_position():
    return Position(
        id="123",
        timestamp="2023-09-16",
        code="000333",
        price=100.0,
        vol=100,
        latest_price=100,
        avail_vol=100,
        yesterday_vol=100,
    )


@pytest.fixture
def sample_account():
    return BacktestAccount(
        free_cash_amount=100000,
        broker_commission_rate=0.05,
        min_broker_commission_fee=5.0,
        stamp_duty_rate=0.1,
    )


@pytest.fixture
def empty_holdings():
    return Holdings()


@pytest.fixture
def sample_holdings(empty_holdings: Holdings, sample_position: Position):
    empty_holdings.positions[sample_position.code] = sample_position
    return empty_holdings


def test_holding_initialization(empty_holdings: Holdings):
    assert len(empty_holdings.positions) == 0
    assert not empty_holdings.position_codes


def test_update_price(sample_holdings: Holdings):
    # Mock the PriceLookupFun
    price_lookup = MagicMock()
    price_lookup.return_value = 110.0

    # Update the price using the mocked price lookup
    sample_holdings.update_price(price_lookup)

    for _, pos in sample_holdings:
        assert pos.latest_price == 110.0


def test_buy_positions(sample_account: BacktestAccount, sample_position: Position):
    broker_commission_fee = sample_account.get_broker_commission_fee(
        sample_position.cost
    )
    expect_free_cash = (
        sample_account.free_cash_amount - sample_position.cost - broker_commission_fee
    )
    initial_total_asset_value = sample_account.total_asset_value
    sample_account.buy([sample_position])

    assert sample_account.free_cash_amount == expect_free_cash
    assert (
        sample_account.total_asset_value
        == initial_total_asset_value - broker_commission_fee
    )


@pytest.mark.parametrize("pct_chg", [-5, 0, 5])
def test_sell_position(
    pct_chg: int, sample_account: BacktestAccount, sample_position: Position
):
    initial_asset_value = sample_account.total_asset_value
    sample_account.buy([sample_position])
    sample_position.update_price(sample_position.price_at_pct_change(pct_chg))
    sample_account.sell([sample_position])

    pos_net_return = sample_account.get_position_net_pct_chg(sample_position)

    assert sample_position.code not in sample_account.holdings
    # Position net return should be less than the pct chg of the price because of trading fees applied
    assert pos_net_return * 100 < pct_chg
    # Check the total asset calculation is correct
    assert (
        initial_asset_value + sample_position.cost * pos_net_return
        == sample_account.total_asset_value
    )


def test_clear_positions(sample_account: BacktestAccount, sample_position: Position):
    kopy1 = sample_position.model_copy(update={"code": "000001"})
    kopy2 = sample_position.model_copy(update={"code": "000002"})
    sample_account.holdings.buy([sample_position, kopy1, kopy2])
    sample_account.clear()
    assert len(sample_account.holdings.positions) == 0
