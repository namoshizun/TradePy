import numpy as np
import pytest
from tradepy.core.budget_allocator import evenly_distribute


@pytest.fixture
def stocks():
    return np.array(
        [
            [0, 10],
            [1, 20],
            [2, 30],
        ]
    )


def test_evenly_distribute_valid_budget(stocks: np.ndarray):
    budget = 1e6
    min_trade_cost = 8500
    trade_lot_vol = 100
    allocation = evenly_distribute(stocks, budget, min_trade_cost, trade_lot_vol)
    price_per_stock = stocks[:, 1]
    lots_per_stock = allocation[:, 1]

    # Check if the result is a numpy array
    assert isinstance(allocation, np.ndarray)

    # Check if the shape of the result matches the input stocks
    assert allocation.shape == stocks.shape

    # Check if the total cost of the result does not exceed the budget
    amount_per_stock = price_per_stock * trade_lot_vol * lots_per_stock
    assert amount_per_stock.sum() <= budget

    # Check the minimum trade amount per stock
    assert amount_per_stock.min() >= min_trade_cost


def test_evenly_distribute_insufficient_budget(stocks: np.ndarray):
    budget = 50  # Budget is too low to buy any shares
    min_trade_cost = 8500
    trade_lot_vol = 100
    allocations = evenly_distribute(stocks, budget, min_trade_cost, trade_lot_vol)
    assert allocations[:, 1].sum() == 0


def test_evenly_distribute_unmet_min_trade_cost(stocks: np.ndarray):
    budget = 1e6
    min_trade_cost = 2000000
    trade_lot_vol = 100

    allocations = evenly_distribute(stocks, budget, min_trade_cost, trade_lot_vol)
    assert allocations[:, 1].sum() == 0


def test_evenly_distribute_remove_option(stocks: np.ndarray):
    budget = 1e6
    min_trade_cost = int(0.9 * budget // (len(stocks) - 1))
    trade_lot_vol = 100
    result = evenly_distribute(stocks, budget, min_trade_cost, trade_lot_vol)

    # Check if the shape of the result is less than the input stocks (one option removed)
    assert len(result) == len(stocks) - 1
