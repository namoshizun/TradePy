import random
from collections.abc import Iterable
from typing import TYPE_CHECKING

import numpy as np
import pyximport

pyximport.install(language_level=3)

from tradepy.strategy.portfolio_alloc import portfolio_alloc  # noqa: E402

if TYPE_CHECKING:
    from tradepy.core.position import Position


def _prices(values: list[float]) -> np.ndarray:
    return np.array(values, dtype=np.float64)


def _position_values(positions: Iterable["Position"]) -> list[float]:
    return [position.price * position.vol for position in positions]


def test_portfolio_alloc_respects_lot_budget_and_position_bounds() -> None:
    positions = portfolio_alloc(
        ["A", "B", "C"],
        _prices([10, 20, 25]),
        budget=18_000,
        max_opens_count=3,
        position_max_value=7_000,
        position_min_value=3_000,
    )

    values = _position_values(positions)

    assert len(positions) == 3
    assert sum(values) <= 18_000
    assert all(position.vol % 100 == 0 for position in positions)
    assert all(3_000 <= value <= 7_000 for value in values)
    assert max(values) - min(values) <= 2_000


def test_portfolio_alloc_randomly_caps_to_max_opens_count() -> None:
    random.seed(1)
    codes = [f"S{i}" for i in range(10)]

    positions = portfolio_alloc(
        codes,
        _prices([10] * 10),
        budget=100_000,
        max_opens_count=4,
        position_max_value=20_000,
        position_min_value=1_000,
    )

    allocated_codes = {position.code for position in positions}

    assert len(positions) == 4
    assert allocated_codes < set(codes)


def test_portfolio_alloc_randomly_drops_options_until_minimums_fit() -> None:
    random.seed(2)

    positions = portfolio_alloc(
        ["A", "B", "C", "D"],
        _prices([10, 10, 10, 10]),
        budget=2_500,
        max_opens_count=4,
        position_max_value=5_000,
        position_min_value=1_000,
    )

    values = _position_values(positions)

    assert len(positions) == 2
    assert sum(values) <= 2_500
    assert all(value >= 1_000 for value in values)


def test_portfolio_alloc_skips_options_that_cannot_fit_bounds() -> None:
    positions = portfolio_alloc(
        ["TOO_EXPENSIVE", "BUYABLE"],
        _prices([100, 10]),
        budget=10_000,
        max_opens_count=2,
        position_max_value=5_000,
        position_min_value=500,
    )

    assert [position.code for position in positions] == ["BUYABLE"]
    assert positions[0].vol == 500
    assert positions[0].price * positions[0].vol == 5_000


def test_portfolio_alloc_accepts_numpy_code_arrays_from_strategy_path() -> None:
    positions = portfolio_alloc(
        np.array(["A", "B"]),
        _prices([10, 10]),
        budget=2_000,
        max_opens_count=2,
        position_max_value=1_000,
        position_min_value=1_000,
    )

    assert [position.code for position in positions] == ["A", "B"]


def test_portfolio_alloc_evens_out_values_across_different_lot_costs() -> None:
    positions = portfolio_alloc(
        ["CHEAP", "PRICEY"],
        _prices([10, 50]),
        budget=20_000,
        max_opens_count=2,
        position_max_value=12_000,
        position_min_value=1_000,
    )

    assert _position_values(positions) == [10_000, 10_000]


def test_portfolio_alloc_returns_empty_on_degenerate_inputs() -> None:
    no_codes = portfolio_alloc(
        [],
        _prices([]),
        budget=10_000,
        max_opens_count=5,
        position_max_value=5_000,
        position_min_value=1_000,
    )
    no_opens_allowed = portfolio_alloc(
        ["A"],
        _prices([10]),
        budget=10_000,
        max_opens_count=0,
        position_max_value=5_000,
        position_min_value=1_000,
    )
    budget_below_any_minimum = portfolio_alloc(
        ["A"],
        _prices([10]),
        budget=500,
        max_opens_count=1,
        position_max_value=5_000,
        position_min_value=1_000,
    )

    assert no_codes == []
    assert no_opens_allowed == []
    assert budget_below_any_minimum == []


def test_portfolio_alloc_invariants_hold_for_random_inputs() -> None:
    random.seed(42)

    for _ in range(200):
        n = random.randint(1, 30)
        prices = [round(random.uniform(1, 300), 2) for _ in range(n)]
        budget = random.uniform(0, 500_000)
        max_opens_count = random.randint(1, 10)
        position_min_value = random.uniform(500, 20_000)
        position_max_value = position_min_value * random.uniform(1, 10)

        positions = portfolio_alloc(
            [f"S{i}" for i in range(n)],
            _prices(prices),
            budget=budget,
            max_opens_count=max_opens_count,
            position_max_value=position_max_value,
            position_min_value=position_min_value,
        )

        values = _position_values(positions)

        assert len(positions) <= max_opens_count
        assert sum(values) <= budget + 1e-6
        assert all(position.vol > 0 and position.vol % 100 == 0 for position in positions)
        assert all(
            position_min_value - 1e-6 <= value <= position_max_value + 1e-6
            for value in values
        )
