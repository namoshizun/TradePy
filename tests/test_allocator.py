import random
from collections.abc import Iterable

import pyximport

pyximport.install(language_level=3)

from tradepy.strategy.portfolio_alloc import (  # noqa: E402
    BudgetAllocation,
    portfolio_alloc,
)


def _options(codes: list[str], prices: list[float]) -> list[tuple[str, float]]:
    return list(zip(codes, prices, strict=True))


def _allocation_values(allocations: Iterable[BudgetAllocation]) -> list[float]:
    return [alloc.price * alloc.vol for alloc in allocations]


def test_portfolio_alloc_respects_lot_budget_and_position_bounds() -> None:
    allocations = portfolio_alloc(
        _options(["A", "B", "C"], [10, 20, 25]),
        18_000,
        3,
        7_000,
        3_000,
    )

    values = _allocation_values(allocations)

    assert len(allocations) == 3
    assert all(isinstance(alloc, BudgetAllocation) for alloc in allocations)
    assert sum(values) <= 18_000
    assert all(alloc.vol % 100 == 0 for alloc in allocations)
    assert all(3_000 <= value <= 7_000 for value in values)
    assert max(values) - min(values) <= 2_000


def test_portfolio_alloc_randomly_caps_to_max_opens_count() -> None:
    random.seed(1)
    codes = [f"S{i}" for i in range(10)]

    allocations = portfolio_alloc(
        _options(codes, [10.0] * 10),
        100_000,
        4,
        20_000,
        1_000,
    )

    allocated_codes = {alloc.code for alloc in allocations}

    assert len(allocations) == 4
    assert allocated_codes < set(codes)


def test_portfolio_alloc_randomly_drops_options_until_minimums_fit() -> None:
    random.seed(2)

    allocations = portfolio_alloc(
        _options(["A", "B", "C", "D"], [10, 10, 10, 10]),
        2_500,
        4,
        5_000,
        1_000,
    )

    values = _allocation_values(allocations)

    assert len(allocations) == 2
    assert sum(values) <= 2_500
    assert all(value >= 1_000 for value in values)


def test_portfolio_alloc_skips_options_that_cannot_fit_bounds() -> None:
    allocations = portfolio_alloc(
        _options(["TOO_EXPENSIVE", "BUYABLE"], [100, 10]),
        10_000,
        2,
        5_000,
        500,
    )

    assert [alloc.code for alloc in allocations] == ["BUYABLE"]
    assert allocations[0].vol == 500
    assert allocations[0].price * allocations[0].vol == 5_000


def test_portfolio_alloc_accepts_option_tuples_from_strategy_path() -> None:
    allocations = portfolio_alloc(
        [("A", 10.0), ("B", 10.0)],
        2_000,
        2,
        1_000,
        1_000,
    )

    assert [alloc.code for alloc in allocations] == ["A", "B"]


def test_portfolio_alloc_evens_out_values_across_different_lot_costs() -> None:
    allocations = portfolio_alloc(
        _options(["CHEAP", "PRICEY"], [10, 50]),
        20_000,
        2,
        12_000,
        1_000,
    )

    assert _allocation_values(allocations) == [10_000, 10_000]


def test_portfolio_alloc_returns_empty_on_degenerate_inputs() -> None:
    no_codes = portfolio_alloc([], 10_000, 5, 5_000, 1_000)
    no_opens_allowed = portfolio_alloc(_options(["A"], [10]), 10_000, 0, 5_000, 1_000)
    budget_below_any_minimum = portfolio_alloc(
        _options(["A"], [10]),
        500,
        1,
        5_000,
        1_000,
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

        allocations = portfolio_alloc(
            _options([f"S{i}" for i in range(n)], prices),
            budget,
            max_opens_count,
            position_max_value,
            position_min_value,
        )

        values = _allocation_values(allocations)

        assert len(allocations) <= max_opens_count
        assert sum(values) <= budget + 1e-6
        assert all(
            alloc.vol > 0 and alloc.vol % 100 == 0 for alloc in allocations
        )
        assert all(
            position_min_value - 1e-6 <= value <= position_max_value + 1e-6
            for value in values
        )
