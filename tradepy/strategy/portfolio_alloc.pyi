from collections import namedtuple
from collections.abc import Sequence

BudgetAllocation = namedtuple("BudgetAllocation", ["code", "price", "vol"])

def portfolio_alloc(
    options: Sequence[tuple[str, float]],
    budget: float,
    max_opens_count: int,
    position_max_value: float,
    position_min_value: float,
) -> list[BudgetAllocation]: ...
