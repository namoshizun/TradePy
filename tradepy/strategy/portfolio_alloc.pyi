from collections.abc import Sequence

import numpy as np
from numpy.typing import NDArray

from tradepy.core.position import Position

def portfolio_alloc(
    codes: Sequence[str] | NDArray[np.str_],
    buy_prices: NDArray[np.float64],
    budget: float,
    max_opens_count: int,
    position_max_value: float,
    position_min_value: float,
) -> list[Position]: ...
