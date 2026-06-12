import threading
import time
from collections import deque
from functools import wraps
from typing import Any, Callable, TypeAlias, TypeVar

from polars.expr import Expr as PolarsExpr

_StrategyT = TypeVar("_StrategyT")


def _parse_throttle_rate(rate: str) -> tuple[int, float]:
    seconds_multipler = {"s": 1.0, "m": 60.0, "h": 3600.0}

    try:
        count_str, unit = rate.split("/", 1)
        limit = int(count_str)
        period = seconds_multipler[unit]
    except (ValueError, KeyError) as e:
        raise ValueError(
            f'Invalid throttle rate {rate!r}; expected form like "200/m" '
            f"with unit one of {', '.join(seconds_multipler)}"
        ) from e
    if limit < 1:
        raise ValueError(f"Throttle count must be at least 1, got {rate!r}")
    return limit, period


def throttle(rate: str):
    limit, period_seconds = _parse_throttle_rate(rate)
    lock = threading.Lock()
    call_times: deque[float] = deque()

    def prune_expired(now: float) -> None:
        while call_times and call_times[0] <= now - period_seconds:
            call_times.popleft()

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with lock:
                now = time.monotonic()
                prune_expired(now)

                if len(call_times) >= limit:
                    time.sleep(call_times[0] + period_seconds - now)
                    now = time.monotonic()
                    prune_expired(now)

                call_times.append(now)

            return func(*args, **kwargs)

        return wrapper

    return decorator


IndicatorComputeFunc: TypeAlias = Callable[
    [_StrategyT], PolarsExpr | tuple[PolarsExpr, ...]
]


def indicator(not_na: bool = True):

    def inner(
        func: IndicatorComputeFunc,
    ) -> IndicatorComputeFunc:

        @wraps(func)
        def wrapper(self: _StrategyT) -> PolarsExpr | tuple[PolarsExpr, ...]:
            return func(self)

        setattr(
            wrapper,
            "_tradepy_indicator_compute",
            True,
        )
        setattr(
            wrapper,
            "_tradepy_indicator_not_na",
            not_na,
        )
        return wrapper

    return inner
