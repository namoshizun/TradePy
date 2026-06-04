import threading
import time
from collections import deque
from functools import wraps
from typing import Any, Callable, Optional, get_args

from tradepy.core.types import ExchangeType, MarketType


def convert_code_to_market(code: str) -> MarketType:
    mapping: dict[tuple, MarketType] = {
        ("688",): "科创板",
        ("689",): "CDR",
        ("30",): "创业板",
        ("600", "601", "603", "605"): "上证主板",
        ("000", "001", "002", "003"): "深证主板",
        ("82", "83", "87", "88", "920"): "北交所",
        ("40", "42", "43"): "新三板",
    }

    for prefix, market in mapping.items():
        if code.startswith(prefix):
            return market

    raise ValueError(f"Unknown code {code}")


def convert_code_to_exchange(code: str) -> ExchangeType:
    market = convert_code_to_market(code)
    match market:
        case "科创板" | "上证主板" | "CDR":
            return "SH"
        case "创业板" | "深证主板" | "新三板":
            return "SZ"
        case "北交所":
            return "BJ"
    raise ValueError(f"Unknown code {code}")


def get_param_type(cls: type) -> Optional[type]:
    for base in getattr(cls, "__orig_bases__", []):
        if getattr(base, "__origin__", None):
            args = get_args(base)
            if args:
                return args[0]


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
