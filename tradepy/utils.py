import importlib
import time
from typing import Any, Literal, Optional, get_args, overload

import polars as pl
from loguru import logger

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


def import_class(path: str) -> type:
    *module_path, class_name = path.split(".")
    module_path = ".".join(module_path)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def calc_pct_chg(base_price: float, then_price: float) -> float:
    res = 100 * (then_price - base_price) / base_price
    return round(res, 2)


class Timer:
    def __init__(
        self,
        unit: Literal["s", "ms"] = "ms",
        warning_thresh: float | int | None = None,
        warning_message: str | None = None,
        rounded: bool = False,
    ):
        self.unit = unit
        self.rounded = rounded
        self.warning_thresh = warning_thresh
        self.warning_message = warning_message

    def elapsed(self):
        return time.monotonic() - self.start

    def __enter__(self):
        self.start = time.monotonic()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        cost = self.elapsed()
        if self.unit == "ms":
            self.duration = cost * 1000
        else:
            self.duration = cost

        if self.rounded:
            self.duration = round(self.duration)

        if (
            self.warning_thresh
            and self.warning_message
            and self.duration > self.warning_thresh
        ):
            try:
                logger.warning(
                    self.warning_message.format(duration=self.duration)
                )
            except Exception:
                logger.warning(self.warning_message)


@overload
def ensure_laziness(
    df: pl.DataFrame | pl.LazyFrame, lazy: Literal[True]
) -> pl.LazyFrame: ...
@overload
def ensure_laziness(
    df: pl.DataFrame | pl.LazyFrame, lazy: Literal[False]
) -> pl.DataFrame: ...


def ensure_laziness(
    df: pl.DataFrame | pl.LazyFrame, lazy: bool
) -> pl.DataFrame | pl.LazyFrame:
    if lazy and isinstance(df, pl.DataFrame):
        return df.lazy()  # pyright: ignore[reportReturnType]

    if isinstance(df, pl.LazyFrame) and not lazy:
        return df.collect()  # pyright: ignore[reportReturnType]

    return df
