from typing import Optional, get_args

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
