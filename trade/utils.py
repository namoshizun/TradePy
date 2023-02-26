from functools import wraps
from datetime import date, timedelta
from dateutil import parser as date_parser


def get_latest_trade_date() -> date:
    today = date.today()
    wkday = today.weekday()

    if wkday >= 5:
        friday = today - timedelta(days=wkday - 4)
        return friday

    return today


def chunks(lst, batch_size: int):
    for i in range(0, len(lst), batch_size):
        yield lst[i: i + batch_size]


def calc_pct_chg(base_price, then_price) -> float:
    res = 100 * (then_price - base_price) / base_price
    return round(res, 2)


def calc_days_diff(d1: date | str, d2: date | str) -> int:
    if isinstance(d1, str):
        d1 = date_parser.parse(d1).date()

    if isinstance(d2, str):
        d2 = date_parser.parse(d2).date()

    return (d1 - d2).days


def round_val(fun):
    @wraps(fun)
    def inner(*args, **kwargs):
        val = fun(*args, **kwargs)
        return round(val, 2)
    return inner
