import numpy as np
import pandas as pd
import importlib
from functools import wraps
from datetime import date
from dateutil import parser as date_parser

import tradepy.trade_cal


def get_latest_trade_date() -> date:
    today = date.today()
    today_str = str(today)

    for idx, trade_date in enumerate(tradepy.trade_cal.trade_cal):
        if trade_date == today_str:
            return today
        elif trade_date < today_str:
            return date_parser.parse(tradepy.trade_cal.trade_cal[idx]).date()

    raise Exception('Unexpectedly unable to find the latest trade date?!')


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


def optimize_dtype_memory(df: pd.DataFrame):
    for col in df.columns:
        if df[col].dtype.kind in 'bifc':
            numeric_data = df[col].dropna()
            if numeric_data.empty:
                continue

            min_val = numeric_data.min()
            max_val = numeric_data.max()

            if np.isfinite(min_val) and np.isfinite(max_val):
                if np.issubdtype(numeric_data.dtype, np.integer):
                    if min_val >= np.iinfo(np.int8).min and max_val <= np.iinfo(np.int8).max:
                        df[col] = df[col].astype(np.int8)
                    elif min_val >= np.iinfo(np.int16).min and max_val <= np.iinfo(np.int16).max:
                        df[col] = df[col].astype(np.int16)
                    elif min_val >= np.iinfo(np.int32).min and max_val <= np.iinfo(np.int32).max:
                        df[col] = df[col].astype(np.int32)
                    else:
                        df[col] = df[col].astype(np.int64)
                else:
                    if min_val >= np.finfo(np.float16).min and max_val <= np.finfo(np.float16).max:
                        df[col] = df[col].astype(np.float16)
                    elif min_val >= np.finfo(np.float32).min and max_val <= np.finfo(np.float32).max:
                        df[col] = df[col].astype(np.float32)
                    else:
                        df[col] = df[col].astype(np.float64)
    return df


def import_class(path: str) -> type:
    *module_path, class_name = path.split('.')
    module_path = '.'.join(module_path)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)
