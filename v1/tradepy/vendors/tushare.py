import os
import time

import tushare as ts
import pandas as pd
from functools import wraps

import tradepy
from tradepy.conversion import (
    convert_code_to_exchange,
    convert_tushare_date_to_iso_format,
)

ts_api = ts.pro_api(
    os.environ.get(
        "TUSHARE_TOKEN",
        "2e8200f738bd44a70ca308b95ae1708b6bac03abd37e16534b9418ae"
        # Just chill.. it is a free token
    )
)


def retry_on_exception(max_retries=4, wait_interval=30):
    def decor(fun):
        @wraps(fun)
        def inner(*args, **kwargs):
            for _ in range(max_retries):
                try:
                    return fun(*args, **kwargs)
                except Exception as exc:
                    if "您每分钟最多访问" in str(exc):
                        time.sleep(wait_interval)
                    else:
                        raise exc

            tradepy.LOG.error(f"{fun}: 接口调用失败")

        return inner

    return decor


@retry_on_exception()
def get_name_change_history(code: str, n_retry_empty_response: int = 0) -> pd.DataFrame:
    exchange = convert_code_to_exchange(code)
    df = ts_api.namechange(ts_code=f"{code}.{exchange}", fields="name,start_date")

    if df.empty:
        if n_retry_empty_response > 3:
            raise ValueError(f"获取{code}名称变更记录失败")
        time.sleep(5)
        return get_name_change_history(code, n_retry_empty_response + 1)

    to_iso_date = (
        lambda value: value
        if value is None
        else convert_tushare_date_to_iso_format(value)
    )

    df.rename(columns={"start_date": "timestamp", "name": "company"}, inplace=True)
    df.drop_duplicates("timestamp", inplace=True)
    df["code"] = code
    df["timestamp"] = df["timestamp"].map(to_iso_date)
    df.set_index("timestamp", inplace=True)
    return df
