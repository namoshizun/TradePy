import time
import tushare as ts
import pandas as pd
from functools import wraps

import tradepy
from tradepy.conversion import (
    convert_code_to_exchange,
    convert_tushare_date_to_iso_format,
)

ts_api = ts.pro_api()


def retry(max_retries=4, wait_interval=30):
    def decor(fun):
        @wraps(fun)
        def inner(*args, **kwargs):
            n = 0
            while n < max_retries:
                try:
                    return fun(*args, **kwargs)
                except Exception as exc:
                    if "您每分钟最多访问" in str(exc):
                        time.sleep(wait_interval)
                        n += 1
                    else:
                        raise exc

            tradepy.LOG.error(f"{fun}: 接口调用失败")

        return inner

    return decor


@retry()
def get_name_change_history(code: str) -> pd.DataFrame:
    exchange = convert_code_to_exchange(code)
    df = ts_api.namechange(ts_code=f"{code}.{exchange}", fields="name,start_date")

    to_iso_date = (
        lambda value: value
        if value is None
        else convert_tushare_date_to_iso_format(value)
    )

    df.rename(columns={"start_date": "timestamp", "name": "company"}, inplace=True)
    df.drop_duplicates("timestamp", inplace=True)
    df["timestamp"] = df["timestamp"].map(to_iso_date)
    df.set_index("timestamp", inplace=True)
    return df
