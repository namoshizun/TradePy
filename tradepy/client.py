import datetime
import time
import pandas as pd
import akshare as ak
from typing import Any, Literal
from functools import wraps

import tradepy
from tradepy.convertion import (
    convert_code_to_exchange,
    convert_akshare_hist_data,
    convert_akshare_stock_info,
    convert_akshare_sector_listing,
    convert_akshare_sector_ticks,
    convert_akshare_stock_index_ticks,
    convert_code_to_market
)


def retry(max_retries=3, wait_interval=5):
    def decor(fun):
        @wraps(fun)
        def inner(*args, **kwargs):
            n = 0
            while n < max_retries:
                try:
                    return fun(*args, **kwargs)
                except Exception:
                    tradepy.LOG.warn(f'接口报错，{wait_interval}秒后尝试第{n + 1}/{max_retries}次')
                    time.sleep(wait_interval)
        return inner
    return decor


class AkShareClient:

    # ----
    # Misc
    def get_adjust_factor(self, code: str, adjust: Literal["hfq", "qfq"] = "hfq"):
        if code == "689009":
            # Oh well...
            return pd.DataFrame()

        exchange = convert_code_to_exchange(code)
        symbol = f'{exchange.lower()}{code}'

        df = ak.stock_zh_a_daily(symbol=symbol, adjust=f"{adjust}-factor")
        df.rename(columns={"date": "timestamp"}, inplace=True)

        df["code"] = code
        df["timestamp"] = df["timestamp"].dt.date.astype(str)
        return df

    def get_a_stocks_list(self) -> pd.DataFrame:
        all_df = ak.stock_info_a_code_name().set_index("code")
        A_board = [
            code
            for code in all_df.index
            if convert_code_to_market(code) in ("科创板", "中小板", "创业板", "上证主板", "深证主板")
        ]
        return all_df.loc[A_board]

    # ------
    # Stocks
    def get_daily(self, code: str, start_date: datetime.date | str):
        if isinstance(start_date, str):
            start_date = datetime.date.fromisoformat(start_date)

        df = retry()(ak.stock_zh_a_hist)(
            symbol=code,
            start_date=start_date.strftime('%Y%m%d'),
            period="daily"
        )

        assert isinstance(df, pd.DataFrame)
        if df.empty:
            return df

        df = convert_akshare_hist_data(df)

        indicators_df = retry()(ak.stock_a_lg_indicator)(symbol=code)
        assert isinstance(indicators_df, pd.DataFrame)
        indicators_df.rename(columns={
            "trade_date": "timestamp",
            "total_mv": "mkt_cap",
        }, inplace=True)
        indicators_df["mkt_cap"] *= 1e-4  # Convert to 100 mils
        indicators_df["mkt_cap"] = indicators_df["mkt_cap"].round(4)
        indicators_df['timestamp'] = indicators_df['timestamp'].astype(str)
        return pd.merge(df, indicators_df, on="timestamp")

    def get_stock_info(self, code: str) -> dict[str, Any]:
        df = ak.stock_individual_info_em(symbol=code)

        return convert_akshare_stock_info({
            row["item"]: row["value"]
            for _, row in df.iterrows()
        })

    # -----
    # Index
    def get_sector_index_ticks(self, name: str) -> pd.DataFrame:
        df = ak.stock_board_industry_hist_em(
            symbol=name,
            start_date="20000101",
            end_date="20990101",
            period="日k"
        )
        df = convert_akshare_sector_ticks(df)
        df["name"] = name
        return df

    def get_sectors_listing(self) -> pd.DataFrame:
        return convert_akshare_sector_listing(ak.stock_board_industry_name_em())

    def get_broad_based_index_ticks(self,
                                    code: str,
                                    start_date: str = "1900-01-01") -> pd.DataFrame:
        df = ak.stock_zh_index_daily(symbol=code)
        df = convert_akshare_stock_index_ticks(df)
        return df.query('timestamp >= @start_date')
