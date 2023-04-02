import datetime
import time
import traceback
import pandas as pd
import akshare as ak
from typing import Any, Literal
from functools import wraps

import tradepy
from tradepy.conversion import (
    convert_akshare_broad_based_index_current_quote,
    convert_akshare_current_quotation,
    convert_akshare_minute_bar,
    convert_broad_based_index_name_to_code,
    convert_code_to_exchange,
    convert_akshare_hist_data,
    convert_akshare_stock_info,
    convert_akshare_sector_listing,
    convert_akshare_sector_day_bars,
    convert_akshare_stock_index_ticks,
    convert_code_to_market,
    convert_akshare_sector_current_quote,
)
from tradepy.utils import get_latest_trade_date


def retry(max_retries=3, wait_interval=5):
    def decor(fun):
        @wraps(fun)
        def inner(*args, **kwargs):
            n = 0
            while n < max_retries:
                try:
                    return fun(*args, **kwargs)
                except Exception:
                    tradepy.LOG.warn(f'接口报错，{wait_interval}秒后尝试第{n + 1}/{max_retries}次. \n\n {traceback.format_exc()}')
                    time.sleep(wait_interval)
                    n += 1
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

        df = retry()(ak.stock_zh_a_daily)(symbol=symbol, adjust=f"{adjust}-factor")
        assert isinstance(df, pd.DataFrame)
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
            print(f'未找到{code}日K数据. 起始日期{start_date}')
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

    def get_minute_bar(self,
                       code: str,
                       start_date: datetime.date | str,
                       period="1") -> pd.DataFrame:
        if isinstance(start_date, str):
            start_date = datetime.date.fromisoformat(start_date)

        df = ak.stock_zh_a_hist_min_em(symbol=code, start_date=str(start_date), period=period)
        df = convert_akshare_minute_bar(df)
        return df

    def get_stock_info(self, code: str) -> dict[str, Any]:
        df = ak.stock_individual_info_em(symbol=code)

        return convert_akshare_stock_info({
            row["item"]: row["value"]
            for _, row in df.iterrows()
        })

    def get_current_quote(self) -> pd.DataFrame:
        df = ak.stock_zh_a_spot_em()
        df = convert_akshare_current_quotation(df)
        df.set_index("code", inplace=True)
        return df

    # -----
    # Index
    def get_sector_index_day_bars(self, name: str) -> pd.DataFrame:
        df = ak.stock_board_industry_hist_em(
            symbol=name,
            start_date="20000101",
            end_date="20990101",
            period="日k"
        )
        df = convert_akshare_sector_day_bars(df)
        df["name"] = name
        return df

    def get_sector_index_current_quote(self, name_or_code: str) -> dict[str, Any]:
        df = ak.stock_board_industry_spot_em(name_or_code)
        data = convert_akshare_sector_current_quote({
            row.item: row.value
            for row in df.itertuples()
        })
        data["timestamp"] = str(get_latest_trade_date())  # type: ignore
        return data

    def get_sectors_listing(self) -> pd.DataFrame:
        return convert_akshare_sector_listing(ak.stock_board_industry_name_em())

    def get_broad_based_index_day_bars(self,
                                       code: str,
                                       start_date: str = "1900-01-01") -> pd.DataFrame:
        df = ak.stock_zh_index_daily(symbol=code)
        df = convert_akshare_stock_index_ticks(df)
        return df.query('timestamp >= @start_date')

    def get_broad_based_index_current_quote(self, *names: str):
        df = ak.stock_zh_index_spot()
        df = convert_akshare_broad_based_index_current_quote(df).set_index("code")

        codes = list(map(convert_broad_based_index_name_to_code, names))
        df = df.loc[codes].copy()
        df["timestamp"] = str(get_latest_trade_date())
        return df
