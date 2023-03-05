import datetime
import pandas as pd
import akshare as ak
from typing import Any, Literal

from tradepy.convertion import (
    convert_code_to_exchange,
    convert_akshare_hist_data,
    convert_akshare_stock_info,
    convert_akshare_industry_listing,
    convert_akshare_industry_ticks,
    convert_akshare_stock_index_ticks
)


class AkShareClient:

    def get_daily(self, code: str, start_date: datetime.date | str):
        if isinstance(start_date, str):
            start_date = datetime.date.fromisoformat(start_date)

        df = ak.stock_zh_a_hist(
            symbol=code,
            start_date=start_date.strftime('%Y%m%d'),
            period="daily"
        )
        return convert_akshare_hist_data(df)

    def get_adjust_factor(self, code: str, adjust: Literal["hfq", "qfq"] = "hfq"):
        if code == "689009":
            # No idea why this company is so special...
            return pd.DataFrame()

        exchange = convert_code_to_exchange(code)
        symbol = f'{exchange.lower()}{code}'

        df = ak.stock_zh_a_daily(symbol=symbol, adjust=f"{adjust}-factor")
        df.rename(columns={"date": "timestamp"}, inplace=True)

        df["code"] = code
        df["timestamp"] = df["timestamp"].dt.date.astype(str)
        return df

    def get_stock_info(self, code: str) -> dict[str, Any]:
        df = ak.stock_individual_info_em(symbol=code)
        return convert_akshare_stock_info({
            row["item"]: row["value"]
            for _, row in df.iterrows()
        })

    def get_industry_index_ticks(self, name: str) -> pd.DataFrame:
        df = ak.stock_board_industry_hist_em(
            symbol=name,
            start_date="20000101",
            end_date="20990101",
            period="日k"
        )
        df = convert_akshare_industry_ticks(df)
        df["name"] = name
        return df

    def get_industry_listing(self) -> pd.DataFrame:
        return convert_akshare_industry_listing(ak.stock_board_industry_name_em())

    def get_stock_index_ticks(self, code: str) -> pd.DataFrame:
        df = ak.stock_zh_index_daily(symbol=code)
        return convert_akshare_stock_index_ticks(df)
