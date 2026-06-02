from datetime import date

import pandas as pd
import polars as pl
import tushare as ts

from tradepy.core.types import (
    DayKlinesDataFrame,
    DayKlinesModel,
    StockNameChangesModel,
    StocksBasicDataFrame,
    StocksBasicModel,
    StocksListDataFrame,
    StocksListModel,
)
from tradepy.utils import convert_code_to_exchange


class TushareClient:
    def __init__(self, token: str):
        self.api = ts.pro_api(token)

    def get_stock_basic(
        self, code: str, since: date, until: date
    ) -> StocksBasicDataFrame:
        df = self.api.daily_basic(
            ts_code=code,
            start_date=since.strftime("%Y%m%d"),
            end_date=until.strftime("%Y%m%d"),
            fields="trade_date,turnover_rate,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share",
        )
        df.rename(
            columns={
                "trade_date": "date",
                "dv_ratio": "dv",
                "total_share": "total_shares",
                "float_share": "float_shares",
                "free_share": "free_shares",
            },
            inplace=True,
        )
        df["date"] = pd.to_datetime(df["date"])
        df["code"] = code
        df["type"] = "stock"
        df["exchange"] = convert_code_to_exchange(code)
        df["sw_level_1"] = pd.NA
        df["sw_level_2"] = pd.NA
        df["sw_level_3"] = pd.NA

        return pl.from_pandas(  # pyright: ignore[reportReturnType, reportUnknownVariableType]
            df[StocksBasicModel.columns()],
            schema_overrides=StocksBasicModel.schema(),
            nan_to_null=True,
        )

    def get_day_klines(
        self, code: str, since: date, until: date
    ) -> DayKlinesDataFrame:
        df = self.api.daily(
            ts_code=code,
            start_date=since.strftime("%Y%m%d"),
            end_date=until.strftime("%Y%m%d"),
        )
        df.rename(
            columns={"trade_date": "date"},
            inplace=True,
        )
        df["date"] = pd.to_datetime(df["date"])
        df["amount"] /= 10
        return pl.from_pandas(  # pyright: ignore[reportReturnType, reportUnknownVariableType]
            df[DayKlinesModel.columns()],
            schema_overrides=DayKlinesModel.schema(),
            nan_to_null=True,
        )

    def get_trade_dates(self, since: date) -> set[date]:
        df = self.api.trade_cal(
            exchange="SSE",
            start_date=since.strftime("%Y%m%d"),
            end_date=date.today().strftime("%Y%m%d"),
        )
        return set(pd.to_datetime(df["cal_date"]).dt.date)

    def get_name_changes(self, code: str):
        df = self.api.namechange(
            ts_code=code,
            fields="ts_code,name,start_date,change_reason",
        ).rename(
            columns={
                "ts_code": "code",
                "start_date": "since",
                "change_reason": "reason",
            },
        )
        df["since"] = pd.to_datetime(df["since"])
        return (
            pl.from_pandas(  # pyright: ignore[reportReturnType, reportUnknownVariableType]
                df[StockNameChangesModel.columns()],
                schema_overrides=StockNameChangesModel.schema(),
                nan_to_null=True,
            )
            .unique()
            .sort(by=["since"])  # pyright: ignore[reportCallIssue]
        )

    def get_stock_list(self) -> StocksListDataFrame:
        fields = "ts_code,name,area,list_date,delist_date,is_hs,list_status"
        df = pd.concat(
            [
                self.api.stock_basic(
                    list_status="L",
                    fields=fields,
                ),
                self.api.stock_basic(
                    list_status="D",
                    fields=fields,
                ),
            ]
        ).rename(
            columns={"ts_code": "code"},
        )
        df["list_date"] = pd.to_datetime(df["list_date"])
        df["delist_date"] = pd.to_datetime(df["delist_date"])
        df["is_hs"] = df["is_hs"] != "N"
        df["is_listing"] = df["list_status"] == "L"
        return pl.from_pandas(  # pyright: ignore[reportReturnType, reportUnknownVariableType]
            df[StocksListModel.columns()],
            schema_overrides=StocksListModel.schema(),
            nan_to_null=True,
        )
