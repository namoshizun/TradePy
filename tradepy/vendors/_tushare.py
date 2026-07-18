from datetime import date

import pandas as pd
import polars as pl
import tushare as ts
from cachetools import TTLCache, cachedmethod
from tenacity import (
    Retrying,
    retry,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

from tradepy import config
from tradepy.core.trade_cal import TradeCalendar
from tradepy.core.types import (
    DayKlinesDataFrame,
    DayKlinesModel,
    StockNameChangesDataFrame,
    StockNameChangesModel,
    StockPriceAdjustFactorsDataFrame,
    StockPriceAdjustFactorsModel,
    StocksBasicDataFrame,
    StocksBasicModel,
    StocksListDataFrame,
    StocksListModel,
)
from tradepy.decors import throttle
from tradepy.utils import convert_code_to_exchange

RETRY_ARGS = {
    "stop": stop_after_attempt(3),
    "wait": wait_exponential(multiplier=1, min=3, max=12) + wait_random(1, 3),
}


class TushareClient:
    def __init__(
        self, token: str = config.common.tushare_token.get_secret_value()
    ):
        self.api = ts.pro_api(token)

    @retry(**RETRY_ARGS)
    @throttle("200/m")
    def get_stock_basic(
        self,
        *,
        code: str | None = None,
        since: date | None = None,
        until: date | None = None,
        trade_date: date | None = None,
    ) -> StocksBasicDataFrame:
        if trade_date is None:
            assert code is not None and since is not None and until is not None
            args = {
                "ts_code": code,
                "start_date": since.strftime("%Y%m%d"),
                "end_date": until.strftime("%Y%m%d"),
            }
        else:
            args = {
                "trade_date": trade_date.strftime("%Y%m%d"),
            }

        df = self.api.daily_basic(**args)
        df.rename(
            columns={
                "ts_code": "code",
                "trade_date": "date",
                "dv_ratio": "dv",
                "total_share": "total_shares",
                "float_share": "float_shares",
                "free_share": "free_shares",
            },
            inplace=True,
        )
        df["date"] = pd.to_datetime(df["date"])
        df["type"] = "stock"
        df["exchange"] = df["code"].map(convert_code_to_exchange)

        return pl.from_pandas(  # pyright: ignore[reportReturnType, reportUnknownVariableType]
            df[StocksBasicModel.columns()],
            schema_overrides=StocksBasicModel.schema(),
            nan_to_null=True,
        )

    @throttle("500/m")
    def get_stock_day_klines(
        self,
        *,
        code: str | None = None,
        since: date | None = None,
        until: date | None = None,
        trade_date: date | None = None,
    ) -> DayKlinesDataFrame:
        if trade_date is None:
            assert code is not None and since is not None and until is not None
            args = {
                "ts_code": code,
                "start_date": since.strftime("%Y%m%d"),
                "end_date": until.strftime("%Y%m%d"),
            }
        else:
            args = {
                "trade_date": trade_date.strftime("%Y%m%d"),
            }

        for attempt in Retrying(**RETRY_ARGS):
            with attempt:
                df = self.api.daily(**args)
                if df.empty:
                    return pl.DataFrame(schema=DayKlinesModel.schema())  # pyright: ignore[reportReturnType]

                df.rename(
                    columns={"trade_date": "date", "ts_code": "code"},
                    inplace=True,
                )
                df["date"] = pd.to_datetime(df["date"])
                df["amount"] /= 10
                return pl.from_pandas(  # pyright: ignore[reportReturnType, reportUnknownVariableType]
                    df[DayKlinesModel.columns()],
                    schema_overrides=DayKlinesModel.schema(),
                    nan_to_null=True,
                )

        raise Exception("Should not reach here")

    @cachedmethod(cache=lambda _: TTLCache(maxsize=10, ttl=60 * 60 * 24))
    def get_trade_calendar(
        self, since: date = date(2008, 1, 1)
    ) -> TradeCalendar:
        eoy = date.today().replace(month=12, day=31)
        for attempt in Retrying(**RETRY_ARGS):
            with attempt:
                df = self.api.trade_cal(
                    exchange="SSE",
                    start_date=since.strftime("%Y%m%d"),
                    end_date=eoy.strftime("%Y%m%d"),
                )

                dates = set(
                    sorted(
                        f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"
                        for _, dt, is_open, _ in df.itertuples(index=False)
                        if is_open
                    )
                )
                return TradeCalendar(dates)

        raise Exception("Should not reach here")

    def get_name_changes(
        self, since: date = date(1990, 1, 1)
    ) -> StockNameChangesDataFrame:
        dfs = []

        for year in range(since.year, date.today().year + 3):
            for attempt in Retrying(**RETRY_ARGS):
                with attempt:
                    df = self.api.namechange(
                        start_date=f"{year}0101",
                        end_date=f"{year + 3}1231",
                        fields="ts_code,name,start_date,change_reason",
                    ).rename(
                        columns={
                            "ts_code": "code",
                            "start_date": "since",
                            "change_reason": "reason",
                        },
                    )
                    df["since"] = pd.to_datetime(df["since"])
                    dfs.append(df)

        df = pd.concat(dfs)
        df.drop_duplicates(subset=["code", "name"], inplace=True)
        df.sort_values(by=["code", "since"], inplace=True)

        return pl.from_pandas(  # pyright: ignore[reportReturnType, reportUnknownVariableType]
            df[StockNameChangesModel.columns()],
            schema_overrides=StockNameChangesModel.schema(),
            nan_to_null=True,
        )

    @retry(**RETRY_ARGS)
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

    @retry(**RETRY_ARGS)
    @throttle("200/m")
    def get_stock_price_adjust_factors(
        self, code: str
    ) -> StockPriceAdjustFactorsDataFrame:
        df = self.api.adj_factor(ts_code=code).rename(
            columns={
                "ts_code": "code",
                "trade_date": "date",
                "adj_factor": "backward",
            },
        )
        df["date"] = pd.to_datetime(df["date"])
        return pl.from_pandas(  # pyright: ignore[reportReturnType, reportUnknownVariableType]
            df[StockPriceAdjustFactorsModel.columns()],
            schema_overrides=StockPriceAdjustFactorsModel.schema(),
            nan_to_null=True,
        )
