import math
from datetime import datetime

import pandas as pd
import polars as pl
from pandera.typing.pandas import DataFrame
from tickflow import TickFlow

from tradepy.core.types import (
    AdjustType,
    DayKlinesDataFrame,
    DayKlinesModel,
    InstrumentType,
    Period,
)


class TickFlowClient:
    def __init__(self, api_key: str):
        self.tf = TickFlow(api_key=api_key)

    def _batch_klines(
        self,
        symbols: list[str],
        period: Period,
        since: datetime,
        until: datetime,
        adjust: AdjustType = "none",
    ) -> DataFrame:
        assert len(symbols) <= 100, "Tickflow批量查询标的池数量必须小于100"
        count_multipler = {
            "1d": 1,
            "1w": 1 / 7,
            "1M": 1 / 30,
            "1Q": 1 / 12,
            "1Y": 1 / 365,
            "60m": 4,  # A-stock trading hours
            "30m": 8,
            "15m": 16,
            "10m": 24,
            "5m": 48,
            "1m": 288,
        }
        days = max((until - since).days, 1) + 1
        count = math.ceil(days * count_multipler[period])

        result = self.tf.klines.batch(
            symbols,
            period=period,
            count=count,
            start_time=int(since.timestamp() * 1000),
            end_time=int(until.timestamp() * 1000),
            adjust=adjust,
            as_dataframe=True,
        )

        df = pd.concat(result.values(), ignore_index=True)
        return df

    def get_day_klines(
        self,
        symbols: list[str],
        since: datetime,
        until: datetime,
        adjust: AdjustType = "none",
    ) -> DayKlinesDataFrame:
        df = self._batch_klines(symbols, "1d", since, until, adjust)
        df.rename(
            columns={"symbol": "code", "trade_date": "date", "volume": "vol"},
            inplace=True,
        )
        return pl.from_pandas(  # pyright: ignore[reportReturnType, reportUnknownVariableType]
            df[DayKlinesModel.columns()],
            schema_overrides=DayKlinesModel.schema(),
        )

    def get_all_instruments(self) -> dict[InstrumentType, list[str]]:
        stocks = self.tf.universes.get("CN_Equity_A")
        etf = self.tf.universes.get("CN_ETF")
        hs_index = self.tf.universes.get("CN_Index")
        bj_index = self.tf.universes.get("CN_Index_BJ")

        return {
            "stock": stocks["symbols"],
            "etf": etf["symbols"],
            "index": hs_index["symbols"] + bj_index["symbols"],
        }
