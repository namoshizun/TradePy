from typing import Literal, TypeAlias

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import DataFrame

Period: TypeAlias = Literal[
    "1m", "5m", "10m", "15m", "30m", "60m", "1d", "1w", "1M", "1Q", "1Y"
]

AdjustType: TypeAlias = Literal[
    "forward", "backward", "forward_additive", "backward_additive", "none"
]

InstrumentType: TypeAlias = Literal["stock", "etf", "index"]

Exchange: TypeAlias = Literal["SZ", "SH", "BJ"]


class BaseFrameModel(pa.DataFrameModel):
    @classmethod
    def columns(cls) -> list[str]:
        return list(cls.to_schema().columns.keys())

    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema(
            {
                name: col.dtype.type
                for name, col in cls.to_schema().columns.items()
            }
        )


# -- Kline ----------------------------
class KlinesModel(BaseFrameModel):
    code: pl.Categorical
    name: pl.Categorical
    open: pl.Float32
    high: pl.Float32
    low: pl.Float32
    close: pl.Float32
    vol: pl.Int32
    amount: pl.Int64
    pct_chg: pl.Float16


class DayKlinesModel(KlinesModel):
    date: pl.Date


DayKlinesDataFrame = DataFrame[DayKlinesModel]


# -- Instrument -----------------------
class InstrumentInfoModel(BaseFrameModel):
    code: pl.Categorical
    name: pl.Categorical
    type: pl.Categorical  # stock, etf, index
    exchange: pl.Categorical


class StocksBasicModel(InstrumentInfoModel):
    total_shares: pl.Int32  # 单位: 万股
    float_shares: pl.Int32
    free_shares: pl.Int32
    turnover_rate: pl.Float16
    pe: pl.Float32
    pe_ttm: pl.Float16
    pb: pl.Float16
    ps: pl.Float16
    ps_ttm: pl.Float16
    dv: pl.Float16
    dv_ttm: pl.Float16
    sw_level_1: pl.Categorical  # 1: 申万一级行业
    sw_level_2: pl.Categorical  # 2: 申万二级行业
    sw_level_3: pl.Categorical  # 3: 申万三级行业


class ETFBasicModel(InstrumentInfoModel):
    total_shares: pl.Int64
    turnover_rate: pl.Float16


StocksBasicDataFrame = DataFrame[StocksBasicModel]
ETFBasicDataFrame = DataFrame[ETFBasicModel]


# -- Industry ------------------------
class SWIndustryListModel(BaseFrameModel):
    code: pl.Categorical
    name: pl.Categorical
    sw_level_1: pl.Int8
    sw_level_2: pl.Int8
    sw_level_3: pl.Int8
    version_year: pl.Int16  # 2014 or 2021


SWIndustryListDataFrame = DataFrame[SWIndustryListModel]
