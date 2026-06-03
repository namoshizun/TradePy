from typing import Literal, TypeAlias

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import DataFrame

# -- Basic Types -----------------------
Period: TypeAlias = Literal[
    "1m", "5m", "10m", "15m", "30m", "60m", "1d", "1w", "1M", "1Q", "1Y"
]

AdjustType: TypeAlias = Literal[
    "forward", "backward", "forward_additive", "backward_additive", "none"
]

InstrumentType: TypeAlias = Literal["stock", "etf", "index"]

ExchangeType: TypeAlias = Literal["SZ", "SH", "BJ"]

BroadIndexType: TypeAlias = Literal[
    "SSE",
    "SZSE",
    "ChiNext",
    "STAR",
    "CSI-300",
    "CSI-500",
    "CSI-1000",
    "SSE-50",
]


MarketType: TypeAlias = Literal[
    "上证主板",
    "深证主板",
    "创业板",
    "北交所",
    "科创板",
    "北交所",
    "CDR",
    "新三板",
]


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
    open: pl.Float32
    high: pl.Float32
    low: pl.Float32
    close: pl.Float32
    vol: pl.Int32  # 手
    amount: pl.Int32  # 万元
    pct_chg: pl.Float16


class DayKlinesModel(KlinesModel):
    code: pl.Categorical
    date: pl.Date


DayKlinesDataFrame = DataFrame[DayKlinesModel]


# -- Instrument -----------------------
class InstrumentInfoModel(BaseFrameModel):
    code: pl.Categorical
    type: pl.Categorical  # stock, etf, index
    exchange: pl.Categorical


class StocksBasicModel(InstrumentInfoModel):
    date: pl.Date
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


class StocksListModel(BaseFrameModel):
    code: pl.Categorical
    name: pl.Categorical
    area: pl.Categorical  # 省份地区
    is_listing: pl.Boolean  # True: 上市, False: 退市
    list_date: pl.Date
    delist_date: pl.Date
    is_hs: pl.Boolean  # True: 沪深港通, False: 非沪深港通


StocksListDataFrame = DataFrame[StocksListModel]


class ETFBasicModel(InstrumentInfoModel):
    date: pl.Date
    total_shares: pl.Int64
    turnover_rate: pl.Float16


StocksBasicDataFrame = DataFrame[StocksBasicModel]
ETFBasicDataFrame = DataFrame[ETFBasicModel]


class StockNameChangesModel(BaseFrameModel):
    code: pl.Categorical
    name: pl.Categorical
    since: pl.Date
    reason: pl.Categorical


StockNameChangesDataFrame = DataFrame[StockNameChangesModel]


class StockPriceAdjustFactorsModel(BaseFrameModel):
    code: pl.Categorical
    date: pl.Date
    forward: pl.Float32
    backward: pl.Float32


StockPriceAdjustFactorsDataFrame = DataFrame[StockPriceAdjustFactorsModel]


# -- Industry ------------------------
class SWIndustryListModel(BaseFrameModel):
    code: pl.Categorical
    name: pl.Categorical
    sw_level_1: pl.Int8
    sw_level_2: pl.Int8
    sw_level_3: pl.Int8
    version_year: pl.Int16  # 2014 or 2021


class SWStockIndustryModel(BaseFrameModel):
    code: pl.Categorical
    since: pl.Date
    industry_code: pl.Categorical


SWIndustryListDataFrame = DataFrame[SWIndustryListModel]
SWStockIndustryDataFrame = DataFrame[SWStockIndustryModel]
