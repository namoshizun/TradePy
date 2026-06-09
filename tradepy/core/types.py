from dataclasses import dataclass
from typing import Literal, TypeAlias

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import DataFrame, LazyFrame

# -- Basic Types -----------------------
TradeActionType = Literal[
    "开仓",
    "平仓",
    "止损",
    "止盈",
]

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
LazyDayKlinesDataFrame = LazyFrame[DayKlinesModel]


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


StocksBasicDataFrame = DataFrame[StocksBasicModel]
LazyStocksBasicDataFrame = LazyFrame[StocksBasicModel]


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
    backward: pl.Float32


StockPriceAdjustFactorsDataFrame = DataFrame[StockPriceAdjustFactorsModel]
LazyStockPriceAdjustFactorsDataFrame = LazyFrame[StockPriceAdjustFactorsModel]


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
LazySWStockIndustryDataFrame = LazyFrame[SWStockIndustryModel]


# -- Assembled snapshots dataframes -----------------------
class StockDailyMetricsModel(DayKlinesModel, StocksBasicModel):
    industry_code: pl.Categorical
    adj_factor: pl.Float32


StockDailyMetricsDataFrame = DataFrame[StockDailyMetricsModel]
LazyStockDailyMetricsDataFrame = LazyFrame[StockDailyMetricsModel]


# -- Trading -----------------------
@dataclass(frozen=True, slots=True)
class BarData:
    code: str
    open: float
    close: float
    high: float
    low: float
    vol: int
    pct_chg: float
    adj_factor: float
    sell_price: float | None

    @property
    def orig_open(self) -> float:
        return self.open / self.adj_factor

    @property
    def orig_high(self) -> float:
        return self.high / self.adj_factor

    @property
    def orig_low(self) -> float:
        return self.low / self.adj_factor

    @property
    def orig_close(self) -> float:
        return self.close / self.adj_factor
