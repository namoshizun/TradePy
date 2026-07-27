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
    total_mv: pl.Int32  # 单位: 亿元
    circ_mv: pl.Int32  # 单位: 亿元
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
LazyStocksListDataFrame = LazyFrame[StocksListModel]


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
LazyStockNameChangesDataFrame = LazyFrame[StockNameChangesModel]


class StockPriceAdjustFactorsModel(BaseFrameModel):
    code: pl.Categorical
    date: pl.Date
    backward: pl.Float32


StockPriceAdjustFactorsDataFrame = DataFrame[StockPriceAdjustFactorsModel]
LazyStockPriceAdjustFactorsDataFrame = LazyFrame[StockPriceAdjustFactorsModel]


# -- Industry ------------------------
class SWIndustryListModel(BaseFrameModel):
    code: pl.Categorical
    level_1: pl.Categorical
    level_2: pl.Categorical
    level_3: pl.Categorical
    version_year: pl.Int16  # 2014 or 2021


class SWStockIndustryModel(BaseFrameModel):
    code: pl.Categorical
    since: pl.Date
    industry_code: pl.Categorical


SWIndustryListDataFrame = DataFrame[SWIndustryListModel]
SWStockIndustryDataFrame = DataFrame[SWStockIndustryModel]
LazySWStockIndustryDataFrame = LazyFrame[SWStockIndustryModel]


# -- Financial indicators -----------------------
class FinancialIndicatorsModel(BaseFrameModel):
    # See: https://tushare.pro/document/2?doc_id=79
    code: pl.Categorical
    ann_date: pl.Date
    period: pl.Int16  # 财年: 2010, 2011, ...
    quarter: pl.Int8  # 季度: 1, 2, 3, 4
    eps: pl.Float64
    dt_eps: pl.Float64
    total_revenue_ps: pl.Float64
    revenue_ps: pl.Float64
    capital_rese_ps: pl.Float64
    surplus_rese_ps: pl.Float64
    undist_profit_ps: pl.Float64
    extra_item: pl.Float64
    profit_dedt: pl.Float64
    gross_margin: pl.Float64
    current_ratio: pl.Float64
    quick_ratio: pl.Float64
    cash_ratio: pl.Float64
    ar_turn: pl.Float64
    ca_turn: pl.Float64
    fa_turn: pl.Float64
    assets_turn: pl.Float64
    op_income: pl.Float64
    ebit: pl.Float64
    ebitda: pl.Float64
    fcff: pl.Float64
    fcfe: pl.Float64
    current_exint: pl.Float64
    noncurrent_exint: pl.Float64
    interestdebt: pl.Float64
    netdebt: pl.Float64
    tangible_asset: pl.Float64
    working_capital: pl.Float64
    networking_capital: pl.Float64
    invest_capital: pl.Float64
    retained_earnings: pl.Float64
    diluted2_eps: pl.Float64
    bps: pl.Float64
    ocfps: pl.Float64
    retainedps: pl.Float64
    cfps: pl.Float64
    ebit_ps: pl.Float64
    fcff_ps: pl.Float64
    fcfe_ps: pl.Float64
    netprofit_margin: pl.Float64
    grossprofit_margin: pl.Float64
    cogs_of_sales: pl.Float64
    expense_of_sales: pl.Float64
    profit_to_gr: pl.Float64
    saleexp_to_gr: pl.Float64
    adminexp_of_gr: pl.Float64
    finaexp_of_gr: pl.Float64
    impai_ttm: pl.Float64
    gc_of_gr: pl.Float64
    op_of_gr: pl.Float64
    ebit_of_gr: pl.Float64
    roe: pl.Float64
    roe_waa: pl.Float64
    roe_dt: pl.Float64
    roa: pl.Float64
    npta: pl.Float64
    roic: pl.Float64
    roe_yearly: pl.Float64
    roa2_yearly: pl.Float64
    debt_to_assets: pl.Float64
    assets_to_eqt: pl.Float64
    dp_assets_to_eqt: pl.Float64
    ca_to_assets: pl.Float64
    nca_to_assets: pl.Float64
    tbassets_to_totalassets: pl.Float64
    int_to_talcap: pl.Float64
    eqt_to_talcapital: pl.Float64
    currentdebt_to_debt: pl.Float64
    longdeb_to_debt: pl.Float64
    ocf_to_shortdebt: pl.Float64
    debt_to_eqt: pl.Float64
    eqt_to_debt: pl.Float64
    eqt_to_interestdebt: pl.Float64
    tangibleasset_to_debt: pl.Float64
    tangasset_to_intdebt: pl.Float64
    tangibleasset_to_netdebt: pl.Float64
    ocf_to_debt: pl.Float64
    turn_days: pl.Float64
    roa_yearly: pl.Float64
    roa_dp: pl.Float64
    fixed_assets: pl.Float64
    profit_to_op: pl.Float64
    q_saleexp_to_gr: pl.Float64
    q_gc_to_gr: pl.Float64
    q_roe: pl.Float64
    q_dt_roe: pl.Float64
    q_npta: pl.Float64
    q_ocf_to_sales: pl.Float64
    basic_eps_yoy: pl.Float64
    dt_eps_yoy: pl.Float64
    cfps_yoy: pl.Float64
    op_yoy: pl.Float64
    ebt_yoy: pl.Float64
    netprofit_yoy: pl.Float64
    dt_netprofit_yoy: pl.Float64
    ocf_yoy: pl.Float64
    roe_yoy: pl.Float64
    bps_yoy: pl.Float64
    assets_yoy: pl.Float64
    eqt_yoy: pl.Float64
    tr_yoy: pl.Float64
    or_yoy: pl.Float64
    q_sales_yoy: pl.Float64
    q_op_qoq: pl.Float64
    equity_yoy: pl.Float64
    invturn_days: pl.Float64
    arturn_days: pl.Float64
    inv_turn: pl.Float64
    valuechange_income: pl.Float64
    interst_income: pl.Float64
    daa: pl.Float64
    roe_avg: pl.Float64
    opincome_of_ebt: pl.Float64
    investincome_of_ebt: pl.Float64
    n_op_profit_of_ebt: pl.Float64
    tax_to_ebt: pl.Float64
    dtprofit_to_profit: pl.Float64
    salescash_to_or: pl.Float64
    ocf_to_or: pl.Float64
    ocf_to_opincome: pl.Float64
    capitalized_to_da: pl.Float64
    ocf_to_interestdebt: pl.Float64
    ocf_to_netdebt: pl.Float64
    ebit_to_interest: pl.Float64
    longdebt_to_workingcapital: pl.Float64
    ebitda_to_debt: pl.Float64
    profit_prefin_exp: pl.Float64
    non_op_profit: pl.Float64
    op_to_ebt: pl.Float64
    nop_to_ebt: pl.Float64
    ocf_to_profit: pl.Float64
    cash_to_liqdebt: pl.Float64
    cash_to_liqdebt_withinterest: pl.Float64
    op_to_liqdebt: pl.Float64
    op_to_debt: pl.Float64
    roic_yearly: pl.Float64
    total_fa_trun: pl.Float64
    q_opincome: pl.Float64
    q_investincome: pl.Float64
    q_dtprofit: pl.Float64
    q_eps: pl.Float64
    q_netprofit_margin: pl.Float64
    q_gsprofit_margin: pl.Float64
    q_exp_to_sales: pl.Float64
    q_profit_to_gr: pl.Float64
    q_adminexp_to_gr: pl.Float64
    q_finaexp_to_gr: pl.Float64
    q_impair_to_gr_ttm: pl.Float64
    q_op_to_gr: pl.Float64
    q_opincome_to_ebt: pl.Float64
    q_investincome_to_ebt: pl.Float64
    q_dtprofit_to_profit: pl.Float64
    q_salescash_to_or: pl.Float64
    q_ocf_to_or: pl.Float64
    q_gr_yoy: pl.Float64
    q_gr_qoq: pl.Float64
    q_sales_qoq: pl.Float64
    q_op_yoy: pl.Float64
    q_profit_yoy: pl.Float64
    q_profit_qoq: pl.Float64
    q_netprofit_yoy: pl.Float64
    q_netprofit_qoq: pl.Float64
    rd_exp: pl.Float64


StockFinancialIndicatorsDataFrame = DataFrame[FinancialIndicatorsModel]
LazyStockFinancialIndicatorsDataFrame = LazyFrame[FinancialIndicatorsModel]


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
    sell_price: float | None
    buy_price: float | None = None
    is_held: bool = False
