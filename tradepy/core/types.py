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


# -- Financial indicators -----------------------
class FinancialIndicatorsModel(BaseFrameModel):
    code: pl.Categorical
    ann_date: pl.Date
    period: pl.Date
    quarter: pl.Int8
    eps: pl.Float64  # 基本每股收益
    dt_eps: pl.Float64  # 稀释每股收益
    total_revenue_ps: pl.Float64  # 每股营业总收入
    revenue_ps: pl.Float64  # 每股营业收入
    capital_rese_ps: pl.Float64  # 每股资本公积
    surplus_rese_ps: pl.Float64  # 每股盈余公积
    undist_profit_ps: pl.Float64  # 每股未分配利润
    extra_item: pl.Float64  # 非经常性损益
    profit_dedt: pl.Float64  # 扣除非经常性损益后的净利润
    gross_margin: pl.Float64  # 毛利
    current_ratio: pl.Float64  # 流动比率
    quick_ratio: pl.Float64  # 速动比率
    cash_ratio: pl.Float64  # 保守速动比率
    ar_turn: pl.Float64  # 存货周转天数
    ca_turn: pl.Float64  # 应收账款周转天数
    fa_turn: pl.Float64  # 存货周转率
    assets_turn: pl.Float64  # 应收账款周转率
    op_income: pl.Float64  # 流动资产周转率
    ebit: pl.Float64  # 固定资产周转率
    ebitda: pl.Float64  # 总资产周转率
    fcff: pl.Float64  # 经营活动净收益
    fcfe: pl.Float64  # 价值变动净收益
    current_exint: pl.Float64  # 利息费用
    noncurrent_exint: pl.Float64  # 折旧与摊销
    interestdebt: pl.Float64  # 息税前利润
    netdebt: pl.Float64  # 息税折旧摊销前利润
    tangible_asset: pl.Float64  # 企业自由现金流量
    working_capital: pl.Float64  # 股权自由现金流量
    networking_capital: pl.Float64  # 无息流动负债
    invest_capital: pl.Float64  # 无息非流动负债
    retained_earnings: pl.Float64  # 带息债务
    diluted2_eps: pl.Float64  # 净债务
    bps: pl.Float64  # 有形资产
    ocfps: pl.Float64  # 营运资金
    retainedps: pl.Float64  # 营运流动资本
    cfps: pl.Float64  # 全部投入资本
    ebit_ps: pl.Float64  # 留存收益
    fcff_ps: pl.Float64  # 期末摊薄每股收益
    fcfe_ps: pl.Float64  # 每股净资产
    netprofit_margin: pl.Float64  # 每股经营活动产生的现金流量净额
    grossprofit_margin: pl.Float64  # 每股留存收益
    cogs_of_sales: pl.Float64  # 每股现金流量净额
    expense_of_sales: pl.Float64  # 每股息税前利润
    profit_to_gr: pl.Float64  # 每股企业自由现金流量
    saleexp_to_gr: pl.Float64  # 每股股东自由现金流量
    adminexp_of_gr: pl.Float64  # 销售净利率
    finaexp_of_gr: pl.Float64  # 销售毛利率
    impai_ttm: pl.Float64  # 销售成本率
    gc_of_gr: pl.Float64  # 销售期间费用率
    op_of_gr: pl.Float64  # 净利润/营业总收入
    ebit_of_gr: pl.Float64  # 销售费用/营业总收入
    roe: pl.Float64  # 管理费用/营业总收入
    roe_waa: pl.Float64  # 财务费用/营业总收入
    roe_dt: pl.Float64  # 资产减值损失/营业总收入
    roa: pl.Float64  # 营业总成本/营业总收入
    npta: pl.Float64  # 营业利润/营业总收入
    roic: pl.Float64  # 息税前利润/营业总收入
    roe_yearly: pl.Float64  # 净资产收益率
    roa2_yearly: pl.Float64  # 加权平均净资产收益率
    debt_to_assets: pl.Float64  # 净资产收益率(扣除非经常损益)
    assets_to_eqt: pl.Float64  # 总资产报酬率
    dp_assets_to_eqt: pl.Float64  # 总资产净利润
    ca_to_assets: pl.Float64  # 投入资本回报率
    nca_to_assets: pl.Float64  # 年化净资产收益率
    tbassets_to_totalassets: pl.Float64  # 年化总资产报酬率
    int_to_talcap: pl.Float64  # 平均净资产收益率(增发条件)
    eqt_to_talcapital: pl.Float64  # 经营活动净收益/利润总额
    currentdebt_to_debt: pl.Float64  # 价值变动净收益/利润总额
    longdeb_to_debt: pl.Float64  # 营业外收支净额/利润总额
    ocf_to_shortdebt: pl.Float64  # 所得税/利润总额
    debt_to_eqt: pl.Float64  # 扣除非经常损益后的净利润/净利润
    eqt_to_debt: pl.Float64  # 销售商品提供劳务收到的现金/营业收入
    eqt_to_interestdebt: pl.Float64  # 经营活动产生的现金流量净额/营业收入
    tangibleasset_to_debt: (
        pl.Float64
    )  # 经营活动产生的现金流量净额/经营活动净收益
    tangasset_to_intdebt: pl.Float64  # 资本支出/折旧和摊销
    tangibleasset_to_netdebt: pl.Float64  # 资产负债率
    ocf_to_debt: pl.Float64  # 权益乘数
    turn_days: pl.Float64  # 权益乘数(杜邦分析)
    roa_yearly: pl.Float64  # 流动资产/总资产
    roa_dp: pl.Float64  # 非流动资产/总资产
    fixed_assets: pl.Float64  # 有形资产/总资产
    profit_to_op: pl.Float64  # 带息债务/全部投入资本
    q_saleexp_to_gr: pl.Float64  # 归属于母公司的股东权益/全部投入资本
    q_gc_to_gr: pl.Float64  # 流动负债/负债合计
    q_roe: pl.Float64  # 非流动负债/负债合计
    q_dt_roe: pl.Float64  # 经营活动产生的现金流量净额/流动负债
    q_npta: pl.Float64  # 产权比率
    q_ocf_to_sales: pl.Float64  # 归属于母公司的股东权益/负债合计
    basic_eps_yoy: pl.Float64  # 归属于母公司的股东权益/带息债务
    dt_eps_yoy: pl.Float64  # 有形资产/负债合计
    cfps_yoy: pl.Float64  # 有形资产/带息债务
    op_yoy: pl.Float64  # 有形资产/净债务
    ebt_yoy: pl.Float64  # 经营活动产生的现金流量净额/负债合计
    netprofit_yoy: pl.Float64  # 经营活动产生的现金流量净额/带息债务
    dt_netprofit_yoy: pl.Float64  # 经营活动产生的现金流量净额/净债务
    ocf_yoy: pl.Float64  # 已获利息倍数(EBIT/利息费用)
    roe_yoy: pl.Float64  # 长期债务与营运资金比率
    bps_yoy: pl.Float64  # 息税折旧摊销前利润/负债合计
    assets_yoy: pl.Float64  # 营业周期
    eqt_yoy: pl.Float64  # 年化总资产净利率
    tr_yoy: pl.Float64  # 总资产净利率(杜邦分析)
    or_yoy: pl.Float64  # 固定资产合计
    q_sales_yoy: pl.Float64  # 扣除财务费用前营业利润
    q_op_qoq: pl.Float64  # 非营业利润
    equity_yoy: pl.Float64  # 营业利润／利润总额
    invturn_days: pl.Float64  # 非营业利润／利润总额
    arturn_days: pl.Float64  # 经营活动产生的现金流量净额／营业利润
    inv_turn: pl.Float64  # 货币资金／流动负债
    valuechange_income: pl.Float64  # 货币资金／带息流动负债
    interst_income: pl.Float64  # 营业利润／流动负债
    daa: pl.Float64  # 营业利润／负债合计
    roe_avg: pl.Float64  # 年化投入资本回报率
    opincome_of_ebt: pl.Float64  # 固定资产合计周转率
    investincome_of_ebt: pl.Float64  # 利润总额／营业收入
    n_op_profit_of_ebt: pl.Float64  # 经营活动单季度净收益
    tax_to_ebt: pl.Float64  # 价值变动单季度净收益
    dtprofit_to_profit: pl.Float64  # 扣除非经常损益后的单季度净利润
    salescash_to_or: pl.Float64  # 每股收益(单季度)
    ocf_to_or: pl.Float64  # 销售净利率(单季度)
    ocf_to_opincome: pl.Float64  # 销售毛利率(单季度)
    capitalized_to_da: pl.Float64  # 销售期间费用率(单季度)
    ocf_to_interestdebt: pl.Float64  # 净利润／营业总收入(单季度)
    ocf_to_netdebt: pl.Float64  # 销售费用／营业总收入 (单季度)
    ebit_to_interest: pl.Float64  # 管理费用／营业总收入 (单季度)
    longdebt_to_workingcapital: pl.Float64  # 财务费用／营业总收入 (单季度)
    ebitda_to_debt: pl.Float64  # 资产减值损失／营业总收入(单季度)
    profit_prefin_exp: pl.Float64  # 营业总成本／营业总收入 (单季度)
    non_op_profit: pl.Float64  # 营业利润／营业总收入(单季度)
    op_to_ebt: pl.Float64  # 净资产收益率(单季度)
    nop_to_ebt: pl.Float64  # 净资产单季度收益率(扣除非经常损益)
    ocf_to_profit: pl.Float64  # 总资产净利润(单季度)
    cash_to_liqdebt: pl.Float64  # 经营活动净收益／利润总额(单季度)
    cash_to_liqdebt_withinterest: pl.Float64  # 价值变动净收益／利润总额(单季度)
    op_to_liqdebt: pl.Float64  # 扣除非经常损益后的净利润／净利润(单季度)
    op_to_debt: pl.Float64  # 销售商品提供劳务收到的现金／营业收入(单季度)
    roic_yearly: pl.Float64  # 经营活动产生的现金流量净额／营业收入(单季度)
    total_fa_trun: (
        pl.Float64
    )  # 经营活动产生的现金流量净额／经营活动净收益(单季度)
    q_opincome: pl.Float64  # 基本每股收益同比增长率(%)
    q_investincome: pl.Float64  # 稀释每股收益同比增长率(%)
    q_dtprofit: pl.Float64  # 每股经营活动产生的现金流量净额同比增长率(%)
    q_eps: pl.Float64  # 营业利润同比增长率(%)
    q_netprofit_margin: pl.Float64  # 利润总额同比增长率(%)
    q_gsprofit_margin: pl.Float64  # 归属母公司股东的净利润同比增长率(%)
    q_exp_to_sales: (
        pl.Float64
    )  # 归属母公司股东的净利润-扣除非经常损益同比增长率(%)
    q_profit_to_gr: pl.Float64  # 经营活动产生的现金流量净额同比增长率(%)
    q_adminexp_to_gr: pl.Float64  # 净资产收益率(摊薄)同比增长率(%)
    q_finaexp_to_gr: pl.Float64  # 每股净资产相对年初增长率(%)
    q_impair_to_gr_ttm: pl.Float64  # 资产总计相对年初增长率(%)
    q_op_to_gr: pl.Float64  # 归属母公司的股东权益相对年初增长率(%)
    q_opincome_to_ebt: pl.Float64  # 营业总收入同比增长率(%)
    q_investincome_to_ebt: pl.Float64  # 营业收入同比增长率(%)
    q_dtprofit_to_profit: pl.Float64  # 营业总收入同比增长率(%)(单季度)
    q_salescash_to_or: pl.Float64  # 营业总收入环比增长率(%)(单季度)
    q_ocf_to_or: pl.Float64  # 营业收入同比增长率(%)(单季度)
    q_gr_yoy: pl.Float64  # 营业收入环比增长率(%)(单季度)
    q_gr_qoq: pl.Float64  # 营业利润同比增长率(%)(单季度)
    q_sales_qoq: pl.Float64  # 营业利润环比增长率(%)(单季度)
    q_op_yoy: pl.Float64  # 净利润同比增长率(%)(单季度)
    q_profit_yoy: pl.Float64  # 净利润环比增长率(%)(单季度)
    q_profit_qoq: pl.Float64  # 归属母公司股东的净利润同比增长率(%)(单季度)
    q_netprofit_yoy: pl.Float64  # 归属母公司股东的净利润环比增长率(%)(单季度)
    q_netprofit_qoq: pl.Float64  # 净资产同比增长率
    rd_exp: pl.Float64  # 研发费用


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
