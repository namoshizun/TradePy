"""
小市值价值股策略:

    - PS、PB均大于0
    - 最近一季产权比率： < 1
    - 市值：小于 0.5*行业均值且小于 300亿
    - PE_ttm: < 行业加权均值
    - PB: < 行业加权均值
    - PS_ttm: < 行业加权均值
    - 最近一季每股自由现金流量： > 行业加权均值
    - 近5年平均总资产报酬率: > 行业加权均值
    - 近5年平均投入资本报酬率: > 行业加权均值


调仓频率：一年三次，分别在4月、8月和10月底，若某一期没有满足条件的股票则保持上一期持仓；
加权方式：流通市值加权；
比较基准：中证500。
"""

import polars as pl

from tradepy.strategy import (
    Average,
    BacktestStrategyBase,
    OriginalPrice,
    WeightedAverage,
)


class MultiFactorSmallCapStrategy(BacktestStrategyBase):
    def compute_indicators(self, df: pl.DataFrame) -> pl.DataFrame:
        # Compute 5-year average ROA / ROIC from year-end (Q4) reports
        avg_5y = (
            df.filter(pl.col("quarter") == 4)
            .unique(subset=["code", "period"])
            .sort("code", "period")
            .select(
                "code",
                "period",
                *(
                    pl.col(col)
                    .rolling_mean(window_size=5, min_samples=5)
                    .over("code")
                    .alias(f"{col}_5y")
                    for col in ("roa", "roic")
                ),
            )
        )
        df = df.join(avg_5y, on=["code", "period"], how="left").drop_nulls()
        return super().compute_indicators(df)

    def buy(
        self,
        ps: float,
        debt_to_eqt: float,
        total_mv: float,
        pe_ttm: float,
        pb: float,
        ps_ttm: float,
        fcff_ps: float,
        roa_5y: float,
        roic_5y: float,
        base_avg_pe_ttm: float = WeightedAverage(
            column="pe_ttm", weights="circ_mv", over="industry_code"
        ),
        base_avg_pb: float = WeightedAverage(
            column="pb", weights="circ_mv", over="industry_code"
        ),
        base_avg_ps_ttm: float = WeightedAverage(
            column="ps_ttm", weights="circ_mv", over="industry_code"
        ),
        base_avg_fcff_ps: float = WeightedAverage(
            column="fcff_ps", weights="circ_mv", over="industry_code"
        ),
        base_avg_total_mv: float = Average(
            column="total_mv", over="industry_code"
        ),
        base_avg_roa_5y: float = WeightedAverage(
            column="roa_5y", weights="circ_mv", over="industry_code"
        ),
        base_avg_roic_5y: float = WeightedAverage(
            column="roic_5y", weights="circ_mv", over="industry_code"
        ),
        orig_open: float = OriginalPrice(column="open"),
    ):
        if ps <= 0 or pb <= 0 or debt_to_eqt >= 1:
            return None

        if (total_mv < 300e8) or (total_mv > base_avg_total_mv * 0.5):
            return None

        # By evaluation indicators
        if (
            (pe_ttm > base_avg_pe_ttm)
            or (pb > base_avg_pb)
            or (ps_ttm > base_avg_ps_ttm)
        ):
            return None

        # By profitability indicators
        if (
            (fcff_ps < base_avg_fcff_ps)
            or (roa_5y < base_avg_roa_5y)
            or (roic_5y < base_avg_roic_5y)
        ):
            return None

        return orig_open


if __name__ == "__main__":
    from tradepy.core.config import StrategyConf

    st = MultiFactorSmallCapStrategy(StrategyConf())
