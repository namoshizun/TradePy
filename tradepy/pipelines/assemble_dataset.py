from datetime import date

import polars as pl

from tradepy import config
from tradepy.core.types import (
    LazyDayKlinesDataFrame,
    LazyStockDailyMetricsDataFrame,
    LazyStockNameChangesDataFrame,
    LazyStockPriceAdjustFactorsDataFrame,
    LazyStocksBasicDataFrame,
    LazySWStockIndustryDataFrame,
)
from tradepy.depot import (
    StocksAdjustFactorsDepository,
    StocksDayBasicsDepository,
    StocksDayKlinesDepository,
    StocksIndustryClassListingDepository,
)
from tradepy.depot.listings import StockNameChangesDepository
from tradepy.pipelines import Pipeline


class AssembleDatasetPipeline(Pipeline):
    def __init__(self, since: date, until: date):
        self._since = since
        self._until = until

    def _build_stocks_df(
        self,
        klines_df: LazyDayKlinesDataFrame,
        basics_df: LazyStocksBasicDataFrame,
        adj_df: LazyStockPriceAdjustFactorsDataFrame,
        ind_class_df: LazySWStockIndustryDataFrame,
        name_changes_df: LazyStockNameChangesDataFrame,
    ) -> LazyStockDailyMetricsDataFrame:
        return (  # pyright: ignore[reportReturnType]
            klines_df.join(adj_df, on=["code", "date"], how="inner")
            .rename({"backward": "adj_factor"})
            .with_columns(
                pl.col(c) * pl.col("adj_factor")
                for c in ("open", "high", "low", "close")
            )
            .join(basics_df, on=["code", "date"], how="inner")
            .sort("date")
            .join_asof(
                ind_class_df.sort("since"),
                left_on="date",
                right_on="since",
                by="code",
                strategy="backward",
                check_sortedness=False,
            )
            .drop("since")
            .drop_nulls(subset=["industry_code"])
            .join_asof(
                name_changes_df,
                left_on="date",
                right_on="since",
                by="code",
                strategy="backward",
                check_sortedness=False,
            )
            .drop("since", "reason")
            .sort("code", "date")
        )

    def execute(self) -> LazyStockDailyMetricsDataFrame:
        indu_class_depot = StocksIndustryClassListingDepository(
            config.common.get_stock_industry_class_path()
        )
        day_klines_depot = StocksDayKlinesDepository(
            config.common.get_stock_day_klines_path(), self._since, self._until
        )
        day_basics_depot = StocksDayBasicsDepository(
            config.common.get_stock_day_basics_path(), self._since, self._until
        )

        adjust_factors_depot = StocksAdjustFactorsDepository(
            config.common.get_adjust_factors_path()
        )
        name_changes_depot = StockNameChangesDepository(
            config.common.get_stock_name_changes_path()
        )

        return self._build_stocks_df(
            klines_df=day_klines_depot.load(lazy=True),
            basics_df=day_basics_depot.load(lazy=True),
            adj_df=adjust_factors_depot.load(lazy=True),
            ind_class_df=indu_class_depot.load(lazy=True),
            name_changes_df=name_changes_depot.load(lazy=True),
        )
