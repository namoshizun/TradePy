import abc
from datetime import date
from typing import Any

import polars as pl
from pandera.typing.polars import LazyFrame

from tradepy.core.types import (
    LazyDayKlinesDataFrame,
    LazyStockNameChangesDataFrame,
    LazyStockPriceAdjustFactorsDataFrame,
    StocksBasicModel,
    SWStockIndustryModel,
)
from tradepy.depot import (
    StockNameChangesDepository,
    StocksAdjustFactorsDepository,
    StocksDayBasicsDepository,
    StocksDayKlinesDepository,
    StocksIndustryClassListingDepository,
)
from tradepy.pipelines import Pipeline


class IngredientData(abc.ABC):
    def __init__(self, columns: list[str] | None = None):
        self.columns = columns or []

    @abc.abstractmethod
    def load(self, *args: Any, **kwargs: Any) -> LazyFrame[Any]:
        raise NotImplementedError

    @abc.abstractmethod
    def apply(
        self, main_df: LazyFrame[Any], df: LazyFrame[Any]
    ) -> LazyFrame[Any]:
        raise NotImplementedError


class StockDayBasicsData(IngredientData):
    def __init__(
        self, since: date, until: date, columns: list[str] | None = None
    ):
        super().__init__(columns)
        self.since = since
        self.until = until

    def load(self) -> LazyFrame[Any]:
        depot = StocksDayBasicsDepository(self.since, self.until)
        cols = self.columns or StocksBasicModel.columns()
        cols = list(set(cols) | {"code", "date"})
        return depot.load(lazy=True).select(*cols)  # pyright: ignore[reportReturnType]

    def apply(
        self, main_df: LazyFrame[Any], df: LazyFrame[Any]
    ) -> LazyFrame[Any]:
        return main_df.join(df, on=["code", "date"], how="inner")  # pyright: ignore[reportReturnType]


class StocksIndustryClassData(IngredientData):
    def load(self) -> LazyFrame[Any]:
        depot = StocksIndustryClassListingDepository()
        cols = self.columns or SWStockIndustryModel.columns()
        cols = list(set(cols) | {"since", "code"})
        return depot.load(lazy=True).select(*cols).sort("since")  # pyright: ignore[reportReturnType]

    def apply(
        self, main_df: LazyFrame[Any], df: LazyFrame[Any]
    ) -> LazyFrame[Any]:
        return (
            main_df.join_asof(  # pyright: ignore[reportReturnType]
                df,
                left_on="date",
                right_on="since",
                by="code",
                strategy="backward",
                check_sortedness=False,
            )
            .drop("since")
            .drop_nulls(subset=["industry_code"])
        )


class AssembleDatasetPipeline(Pipeline):
    def __init__(
        self,
        since: date,
        until: date,
        ingredients: list[IngredientData] | None = None,
    ):
        self._since = since
        self._until = until
        self._ingredients = ingredients or []

    def _build_main_df(
        self,
        klines_df: LazyDayKlinesDataFrame,
        adj_df: LazyStockPriceAdjustFactorsDataFrame,
        name_changes_df: LazyStockNameChangesDataFrame,
    ) -> LazyFrame[Any]:
        return (  # pyright: ignore[reportReturnType]
            klines_df.join(adj_df, on=["code", "date"], how="inner")
            .rename({"backward": "adj_factor"})
            .with_columns(
                pl.col(c) * pl.col("adj_factor")
                for c in ("open", "high", "low", "close")
            )
            .sort("date")
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

    def execute(self) -> LazyFrame[Any]:
        # Load and build the minimal-viable dataframe
        day_klines_depot = StocksDayKlinesDepository(self._since, self._until)
        adjust_factors_depot = StocksAdjustFactorsDepository()
        name_changes_depot = StockNameChangesDepository()

        df = self._build_main_df(
            klines_df=day_klines_depot.load(lazy=True),
            adj_df=adjust_factors_depot.load(lazy=True),
            name_changes_df=name_changes_depot.load(lazy=True),
        )

        # Patch whatever auxiliary data from provided data depositories
        for ind in self._ingredients:
            ind_df = ind.load()
            df = ind.apply(df, ind_df)

        return df
