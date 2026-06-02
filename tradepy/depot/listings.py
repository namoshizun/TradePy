from datetime import date
from pathlib import Path
from typing import Any, TypeVar

import polars as pl
from pandera.typing.polars import DataFrame

from tradepy.core.types import (
    BaseFrameModel,
    StocksListModel,
    SWStockIndustryModel,
)
from tradepy.depot import DataDepository
from tradepy.utils import get_param_type

T = TypeVar("T", bound=BaseFrameModel)


class GenericListingDepot(DataDepository[DataFrame[T]]):
    _model: BaseFrameModel

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)
        if _model := get_param_type(cls):
            cls._model = _model  # pyright: ignore[reportAttributeAccessIssue]
        else:
            raise TypeError(f"Cannot infer model schema for {cls.__name__}")

    def make_metadata(self) -> dict[str, str]:
        return {
            "last_updated": date.today().isoformat(),
        }

    def is_outdated(self) -> bool:
        if meta := self.read_metadata():
            return self.make_metadata() != meta
        return True

    def save(self, data: DataFrame[T], **kwargs: Any) -> Path:
        return super().save(data, metadata=self.make_metadata())

    def load(self) -> DataFrame[T]:
        return pl.read_parquet(self.path, schema=self._model.schema())  # pyright: ignore[reportReturnType]


class StocksListingDepository(GenericListingDepot[StocksListModel]):
    pass


class StocksIndustryClassListingDepository(
    GenericListingDepot[SWStockIndustryModel]
):
    pass
