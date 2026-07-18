from datetime import date
from pathlib import Path
from typing import Any, TypeVar

from pandera.typing.polars import DataFrame

from tradepy.core.types import (
    BaseFrameModel,
    StockNameChangesModel,
    StocksListModel,
    SWStockIndustryModel,
)
from tradepy.depot import DataDepository

T = TypeVar("T", bound=BaseFrameModel)


class GenericListingDepot(DataDepository[T]):
    _model: BaseFrameModel

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


class StocksListingDepository(GenericListingDepot[StocksListModel]):
    pass


class StocksIndustryClassListingDepository(
    GenericListingDepot[SWStockIndustryModel]
):
    pass


class StockNameChangesDepository(GenericListingDepot[StockNameChangesModel]):
    pass
