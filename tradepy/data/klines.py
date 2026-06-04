from typing import Generic, TypeVar

import pandera.polars as pa
import polars as pl
from pandera.typing.polars import DataFrame

from tradepy.core.types import DayKlinesModel

T = TypeVar("T", bound=pa.DataFrameModel)


class BaseKline(Generic[T]):
    model: type[T]

    def __init__(self, data: DataFrame[T]):
        self.data: DataFrame[T] = self.model.validate(data)


class DateKlinesData(BaseKline[DayKlinesModel]):
    model = DayKlinesModel

    def filter_code(self, code: str) -> DataFrame[DayKlinesModel]:
        return self.data.filter(pl.col("code") == code)
