from abc import abstractmethod
from pathlib import Path
from typing import Any, Generic, TypeVar

import polars as pl
from pandera.typing.polars import DataFrame

T = TypeVar("T", bound=DataFrame)


class DataDepository(Generic[T]):
    def __init__(self, path: Path):
        self.path = path

    def read_metadata(self) -> dict[str, Any]:
        if not self.path.exists():
            return dict()

        meta = pl.read_parquet_metadata(self.path)
        meta.pop("ARROW:schema")
        return meta

    def save(
        self,
        data: T,
        *,
        key: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Path:
        if not key:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            data.write_parquet(self.path, metadata=metadata)
            return self.path

        *folders, filename = key.split("/")
        file_path = self.path / "/".join(folders) / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        data.write_parquet(file_path, metadata=metadata)
        return file_path

    def exists(self, key: str | None = None) -> bool:
        if not key:
            return self.path.exists()

        *folders, filename = key.split("/")
        file_path = self.path / "/".join(folders) / filename
        return file_path.exists()

    @abstractmethod
    def load(self, *args: Any, **kwargs: Any) -> T:
        raise NotImplementedError

    @abstractmethod
    def is_outdated(self) -> bool:
        raise NotImplementedError


from .listings import (
    StocksIndustryClassListingDepository,
    StocksListingDepository,
)

__all__ = ["StocksListingDepository", "StocksIndustryClassListingDepository"]


"""
listing.parquet
./stocks/
    listing.csv
    industry_class.parquet
    basics/
        2026-06-01.parquet
        ...
    day/
        2026-06-01.parquet
        ...
./etf
    listing.csv
    day/
        ...
"""
