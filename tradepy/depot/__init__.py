from abc import abstractmethod
from collections.abc import Generator
from pathlib import Path
from typing import Any, Generic, TypeVar

import polars as pl
from pandera.typing.polars import DataFrame

from tradepy.core.types import BaseFrameModel
from tradepy.utils import get_param_type

T = TypeVar("T", bound=BaseFrameModel)


class DataDepository(Generic[T]):
    _model: BaseFrameModel

    def __init__(self, path: Path | str):
        if isinstance(path, str):
            path = Path(path)

        self.path = path

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)
        if _model := get_param_type(cls):
            cls._model = _model  # pyright: ignore[reportAttributeAccessIssue]
        else:
            raise TypeError(f"Cannot infer model schema for {cls.__name__}")

    def read_metadata(self) -> dict[str, Any]:
        if not self.path.exists():
            return dict()

        meta = pl.read_parquet_metadata(self.path)
        meta.pop("ARROW:schema")
        return meta

    def save(
        self,
        data: DataFrame[T],
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

    def walk(self) -> Generator[Path, None, None]:
        def _walk(path: Path) -> Generator[Path, None, None]:
            if path.is_file():
                yield path
                return

            for _p in path.rglob("*"):
                if _p.is_file():
                    yield _p
                else:
                    yield from _walk(_p)

        return _walk(self.path)

    def load(self) -> DataFrame[T]:
        if self.path.is_file():
            return pl.read_parquet(self.path, schema=self._model.schema())  # pyright: ignore[reportReturnType]

        df: DataFrame[T] = pl.scan_parquet(  # pyright: ignore[reportAssignmentType]
            (self.path / "*.parquet").absolute().as_posix(),
            schema=self._model.schema(),
        ).collect()
        return df

    @abstractmethod
    def is_outdated(self) -> bool:
        raise NotImplementedError


from .klines import StocksDayKlinesDepository
from .listings import (
    StocksIndustryClassListingDepository,
    StocksListingDepository,
)

__all__ = [
    "StocksListingDepository",
    "StocksIndustryClassListingDepository",
    "StocksDayKlinesDepository",
]


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
