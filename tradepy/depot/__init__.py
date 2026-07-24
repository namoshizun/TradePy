import json
from collections.abc import Generator
from datetime import date
from pathlib import Path
from typing import Any, Generic, Literal, TypedDict, TypeVar, overload

import polars as pl
from pandera.typing.polars import DataFrame, LazyFrame

from tradepy.core.types import BaseFrameModel
from tradepy.utils import ensure_laziness, get_param_type

T = TypeVar("T", bound=BaseFrameModel)


class UpdateMark(TypedDict):
    last_updated: str


DEFAULT_UPDATE_MARK: UpdateMark = {"last_updated": "1970-01-01"}


class DataDepository(Generic[T]):
    _model: BaseFrameModel
    _default_path: Path
    _update_period: Literal["daily", "weekly", "monthly", "yearly"]

    def __init__(self, path: Path | str | None = None):
        if isinstance(path, str):
            path = Path(path)

        if path is None:
            assert hasattr(self, "_default_path")
            path = self._default_path

        self.path = path

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)
        if _model := get_param_type(cls):
            cls._model = _model  # pyright: ignore[reportAttributeAccessIssue]
        else:
            raise TypeError(f"Cannot infer model schema for {cls.__name__}")

    def save(
        self,
        data: DataFrame[T],
        *,
        key: str | None = None,
        metadata: UpdateMark | None = None,  # pyright: ignore[reportRedeclaration]
    ) -> Path:
        if data.is_empty():
            return self.path

        if not metadata:
            metadata: UpdateMark = {
                "last_updated": date.today().isoformat(),
            }
        assert metadata is not None

        if not key:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            data.write_parquet(self.path, metadata=metadata)  # pyright: ignore[reportArgumentType]
            return self.path

        *folders, filename = key.split("/")
        file_path = self.path / "/".join(folders) / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        data.write_parquet(file_path, metadata=metadata)  # pyright: ignore[reportArgumentType]
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

    def sources(self) -> list[str]:
        if self.path.is_file():
            return [self.path.absolute().as_posix()]

        return [p.absolute().as_posix() for p in self.path.glob("*.parquet")]

    @overload
    def load(self, lazy: Literal[True]) -> LazyFrame[T]: ...

    @overload
    def load(self, lazy: Literal[False] = False) -> DataFrame[T]: ...

    def load(self, lazy: bool = False) -> DataFrame[T] | LazyFrame[T]:
        schema = self._model.schema()

        if not (sources := self.sources()):
            df: Any = pl.DataFrame(schema=schema)
        else:
            df = pl.scan_parquet(
                sources,
                schema=schema,
                extra_columns="ignore",
                missing_columns="insert",
            )

        return ensure_laziness(df, lazy)

    def _read_update_mark(self) -> UpdateMark:
        if not self.path.exists():
            return DEFAULT_UPDATE_MARK

        # The update mark is directly stored in the parquet file.
        if self.path.is_file():
            data = pl.read_parquet_metadata(self.path)
            data.pop("ARROW:schema")
        else:
            mark_file = self.path / "update-mark.json"
            if not mark_file.exists():
                return DEFAULT_UPDATE_MARK

            with mark_file.open("r") as f:
                data = json.load(f)

        if not data.get("last_updated"):
            return DEFAULT_UPDATE_MARK

        return data  # pyright: ignore[reportReturnType]

    def is_outdated(self) -> bool:
        update_mark = self._read_update_mark()
        last_updated = date.fromisoformat(update_mark["last_updated"])
        if last_updated == DEFAULT_UPDATE_MARK["last_updated"]:
            return True

        today = date.today()
        if self._update_period == "daily":
            return last_updated != today
        if self._update_period == "weekly":
            return last_updated.isocalendar()[:2] != today.isocalendar()[:2]
        if self._update_period == "monthly":
            return (last_updated.year, last_updated.month) != (
                today.year,
                today.month,
            )
        if self._update_period == "yearly":
            return last_updated.year != today.year
        return False

    def mark_updated(self):
        if self.path.is_file():
            # Already did this in the `save` operation
            return

        mark_file = self.path / "update-mark.json"
        with mark_file.open("w+") as f:
            json.dump({"last_updated": date.today().isoformat()}, f)


from .financial import FinancialIndicatorsDepository
from .klines import StocksDayBasicsDepository, StocksDayKlinesDepository
from .misc import (
    StockNameChangesDepository,
    StocksAdjustFactorsDepository,
    StocksIndustryClassListingDepository,
    StocksListingDepository,
)

__all__ = [
    "StocksListingDepository",
    "StocksDayBasicsDepository",
    "StocksIndustryClassListingDepository",
    "StocksDayKlinesDepository",
    "StocksAdjustFactorsDepository",
    "StockNameChangesDepository",
    "FinancialIndicatorsDepository",
]
