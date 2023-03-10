import abc
from pathlib import Path
import pandas as pd
from contextlib import suppress
from tqdm import tqdm
from functools import partial
from typing import Any, Generator

import tradepy
from tradepy.convertion import convert_code_to_market


class GenericBarsDepot:

    folder_name: str
    caches: dict[str, Any] = dict()

    def __init__(self) -> None:
        assert isinstance(self.folder_name, str)
        self.folder = tradepy.config.dataset_dir / self.folder_name
        self.folder.mkdir(parents=True, exist_ok=True)

    def size(self) -> int:
        return sum(1 for _ in self.folder.iterdir())

    def save(self, df: pd.DataFrame, filename: str):
        assert filename.endswith('csv')
        df.to_csv(self.folder / filename, index=False)

    def append(self, df: pd.DataFrame, filename: str):
        assert filename.endswith('csv')
        path = self.folder / filename

        if not path.exists():
            df.to_csv(path, index=False)

        _df = pd.read_csv(path, index_col=None)

        (
            pd.concat([df, _df])
            .drop_duplicates()
            .to_csv(path, index=False)
        )

    def exists(self, name: str):
        return (self.folder / f'{name}.csv').exists()

    def traverse(self, always_load=False):
        load = partial(pd.read_csv)

        for path in tqdm(self.folder.iterdir()):
            if str(path).endswith('.csv'):
                if always_load:
                    yield path.stem, load(path)
                else:
                    should_load = yield path.stem
                    if should_load:
                        yield load(path)

    def _generic_load_bars(self,
                            index_by: str | list[str] = "code",
                            cache_key=None,
                            cache=False) -> pd.DataFrame:

        if cache:
            assert cache_key
            if cache_key in self.caches:
                return self.caches[cache_key]

        def loader() -> Generator[pd.DataFrame, None, None]:
            for code, df in self.traverse(always_load=True):
                df["code"] = code
                yield df

        df = pd.concat(loader())
        df.set_index(index_by, inplace=True)
        df.sort_index(inplace=True)

        with suppress(KeyError):
            df.sort_values("timestamp", inplace=True)

        if cache:
            assert cache_key
            self.caches[cache_key] = df.copy()
        return df

    @abc.abstractmethod
    def _load(self, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def load(cls, *args, **kwargs) -> pd.DataFrame:
        self = cls()
        return self._load(*args, **kwargs)


class StocksDailyBarsDepot(GenericBarsDepot):

    folder_name = "daily.stocks"
    default_loaded_fields = "timestamp,code,company,market,open,high,low,close,vol,chg,pct_chg,mkt_cap_rank"

    def _load(self,
             index_by: str | list[str] = "code",
             since_date: str | None = None,
             fields: str = default_loaded_fields) -> pd.DataFrame:

        def loader() -> Generator[pd.DataFrame, None, None]:
            source_iter = self.traverse()

            while True:
                try:
                    code: str = next(source_iter)
                except StopIteration:
                    break

                if not tradepy.listing.has_code(code):
                    continue

                df = source_iter.send(True)
                assert isinstance(df, pd.DataFrame)
                df["code"] = code

                if not set(["company", "market"]).issubset(df.columns):
                    company = tradepy.listing.get_by_code(code).name
                    market = convert_code_to_market(code)
                    df[["company", "market"]] = company, market

                if since_date:
                    yield df.query(f'timestamp >= "{since_date}"')
                else:
                    yield df

        df = pd.concat(loader())

        cat_columns = ["company", "market"]
        for col in cat_columns:
            df[col] = df[col].astype("category")

        df.set_index(index_by, inplace=True)
        df.sort_index(inplace=True)

        if fields != "all":
            _fields = fields.split(",")
            if isinstance(index_by, str) and index_by in _fields:
                _fields.remove(index_by)
            else:
                _fields = list(set(_fields) - set(index_by))
            return df[_fields]
        return df


class BroadBasedIndexBarsDepot(GenericBarsDepot):

    folder_name = "daily.broad-based"

    def _load(self, index_by: str | list[str] = "code", cache=True) -> pd.DataFrame:
        return self._generic_load_bars(index_by, cache_key=self.folder_name, cache=cache)


class SectorIndexBarsDepot(GenericBarsDepot):

    folder_name = "daily.sectors"

    def _load(self, index_by: str | list[str] = "name", cache=True) -> pd.DataFrame:
        df = self._generic_load_bars(index_by, cache_key=self.folder_name, cache=cache)
        assert isinstance(df, pd.DataFrame)

        # Optimize memory consumption
        if df["code"].dtype.name != "category":
            df["code"] = df["code"].astype("category")

        for col, dtype in df.dtypes.items():
            if str(dtype) == "float64":
                df[col] = df[col].astype("float32")
            elif str(dtype) == "int64":
                df[col] = df[col].astype("int32")
        return df


class AdjustFactorDepot:

    file_name = "adjust_factors.csv"

    @staticmethod
    def file_path() -> Path:
        return tradepy.config.dataset_dir / AdjustFactorDepot.file_name

    @staticmethod
    def load() -> pd.DataFrame:
        path = AdjustFactorDepot.file_path()
        df = pd.read_csv(path, dtype={
            "code": str,
            "date": str,
            "hfq_factor": float
        }, index_col="code")
        return df


class ListingDepot:

    file_name = "listing.csv"

    @staticmethod
    def file_path() -> Path:
        return tradepy.config.dataset_dir / ListingDepot.file_name

    @staticmethod
    def load() -> pd.DataFrame:
        path = ListingDepot.file_path()
        return pd.read_csv(path, index_col="code", dtype={"code": str})
