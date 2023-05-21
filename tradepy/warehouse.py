import abc
import pandas as pd
from pathlib import Path
from contextlib import suppress
from tqdm import tqdm
from typing import Any, Generator
from functools import cache, partial

import tradepy
from tradepy.conversion import convert_code_to_market
from tradepy.core.adjust_factors import AdjustFactors
from tradepy.utils import get_latest_trade_date


class GenericBarsDepot:
    folder_name: str
    caches: dict[str, Any] = dict()

    def __init__(self) -> None:
        assert isinstance(self.folder_name, str)
        self.folder = tradepy.config.database_dir / self.folder_name
        self.folder.mkdir(parents=True, exist_ok=True)

    @classmethod
    def clear_cache(cls):
        cls.caches.clear()

    def size(self) -> int:
        return sum(1 for _ in self.folder.iterdir())

    def save(self, df: pd.DataFrame, filename: str):
        assert filename.endswith("csv")
        df.to_csv(self.folder / filename, index=False)

    def append(self, df: pd.DataFrame, filename: str):
        assert filename.endswith("csv")
        path = self.folder / filename

        if not path.exists():
            df.to_csv(path, index=False)

        _df = pd.read_csv(path, index_col=None)

        (pd.concat([df, _df]).drop_duplicates().to_csv(path, index=False))

    def exists(self, name: str):
        return (self.folder / f"{name}.csv").exists()

    def find(self, codes: list[str] | None = None, always_load=False):
        def get_iterator():
            if not codes:
                if (total := sum(1 for _ in self.folder.iterdir())) > 1000:
                    miniters = total // 20  # to console per 5%
                else:
                    miniters = 0  # auto
                return tqdm(self.folder.iterdir(), miniters=miniters)

            return (self.folder / f"{code}.csv" for code in codes)

        load = partial(pd.read_csv)
        for path in get_iterator():
            if str(path).endswith(".csv"):
                if always_load:
                    yield path.stem, load(path)
                else:
                    should_load = yield path.stem
                    if should_load:
                        yield load(path)

    def _generic_load_bars(
        self, index_by: str | list[str] = "code", cache_key=None, cache=False
    ) -> pd.DataFrame:
        if cache_key in self.caches:
            assert cache_key
            if cache:
                return self.caches[cache_key]
            del self.caches[cache_key]

        def loader() -> Generator[pd.DataFrame, None, None]:
            for code, df in self.find(always_load=True):
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
    folder_name = "daily-stocks"
    default_loaded_fields = "timestamp,code,company,market,open,high,low,close,turnover,vol,chg,pct_chg,mkt_cap,mkt_cap_rank"

    def _load(
        self,
        codes: list[str] | None = None,
        index_by: str | list[str] = "code",
        since_date: str | None = None,
        until_date: str | None = None,
        fields: str = default_loaded_fields,
    ) -> pd.DataFrame:
        def loader() -> Generator[pd.DataFrame, None, None]:
            source_iter = self.find(codes)

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

                if (
                    not set(["company", "market"]).issubset(df.columns)
                    or df[["company", "market"]].isna().any(axis=1).any()
                ):
                    company = tradepy.listing.get_by_code(code).name
                    market = convert_code_to_market(code)
                    df[["company", "market"]] = company, market
                if since_date or until_date:
                    _since_date = since_date or "2000-01-01"  # noqa
                    _until_date = until_date or "3000-01-01"  # noqa
                    yield df.query("@_until_date >= timestamp >= @_since_date")
                else:
                    yield df

        df = pd.concat(loader())

        cat_columns = ["company", "market", "code", "timestamp"]
        for col in cat_columns:
            df[col] = df[col].astype("category")

        df.set_index(index_by, inplace=True, drop=False)

        if "timestamp" not in df.index.names:
            df.sort_values("timestamp", inplace=True)
        else:
            df.sort_index(inplace=True, level="timestamp")

        if fields != "all":
            _fields = fields.split(",")
            return df[_fields]
        return df


class StockMinuteBarsDepot(GenericBarsDepot):
    folder_name = "daily-stocks-minutes"

    @staticmethod
    def file_path() -> Path:
        return tradepy.config.database_dir / AdjustFactorDepot.file_name

    def _load(
        self,
        index_by: str | list[str] = "timestamp",
        date: str | None = None,
    ) -> pd.DataFrame:
        date = date or str(get_latest_trade_date())
        if not self.exists(date):
            raise FileNotFoundError(f"Minute bars data not found for date: {date}")

        path = tradepy.config.database_dir / self.folder_name / f"{date}.csv"
        df = pd.read_csv(path, index_col=index_by, dtype={"code": str})
        df.sort_index(inplace=True)
        return df


class BroadBasedIndexBarsDepot(GenericBarsDepot):
    folder_name = "daily-broad-based"

    def _load(self, index_by: str | list[str] = "code", cache=True) -> pd.DataFrame:
        return self._generic_load_bars(
            index_by, cache_key=self.folder_name, cache=cache
        )


class SectorIndexBarsDepot(GenericBarsDepot):
    folder_name = "daily-sectors"

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
        return tradepy.config.database_dir / AdjustFactorDepot.file_name

    @staticmethod
    @cache
    def load() -> AdjustFactors:
        path = AdjustFactorDepot.file_path()
        df = pd.read_csv(
            path,
            dtype={"code": str, "date": str, "hfq_factor": float},
            index_col="code",
        )
        df.sort_values(["code", "timestamp"], inplace=True)
        return AdjustFactors(df)


class ListingDepot:
    file_name = "listing.csv"

    @staticmethod
    def file_path() -> Path:
        return tradepy.config.database_dir / ListingDepot.file_name

    @staticmethod
    def load() -> pd.DataFrame:
        path = ListingDepot.file_path()
        return pd.read_csv(path, index_col="code", dtype={"code": str})


class RestrictedSharesReleaseDepot:
    file_name = "restricted_shares_release.csv"

    @staticmethod
    def file_path() -> Path:
        return tradepy.config.database_dir / RestrictedSharesReleaseDepot.file_name

    @staticmethod
    @cache
    def load() -> pd.DataFrame:
        path = RestrictedSharesReleaseDepot.file_path()
        df = pd.read_csv(path, index_col=["code", "index"], dtype={"code": str})
        df.sort_values(["code", "index"], inplace=True)
        return df
