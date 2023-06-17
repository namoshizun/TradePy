import abc
import pandas as pd
from pathlib import Path
from contextlib import suppress
from tqdm import tqdm
from typing import Any, Generator
from functools import partial

import tradepy


class GenericListingDepot:
    file_name: str

    @classmethod
    def file_path(cls) -> Path:
        return tradepy.config.common.database_dir / cls.file_name

    @classmethod
    def load(cls) -> pd.DataFrame:
        path = cls.file_path()
        return pd.read_csv(path, index_col="code", dtype={"code": str})


class GenericBarsDepot:
    folder_name: str
    caches: dict[str, Any] = dict()

    def __init__(self) -> None:
        assert isinstance(self.folder_name, str)
        self.folder = tradepy.config.common.database_dir / self.folder_name
        self.folder.mkdir(parents=True, exist_ok=True)

    @classmethod
    def clear_cache(cls):
        cls.caches.clear()

    def size(self) -> int:
        return sum(1 for _ in self.folder.iterdir())

    def save(self, df: pd.DataFrame, filename: str) -> Path:
        assert filename.endswith("csv")
        out_path = self.folder / filename
        df.to_csv(out_path, index=False)
        return out_path

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
