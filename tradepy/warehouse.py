import pandas as pd
from contextlib import suppress
from tqdm import tqdm
from functools import partial
from typing import Any, Generator
from pathlib import Path

import tradepy
from tradepy.convertion import convert_code_to_exchange, convert_code_to_market


class TicksDepot:

    caches: dict[str, Any] = dict()

    def __init__(self, folder: str) -> None:
        self.folder = Path('datasets') / folder
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

    def load_stocks_ticks(self,
                          index_by: str | list[str] = "code",
                          since_date: str | None = None) -> pd.DataFrame:
        assert self.folder.name == "daily.stocks", self.folder

        def loader() -> Generator[pd.DataFrame, None, None]:
            source_iter = self.traverse()

            while True:
                try:
                    code: str = next(source_iter)
                except StopIteration:
                    break

                if not tradepy.listing.has_code(code):
                    continue

                company = tradepy.listing.get_by_code(code).name
                market = convert_code_to_market(code)

                df = source_iter.send(True)
                exchange = convert_code_to_exchange(code)
                df[["company", "code", "market", "exchange"]] = company, code, market, exchange

                if since_date:
                    yield df.query(f'timestamp >= "{since_date}"')
                else:
                    yield df

        df = pd.concat(loader())

        cat_columns = ["company", "market", "exchange"]
        for col in cat_columns:
            df[col] = df[col].astype("category")

        df.set_index(index_by, inplace=True)
        df.sort_index(inplace=True)
        return df

    def __generic_load_ticks(self,
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

    def load_index_ticks(self, index_by: str | list[str] = "code", cache=False) -> pd.DataFrame:
        assert self.folder.name == "daily.index", self.folder
        return self.__generic_load_ticks(index_by, cache_key="index-ticks", cache=cache)

    def load_industry_ticks(self, index_by: str | list[str] = "name", cache=False) -> pd.DataFrame:
        assert self.folder.name == "daily.industry", self.folder
        df = self.__generic_load_ticks(index_by, cache_key="industry-ticks", cache=cache)

        # Optimize memory consumption
        df["code"] = df["code"].astype("category")

        for col, dtype in df.dtypes.items():
            if str(dtype) == "float64":
                df[col] = df[col].astype("float32")
            elif str(dtype) == "int64":
                df[col] = df[col].astype("int32")
        return df


class TradeCalendarDepot:

    path = "./datasets/trade_cal.csv"

    @staticmethod
    def load(since_date: str = "1900-01-01", end_date: str = "3000-01-01") -> pd.DataFrame:
        df = pd.read_csv(TradeCalendarDepot.path)
        df = df[df["cal_date"] >= since_date]
        df = df[df["cal_date"] <= end_date]
        return df.set_index("cal_date").sort_index()


class AdjustFactorDepot:

    path = "./datasets/adjust_factors.csv"

    @staticmethod
    def load() -> pd.DataFrame:
        df = pd.read_csv(AdjustFactorDepot.path, dtype={
            "code": str,
            "date": str,
            "hfq_factor": float
        })
        df.set_index("code", inplace=True)
        return df


class ListingDepot:

    path = "./datasets/listing.csv"

    @staticmethod
    def load() -> pd.DataFrame:
        return pd.read_csv(ListingDepot.path).set_index("code")
