import pandas as pd
from typing import Generator

import tradepy
from tradepy.depot.base import GenericBarsDepot, GenericListingDepot
from tradepy.types import MarketType


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
        markets: tuple[MarketType, ...] | None = None,
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

                if markets:
                    df = df.query("market in @markets").copy()

                if since_date or until_date:
                    _since_date = since_date or "2000-01-01"  # noqa
                    _until_date = until_date or "3000-01-01"  # noqa
                    yield df.query("@_until_date >= timestamp >= @_since_date")
                else:
                    yield df

        df = pd.concat(loader())

        # Convert "中小板" to "深证主板" for legacy reason
        df["market"].replace("中小板", "深证主板", inplace=True)

        # Optimize the memory storage
        cat_columns = ["company", "market"]
        for col in cat_columns:
            df[col] = df[col].astype("category")

        df.set_index(index_by, inplace=True, drop=False)

        # Sort by time order
        if "timestamp" not in df.index.names:
            df.sort_values("timestamp", inplace=True)
        else:
            df.sort_index(level="timestamp", inplace=True)

        if fields != "all":
            _fields = fields.split(",")
            return df[_fields]
        return df


class StockMinuteBarsDepot(GenericBarsDepot):
    folder_name = "daily-stocks-minutes"

    def _load(self, month: str) -> pd.DataFrame:
        file = self.folder / f"{month}.pkl"
        df = pd.read_pickle(file)
        if df.index.names != ["date", "code"]:
            df.set_index(["date", "code"], inplace=True)
            df.sort_index(inplace=True)
        return df


class StockListingDepot(GenericListingDepot):
    file_name = "listing.csv"
