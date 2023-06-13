import pandas as pd
from typing import Generator
from pathlib import Path

import tradepy
from tradepy.conversion import convert_code_to_market
from tradepy.depot.base import GenericBarsDepot, GenericListingDepot
from tradepy.depot.misc import AdjustFactorDepot
from tradepy.utils import get_latest_trade_date


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

        cat_columns = ["company", "market"]
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
        return tradepy.config.common.database_dir / AdjustFactorDepot.file_name

    def _load(
        self,
        index_by: str | list[str] = "timestamp",
        date: str | None = None,
    ) -> pd.DataFrame:
        date = date or str(get_latest_trade_date())
        if not self.exists(date):
            raise FileNotFoundError(f"Minute bars data not found for date: {date}")

        path = tradepy.config.common.database_dir / self.folder_name / f"{date}.csv"
        df = pd.read_csv(path, index_col=index_by, dtype={"code": str})
        df.sort_index(inplace=True)
        return df


class StockListingDepot(GenericListingDepot):
    file_name = "listing.csv"
