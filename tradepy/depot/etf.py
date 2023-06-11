import pandas as pd
from tradepy.depot.base import GenericListingDepot, GenericBarsDepot


class ETFListingDepot(GenericListingDepot):
    file_name = "etf-listing.csv"


class ETFDailyBarsDepot(GenericBarsDepot):
    folder_name = "daily-etfs"

    def _load(self, index_by: str | list[str] = "code", cache=False) -> pd.DataFrame:
        return self._generic_load_bars(index_by)
