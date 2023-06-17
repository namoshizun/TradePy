import pandas as pd
from tradepy.depot.base import GenericBarsDepot


class StockFuturesDailyBarsDepot(GenericBarsDepot):
    folder_name = "daily-stock-futures"

    def _load(self, index_by: str | list[str] = "code", cache=True) -> pd.DataFrame:
        return self._generic_load_bars(index_by, cache)
