import pandas as pd
from tradepy.depot.base import GenericBarsDepot


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
