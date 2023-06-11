from functools import cache
from pathlib import Path
import pandas as pd

import tradepy
from tradepy.core.adjust_factors import AdjustFactors


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
