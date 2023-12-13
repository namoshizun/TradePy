from functools import cache
from pathlib import Path
import time
import pandas as pd

import tradepy
from tradepy.core.adjust_factors import AdjustFactors


class AdjustFactorDepot:
    file_name = "adjust_factors.csv"

    @staticmethod
    def file_path() -> Path:
        return tradepy.config.common.database_dir / AdjustFactorDepot.file_name

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
        return (
            tradepy.config.common.database_dir / RestrictedSharesReleaseDepot.file_name
        )

    @staticmethod
    @cache
    def load() -> pd.DataFrame:
        path = RestrictedSharesReleaseDepot.file_path()
        df = pd.read_csv(path, index_col=["code", "index"], dtype={"code": str})
        df.sort_values(["code", "index"], inplace=True)
        return df


class CompanyNameChangesDepot:
    file_name = "company_name_changes.pkl"
    cache_path = Path.home() / ".tradepy" / "cache" / file_name

    @staticmethod
    def load_cached() -> pd.DataFrame | None:
        file = CompanyNameChangesDepot.cache_path
        file.parent.mkdir(parents=True, exist_ok=True)
        if not file.exists():
            return

        max_age = 7  # days
        create_time = file.stat().st_ctime
        expired = (time.time() - create_time) / 86400 > max_age
        if expired:
            return

        return pd.read_pickle(file)

    @staticmethod
    @cache
    def load() -> pd.DataFrame:
        _ = CompanyNameChangesDepot
        cached_df = _.load_cached()
        if cached_df is not None:
            return cached_df

        if tradepy.is_ci():
            url = f"https://raw.githubusercontent.com/namoshizun/TradePy/main/dataset/{_.file_name}"
        else:
            url = f"https://gitee.com/dilu3100/TradePy/raw/main/dataset/{_.file_name}"

        df = pd.read_pickle(url)
        df.to_pickle(_.cache_path)
        return df
