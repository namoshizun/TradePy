import numpy as np
import pandas as pd
from datetime import date, timedelta
from tqdm import tqdm

import tradepy
from tradepy import LOG
from tradepy.utils import get_latest_trade_date
from tradepy.depot.etf import ETFListingDepot
from tradepy.collectors import DataCollector


class ETFDayBarsCollector(DataCollector):
    def __init__(self, since_date: str | date | None = None) -> None:
        if not since_date:
            since_date = get_latest_trade_date()
        elif isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        self.since_date: date = since_date
        self.repo = ETFListingDepot()

    def _jobs_generator(self):
        LOG.info(f"检查本地ETF数据是否需要更新. 起始日期 {self.since_date}")
        repo_iter = self.repo.find(always_load=True)
        curr_codes = list()

        for code, df in repo_iter:
            # Legacy
            if len(parts := code.split(".")) == 2:
                code = parts[0]

            assert isinstance(df, pd.DataFrame)
            curr_codes.append(code)

            try:
                latest_date = "2010-01-01"
                if not df.empty:
                    latest_date = df["timestamp"].max()

                latest_date = date.fromisoformat(latest_date)

                if latest_date < self.since_date:
                    start_date = latest_date + timedelta(days=1)
                    yield {"code": code, "start_date": start_date}
            except Exception as exc:
                LOG.info(
                    f"!!!!!!!!! failed to genereate update job for {code} !!!!!!!!!"
                )
                raise exc

        LOG.info("添加新ETF")
        new_listings = set(tradepy.listing.codes) - set(curr_codes)
        for code in new_listings:
            yield {
                "code": code,
                "start_date": self.since_date.fromisoformat("2010-01-01"),
            }

    def _compute_mkt_cap_percentile_ranks(self, df: pd.DataFrame):