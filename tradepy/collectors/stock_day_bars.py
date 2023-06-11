import numpy as np
import pandas as pd
from datetime import date, timedelta
from tqdm import tqdm

import tradepy
from tradepy import LOG
from tradepy.utils import get_latest_trade_date
from tradepy.warehouse import StocksDailyBarsDepot
from tradepy.collectors import DataCollector


class StockDayBarsCollector(DataCollector):
    def __init__(self, since_date: str | date | None = None) -> None:
        if not since_date:
            since_date = get_latest_trade_date()
        elif isinstance(since_date, str):
            since_date = date.fromisoformat(since_date)

        self.since_date: date = since_date
        self.repo = StocksDailyBarsDepot()

    def _jobs_generator(self):
        LOG.info(f"检查本地个股数据是否需要更新. 起始日期 {self.since_date}")
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

        LOG.info("添加新个股")
        new_listings = set(tradepy.listing.codes) - set(curr_codes)
        for code in new_listings:
            yield {
                "code": code,
                "start_date": self.since_date.fromisoformat("2010-01-01"),
            }

    def _compute_mkt_cap_percentile_ranks(self, df: pd.DataFrame):
        for _, day_df in tqdm(df.groupby(level="timestamp")):
            if ("mkt_cap_rank" in day_df) and (day_df["mkt_cap_rank"].notnull().all()):
                yield day_df
                continue

            mkt_cap_lst = [row.mkt_cap for row in day_df.itertuples()]

            mkt_cap_percentiles = np.percentile(mkt_cap_lst, q=range(100))
            day_df["mkt_cap_rank"] = [
                (mkt_cap_percentiles < v).sum() / len(mkt_cap_percentiles)
                for v in mkt_cap_lst
            ]
            yield day_df

    def run(self, batch_size=50, iteration_pause=5):
        LOG.info("=============== 开始更新个股日K数据 ===============")
        jobs = list(self._jobs_generator())

        results_gen = self.run_batch_jobs(
            jobs,
            batch_size,
            iteration_pause=iteration_pause,
            fun=tradepy.ak_api.get_stock_daily,
        )
        for args, ticks_df in results_gen:
            if ticks_df.empty:
                LOG.info(f"找不到{args['code']}日K数据. Args = {args}")
            else:
                code = args["code"]
                self.repo.append(ticks_df, f"{code}.csv")

        LOG.info("计算个股的每日市值分位")
        df = self.repo.load(index_by="timestamp", fields="all")
        df = pd.concat(self._compute_mkt_cap_percentile_ranks(df))
        df.reset_index(inplace=True, drop=True)

        LOG.info("保存中")
        for code, sub_df in df.groupby("code"):
            sub_df.drop("code", axis=1, inplace=True)
            assert isinstance(code, str)
            self.repo.save(sub_df, filename=code + ".csv")
