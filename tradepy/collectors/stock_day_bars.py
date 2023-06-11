import numpy as np
import pandas as pd
from tqdm import tqdm

import tradepy
from tradepy import LOG
from tradepy.depot.stocks import StocksDailyBarsDepot, StockListingDepot
from tradepy.collectors.base import DayBarsCollector


class StockDayBarsCollector(DayBarsCollector):
    bars_depot_class = StocksDailyBarsDepot
    listing_depot_class = StockListingDepot

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
        jobs = list(self.jobs_generator())

        results_gen = self.run_batch_jobs(
            jobs,
            batch_size,
            iteration_pause=iteration_pause,
            fun=tradepy.ak_api.get_stock_daily,
        )
        for args, bars_df in results_gen:
            if bars_df.empty:
                LOG.info(f"找不到{args['code']}日K数据. Args = {args}")
            else:
                code = args["code"]
                self.repo.append(bars_df, f"{code}.csv")

        LOG.info("计算个股的每日市值分位")
        df = self.repo.load(index_by="timestamp", fields="all")
        df = pd.concat(self._compute_mkt_cap_percentile_ranks(df))
        df.reset_index(inplace=True, drop=True)

        LOG.info("保存中")
        for code, sub_df in df.groupby("code"):
            sub_df.drop("code", axis=1, inplace=True)
            assert isinstance(code, str)
            self.repo.save(sub_df, filename=code + ".csv")
