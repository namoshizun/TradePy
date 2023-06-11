import numpy as np
import pandas as pd
from tqdm import tqdm

import tradepy
from tradepy import LOG
from tradepy.collectors import DataCollector
from tradepy.depot.misc import AdjustFactorDepot


class AdjustFactorCollector(DataCollector):
    def _jobs_generator(self):
        for code in tradepy.listing.codes:
            yield {"code": code}

    def _add_period_padding(self, df: pd.DataFrame):
        def pad(sub_df: pd.DataFrame):
            sub_df.sort_values("timestamp", inplace=True)

            if sub_df.query('timestamp == "1900-01-01"').empty:
                pad_start = sub_df.iloc[0].copy()
                pad_start["timestamp"] = "1900-01-01"
                sub_df.loc[len(sub_df)] = pad_start

            if sub_df.query('timestamp == "3000-01-01"').empty:
                pad_end = sub_df.iloc[-1].copy()
                pad_end["timestamp"] = "3000-01-01"
                pad_end["hfq_factor"] = np.nan
                sub_df.loc[len(sub_df)] = pad_end

            sub_df.sort_values("timestamp", inplace=True)
            return sub_df

        return pd.concat(pad(g) for _, g in tqdm(df.groupby("code")))

    def run(self, batch_size: int = 50):
        jobs = list(self._jobs_generator())

        LOG.info("=============== 开始更新个股复权因子 ===============")
        LOG.info("下载中")
        results_gen = self.run_batch_jobs(
            jobs,
            batch_size,
            fun=tradepy.ak_api.get_adjust_factor,
            iteration_pause=2,
        )

        df = pd.concat(adjust_factors_df for _, adjust_factors_df in results_gen)
        df.rename(columns={"date": "timestamp"}, inplace=True)

        LOG.info("添加头尾时间边界1900, 3000")
        df = self._add_period_padding(df)
        df.set_index("code", inplace=True)

        df.sort_values(["code", "timestamp"], inplace=True)
        df.round(4).to_csv(out_path := AdjustFactorDepot.file_path())
        LOG.info(f"已下载至 {out_path}")
        return df
