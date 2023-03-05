import numpy as np
import pandas as pd
from tqdm import tqdm

import tradepy
from tradepy.collectors import DataCollector
from tradepy.warehouse import AdjustFactorDepot


class AdjustFactorCollector(DataCollector):

    def __init__(self, batch_size: int = 50):
        self.batch_size = batch_size

    def _jobs_generator(self):
        for code in tradepy.listing.codes:
            yield {
                "code": code
            }

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

    def run(self):
        print()
        jobs = list(self._jobs_generator())

        print('>>> Downloading')
        results_gen = self.run_batch_jobs(
            jobs,
            self.batch_size,
            fun=tradepy.ak_api.get_adjust_factor,
            iteration_pause=3,
        )

        df = pd.concat(
            adjust_factors_df
            for _, adjust_factors_df in results_gen
        )
        df = df.round(7)
        df.rename(columns={"date": "timestamp"}, inplace=True)

        print('>>> Add time period paddings')
        df = self._add_period_padding(df)
        df.set_index("code", inplace=True)

        print('>>> Outputting')
        df.sort_values(["code", "timestamp"], inplace=True)
        df.to_csv(AdjustFactorDepot.path)
        return df
