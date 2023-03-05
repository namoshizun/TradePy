import pandas as pd
import numpy as np
from tqdm import tqdm

import trade
from trade.convertion import TickFields
from trade.warehouse import TicksDepot
from trade.collectors import DataCollector


class MarketCapitalsCollector(DataCollector):

    @staticmethod
    def compute_mkt_cap_percentile_ranks(df):
        for _, day_df in tqdm(df.groupby(level="timestamp")):
            mkt_cap_lst = []

            for _, row in day_df.iterrows():
                code = row["code"]
                mkt_cap = trade.listing.get_by_code(code).get_market_cap_at(row["close"])
                mkt_cap_lst.append(round(mkt_cap, 2))

            mkt_cap_percentiles = np.percentile(mkt_cap_lst, q=range(100))

            day_df["mkt_cap"] = mkt_cap_lst
            day_df["mkt_cap_rank"] = [
                (mkt_cap_percentiles < v).sum() / len(mkt_cap_percentiles)
                for v in mkt_cap_lst
            ]
            yield day_df

    def run(self):
        repo = TicksDepot("daily.stocks")
        df = repo.load_stocks_ticks(index_by=["timestamp"])
        df = pd.concat(self.compute_mkt_cap_percentile_ranks(df))
        df.reset_index(inplace=True, drop=False)

        for code, sub_df in df.groupby("code"):
            columns = TickFields + ["mkt_cap", "mkt_cap_rank"]
            repo.save(sub_df[columns], filename=code + ".csv")
