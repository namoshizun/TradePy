import pandas as pd
import tradepy
from tradepy import LOG
from tradepy.collectors.base import DataCollector
from tradepy.depot.etf import ETFListingDepot


class ETFListingCollector(DataCollector):
    def run(self, write_file: bool = True) -> pd.DataFrame:
        LOG.info("=============== 开始更新ETF列表 ===============")
        listing_df = tradepy.ak_api.get_etf_listing()

        if write_file:
            listing_df.to_csv(out_path := ETFListingDepot.file_path(), index=False)
            LOG.info(f"已下载至 {out_path}")

        return listing_df
