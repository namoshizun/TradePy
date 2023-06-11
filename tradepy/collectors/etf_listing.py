import tradepy
from tradepy import LOG
from tradepy.collectors import DataCollector
from tradepy.depot.etf import ETFListingDepot


class ETFListingCollector(DataCollector):
    def run(self):
        LOG.info("=============== 开始更新ETF列表 ===============")
        listing_df = tradepy.ak_api.get_etf_listing()
        listing_df.to_csv(out_path := ETFListingDepot.file_path(), index=False)
        LOG.info(f"已下载至 {out_path}")
