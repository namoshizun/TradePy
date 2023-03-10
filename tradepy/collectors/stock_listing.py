import pandas as pd

import tradepy
from tradepy import LOG
from tradepy.collectors import DataCollector
from tradepy.warehouse import ListingDepot


class StocksListingCollector(DataCollector):

    def _jobs_generator(self, listing_df):
        for code in listing_df.index:
            yield {
                "code": code
            }

    def run(self, batch_size: int = 50):
        LOG.info('=============== 开始更新A股上市公司列表 ===============')
        listing_df = tradepy.ak_api.get_a_stocks_list()

        LOG.info("获取个股的东财行业分类信息")
        # NOTE: We adopt EM's sector tags so that it is easier to look up stock's related sector index data
        results_gen = self.run_batch_jobs(
            list(self._jobs_generator(listing_df)),
            batch_size,
            fun=tradepy.ak_api.get_stock_info,
            iteration_pause=3,
        )

        em_listing_df = pd.DataFrame([row for _, row in results_gen]).set_index("code")

        listing_df = listing_df.join(
            em_listing_df[["sector", "listdate", "total_share", "float_share"]],
            on="code",
        )

        listing_df.to_csv(out_path := ListingDepot.file_path())
        LOG.info(f'已下载至 {out_path}')
