import tradepy
from tradepy import LOG
from tradepy.collectors import DataCollector
from tradepy.warehouse import BroadBasedIndexBarsDepot, SectorIndexBarsDepot


class EastMoneySectorIndexCollector(DataCollector):

    def _jobs_generator(self, listing_df):
        for name in listing_df["name"]:
            yield {
                "name": name,
            }

    def run(self, since_date="2000-01-01", batch_size: int = 20):
        LOG.info('=============== 开始更新行业指数 ===============')
        LOG.info("下载东财行业列表")
        listing_df = tradepy.ak_api.get_sectors_listing()

        LOG.info("下载行业指数日K数据")
        results_gen = self.run_batch_jobs(
            list(self._jobs_generator(listing_df)),
            batch_size,
            fun=tradepy.ak_api.get_sector_index_ticks,
            iteration_pause=3,
        )

        repo = SectorIndexBarsDepot()
        for args, bars_df in results_gen:
            bars_df = bars_df.query('timestamp >= @since_date').copy()
            name = args["name"]  # noqa
            code = listing_df.query('name == @name').iloc[0]["code"]
            bars_df["code"] = code

            repo.save(
                self.precompute_indicators(bars_df.copy()),
                f'{code}.csv'
            )


class BroadBasedIndexCollector(DataCollector):

    code_to_index_name = {
        "sh000001": "SSE",
        "sz399001": "SZSE",
        "sz399006": "ChiNext",
        "sh000688": "STAR",
        "sh000300": "CSI-300",
        "sh000905": "CSI-500",
        "sh000852": "CSI-1000",
        "sh000016": "SSE-50",
    }

    def run(self, start_date: str = "2000-01-01"):
        LOG.info('=============== 开始更新宽基指数 ===============')
        repo = BroadBasedIndexBarsDepot()
        for code, name in self.code_to_index_name.items():
            LOG.info(f'下载 {name}')
            df = tradepy.ak_api.get_broad_based_index_ticks(code, start_date)
            repo.save(
                self.precompute_indicators(df.copy()),
                f'{name}.csv'
            )
