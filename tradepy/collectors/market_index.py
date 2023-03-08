import tradepy
from tradepy.collectors import DataCollector
from tradepy.warehouse import BroadBasedIndexTicksDepot, SectorIndexTicksDepot


# TODO: avoid duplication


class EastMoneySectorIndexCollector(DataCollector):

    def _jobs_generator(self, listing_df):
        for name in listing_df["name"]:
            yield {
                "name": name,
            }

    def run(self, since_date="1990-01-01", batch_size: int = 20):
        print("下载东财行业列表")
        listing_df = tradepy.ak_api.get_sectors_listing()

        print("下载行业指数日K数据")
        results_gen = self.run_batch_jobs(
            list(self._jobs_generator(listing_df)),
            batch_size,
            fun=tradepy.ak_api.get_sector_index_ticks,
            iteration_pause=3,
        )

        print("保存中")
        repo = SectorIndexTicksDepot()
        for args, ticks_df in results_gen:
            ticks_df = ticks_df.query('timestamp >= @since_date').copy()
            name = args["name"]  # noqa
            code = listing_df.query('name == @name').iloc[0]["code"]
            ticks_df["code"] = code

            repo.append(
                self.precompute_indicators(ticks_df.copy()),
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

    def run(self):
        repo = BroadBasedIndexTicksDepot()
        for code, name in self.code_to_index_name.items():
            df = tradepy.ak_api.get_broad_based_index_ticks(code)
            repo.append(
                self.precompute_indicators(df.copy()),
                f'{name}.csv'
            )
