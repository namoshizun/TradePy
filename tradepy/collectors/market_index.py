import tradepy
import pandas as pd
from tradepy import LOG
from tradepy.collectors import DataCollector
from tradepy.warehouse import BroadBasedIndexBarsDepot, SectorIndexBarsDepot
from tradepy.conversion import broad_index_code_name_mapping


class EastMoneySectorIndexCollector(DataCollector):
    def _jobs_generator(self, listing_df):
        for name in listing_df["name"]:
            yield {
                "name": name,
            }

    def run(self, start_date="2000-01-01", batch_size: int = 20):
        LOG.info("=============== 开始更新行业指数 ===============")
        LOG.info("下载东财行业列表")
        listing_df = tradepy.ak_api.get_sectors_listing()

        LOG.info("下载行业指数日K数据")
        results_gen = self.run_batch_jobs(
            list(self._jobs_generator(listing_df)),
            batch_size,
            fun=tradepy.ak_api.get_sector_index_day_bars,
            iteration_pause=3,
        )

        repo = SectorIndexBarsDepot()
        for args, bars_df in results_gen:
            bars_df = bars_df.query("timestamp >= @start_date").copy()
            name = args["name"]  # noqa
            code = listing_df.query("name == @name").iloc[0]["code"]
            bars_df["code"] = code

            repo.save(self.precompute_indicators(bars_df.copy()), f"{code}.csv")


class BroadBasedIndexCollector(DataCollector):
    def run(self, start_date: str = "2000-01-01"):
        LOG.info("=============== 开始更新宽基指数 ===============")
        repo = BroadBasedIndexBarsDepot()
        index_names = list(broad_index_code_name_mapping.values())

        curr_quote_df = tradepy.ak_api.get_broad_based_index_current_quote(*index_names)
        latest_ts = curr_quote_df["timestamp"].values[0]

        for code, name in broad_index_code_name_mapping.items():
            LOG.info(f"下载 {name}")
            df = tradepy.ak_api.get_broad_based_index_day_bars(code, start_date)

            if df["timestamp"].max() < latest_ts:
                # The day bars does not include the current day if the market is still open.
                # So we need to patch the current quotation to the day bars data.
                latest_quote = curr_quote_df.query("code == @code").copy()
                df = pd.concat([df, latest_quote[df.columns]])

            repo.save(self.precompute_indicators(df.copy()), f"{name}.csv")
