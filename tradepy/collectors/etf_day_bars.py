import tradepy
from tradepy import LOG
from tradepy.depot.etf import ETFListingDepot, ETFDailyBarsDepot
from tradepy.collectors.base import DayBarsCollector


class ETFDayBarsCollector(DayBarsCollector):
    bars_depot_class = ETFDailyBarsDepot
    listing_depot_class = ETFListingDepot

    def run(self, batch_size=50, iteration_pause=5, min_mkt_cap=0):
        LOG.info("=============== 开始更新ETF日K数据 ===============")
        listing_df = self.listing_depot_class.load()

        jobs = list(self.jobs_generator())
        if min_mkt_cap > 0:
            jobs = [
                job
                for job in jobs
                if listing_df.loc[job["code"]]["mkt_cap"] >= min_mkt_cap
            ]

        results_gen = self.run_batch_jobs(
            jobs,
            batch_size,
            iteration_pause=iteration_pause,
            fun=tradepy.ak_api.get_etf_daily,
        )
        for args, bars_df in results_gen:
            if bars_df.empty:
                LOG.info(f"找不到{args['code']}日K数据. Args = {args}")
            else:
                code = args["code"]
                bars_df["name"] = listing_df.loc[code]["name"]
                self.repo.append(bars_df, f"{code}.csv")
