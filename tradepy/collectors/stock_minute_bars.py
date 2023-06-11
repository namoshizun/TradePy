import datetime

import pandas as pd
import tradepy
from tradepy.collectors import DataCollector
from tradepy.utils import get_latest_trade_date
from tradepy.depot.stocks import StockMinuteBarsDepot

LOG = tradepy.LOG


class StockMinuteBarsCollector(DataCollector):
    def __init__(self, date: str | datetime.date | None = None) -> None:
        if not date:
            date = get_latest_trade_date()
        elif isinstance(date, str):
            date = datetime.date.fromisoformat(date)

        self.date: datetime.date = date
        self.repo = StockMinuteBarsDepot()

    def _jobs_generator(self):
        stock_codes = tradepy.listing.codes

        for code in stock_codes:
            yield {"code": code, "start_date": self.date}

    def run(self, batch_size=50, iteration_pause=3):
        jobs = list(self._jobs_generator())

        LOG.info(f"=============== 开始下载{self.date}分时数据 ===============")

        results_gen = self.run_batch_jobs(
            jobs,
            batch_size,
            iteration_pause=iteration_pause,
            fun=tradepy.ak_api.get_minute_bar,
        )

        bars_list = []
        for args, bars_df in results_gen:
            bars_df["code"] = args["code"]
            bars_list.append(bars_df)

        LOG.info("保存中")
        df = pd.concat(bars_list)
        self.repo.save(df, filename=f"{self.date}.csv")
