import tradepy
from tradepy import LOG
from tradepy.depot.futures import StockFuturesDailyBarsDepot
from tradepy.collectors.base import DayBarsCollector


class StockFuturesDayBarsCollector(DayBarsCollector):
    bars_depot_class = StockFuturesDailyBarsDepot

    def run(self):
        LOG.info("=============== 开始更新股指期货日K数据 ===============")

        for code in ["IF", "IH", "IC"]:
            df = tradepy.ak_api.get_stock_futures_daily(code)
            df["code"] = code
            out_path = self.repo.save(df.copy(), f"{code}.csv")
            LOG.info(f"已下载至 {out_path}")
