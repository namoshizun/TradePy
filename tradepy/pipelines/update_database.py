from datetime import date

from loguru import logger

from tradepy import config
from tradepy.core.types import DayKlinesDataFrame
from tradepy.depot import (
    StocksDayKlinesDepository,
    StocksIndustryClassListingDepository,
    StocksListingDepository,
)
from tradepy.pipelines import Pipeline
from tradepy.pipelines.data_fetcher import DataFetcher, DataFetchJob
from tradepy.vendors import (
    TushareClient,
    fetch_stock_industry_classification_history,
)


class UpdateDatabasePipeline(Pipeline):
    def __init__(self, since: date, until: date):
        self._since = since
        self._until = until

        self.ts_client = TushareClient(
            config.common.tushare_token.get_secret_value()
        )

    def _refresh_stocks_listing(self) -> StocksListingDepository:
        depot = StocksListingDepository(config.common.get_stock_listing_path())

        if not depot.is_outdated():
            logger.info("✅ [股票列表] 已是最新")
            return depot

        logger.opt(raw=True).info("🔄 [股票列表] 更新中...", end="")
        df = self.ts_client.get_stock_list()
        depot.save(df)

        logger.opt(raw=True).info(" ok\n")
        return depot

    def _refresh_stocks_industry_class(
        self,
    ) -> StocksIndustryClassListingDepository:
        depot = StocksIndustryClassListingDepository(
            config.common.get_stock_industry_class_path()
        )

        if not depot.is_outdated():
            logger.info("✅ [股票行业分类] 已是最新")
            return depot

        logger.opt(raw=True).info("🔄 [股票行业分类] 更新中...", end="")
        df = fetch_stock_industry_classification_history()
        depot.save(df)

        logger.opt(raw=True).info(" ok\n")
        return depot

    def _refresh_day_klines(self) -> StocksDayKlinesDepository:
        depot = StocksDayKlinesDepository(
            config.common.get_stock_day_klines_path(), self._since, self._until
        )

        if not depot.is_outdated():
            logger.info("✅ [股票日线数据] 已是最新")
            return depot

        trade_cal = self.ts_client.get_trade_calendar()
        trade_dates = trade_cal.dates_between(self._since, self._until)
        trade_dates = [
            dt for dt in trade_dates if not depot.exists(f"{dt}.parquet")
        ]

        if not trade_dates:
            logger.info("✅ [股票日线数据] 已是最新")
            return depot

        jobs: list[DataFetchJob[DayKlinesDataFrame]] = [
            DataFetchJob(
                func=self.ts_client.get_stock_day_klines,
                args={"trade_date": date.fromisoformat(trade_date)},
            )
            for trade_date in sorted(trade_dates)
        ]

        logger.info(f"🔄 [股票日线数据] 更新中 ... {len(jobs)} 条")
        fetcher = DataFetcher(title="[股票日线数据]")
        for job in fetcher.submit(jobs):
            trade_date = job.args["trade_date"]
            if job.error_message:
                logger.error("[股票日线数据] {} 下载失败!", trade_date)
                continue

            assert job.result is not None
            depot.save(job.result, key=f"{trade_date}.parquet")

        return depot

    def _refresh_stocks_basics(self): ...

    def _refresh_adjust_factors(self): ...

    def execute(self):
        logger.info("🚀 开始更新本地数据...")
        self._refresh_stocks_listing()
        self._refresh_day_klines()
        self._refresh_stocks_basics()
        self._refresh_adjust_factors()
        self._refresh_stocks_industry_class()
