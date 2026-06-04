from datetime import date

import polars as pl
from loguru import logger

from tradepy import config
from tradepy.core.types import (
    DayKlinesDataFrame,
    StockPriceAdjustFactorsDataFrame,
    StocksBasicDataFrame,
    StocksListDataFrame,
)
from tradepy.depot import (
    StocksAdjustFactorsDepository,
    StocksDayBasicsDepository,
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

        trade_cal = self.ts_client.get_trade_calendar()
        self.trade_dates = trade_cal.dates_between(self._since, self._until)

    def _refresh_stocks_listing(self, depot: StocksListingDepository):
        df = self.ts_client.get_stock_list()
        depot.save(df)

    def _refresh_stocks_industry_class(
        self, depot: StocksIndustryClassListingDepository
    ):
        df = fetch_stock_industry_classification_history()
        depot.save(df)

        logger.opt(raw=True).info(" ok\n")

    def _refresh_day_klines(
        self, depot: StocksDayKlinesDepository, listing_df: StocksListDataFrame
    ):
        trade_dates = [
            dt for dt in self.trade_dates if not depot.exists(f"{dt}.parquet")
        ]

        jobs: list[DataFetchJob[DayKlinesDataFrame]] = [
            DataFetchJob(
                func=self.ts_client.get_stock_day_klines,
                args={"trade_date": date.fromisoformat(trade_date)},
            )
            for trade_date in sorted(trade_dates)
        ]

        logger.info(f"🔄 [股票日线K线] 待更新交易日: {len(jobs)} 天")
        fetcher = DataFetcher(title="[股票日线K线]")
        for job in fetcher.submit(jobs):
            trade_date = job.args["trade_date"]
            if job.error_message:
                logger.error("[股票日线K线] {} 下载失败!", trade_date)
                continue

            assert job.result is not None
            df = job.result.join(
                listing_df.filter(pl.col("list_date") <= trade_date).select(
                    "code"
                ),
                on=["code"],
            )
            depot.save(df, key=f"{trade_date}.parquet")  # pyright: ignore[reportArgumentType]

    def _refresh_stocks_basics(
        self, depot: StocksDayBasicsDepository, listing_df: StocksListDataFrame
    ):
        trade_dates = [
            dt for dt in self.trade_dates if not depot.exists(f"{dt}.parquet")
        ]

        jobs: list[DataFetchJob[StocksBasicDataFrame]] = [
            DataFetchJob(
                func=self.ts_client.get_stock_basic,
                args={"trade_date": date.fromisoformat(trade_date)},
            )
            for trade_date in sorted(trade_dates)
        ]

        logger.info(f"🔄 [股票日线基本面] 待更新交易日: {len(jobs)} 天")
        fetcher = DataFetcher(title="[股票日线基本面]")
        for job in fetcher.submit(jobs):
            trade_date = job.args["trade_date"]
            if job.error_message:
                logger.error("[股票日线数据] {} 下载失败!", trade_date)
                continue

            assert job.result is not None
            df = job.result.join(
                listing_df.filter(pl.col("list_date") <= trade_date).select(
                    "code"
                ),
                on=["code"],
            )
            depot.save(df, key=f"{trade_date}.parquet")  # pyright: ignore[reportArgumentType]

    def _refresh_adjust_factors(
        self,
        depot: StocksAdjustFactorsDepository,
        listing_df: StocksListDataFrame,
    ):
        jobs: list[DataFetchJob[StockPriceAdjustFactorsDataFrame]] = [
            DataFetchJob(
                func=self.ts_client.get_stock_price_adjust_factors,
                args={"code": code},
            )
            for code in listing_df["code"]
        ]
        logger.info(f"🔄 [股票复权因子] 待更新股票: {len(jobs)} 只")

        fetcher = DataFetcher(title="[股票复权因子]")
        for job in fetcher.submit(jobs):
            code = job.args["code"]
            if job.error_message:
                logger.error("[股票复权因子] {} 下载失败!", code)
                continue

            assert job.result is not None
            depot.save(job.result, key=f"{code}.parquet")  # pyright: ignore[reportArgumentType]

        depot.mark_updated()

    def execute(self):
        stocks_listing_depot = StocksListingDepository(
            config.common.get_stock_listing_path()
        )
        indu_class_depot = StocksIndustryClassListingDepository(
            config.common.get_stock_industry_class_path()
        )
        day_klines_depot = StocksDayKlinesDepository(
            config.common.get_stock_day_klines_path(), self._since, self._until
        )
        day_basics_depot = StocksDayBasicsDepository(
            config.common.get_stock_day_basics_path(), self._since, self._until
        )

        adjust_factors_depot = StocksAdjustFactorsDepository(
            config.common.get_adjust_factors_path()
        )

        logger.info("🚀 开始更新本地数据...")
        if stocks_listing_depot.is_outdated():
            logger.info("🔄 [股票列表] 更新中...")
            self._refresh_stocks_listing(stocks_listing_depot)
            logger.info("ok")

        listing_df = stocks_listing_depot.load()

        if day_klines_depot.is_outdated():
            logger.info("🔄 [股票日线K线] 更新中...")
            self._refresh_day_klines(day_klines_depot, listing_df)
            logger.info("ok")

        if day_basics_depot.is_outdated():
            logger.info("🔄 [股票日线基本面] 更新中...")
            self._refresh_stocks_basics(day_basics_depot, listing_df)
            logger.info("ok")

        if adjust_factors_depot.is_outdated():
            logger.info("🔄 [股票复权因子] 更新中...")
            self._refresh_adjust_factors(adjust_factors_depot, listing_df)
            logger.info("ok")

        if indu_class_depot.is_outdated():
            logger.info("🔄 [股票行业分类] 更新中...")
            self._refresh_stocks_industry_class(indu_class_depot)
            logger.info("ok")
