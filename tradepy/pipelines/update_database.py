from datetime import date

import polars as pl
from loguru import logger

from tradepy import config
from tradepy.core.types import (
    DayKlinesDataFrame,
    StockFinancialIndicatorsDataFrame,
    StockPriceAdjustFactorsDataFrame,
    StocksBasicDataFrame,
    StocksListDataFrame,
)
from tradepy.depot import (
    FinancialIndicatorsDepository,
    StockNameChangesDepository,
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

        fetcher = DataFetcher(title="[股票复权因子]")
        for job in fetcher.submit(jobs):
            code = job.args["code"]
            if job.error_message:
                logger.error(f"[股票复权因子] {code} 下载失败!")
                continue

            assert job.result is not None
            depot.save(job.result, key=f"{code}.parquet")  # pyright: ignore[reportArgumentType]

    def _refresh_stock_name_changes(self, depot: StockNameChangesDepository):
        df = self.ts_client.get_name_changes()
        depot.save(df)

    def _refresh_financial_indicators(
        self,
        depot: FinancialIndicatorsDepository,
        listing_df: StocksListDataFrame,
    ):
        jobs: list[DataFetchJob[StockFinancialIndicatorsDataFrame]] = [
            DataFetchJob(
                func=self.ts_client.get_stock_financial_indicator,
                args={"code": row["code"], "since": row["list_date"]},
            )
            for row in listing_df.iter_rows(named=True)
        ]

        fetcher = DataFetcher(title="[股票财务指标]")
        for job in fetcher.submit(jobs):
            code = job.args["code"]
            if job.error_message:
                logger.error(f"[股票财务指标] {code} 下载失败!")
                continue

            assert job.result is not None
            depot.save(job.result, key=f"{code}.parquet")  # pyright: ignore[reportArgumentType]

    def execute(self):
        stocks_listing_depot = StocksListingDepository()
        indu_class_depot = StocksIndustryClassListingDepository()
        day_klines_depot = StocksDayKlinesDepository(self._since, self._until)
        day_basics_depot = StocksDayBasicsDepository(self._since, self._until)
        adjust_factors_depot = StocksAdjustFactorsDepository()
        name_changes_depot = StockNameChangesDepository()
        finind_depot = FinancialIndicatorsDepository()

        logger.info("🚀 开始更新本地数据...")
        if stocks_listing_depot.is_outdated():
            logger.info("🔄 [股票列表] 更新中...")
            self._refresh_stocks_listing(stocks_listing_depot)
            stocks_listing_depot.mark_updated()
            logger.info("ok")

        listing_df = stocks_listing_depot.load()

        if day_klines_depot.is_outdated():
            logger.info("🔄 [股票日线K线] 更新中...")
            self._refresh_day_klines(day_klines_depot, listing_df)
            day_klines_depot.mark_updated()
            logger.info("ok")

        if day_basics_depot.is_outdated():
            logger.info("🔄 [股票日线基本面] 更新中...")
            self._refresh_stocks_basics(day_basics_depot, listing_df)
            day_basics_depot.mark_updated()
            logger.info("ok")

        if adjust_factors_depot.is_outdated():
            logger.info("🔄 [股票复权因子] 更新中...")
            self._refresh_adjust_factors(adjust_factors_depot, listing_df)
            adjust_factors_depot.mark_updated()
            logger.info("ok")

        if indu_class_depot.is_outdated():
            logger.info("🔄 [股票行业分类] 更新中...")
            self._refresh_stocks_industry_class(indu_class_depot)
            indu_class_depot.mark_updated()
            logger.info("ok")

        if name_changes_depot.is_outdated():
            logger.info("🔄 [股票名称变更] 更新中...")
            self._refresh_stock_name_changes(name_changes_depot)
            name_changes_depot.mark_updated()
            logger.info("ok")

        if finind_depot.is_outdated():
            logger.info("🔄 [股票财务指标] 更新中...")
            self._refresh_financial_indicators(finind_depot, listing_df)
            finind_depot.mark_updated()
            logger.info("ok")
