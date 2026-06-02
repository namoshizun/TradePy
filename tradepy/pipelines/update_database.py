from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from tradepy import config
from tradepy.depot import (
    StocksIndustryClassListingDepository,
    StocksListingDepository,
)
from tradepy.pipelines import Pipeline
from tradepy.vendors import (
    TushareClient,
    fetch_stock_industry_classification_history,
)

RETRY_ARGS = {
    "stop": stop_after_attempt(3),
    "wait": wait_exponential(multiplier=1, min=3, max=10),
}


class UpdateDatabasePipeline(Pipeline):
    def __init__(self):
        self.stocks_listing_depot = StocksListingDepository(
            config.common.get_stock_listing_path()
        )
        self.stocks_industry_class_depot = StocksIndustryClassListingDepository(
            config.common.get_stock_industry_class_path()
        )

        self.ts_client = TushareClient(
            config.common.tushare_token.get_secret_value()
        )

    @retry(**RETRY_ARGS)
    def _refresh_stocks_listing(self):
        if not self.stocks_listing_depot.is_outdated():
            logger.info("✅ [股票列表] 已是最新")
            return

        logger.opt(raw=True).info("🔄 [股票列表] 更新中...", end="")
        df = self.ts_client.get_stock_list()
        self.stocks_listing_depot.save(df)

        logger.opt(raw=True).info(" ok\n")

    @retry(**RETRY_ARGS)
    def _refresh_stocks_industry_class(self):
        if not self.stocks_industry_class_depot.is_outdated():
            logger.info("✅ [股票行业分类] 已是最新")
            return

        logger.opt(raw=True).info("🔄 [股票行业分类] 更新中...", end="")
        df = fetch_stock_industry_classification_history()
        self.stocks_industry_class_depot.save(df)

        logger.opt(raw=True).info(" ok\n")

    def execute(self):
        logger.info("🚀 开始更新本地数据...")
        self._refresh_stocks_listing()
        self._refresh_stocks_industry_class()
