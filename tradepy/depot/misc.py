from tradepy import config
from tradepy.core.types import (
    StockNameChangesModel,
    StockPriceAdjustFactorsModel,
    StocksListModel,
    SWStockIndustryModel,
)
from tradepy.depot import DataDepository


class StocksListingDepository(DataDepository[StocksListModel]):
    _default_path = config.common.get_stock_listing_path()
    _update_period = "weekly"


class StocksIndustryClassListingDepository(
    DataDepository[SWStockIndustryModel]
):
    _default_path = config.common.get_stock_industry_class_path()
    _update_period = "weekly"


class StockNameChangesDepository(DataDepository[StockNameChangesModel]):
    _default_path = config.common.get_stock_name_changes_path()
    _update_period = "daily"


class StocksAdjustFactorsDepository(
    DataDepository[StockPriceAdjustFactorsModel]
):
    _default_path = config.common.get_adjust_factors_path()
    _update_period = "weekly"
