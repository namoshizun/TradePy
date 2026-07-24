from tradepy import config
from tradepy.core.types import FinancialIndicatorsModel
from tradepy.depot import DataDepository


class FinancialIndicatorsDepository(DataDepository[FinancialIndicatorsModel]):
    _default_path = config.common.get_stock_financial_indicators_path()
    _update_period = "weekly"
