from datetime import date
from pathlib import Path
from typing import TypeVar

from tradepy.core.types import BaseFrameModel, DayKlinesModel, StocksBasicModel
from tradepy.depot import DataDepository
from tradepy.vendors._tushare import TushareClient

T = TypeVar("T", bound=BaseFrameModel)


class GenericDailyDepository(DataDepository[T]):
    def __init__(self, path: Path | str, since: date, until: date):
        super().__init__(path)
        self.since = since
        self.until = until

    def is_outdated(self) -> bool:
        if not self.path.exists():
            return True

        ts_client = TushareClient()
        trade_cal = ts_client.get_trade_calendar()
        trade_dates = trade_cal.dates_between(self.since, self.until)

        return not all(self.exists(f"{dt}.parquet") for dt in trade_dates)


class StocksDayKlinesDepository(GenericDailyDepository[DayKlinesModel]):
    pass


class StocksDayBasicsDepository(GenericDailyDepository[StocksBasicModel]):
    pass
