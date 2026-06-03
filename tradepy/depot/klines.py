from datetime import date
from pathlib import Path

from tradepy.core.types import DayKlinesModel
from tradepy.depot import DataDepository
from tradepy.vendors._tushare import TushareClient


class StocksDayKlinesDepository(DataDepository[DayKlinesModel]):
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
