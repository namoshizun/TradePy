from datetime import date
from pathlib import Path
from typing import TypeVar

from tradepy import config
from tradepy.core.types import BaseFrameModel, DayKlinesModel, StocksBasicModel
from tradepy.depot import DataDepository
from tradepy.vendors._tushare import TushareClient

T = TypeVar("T", bound=BaseFrameModel)


class GenericDailyDepository(DataDepository[T]):
    def __init__(
        self, since: date, until: date, path: Path | str | None = None
    ):
        super().__init__(path)
        self.since = since
        self.until = until

    def sources(self) -> list[str]:
        since_s = self.since.isoformat()
        until_s = self.until.isoformat()
        return sorted(
            p.absolute().as_posix()
            for p in self.path.glob("*.parquet")
            if since_s <= p.stem <= until_s
        )

    def is_outdated(self) -> bool:
        if not self.path.exists():
            return True

        ts_client = TushareClient()
        trade_cal = ts_client.get_trade_calendar()
        trade_dates = trade_cal.dates_between(self.since, self.until)

        return not all(self.exists(f"{dt}.parquet") for dt in trade_dates)


class StocksDayKlinesDepository(GenericDailyDepository[DayKlinesModel]):
    _default_path = config.common.get_stock_day_klines_path()


class StocksDayBasicsDepository(GenericDailyDepository[StocksBasicModel]):
    _default_path = config.common.get_stock_day_basics_path()
