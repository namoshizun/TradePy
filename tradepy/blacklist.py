import re
from datetime import date
from dataclasses import dataclass
import pandas as pd
import tradepy


DATE_REGEX = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


@dataclass
class BlacklistStock:
    code: str
    until: str | None

    def __hash__(self) -> int:
        return int(self.code)


class Blacklist:
    cached: set[BlacklistStock] | None = None
    thing = dict()

    @classmethod
    def read(cls) -> set[BlacklistStock]:
        if cached := cls.cached:
            return cached

        if not (path := tradepy.config.common.blacklist_path):
            return set()

        if not path.exists():
            return set()

        try:
            df = pd.read_csv(path, index_col=None, dtype=str)
            cls.cached = set(BlacklistStock(code, until) for code, until in df.values)
        except pd.errors.EmptyDataError:
            cls.cached = set()

        return cls.cached

    @classmethod
    def contains(cls, code: str, timestamp: str | None = None) -> bool:
        stocks = cls.read()
        if not stocks:
            return False

        try:
            stock = next(stock for stock in stocks if stock.code == code)

            if not stock.until or pd.isna(stock.until):
                return True

            timestamp = timestamp or str(date.today())
            if not DATE_REGEX.match(timestamp):
                raise ValueError(f"无效的日期: {timestamp}")

            return timestamp <= stock.until
        except StopIteration:
            return False

    @classmethod
    def purge_cache(cls):
        cls.cached = None
