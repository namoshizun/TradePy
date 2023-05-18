import os
import tempfile
from datetime import date
from dataclasses import dataclass
import pandas as pd
import tradepy


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

        if not (path := tradepy.config.blacklist_path):
            return set()

        if not path.exists():
            return set()

        df = pd.read_csv(path, index_col=None, dtype=str)
        cls.cached = set(BlacklistStock(code, until) for code, until in df.values)

        return cls.cached

    @classmethod
    def contains(cls, code: str, timestamp: str | None = None) -> bool:
        stocks = cls.read()
        if not stocks:
            return False

        try:
            stock = next(stock for stock in stocks if stock.code == code)

            if not stock.until:
                return True

            timestamp = timestamp or str(date.today())
            return timestamp <= stock.until
        except StopIteration:
            return False


if __name__ == "__main__":
    # Create a tempfile, write some black listed stock code and until-date into it, then test the function
    with tempfile.NamedTemporaryFile("w") as f:
        stocks = pd.DataFrame(
            {"code": ["000001", "000002"], "until": ["2021-01-01", "2030-01-02"]}
        ).set_index("code")
        stocks.to_csv(f.name)
        os.environ["BLACKLIST_PATH"] = f.name

        assert Blacklist.contains("000001", stocks.loc["000001", "until"])
        assert not Blacklist.contains("000001", "2030-01-01")
        assert Blacklist.contains("000002")
        assert not Blacklist.contains("11111")
