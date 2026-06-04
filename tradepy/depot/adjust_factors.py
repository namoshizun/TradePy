import json
from datetime import date

from tradepy.core.types import StockPriceAdjustFactorsModel
from tradepy.depot import DataDepository


class StocksAdjustFactorsDepository(
    DataDepository[StockPriceAdjustFactorsModel]
):
    def is_outdated(self) -> bool:
        mark_file = self.path / "update-mark.json"
        if not mark_file.exists():
            return True

        with mark_file.open("r") as f:
            mark = json.load(f)
            return mark["date"] != date.today().isoformat()

    def mark_updated(self):
        with (self.path / "update-mark.json").open("w+") as f:
            json.dump({"date": date.today().isoformat()}, f)
