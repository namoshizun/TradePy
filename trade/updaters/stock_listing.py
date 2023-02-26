from datetime import date
from typing import Literal

from trade.client import TushareClientPro



TushareListingCols = Literal['ts_code', 'symbol', 'name', 'area', 'market', 'list_date']


class StocksPoolUpdater:

    def __init__(self, api: TushareClientPro) -> None:
        self.api = api

    def _download_listing_df(self):
        ...

    def run(self):
        ...
