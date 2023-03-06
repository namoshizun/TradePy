import pandas as pd
from functools import cache, cached_property
from fuzzywuzzy import fuzz

from tradepy.convertion import convert_code_to_market
from tradepy.types import MarketType


def _fuzzy_match(target, texts) -> str:
    return sorted(
        (
            (text, fuzz.ratio(text, target))
            for text in texts
        ),
        key=lambda x: -x[1]
    )[0][0]


class StocksPool:

    filename = "listing.csv"

    @cached_property
    def df(self):
        from tradepy.warehouse import ListingDepot
        return ListingDepot.load()

    @property
    def names(self) -> list[str]:
        return self.df['name'].unique().tolist()

    @property
    def codes(self) -> list[str]:
        return self.df.index.unique().tolist()

    def _build_stock_obj(self, row: pd.Series):
        d = row.to_dict()
        return Stock(
            code=d['code'],
            name=d['name'],
            industry=d['sector'],
            total_share=d["total_share"]
        )

    @cache
    def get_by_name(self, name: str, fuzzy=False) -> 'Stock':
        if fuzzy:
            name = _fuzzy_match(name, self.names)

        data = self.df.query(f'name == "{name}"')
        return self._build_stock_obj(data.reset_index().iloc[0])

    @cache
    def get_by_code(self, code: str) -> 'Stock':
        data = self.df.loc[code].copy()
        data['code'] = code
        return self._build_stock_obj(data)

    def has_code(self, code: str) -> bool:
        return code in self.df.index

    def export(self, path: str):
        self.df.to_csv(path)


class Stock:

    def __init__(self,
                 code: str,
                 name: str,
                 industry: str,
                 total_share: float) -> None:
        self.code = code
        self.name = name
        self.industry = industry
        self.total_share = float(total_share)  # in 100 millions
        self.market: MarketType = convert_code_to_market(code)

    def get_market_cap_at(self, price):
        return price * self.total_share

    def __str__(self) -> str:
        return f'{self.name} ({self.code}) - {self.industry}, {self.market}'

    def __repr__(self) -> str:
        return str(self)
