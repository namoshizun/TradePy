import tushare
import pandas as pd
from functools import cache
from typing import Optional
from fuzzywuzzy import fuzz

from .types import MarketType


def _fuzzy_match(target, texts) -> str:
    return sorted(
        (
            (text, fuzz.ratio(text, target))
            for text in texts
        ),
        key=lambda x: -x[1]
    )[0][0]


class StocksPool:

    def __init__(self,
                 source_df: Optional[pd.DataFrame]=None,
                 source_file: Optional[str]='datasets/listing.csv'):

        if isinstance(source_df, pd.DataFrame):
            self.df = source_df.copy()
        else:
            assert source_file
            self.df = pd.read_csv(source_file, index_col=None, dtype=str)

        self.df.set_index('code', inplace=True)

    @property
    def names(self) -> list[str]:
        return self.df['company'].unique().tolist()

    @property
    def codes(self) -> list[str]:
        return self.df.index.unique().tolist()

    def _build_stock_obj(self, row: pd.Series):
        d = row.to_dict()
        return Stock(
            code=d['code'],
            ts_code=f"{d['code']}.{d['ts_suffix']}",
            name=d['company'],
            industry=d['industry'],
            market=d['market'],
            total_share=d["total_share"]
        )

    @cache
    def get_by_name(self, name: str, fuzzy=False) -> 'Stock':
        if fuzzy:
            name = _fuzzy_match(name, self.names)

        data = self.df.query(f'company == "{name}"')
        return self._build_stock_obj(data.reset_index().iloc[0])

    @cache
    def get_by_code(self, code: str) -> 'Stock':
        data = self.df.loc[code]
        data['code'] = code
        return self._build_stock_obj(data)

    def has_code(self, code: str) -> bool:
        return code in self.df.index

    def export(self, path: str):
        self.df.to_csv(path)


class Stock:

    def __init__(self,
                 code: str,
                 ts_code: str,
                 name: str,
                 industry: str,
                 total_share: float,
                 market: MarketType) -> None:
        self.code = code
        self.ts_code = ts_code
        self.name = name
        self.industry = industry
        self.market = market
        self.total_share = float(total_share)  # in 100 millions

    def get_market_cap_at(self, price):
        return price * self.total_share

    def get_current_quotes(self):
        df = tushare.get_realtime_quotes(self.code)
        assert len(df) == 1, df
        return df.iloc[0].to_dict()

    def __str__(self) -> str:
        # return f'{self.name} ({self.ts_code}) - {self.market}, {self.industry}'
        return f'{self.name} ({self.code}) - {self.market}, {self.industry}'

    def __repr__(self) -> str:
        return str(self)
