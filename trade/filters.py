import pandas as pd

import trade
from trade.types import MarketType

def remove_st(df: pd.DataFrame):
    selector = df["company"].str.contains("ST")
    return df[~selector]


def remove_market(df: pd.DataFrame,
                  markets_blacklist: set[MarketType]):

    all_companies = df["company"].unique()

    remove_companies = [
        name
        for name in all_companies
        if trade.listing.get_by_name(name).market in markets_blacklist
    ]

    return df.query('company not in @remove_companies')
