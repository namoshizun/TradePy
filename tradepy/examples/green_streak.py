import re
import pandas as pd
from tradepy.core.strategy import Strategy
from tradepy.decorators import tag


class GreenStreakStrategy(Strategy):

    def pre_process(self, bars_df: pd.DataFrame):
        company = bars_df.iloc[0]["company"]
        if re.match(r'^.*(ST|银行)', company, re.I):
            return pd.DataFrame()

        return bars_df

    @tag(notna=True)
    def n_greens(self, chg):
        return (chg < 0).rolling(window=self.streak_window).sum()

    def should_buy(self, n_greens):
        return n_greens >= self.green_streaks_threshold
