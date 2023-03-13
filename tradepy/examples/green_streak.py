import re
import pandas as pd
from tradepy.strategy import Strategy


class GreenStreakStrategy(Strategy):

    def pre_process(self, bars_df: pd.DataFrame):
        company = bars_df.iloc[0]["company"]
        if re.match(r'^.*(ST|银行)', company, re.I):
            return pd.DataFrame()

        return bars_df

    def post_process(self, bars_df: pd.DataFrame):
        bars_df.dropna(inplace=True)
        return bars_df

    def n_greens(self, chg):
        return (chg < 0).rolling(window=self.streak_window).sum()

    def should_buy(self, n_greens, company):
        return n_greens >= self.green_streaks_threshold
