import pandas as pd
import numpy as np
from tradepy.backtesting.strategy import Strategy


class GreenStreakStrategy(Strategy):

    stop_loss = 10
    take_profit = 2
    green_streaks_threshold = 7
    streak_window = 10

    def compute_indicators(self, df: pd.DataFrame):
        _df = df.copy()
        _df["n_greens"] = [
            np.nan if len(window) < self.streak_window else (window < 0).sum()
            for window in df["chg"].rolling(window=self.streak_window)
        ]
        _df.dropna(inplace=True)
        return _df

    def should_buy(self, n_greens, company):
        return (n_greens >= self.green_streaks_threshold) & ~company.str.contains("ST")
