import talib
import re
import pandas as pd
import numpy as np

from trade.backtesting.strategy import Strategy
from trade.warehouse import TicksDepot
from trade.types import Markets  


class MA60SupportStrategy(Strategy):

    def compute_indicators(self, df: pd.DataFrame):
        # Keep ones whose market's index data is available
        market_name_to_index = {
            Markets.SH_MAIN: "SSE",
            Markets.SZ_MAIN: "SZSE",
            Markets.SME: "SZSE",
            Markets.CHI_NEXT: "ChiNext",
        }

        market = df.iloc[0]["market"]
        index_name = market_name_to_index.get(market)

        # Filtering
        if not index_name:
            return pd.DataFrame()

        company = df.iloc[0]["company"]
        if re.match(r'ST|银行', company, re.I):
            return pd.DataFrame()

        # Compute MACD values of its market index
        index_df = TicksDepot("daily.index").load_index_ticks(cache=True).loc[index_name].copy()
        assert isinstance(index_df, pd.DataFrame)

        _, _, macdhist = talib.MACD(index_df["close"])
        index_df["index_macd"] = (macdhist * 2).round(2)

        # Compute market index MA5 & MA20
        index_df["index_ma5"] = talib.SMA(index_df["close"], 5).round(2)
        index_df["index_ma20"] = talib.SMA(index_df["close"], 20).round(2)
        index_df["index_ema5"] = talib.EMA(index_df["close"], 5).round(2)
        index_df["index_ema20"] = talib.EMA(index_df["close"], 20).round(2)

        # Patch market indicators
        index_df.drop(
            index_df.columns.difference(["timestamp", "index_macd", "index_ma5", "index_ma20", "index_ema5", "index_ema20"]),
            axis=1,
            inplace=True
        )
        index_df.set_index("timestamp", inplace=True)
        df = df.join(index_df, on="timestamp")

        # Compute SMA60
        df["ma60"] = talib.SMA(df["close"], 60).round(2)

        # Compute the distances to MA60 as pct change from the MA60
        df["dist_ma60"] = (100 * (df["low"] - df["ma60"]) / df["ma60"]).round(2)

        # Count number of ticks the price is below ma60 in the past few days
        wsize = 4
        df["n_below_ma60_past_4"] = [
            np.nan if len(w) < wsize else (w < 0).sum()
            for w in df["dist_ma60"].rolling(wsize, closed="left")  # excluding the current day
        ]

        # Count number of limit downs in the past 5 days
        wsize = 5
        df["n_limit_downs_past_5"] = [
            np.nan if len(w) < wsize else (w <= -9.5).sum()
            for w in df["pct_chg"].rolling(wsize)
        ]

        df.dropna(inplace=True)
        return df

    def should_buy(self,
                   n_below_ma60_past_4,
                   n_limit_downs_past_5,
                   dist_ma60,
                   index_macd,
                   index_ema5,
                   index_ema20) -> pd.Series:

        market_up_trend = (index_ema5 > index_ema20) & (index_macd >= 5)
        reaching_ma60_from_above = (n_below_ma60_past_4 == 0) & (dist_ma60.abs() <= self.ma60_dist_thres)
        safe_window = n_limit_downs_past_5 == 0
        return market_up_trend & safe_window & reaching_ma60_from_above

    def get_pool_and_budget(self, ticks_df: pd.DataFrame, selector: pd.Series, budget: float) -> tuple[pd.DataFrame, float]:
        n_total = len(ticks_df)
        n_signals = selector.sum()

        if n_signals / n_total > self.max_signal_ratio:
            # This day has a abonrmaly high percent of buy signals
            return pd.DataFrame(), 0

        return super().get_pool_and_budget(ticks_df, selector, budget)
