import talib
import re
import pandas as pd
import numpy as np

from trade.backtesting.strategy import ChinaMarketStrategy
from trade.warehouse import TicksDepot
from trade.types import Markets  


class MA60SupportStrategy(ChinaMarketStrategy):
    # ----------
    # buggy combo
    expiry = 5
    stop_loss = 2.5
    take_profit = 3.5
    min_mkt_capt_rank = 0.6
    ma60_dist_thres = 0.25

    # # ----------
    # # Best combo
    # expiry = 7
    # stop_loss = 2.5
    # take_profit = 3.5
    # min_mkt_capt_rank = 0.3
    # ma60_dist_thres = 0.6

    def compute_indicators(self, df: pd.DataFrame):
        # Keep if its related market's index data is available
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

        df = df.query(f'mkt_cap_rank >= {self.min_mkt_capt_rank}')
        if df.empty:
            return pd.DataFrame()

        # Compute MACD values of its market index
        index_df = TicksDepot("daily.index").load_index_ticks(cache=True).loc[index_name].copy()
        assert isinstance(index_df, pd.DataFrame)

        _, _, macdhist = talib.MACD(index_df["close"])
        index_df["index_macd"] = (macdhist * 2).round(2)

        # Compute market index MA5 & MA20
        index_df["index_ma5"] = talib.SMA(index_df["close"], 5).round(2)
        index_df["index_ma20"] = talib.SMA(index_df["close"], 20).round(2)

        # Patch market indicators
        index_df.drop(
            index_df.columns.difference(["timestamp", "index_macd", "index_ma5", "index_ma20"]),
            axis=1,
            inplace=True
        )
        index_df.set_index("timestamp", inplace=True)

        df = (
            df
            .reset_index()
            .set_index("timestamp")
            .join(index_df)
            .reset_index()
            .set_index("code")
        )

        # Compute SMA60
        df["ma60"] = talib.SMA(df["close"], 60).round(2)

        # Compute the distances to MA60 as pct change from the MA60
        df["dist_ma60"] = (100 * (df["low"] - df["ma60"]) / df["ma60"]).round(2)

        # Count number of times the price is below ma60 in the past few days
        wsize = 4
        df["n_below_ma60"] = [
            np.nan if len(w) < wsize else (w < 0).sum()
            for w in df["dist_ma60"].rolling(wsize, closed="left")  # excluding the current day
        ]

        df.dropna(inplace=True)
        return df

    def should_buy(self, n_below_ma60, dist_ma60, index_macd, index_ma5, index_ma20) -> pd.Series:
        market_up_trend = (index_ma5 > index_ma20) & (index_macd > 0)
        reaching_ma60_from_above = (n_below_ma60 == 0) & (dist_ma60.abs() <= self.ma60_dist_thres)
        return market_up_trend & reaching_ma60_from_above
