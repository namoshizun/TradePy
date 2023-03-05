import talib
import re
import pandas as pd
import numpy as np
from typing import Any
import random

import trade
from trade.backtesting.strategy import Strategy
from trade.warehouse import TicksDepot
from trade.types import Markets  


class MA60SupportStrategy(Strategy):

    def patch_index_data(self, df: pd.DataFrame, idx_df: pd.DataFrame, prefix: str):
        indicator_cols = ["ema5", "ema20", "rsi6", "macd", "nmacd120"]

        ind_df = idx_df[indicator_cols + ["timestamp"]].copy()
        ind_df.set_index("timestamp", inplace=True)
        ind_df.rename(columns={
            col: f"{prefix}_{col}"
            for col in indicator_cols
        }, inplace=True)

        return df.join(ind_df, on="timestamp")

    def compute_indicators(self, df: pd.DataFrame):
        # Keep ones whose market's index data is available
        market_name_to_board = {
            Markets.SH_MAIN: "SSE",
            Markets.SZ_MAIN: "SZSE",
            Markets.SME: "SZSE",
            Markets.CHI_NEXT: "ChiNext",
        }

        board_name = market_name_to_board.get(df.iloc[0]["market"])

        # Filtering
        if not board_name:
            return pd.DataFrame()

        company = df.iloc[0]["company"]
        if re.match(r'^.*(ST|银行)', company, re.I):
            return pd.DataFrame()

        # Compute SMA60
        df["ma60"] = talib.SMA(df["close"], 60).round(2)

        # Compute the distances to SMA60 as pct change from the MA60
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

        # Get indicators from industry ticks data
        try:
            industry = trade.listing.get_by_name(company).industry
        except IndexError:
            return pd.DataFrame()

        ind_df = TicksDepot("daily.industry").load_industry_ticks(cache=True).loc[industry]
        df = self.patch_index_data(df, ind_df, "industry")

        # Get indicators from the main board ticks data
        board_df = TicksDepot("daily.index").load_index_ticks(cache=True).loc[board_name]
        df = self.patch_index_data(df, board_df, "board")

        return df

    def should_buy(self,
                   n_below_ma60_past_4,
                   n_limit_downs_past_5,
                   dist_ma60,
                   board_rsi6,
                   board_macd,
                   board_nmacd120,
                   board_ema5,
                   board_ema20,
                   industry_macd,
                   industry_nmacd120,
                   industry_ema5,
                   industry_ema20) -> bool:

        reaching_ma60_from_above = (n_below_ma60_past_4 == 0) and \
            (abs(dist_ma60) <= self.ma60_dist_thres)

        if not reaching_ma60_from_above:
            return False

        if n_limit_downs_past_5 > 0:
            return False

        if board_rsi6 < 15:
            return True

        if np.isnan(industry_macd):
            ref_macd = board_macd
            ref_nmacd120 = board_nmacd120
            ref_ema5 = board_ema5
            ref_ema20 = board_ema20
        else:
            ref_macd = industry_macd
            ref_nmacd120 = industry_nmacd120
            ref_ema5 = industry_ema5
            ref_ema20 = industry_ema20

        if ref_ema5 > ref_ema20:
            return (ref_nmacd120 >= 0.15) or (ref_macd > 5):  # always prefer nmacd120
        return False

    def should_close(self,
                     board_rsi6,
                     board_macd,
                     board_nmacd120,
                     board_ema5,
                     board_ema20,
                     industry_macd,
                     industry_nmacd120,
                     industry_ema5,
                     industry_ema20) -> bool:

        if board_rsi6 > 85:
            return True

        if np.isnan(industry_macd):
            ref_macd = board_macd
            ref_nmacd120 = board_nmacd120
            ref_ema5 = board_ema5
            ref_ema20 = board_ema20
        else:
            ref_macd = industry_macd
            ref_nmacd120 = industry_nmacd120
            ref_ema5 = industry_ema5
            ref_ema20 = industry_ema20

        if ref_ema5 < ref_ema20:
            return True
        
        if np.isnan(ref_nmacd120):
            return ref_macd < -2  # use macd instead if nmacd not available

        return ref_nmacd120 <= -0.15  # otherwise we prefer nmacd120

    def get_pool_and_budget(self, ticks_df: pd.DataFrame, buy_indices: list[Any], budget: float) -> tuple[pd.DataFrame, float]:
        n_total = len(ticks_df)
        n_signals = len(buy_indices)

        if n_signals / n_total > self.max_signal_ratio:
            # This day has a abonrmaly high percent of buy signals
            return pd.DataFrame(), 0

        return super().get_pool_and_budget(ticks_df, buy_indices, budget)
