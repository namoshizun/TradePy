import talib
import re
import math
import pandas as pd
import numba as nb
from typing import Any

import tradepy
from tradepy.backtesting.strategy import Strategy
from tradepy.warehouse import BroadBasedIndexTicksDepot, SectorIndexTicksDepot
from tradepy.types import Markets


@nb.jit
def calc_chg_degree(values):
    tan = (values[-1] - values[0]) / len(values)
    return math.degrees(math.atan(tan))


ROLL_USE_NUMBA = dict(engine='numba', raw=True)


class MA60SupportStrategyV1(Strategy):

    def __patch_index_data(self, df: pd.DataFrame, idx_df: pd.DataFrame, prefix: str):
        indicator_cols = ["ema5", "ema20", "rsi6", "macd"]

        ind_df = idx_df[indicator_cols + ["timestamp"]].copy()
        ind_df.set_index("timestamp", inplace=True)
        ind_df.rename(columns={
            col: f"{prefix}_{col}"
            for col in indicator_cols
        }, inplace=True)

        df = df.join(ind_df, on="timestamp")
        df[f"{prefix}_macd_deg"] = (
            df[f"{prefix}_macd"]
            .rolling(3)
            .apply(calc_chg_degree, **ROLL_USE_NUMBA)
        )
        return df

    def pre_process(self, bars_df: pd.DataFrame):
        company = bars_df.iloc[0]["company"]
        if re.match(r'^.*(ST|银行)', company, re.I):
            return pd.DataFrame()

        # Keep only if tis market's index data is available
        market_name_to_board = {
            Markets.SH_MAIN: "SSE",
            Markets.SZ_MAIN: "SZSE",
            Markets.SME: "SZSE",
            Markets.CHI_NEXT: "ChiNext",
        }

        board_name = market_name_to_board.get(bars_df.iloc[0]["market"])
        if not board_name:
            return pd.DataFrame()

        # Skip if already patched index data
        if all(f'{idx}_macd_deg' in bars_df for idx in ["sector", "board"]):
            return bars_df

        # Patch index data
        try:
            industry = tradepy.listing.get_by_name(company).industry
        except IndexError:
            return pd.DataFrame()

        sect_df = SectorIndexTicksDepot.load(cache=True).loc[industry]
        bars_df = self.__patch_index_data(bars_df, sect_df, prefix="sector")

        # Get indicators from the main board bars
        board_df = BroadBasedIndexTicksDepot.load(cache=True).loc[board_name]
        return self.__patch_index_data(bars_df, board_df, prefix="board")

    def post_process(self, bars_df: pd.DataFrame):
        bars_df.dropna(subset=[
            "dist_ma60",
            "n_below_ma60_past_4",
            "n_limit_downs_past_5",
            "ma60_chg_20",
        ], inplace=True)
        return bars_df

    def ma60(self, close):
        return talib.SMA(close, 60).round(2)

    def ma60_chg_20(self, ma60):
        wsize = 20
        return ma60.rolling(wsize).apply(calc_chg_degree, **ROLL_USE_NUMBA).round(2)

    def ma250(self, close):
        return talib.SMA(close, 250).round(2)

    def dist_ma60(self, low, ma60):
        return (100 * (low - ma60) / ma60).round(2)

    def n_below_ma60_past_4(self, dist_ma60):
        wsize = 4
        return dist_ma60.rolling(wsize, closed="left").apply(lambda w: (w < 0).sum())

    def n_limit_downs_past_5(self, pct_chg):
        wsize = 5
        return pct_chg.rolling(wsize).apply(lambda w: (w <= -9.5).sum())

    def should_buy(self,
                   mkt_cap_rank,
                   n_below_ma60_past_4,
                   n_limit_downs_past_5,
                   dist_ma60,
                   ma60_chg_20,
                   board_rsi6,
                   board_macd,
                   board_ema5,
                   board_ema20,
                   sector_macd,
                   sector_ema5,
                   sector_ema20) -> bool:

        # Good liquity
        if mkt_cap_rank < 0.2:
            return False

        # Stock trend still not good
        if ma60_chg_20 > 5:
            return False

        reaching_ma60_from_above = (n_below_ma60_past_4 == 0) and \
            (abs(dist_ma60) <= self.ma60_dist_thres)

        # Must be testing the MA60 support from above
        if not reaching_ma60_from_above:
            return False

        if n_limit_downs_past_5 > 0:
            return False

        if board_rsi6 < 15:
            return True

        # The overal market mood is good
        if (board_ema5 > board_ema20) or (sector_ema5 > sector_ema20):
            return (board_macd > 5) or (sector_macd > 0)

        return False

    def should_close(self,
                     board_rsi6,
                     sector_ema5,
                     sector_ema20) -> bool:

        if board_rsi6 > 85:
            return True

        if sector_ema5 < sector_ema20:
            return True

        return False

    def get_pool_and_budget(self, ticks_df: pd.DataFrame, buy_indices: list[Any], budget: float) -> tuple[pd.DataFrame, float]:
        n_total = len(ticks_df)
        n_signals = len(buy_indices)

        if n_signals / n_total > self.max_signal_ratio:
            # This day has a abonrmaly high percent of buy signals
            return pd.DataFrame(), 0

        return super().get_pool_and_budget(ticks_df, buy_indices, budget)
