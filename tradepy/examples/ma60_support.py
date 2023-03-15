import re
import math
import pandas as pd
import numba as nb
import talib

import tradepy
from tradepy.core.strategy import Strategy
from tradepy.decorators import tag
from tradepy.warehouse import BroadBasedIndexBarsDepot, SectorIndexBarsDepot
from tradepy.types import Markets


@nb.jit
def calc_chg_degree(values):
    tan = (values[-1] - values[0]) / len(values)
    return math.degrees(math.atan(tan))


ROLL_USE_NUMBA = dict(engine='numba', raw=True)


class MA60SupportStrategy(Strategy):

    def __patch_index_data(self, df: pd.DataFrame, idx_df: pd.DataFrame, prefix: str):
        ind_df = idx_df[["close", "timestamp"]].copy()
        ind_df.rename(columns={"close": f'{prefix}_close'}, inplace=True)
        ind_df.set_index("timestamp", inplace=True)
        ind_df.dropna(inplace=True)
        return df.join(ind_df, on="timestamp")

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
        if all(f'{idx}_ema5' in bars_df for idx in ["sector", "board"]):
            return bars_df

        # Patch index data
        try:
            industry = tradepy.listing.get_by_name(company).industry
        except IndexError:
            return pd.DataFrame()

        sect_df = SectorIndexBarsDepot.load(cache=True).loc[industry]
        bars_df = self.__patch_index_data(bars_df, sect_df, prefix="sector")

        # Get indicators from the main board bars
        board_df = BroadBasedIndexBarsDepot.load(cache=True).loc[board_name]
        return self.__patch_index_data(bars_df, board_df, prefix="board")

    @tag(notna=True)
    def ma59_ref1(self, close):
        return talib.SMA(close, 59).shift(1).round(2)

    @tag(notna=True)
    def ma60_observed(self, open, ma59_ref1):
        return (ma59_ref1 * 59 + open) / 60

    @tag(notna=True)
    def ma59_ref1_chg_20(self, ma59_ref1):
        wsize = 20
        return ma59_ref1.rolling(wsize).apply(calc_chg_degree, **ROLL_USE_NUMBA).round(2)

    @tag(notna=True)
    def n_below_ma60_past_4(self, low, ma60):
        wsize = 4
        dist_ma60 = (100 * (low - ma60) / ma60).round(2)
        return dist_ma60.rolling(wsize, closed="left").apply(lambda w: (w < 0).sum())

    @tag(notna=True)
    def n_limit_downs_past_5(self, pct_chg):
        wsize = 5
        return pct_chg.rolling(wsize, closed="left").apply(lambda w: (w <= -9.5).sum())

    @tag(notna=True, outputs=["board_ema5", "board_ema20", "board_macd", "board_rsi6"])
    def board_index_indicators(self, board_close: pd.Series):
        ema5 = talib.EMA(board_close, 5)
        ema20 = talib.EMA(board_close, 20)
        _, _, macdhist = talib.MACD(board_close)
        rsi6 = talib.RSI(board_close, 6)
        return ema5, ema20, macdhist * 2, rsi6

    @tag(outputs=["sector_ema5", "sector_ema20", "sector_macd"])
    def sector_index_indicators(self, sector_close):
        ema5 = talib.EMA(sector_close, 5)
        ema20 = talib.EMA(sector_close, 20)
        _, _, macdhist = talib.MACD(sector_close)
        rsi6 = talib.RSI(sector_close, 6)
        return ema5, ema20, macdhist * 2, rsi6

    @tag(notna=True, outputs=["board_ema5_ref1", "board_ema20_ref1", "board_macd_ref1", "board_rsi6_ref1"])
    def board_index_indicators_ref1(self, board_ema5, board_ema20, board_macd, board_rsi6):
        return board_ema5.shift(1), \
            board_ema20.shift(1), \
            board_macd.shift(1), \
            board_rsi6.shift(1)

    @tag(outputs=["sector_ema5_ref1", "sector_ema20_ref1", "sector_macd_ref1"])
    def sector_index_indicators_ref1(self, sector_ema5, sector_ema20, sector_macd):
        return sector_ema5.shift(1), \
            sector_ema20.shift(1), \
            sector_macd.shift(1)

    def __price_within_ma60_support(self, ma60_observed, open, low, high) -> float | None:
        upper_thres = ma60_observed * (1 + self.ma60_dist_thres * 1e-2)
        lower_thres = ma60_observed * (1 - self.ma60_dist_thres * 1e-2)

        if lower_thres < open < upper_thres:
            return open
        elif (open > upper_thres) and (low <= upper_thres):
            return upper_thres
        elif (open < lower_thres) and (high >= lower_thres):
            return lower_thres

        return None

    def should_buy(self,
                   mkt_cap_rank,
                   n_below_ma60_past_4,
                   n_limit_downs_past_5,
                   ma59_ref1_chg_20,
                   ma60_observed,
                   open, low, high,
                   board_rsi6_ref1,
                   board_macd_ref1,
                   board_ema5_ref1,
                   board_ema20_ref1,
                   sector_macd_ref1,
                   sector_ema5_ref1,
                   sector_ema20_ref1) -> bool | float:

        # Good liquity
        if mkt_cap_rank < 0.2:
            return False

        # Stock trend still not good
        if ma59_ref1_chg_20 > 5:
            return False

        # Avoid limit downs
        if n_limit_downs_past_5 > 0:
            return False

        # Must be testing the MA60 support from above
        if n_below_ma60_past_4 > 0:
            return False

        if buy_price := self.__price_within_ma60_support(ma60_observed, open, low, high):
            # The exchange price is within the MA60 support, but we still need to
            # check for Beta signals
            if board_rsi6_ref1 < 15:
                return buy_price

            # The overal market mood is good
            if (board_ema5_ref1 > board_ema20_ref1) or (sector_ema5_ref1 > sector_ema20_ref1):
                if (board_macd_ref1 > 5) or (sector_macd_ref1 > 10):
                    return buy_price

        return False

    def should_close(self,
                     board_rsi6,
                     board_ema5,
                     board_ema20,
                     sector_ema5,
                     sector_ema20,
                     sector_macd) -> bool:

        if board_rsi6 > 85:
            return True

        if (board_ema5 < board_ema20) or (sector_ema5 < sector_ema20):
            return True

        if sector_macd < -15:
            return True

        return False
