import pandas as pd
from datetime import datetime

import tradepy
from tradepy.trade_cal import trade_cal
from tradepy.constants import Timeouts
from tradepy.conversion import convert_code_to_market
from tradepy.decorators import timeout
from tradepy.types import MarketPhase


class AStockExchange:

    @staticmethod
    def market_phase_now():
        _ = MarketPhase

        now = datetime.now()
        if str(now.date()) not in trade_cal:
            return _.CLOSED

        hour, minute = now.hour, now.minute
        if hour == 9:
            if minute < 15:
                return _.PRE_OPEN
            if minute < 25:
                return _.PRE_OPEN_CALL_P1
            elif minute < 30:
                return _.PRE_OPEN_CALL_P2
            else:
                return _.CONT_TRADE

        elif 9 < hour <= 11:
            if hour == 11 and minute >= 30:
                return _.LUNCHBREAK
            return _.CONT_TRADE

        elif 11 < hour < 13:
            return _.LUNCHBREAK

        elif 13 <= hour < 15:
            if hour == 14:
                if 53 <= minute < 57:
                    return _.CONT_TRADE_PRE_CLOSE
                if minute >= 57:
                    return _.PRE_CLOSE_CALL
            return _.CONT_TRADE

        return _.CLOSED

    @staticmethod
    @timeout(seconds=Timeouts.download_quote)
    def get_quote() -> pd.DataFrame:
        df = tradepy.ak_api.get_current_quote()
        df["market"] = df.index.map(convert_code_to_market)
        selector = df["market"].map(lambda market: market in tradepy.config.market_types)
        return df[selector]
