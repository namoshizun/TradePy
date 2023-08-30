import pandas as pd
from datetime import datetime, date

import tradepy
from tradepy.trade_cal import trade_cal
from tradepy.decorators import timeout
from tradepy.types import MarketPhase
from tradepy.vendors.types import AskBid


class AStockExchange:
    @staticmethod
    def is_today_trade_day():
        return str(date.today()) in trade_cal

    @staticmethod
    def market_phase_now():
        _ = MarketPhase

        if not AStockExchange.is_today_trade_day():
            return _.CLOSED

        now = datetime.now()
        hour, minute, second = now.hour, now.minute, now.second
        if hour == 9:
            if minute < 15:
                return _.PRE_OPEN
            if minute < 25 or (
                minute == 25 and second < 30
            ):  # delay a bit to ensure the upstream data sources are ready
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
                if 54 <= minute < 57:
                    return _.CONT_TRADE_PRE_CLOSE
                if minute >= 57:
                    return _.PRE_CLOSE_CALL
            return _.CONT_TRADE

        return _.CLOSED

    @staticmethod
    @timeout(seconds=tradepy.config.trading.timeouts.download_quote)
    def get_quote() -> pd.DataFrame:
        return tradepy.ak_api.get_stock_current_quote()

    @staticmethod
    @timeout(seconds=tradepy.config.trading.timeouts.download_ask_bid)
    def get_bid_ask(code: str) -> AskBid:
        return tradepy.ak_api.get_stock_ask_bid(code)
