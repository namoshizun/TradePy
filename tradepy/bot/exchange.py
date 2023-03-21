import io
import pandas as pd
from datetime import datetime
from celery import shared_task

import tradepy
from tradepy.trade_cal import trade_cal
from tradepy.constants import Timeouts
from tradepy.convertion import convert_code_to_market
from tradepy.decorators import timeit, timeout
from tradepy.types import MarketPhase
from tradepy.bot.celery_app import app as celery_app


class AStockExchange:

    @staticmethod
    def market_phase_now():
        _ = MarketPhase
        # return _.PRE_CLOSE_CALL

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
                if 52 <= minute < 57:
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


@shared_task(
    name="tradepy.fetch_market_quote",
    expires=tradepy.config.tick_fetch_interval * 0.95)
def fetch_market_quote():
    phase = AStockExchange.market_phase_now()
    if phase not in (
        MarketPhase.PRE_OPEN_CALL_P2,
        MarketPhase.CONT_TRADE,
        MarketPhase.PRE_CLOSE_CALL):
        return

    with timeit() as timer:
        df = AStockExchange.get_quote()

    # Serialize the quote frame to a string and send it to the trading engine
    content_buff = io.StringIO()
    df.to_csv(content_buff)
    content_buff.seek(0)

    celery_app.send_task(
        "tradepy.handle_tick",
        kwargs=dict(payload={
            "timestamp": datetime.now(),
            "market_phase": phase,
            "market_quote": content_buff.read()
        }),
    )

    tradepy.LOG.info(f'行情获取API 耗时: {timer["seconds"]}s')
