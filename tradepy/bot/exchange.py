import pandas as pd
from datetime import datetime, timedelta
from celery import shared_task

import tradepy
from tradepy.convertion import convert_code_to_market
from tradepy.decorators import timeit, timeout
from tradepy.types import MarketPhase
from tradepy.bot.celery_app import app as celery_app


class AStockExchange:

    @staticmethod
    def market_phase_now():
        now = datetime.now()
        hour, minute = now.hour, now.minute

        _ = MarketPhase

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
    @timeout(seconds=3)
    def get_quote() -> pd.DataFrame:
        df = tradepy.ak_api.get_current_quote()
        selector = df["code"].map(lambda code: convert_code_to_market(code) in tradepy.config.market_types)
        return df[selector]


@shared_task(
    name="tradepy.fetch_market_quote",
    expires=tradepy.config.tick_fetch_interval * 0.95)
def fetch_market_quote():
    phase = AStockExchange.market_phase_now()
    if phase not in (
        MarketPhase.PRE_OPEN,
        MarketPhase.PRE_OPEN_CALL_P2,
        MarketPhase.INDAY_BID,
        MarketPhase.PRE_CLOSE_CALL):
        return

    with timeit() as timer:
        df = AStockExchange.get_quote()

    payload = {
        "timestamp": datetime.now(),
        "market_phase": phase,
        "market_quote": df.to_json()
    }
    celery_app.send_task("tradepy.handle_tick", kwargs=dict(payload=payload))

    tradepy.LOG.info(f'fetch quote API took: {timer["seconds"]}s')
