import json
from celery import shared_task
import pandas as pd
from tradepy.types import MarketPhase


class TradingEngine:

    def on_pre_market_open(self):
        ...

    def _on_pre_close_bidding(self):
        ...

    def _on_intraday_bidding(self):
        ...

    def handle_tick(self, timestamp, market_phase: MarketPhase, quote_df: pd.DataFrame):
        print(quote_df)


@shared_task(name="tradepy.handle_tick")
def handle_tick(payload):
    quote_json = json.loads(payload["market_quote"])

    TradingEngine().handle_tick(
        timestamp=payload["timestamp"],
        market_phase=payload["market_phase"],
        quote_df=pd.DataFrame(quote_json)
    )
