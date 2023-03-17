import json
import pandas as pd
from datetime import date
from pathlib import Path
from celery import shared_task

from tradepy.types import MarketPhase


class TradingEngine:

    def __init__(self) -> None:
        self.workspace = Path.home() / ".tradepy" / str(date.today())
        self.workspace.mkdir(exist_ok=True, parents=True)

    def on_pre_market_open_call_p2(self, quote_df: pd.DataFrame):
        ...

    def on_cont_trade(self, quote_df: pd.DataFrame):
        ...

    def on_cont_trade_pre_close(self, quote_df: pd.DataFrame):
        ...

    def handle_tick(self, market_phase: MarketPhase, quote_df: pd.DataFrame):
        match market_phase:
            case MarketPhase.PRE_OPEN_CALL_P2:
                self.on_pre_market_open_call_p2(quote_df)

            case MarketPhase.PRE_OPEN_CALL_P2 | MarketPhase.CONT_TRADE:
                self.on_cont_trade(quote_df)

            case MarketPhase.CONT_TRADE_PRE_CLOSE:
                self.on_cont_trade_pre_close(quote_df)


@shared_task(name="tradepy.handle_tick")
def handle_tick(payload):
    quote_json = json.loads(payload["market_quote"])

    TradingEngine().handle_tick(
        market_phase=payload["market_phase"],
        quote_df=pd.DataFrame(quote_json)
    )
