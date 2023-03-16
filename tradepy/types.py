from typing import Literal

import pandas as pd


ExchangeType = Literal[
    "SZ",
    "SH",
    "BJ",
]


MarketType = Literal[
    '上证主板',
    '深证主板',
    '中小板',
    '创业板',
    '北交所',
    '科创板',
    "北交所",
    'CDR',
    "新三板"
]


class Markets:
    SH_MAIN = "上证主板"
    SZ_MAIN = "深证主板"
    SME = "中小板"
    CHI_NEXT = "创业板"
    BSE = "北交所"
    STAR = "科创板"
    CDR = "CDR"


TradeActionType = Literal[
    "开仓",
    "平仓",
    "止损",
    "止盈",
]


class TradeActions:
    OPEN = "开仓"
    CLOSE = "平仓"
    STOP_LOSS = "止损"
    TAKE_PROFIT = "止盈"


IndSeries = pd.Series


class MarketPhase:
    CLOSED = "closed"
    LUNCHBREAK = "lunchbreak"
    PRE_OPEN = "pre-open"
    PRE_OPEN_BID_P1 = "pre-open-bidding-phase-1"
    PRE_OPEN_BID_P2 = "pre-open-bidding-phase-2"
    INDAY_BID = "intraday-bidding"
    PRE_CLOSE_BID = "pre-close-bidding"
