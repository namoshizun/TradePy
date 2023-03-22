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
    LUNCHBREAK = "lunchbreak"  # 1130 - 1300
    PRE_OPEN = "pre-open"  # 0900 - 0915
    PRE_OPEN_CALL_P1 = "pre-open-call-phase-1"  # 0915 - 0925
    PRE_OPEN_CALL_P2 = "pre-open-call-phase-2"  # 0925 - 0930
    CONT_TRADE = "continuous-trading"  # 0930 - 1130, 1300 - 1453
    CONT_TRADE_PRE_CLOSE = "continuous-trading-pre-close"  # 1453 - 1457
    PRE_CLOSE_CALL = "pre-close-bidding"  # 1457 - 1500
