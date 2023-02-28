from typing import Literal, TypedDict


class TickTagSet(TypedDict, total=False):
    code: str
    company: str


TickTags = Literal[
    'code',
    'company',
    'market'
]


FundamentalTags = Literal[
    'code',
    'company',
    'industry',
    'market',
]

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
    'CDR'
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
    EXIT = "平仓"
    STOP_LOSS = "止损"
    TAKE_PROFIT = "止盈"
