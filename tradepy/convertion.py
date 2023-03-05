import pandas as pd
from datetime import date
from dateutil import parser as date_parse

import tradepy
from tradepy.types import ExchangeType, MarketType, Markets


TickFields = [
    'timestamp',  # datetime
    'open',  # float, 开盘价
    'high',  # float 最高
    'low',  # float 最低
    'close',  # float 收盘价
    'vol',  # float (> 0) 量能
    'chg',  # float (...), 价格变化
    'pct_chg',  # float (-100, 100), 涨跌幅
]


ListingFields = [
    'timestamp',  # :[ trade_date ]
    'code',  # :[ ts_code ]
    'ts_suffix',  # :[ ts_code ]
    'market',
    'company',  # :[ name ]
    'industry',
    'pe',  # 市盈率（动）
    "mkcap",
    'float_share',  # 流通股本（亿）
    'total_share',  # 总股本（亿）
    'total_assets',  # 总资产（亿）
    'liquid_assets',  # 流动资产（亿）
    'fixed_assets',  # 固定资产（亿）
    'eps',  # 每股收益
    'bvps',  # 每股净资产
    'pb',  # 市净率
    'rev_yoy',  # 收入同比（%）
    'profit_yoy',  # 利润同比（%）
    'gpr',  # 毛利率（%）
    'npr',  # 净利润率（%）
    'holder_num',  # 股东人数
]


def convert_tushare_v1_hist_data(df: pd.DataFrame):
    _df = df.rename(columns={
        'date': 'timestamp',
        'volume': 'vol',
        'price_change': 'chg',
        'p_change': 'pct_chg'
    })

    if not isinstance(_df['timestamp'].iloc[0], pd.Timestamp):
        _df['timestamp'] = pd.to_datetime(_df['timestamp'])

    return _df[TickFields]


def convert_tushare_v2_hist_data(df: pd.DataFrame):
    _df = df.rename(columns={
        'trade_date': 'timestamp',
        'open_qfq': 'open',
        'high_qfq': 'high',
        'low_qfq': 'low',
        'close_qfq': 'close',
        'change': 'chg',
        'pct_change': 'pct_chg',
    })

    return _df[TickFields]


def convert_tushare_v2_fundamentals_data(df: pd.DataFrame):
    _df = df.rename(columns={
        'trade_date': 'timestamp',
        'name': 'company',
    })

    _df[['code', 'ts_suffix']] = _df['ts_code'].str.split('.', expand=True)
    _df["market"] = list(map(convert_code_to_market, _df["code"]))

    # Patch market cap
    _ts = str(_df.iloc[0]["timestamp"])
    _day_k = tradepy.pro_api.api.daily(start_date=_ts, end_date=_ts)
    _day_k[['code', 'ts_suffix']] = _day_k['ts_code'].str.split('.', expand=True)

    _df = _df.set_index("code").join(_day_k[["code", "close"]].set_index("code"))
    _df["mkcap"] = (_df["close"] * _df["total_share"]).round(3)
    _df.reset_index(inplace=True)
    return _df[ListingFields].dropna()


def convert_code_to_market(code: str) -> MarketType:
    mapping: dict[tuple, MarketType] = {
        ("688",): "科创板",
        ("689",): "CDR",
        ("002",): "中小板",
        ("300", "301"): "创业板",
        ("600", "601", "603", "605"): "上证主板",
        ("000", "001", "003"): "深证主板",
    }

    for prefix, market in mapping.items():
        if code.startswith(prefix):
            return market

    raise ValueError(f'Unknown code {code}')


def convert_code_to_exchange(code: str) -> ExchangeType:
    market = convert_code_to_market(code)
    match market:
        case Markets.STAR | Markets.SH_MAIN | Markets.CDR:
            return "SH"
        case Markets.CHI_NEXT | Markets.SZ_MAIN | Markets.SME:
            return "SZ"
        case Markets.BSE:
            return "BJ"
    raise ValueError(f'Invalid code {code}')


def convert_to_ts_date(dt: date) -> str:
    return dt.strftime("%Y%m%d")


def convert_ts_date_to_iso_format(value: str) -> str:
    return date_parse.parse(value).date().isoformat()


def convert_akshare_hist_data(df: pd.DataFrame):
    return df.rename(columns={
        "日期": "timestamp",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "vol",
        "涨跌幅": "pct_chg",
        "涨跌额": "chg",
    })[TickFields]


def convert_akshare_stock_info(data: dict) -> dict:
    to_100_mil = lambda v: v * 1e-8

    name_translation_and_convert = {
        '总市值': ("mkcap", to_100_mil),
        '行业': ("industry",),
        '上市时间': ("listdate",),
        '股票代码': ("code",),
        '股票简称': ("company",),
        '总股本': ("total_share", to_100_mil),
        '流通股': ("float_share", to_100_mil),
    }

    converted = dict()
    for ch_name, trans_and_conv in name_translation_and_convert.items():
        if len(trans_and_conv) == 2:
            en_name, converter = trans_and_conv
            converted[en_name] = converter(data[ch_name])
        else:
            en_name = trans_and_conv[0]
            converted[en_name] = data[ch_name]

    return converted


def convert_akshare_industry_listing(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        '板块名称': "name",
        '板块代码': "code",
        '最新价': "close",
        '涨跌额': "chg",
        '涨跌幅': "pct_chg",
        '总市值': "mkcap",
        '换手率': "turnover",
    }
    return df.rename(columns=mapping)[list(mapping.values())]


def convert_akshare_industry_ticks(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "日期": "timestamp",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "涨跌幅": "pct_chg",
        "涨跌额": "chg",
        "成交量": "vol",
        "换手率": "turnover",
    }
    return df.rename(columns=mapping)[list(mapping.values())]


def convert_akshare_stock_index_ticks(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns={
        "date": "timestamp",
        "volume": "vol"
    }, inplace=True)

    df["timestamp"] = df["timestamp"].astype(str)
    return df
