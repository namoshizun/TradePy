import pandas as pd
from datetime import date
from dateutil import parser as date_parse

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


def convert_code_to_market(code: str) -> MarketType:
    mapping: dict[tuple, MarketType] = {
        ("688",): "科创板",
        ("689",): "CDR",
        ("002",): "中小板",
        ("300", "301"): "创业板",
        ("600", "601", "603", "605"): "上证主板",
        ("000", "001", "003"): "深证主板",
        ("8",): "北交所",
        ("43",): "新三板",
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
        '行业': ("sector",),
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


def convert_akshare_sector_listing(df: pd.DataFrame) -> pd.DataFrame:
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


def convert_akshare_sector_day_bars(df: pd.DataFrame) -> pd.DataFrame:
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


def convert_akshare_sector_current_quote(data: pd.Series) -> pd.Series:
    mapping = {
        "最新": "close",
        "最高": "high",
        "最低": "low",
        "开盘": "open",
        "成交量": "vol",
        "换手率": "turnover",
        "涨跌额": "chg",
        "涨跌幅": "pct_chg",
        "代码": "code",
    }
    return data.rename(mapping)[list(mapping.values())]


def convert_akshare_stock_index_ticks(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns={
        "date": "timestamp",
        "volume": "vol"
    }, inplace=True)

    df["timestamp"] = df["timestamp"].astype(str)
    return df


def convert_akshare_minute_bar(df: pd.DataFrame) -> pd.DataFrame:
    mappings = {
        '时间': 'timestamp',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'vol',
    }
    df.rename(columns=mappings, inplace=True)
    return df[list(mappings.values())]


def convert_akshare_current_quotation(df: pd.DataFrame) -> pd.DataFrame:
    mappings = {
        "代码": "code",
        "名称": "company",
        "涨跌幅": "pct_chg",
        "涨跌额": "chg",
        "成交量": "vol",
        "最新价": "close",  # kinda weird..
        "最高": "high",
        "最低": "low",
        "今开": "open",
        "换手率": "turnover",
    }
    df.rename(columns=mappings, inplace=True)
    return df[list(mappings.values())]


broad_index_code_name_mapping = {
    "sh000001": "SSE",
    "sz399001": "SZSE",
    "sz399006": "ChiNext",
    "sh000688": "STAR",
    "sh000300": "CSI-300",
    "sh000905": "CSI-500",
    "sh000852": "CSI-1000",
    "sh000016": "SSE-50",
}


def convert_broad_based_index_code_to_name(code: str) -> str:
    return broad_index_code_name_mapping[code]


def convert_broad_based_index_name_to_code(name: str) -> str:
    for code, index_name in broad_index_code_name_mapping.items():
        if index_name == name:
            return code
    raise ValueError(f'Unknown index name {name}')


def convert_akshare_broad_based_index_current_quote(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "最新价": "close",
        "最高": "high",
        "最低": "low",
        "今开": "open",
        "成交量": "vol",
        "代码": "code",
        "名称": "name",
        "涨跌额": "chg",
        "涨跌幅": "pct_chg",
    }
    df.rename(columns=mapping, inplace=True)
    return df[list(mapping.values())]
