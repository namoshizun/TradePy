import pandas as pd
from datetime import date
from dateutil import parser as date_parse

from tradepy.types import BroadIndexType, ExchangeType, MarketType, Markets
from tradepy.vendors.types import AskBid


def convert_code_to_market(code: str) -> MarketType:
    mapping: dict[tuple, MarketType] = {
        ("688",): "科创板",
        ("689",): "CDR",
        ("002",): "深证主板",
        ("300", "301"): "创业板",
        ("600", "601", "603", "605"): "上证主板",
        ("000", "001", "003"): "深证主板",
        ("8",): "北交所",
        ("43",): "新三板",
    }

    for prefix, market in mapping.items():
        if code.startswith(prefix):
            return market

    raise ValueError(f"Unknown code {code}")


def convert_code_to_exchange(code: str) -> ExchangeType:
    market = convert_code_to_market(code)
    match market:
        case Markets.STAR | Markets.SH_MAIN | Markets.CDR:
            return "SH"
        case Markets.CHI_NEXT | Markets.SZ_MAIN | Markets.SME:
            return "SZ"
        case Markets.BSE:
            return "BJ"
    raise ValueError(f"Invalid code {code}")


def convert_to_tushare_date(dt: date) -> str:
    return dt.strftime("%Y%m%d")


def convert_tushare_date_to_iso_format(value: str) -> str:
    return date_parse.parse(value).date().isoformat()


def convert_akshare_hist_data(df: pd.DataFrame):
    fields_map = {
        "日期": "timestamp",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "vol",
        "涨跌幅": "pct_chg",
        "涨跌额": "chg",
        "换手率": "turnover",
    }
    df.rename(columns=fields_map, inplace=True)
    df["timestamp"] = df["timestamp"].astype(str)
    return df[list(fields_map.values())]


def convert_akshare_stock_info(data: dict) -> dict:
    to_100_mil = lambda v: v * 1e-8

    name_translation_and_convert = {
        "总市值": ("mkcap", to_100_mil),
        "行业": ("sector",),
        "上市时间": ("listdate",),
        "股票代码": ("code",),
        "股票简称": ("company",),
        "总股本": ("total_share", to_100_mil),
        "流通股": ("float_share", to_100_mil),
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
        "板块名称": "name",
        "板块代码": "code",
        "最新价": "close",
        "涨跌额": "chg",
        "涨跌幅": "pct_chg",
        "总市值": "mkcap",
        "换手率": "turnover",
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


def convert_akshare_sector_current_quote(data: dict[str, float]) -> dict[str, float]:
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
    return {en_key: data[ch_key] for ch_key, en_key in mapping.items()}


def convert_akshare_stock_index_ticks(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns={"date": "timestamp", "volume": "vol"}, inplace=True)

    df["timestamp"] = df["timestamp"].astype(str)
    return df


def convert_akshare_minute_bar(df: pd.DataFrame) -> pd.DataFrame:
    mappings = {
        "时间": "timestamp",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "vol",
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


broad_index_code_name_mapping: dict[str, BroadIndexType] = {
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
    raise ValueError(f"Unknown index name {name}")


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


def convert_akshare_restricted_releases_records(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "股票代码": "code",
        "股票简称": "company",
        "解禁时间": "timestamp",
        "限售股类型": "shares_category",
        "解禁数量": "num_shares",
        "实际解禁数量": "num_shares_released",
        "实际解禁市值": "mkcap_released",
        "占解禁前流通市值比例": "pct_shares",
    }
    df = df.rename(columns=mapping)[list(mapping.values())]
    df["timestamp"] = df["timestamp"].astype(str)
    df["pct_shares"] *= 100
    df.set_index("code", inplace=True)
    df.sort_values(["code", "timestamp"], inplace=True)
    df["index"] = df.groupby("code").cumcount()
    return df.round(3)


def convert_etf_current_quote(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "代码": "code",
        "名称": "name",
        "最新价": "close",
        "涨跌幅": "pct_chg",
        "涨跌额": "chg",
        "成交量": "vol",
        "最高价": "high",
        "最低价": "low",
        "开盘价": "open",
        "换手率": "turnover",
        "总市值": "mkt_cap",
    }
    df.rename(columns=mapping, inplace=True)
    df = df[list(mapping.values())].copy()
    df["mkt_cap"] *= 1e-8  # convert to 100 mils
    df["mkt_cap"] = df["mkt_cap"].round(3)
    df.sort_values("mkt_cap", inplace=True)
    return df


def convert_etf_day_bar(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "日期": "timestamp",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "vol",
        "换手率": "turnover",
    }
    df.rename(columns=mapping, inplace=True)
    return df[list(mapping.values())]


def convert_stock_futures_day_bar(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "日期": "timestamp",
        "开盘价": "open",
        "收盘价": "close",
        "最高价": "high",
        "最低价": "low",
        "成交量": "vol",
        "持仓量": "open_interest",
    }
    df.rename(columns=mapping, inplace=True)
    df["timestamp"] = df["timestamp"].astype(str)
    return df[list(mapping.values())]


def convert_stock_ask_bid_df(
    df: pd.DataFrame,
) -> AskBid:
    df.set_index("item", inplace=True)
    result = {"buy": [], "sell": []}

    for i in range(1, 6):
        result["sell"].append(df.loc[f"sell_{i}", "value"])
        result["buy"].append(df.loc[f"buy_{i}", "value"])
    return result  # type: ignore


def convert_stock_sz_name_changes(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {"变更日期": "timestamp", "变更后简称": "company", "证券代码": "code"}
    df.rename(columns=mapping, inplace=True)
    df = df[list(mapping.values())].copy()
    df["timestamp"] = df["timestamp"].astype(str)
    df.set_index(["code", "timestamp"], inplace=True)
    df.sort_index(inplace=True)
    return df
