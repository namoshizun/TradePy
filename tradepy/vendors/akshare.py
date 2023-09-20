import datetime
import time
import traceback
import pandas as pd
import akshare as ak
from loguru import logger
from typing import Any, Literal
from functools import wraps

import tradepy
from tradepy.conversion import (
    convert_akshare_broad_based_index_current_quote,
    convert_akshare_current_quotation,
    convert_akshare_minute_bar,
    convert_broad_based_index_name_to_code,
    convert_code_to_exchange,
    convert_akshare_hist_data,
    convert_akshare_stock_info,
    convert_akshare_sector_listing,
    convert_akshare_sector_day_bars,
    convert_akshare_stock_index_ticks,
    convert_code_to_market,
    convert_akshare_sector_current_quote,
    convert_akshare_restricted_releases_records,
    convert_etf_current_quote,
    convert_etf_day_bar,
    convert_stock_futures_day_bar,
    convert_stock_ask_bid_df,
    convert_stock_sz_name_changes,
)
from tradepy.utils import get_latest_trade_date
from tradepy.depot.stocks import StocksDailyBarsDepot
from tradepy.vendors.types import AskBid


def retry(max_retries=3, wait_interval=5):
    def decor(fun):
        @wraps(fun)
        def inner(*args, **kwargs):
            n = 0
            while n < max_retries:
                try:
                    return fun(*args, **kwargs)
                except Exception:
                    tradepy.LOG.warn(
                        f"接口报错，{wait_interval}秒后尝试第{n + 1}/{max_retries}次. \n\n {traceback.format_exc()}"
                    )
                    time.sleep(wait_interval)
                    n += 1

        return inner

    return decor


class AkShareClient:
    # ----
    # Misc
    def get_restricted_releases(
        self,
        start_date: datetime.date | str,
        end_date: datetime.date | str | None = None,
    ) -> pd.DataFrame:
        if isinstance(start_date, str):
            start_date = datetime.date.fromisoformat(start_date)

        if isinstance(end_date, str):
            end_date = datetime.date.fromisoformat(end_date)

        if not end_date:
            today = datetime.date.today()
            end_date = datetime.date(today.year, 12, 31)

        df = ak.stock_restricted_release_detail_em(
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
        )
        return convert_akshare_restricted_releases_records(df)

    def get_adjust_factor(self, code: str, adjust: Literal["hfq", "qfq"] = "hfq"):
        if code == "689009":
            # Oh well...
            return pd.DataFrame()

        exchange = convert_code_to_exchange(code)
        symbol = f"{exchange.lower()}{code}"

        df = retry()(ak.stock_zh_a_daily)(symbol=symbol, adjust=f"{adjust}-factor")
        assert isinstance(df, pd.DataFrame)
        df.rename(columns={"date": "timestamp"}, inplace=True)

        df["code"] = code
        df["timestamp"] = df["timestamp"].dt.date.astype(str)
        return df

    @retry()
    def get_a_stocks_list(self) -> pd.DataFrame:
        all_df = ak.stock_info_a_code_name().set_index("code")
        A_board = [
            code
            for code in all_df.index
            if convert_code_to_market(code) in ("科创板", "创业板", "上证主板", "深证主板")
        ]
        return all_df.loc[A_board]

    # ------
    # Stocks
    def get_stock_daily(
        self,
        code: str,
        start_date: datetime.date | str,
        end_date: datetime.date | str = "20500101",
    ):
        def fetch_legu_indicators():
            indicators_df = ak.stock_a_indicator_lg(symbol=code)
            assert isinstance(indicators_df, pd.DataFrame)
            indicators_df.rename(
                columns={
                    "trade_date": "timestamp",
                    "total_mv": "mkt_cap",
                },
                inplace=True,
            )
            indicators_df["mkt_cap"] *= 1e-4  # Convert to 100 mils
            indicators_df["mkt_cap"] = indicators_df["mkt_cap"].round(4)
            indicators_df["timestamp"] = indicators_df["timestamp"].astype(str)
            return indicators_df

        def fetch_baidu_indicators(start_date: datetime.date):
            today = datetime.date.today()
            date_diff = today - start_date

            if date_diff.days < 365:
                period = "近一年"
            elif date_diff.days < 365 * 3:
                period = "近三年"
            elif date_diff.days < 365 * 5:
                period = "近五年"
            elif date_diff.days < 365 * 10:
                period = "近十年"
            else:
                period = "全部"

            res = ak.stock_zh_valuation_baidu(
                symbol=code, indicator="总市值", period=period
            )
            res.rename(
                columns={
                    "date": "timestamp",
                    "value": "mkt_cap",  # already is in 100 mils
                },
                inplace=True,
            )
            res["timestamp"] = res["timestamp"].astype(str)
            res.set_index("timestamp", inplace=True)
            return res

        if isinstance(start_date, str):
            start_date = datetime.date.fromisoformat(start_date)

        if isinstance(end_date, str):
            end_date = datetime.date.fromisoformat(end_date)

        # Fetch the day-k data
        df = retry()(ak.stock_zh_a_hist)(
            symbol=code,
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            period="daily",
        )

        assert isinstance(df, pd.DataFrame)
        if df.empty:
            print(f"未找到{code}日K数据. 起始日期{start_date}")
            return df

        df = convert_akshare_hist_data(df)

        try:
            indicators_df: pd.DataFrame = retry()(fetch_baidu_indicators)(start_date)  # type: ignore
            # When the period spans a long time, the data may be incomplete.
            # So we need to first calculate the number of shares on days where the data is complete,
            # interpolate the shares number in other days, then calculate those days' market cap.
            df.set_index("timestamp", inplace=True)
            df["shares"] = indicators_df["mkt_cap"] / df["close"]
            df["shares"].bfill(inplace=True)
            df["mkt_cap"] = (df["shares"] * df["close"]).round(2)
            df.drop("shares", axis=1, inplace=True)
            df.reset_index(inplace=True)
            return df
        except Exception:
            logger.warning(f"无法从百度获取{code}的资产技术指标数据, 将使用乐估的数据!")
            try:
                indicators_df = fetch_legu_indicators()
            except Exception:
                logger.warning(f"依旧无法获取{code}的资产技术指标数据, 将使用最近一日的指标!")
                fields = ["timestamp", "mkt_cap"]
                try:
                    indicators_df = (
                        StocksDailyBarsDepot()
                        .load([code], fields=",".join(fields))
                        .sort_values("timestamp")
                        .iloc[-1]
                        .to_frame()
                        .T
                    )
                except FileNotFoundError:
                    logger.error(f"无法在本地找到{code}的资产技术指标数据, 请检查是否已经下载过该股票的日线数据!")
                    return pd.DataFrame()

                indicators_df.reset_index(inplace=True)
                indicators_df = indicators_df[fields]
            return pd.merge(df, indicators_df, on="timestamp")

    def get_minute_bar(
        self, code: str, start_date: datetime.date | str, period="1"
    ) -> pd.DataFrame:
        if isinstance(start_date, str):
            start_date = datetime.date.fromisoformat(start_date)

        df = ak.stock_zh_a_hist_min_em(
            symbol=code, start_date=str(start_date), period=period
        )
        df = convert_akshare_minute_bar(df)
        return df

    @retry()
    def get_stock_info(self, code: str) -> dict[str, Any]:
        df = ak.stock_individual_info_em(symbol=code)

        return convert_akshare_stock_info(
            {row["item"]: row["value"] for _, row in df.iterrows()}
        )

    def get_stock_current_quote(self) -> pd.DataFrame:
        df = ak.stock_zh_a_spot_em()
        df = convert_akshare_current_quotation(df)
        df.set_index("code", inplace=True)
        return df

    def get_stock_ask_bid(self, code: str) -> AskBid:
        df: pd.DataFrame = ak.stock_bid_ask_em(symbol=code)  # type: ignore
        return convert_stock_ask_bid_df(df)

    def get_stock_sz_name_changes(self) -> pd.DataFrame:
        df = ak.stock_info_sz_change_name("简称变更")
        return convert_stock_sz_name_changes(df)

    @retry()
    def get_stock_name_changes_list(self, code: str, current_name: str) -> list[str]:
        df = ak.stock_profile_cninfo(code)
        raw = df["曾用简称"].iloc[0]
        if not raw:
            return []

        return list(map(str.strip, raw.split(">>"))) + [current_name]

    # -----
    # Index
    def get_sector_index_day_bars(self, name: str) -> pd.DataFrame:
        df = ak.stock_board_industry_hist_em(
            symbol=name, start_date="20000101", end_date="20990101", period="日k"
        )
        df = convert_akshare_sector_day_bars(df)
        df["name"] = name
        return df

    def get_sector_index_current_quote(self, name_or_code: str) -> dict[str, Any]:
        df = ak.stock_board_industry_spot_em(name_or_code)
        data = convert_akshare_sector_current_quote(
            {row.item: row.value for row in df.itertuples()}
        )
        data["timestamp"] = str(get_latest_trade_date())  # type: ignore
        return data

    def get_sectors_listing(self) -> pd.DataFrame:
        return convert_akshare_sector_listing(ak.stock_board_industry_name_em())

    def get_broad_based_index_day_bars(
        self, code: str, start_date: str = "1900-01-01"
    ) -> pd.DataFrame:
        df = ak.stock_zh_index_daily(symbol=code)
        df = convert_akshare_stock_index_ticks(df)
        return df.query("timestamp >= @start_date")

    def get_broad_based_index_current_quote(self, *names: str):
        df = ak.stock_zh_index_spot()
        df = convert_akshare_broad_based_index_current_quote(df).set_index("code")

        codes = list(map(convert_broad_based_index_name_to_code, names))
        df = df.loc[codes].copy()
        df["timestamp"] = str(get_latest_trade_date())
        return df

    # ---
    # ETF
    def get_etf_listing(self) -> pd.DataFrame:
        df = self.get_etf_current_quote()
        return df[["code", "name", "mkt_cap"]].copy()

    def get_etf_current_quote(self) -> pd.DataFrame:
        df = ak.fund_etf_spot_em()
        df["timestamp"] = str(get_latest_trade_date())
        return convert_etf_current_quote(df)

    def get_etf_daily(self, code: str, start_date: datetime.date | str) -> pd.DataFrame:
        if isinstance(start_date, str):
            start_date = datetime.date.fromisoformat(start_date)

        df = ak.fund_etf_hist_em(symbol=code, start_date=start_date.strftime("%Y%m%d"))
        return convert_etf_day_bar(df)

    # -------
    # Futures
    def get_stock_futures_daily(
        self,
        index_name: Literal["IF", "IH", "IC"],
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
    ) -> pd.DataFrame:
        date_range_args = {}
        if isinstance(start_date, str):
            start_date = datetime.date.fromisoformat(start_date)
            date_range_args["start_date"] = start_date.strftime("%Y%m%d")

        if isinstance(end_date, str):
            end_date = datetime.date.fromisoformat(end_date)
            date_range_args["end_date"] = end_date.strftime("%Y%m%d")

        df = retry()(ak.futures_main_sina)(symbol=f"{index_name}0", **date_range_args)
        return convert_stock_futures_day_bar(df)
