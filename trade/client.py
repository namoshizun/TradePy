import time
import datetime
import tushare
import pandas as pd
import akshare as ak
from functools import wraps
from datetime import date
from dateutil import parser as date_parser

from trade.utils import get_latest_trade_date
from trade.convertion import (
    convert_tushare_v1_hist_data,
    convert_tushare_v2_hist_data,
    convert_tushare_v2_fundamentals_data,
    convert_to_ts_date,
    convert_ts_date_to_iso_format,
    convert_akshare_hist_data,
)



class AkShareClient:

    def get_daily(self, code: str, start_date: datetime.date | str):
        if isinstance(start_date, str):
            start_date = datetime.date.fromisoformat(start_date)

        df = ak.stock_zh_a_hist(
            symbol=code,
            start_date=start_date.strftime('%Y%m%d'),
            period="daily"
        )
        return convert_akshare_hist_data(df)


class TushareClient:

    def get_daily(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError

    def get_weekly(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError

    def get_monthly(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError

    def get_15m(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError

    def get_60m(self, *args, **kwargs) -> pd.DataFrame:
        raise NotImplementedError



class TushareClientV1(TushareClient):

    def __init__(self) -> None:
        self.api = tushare

    def _get_hist_data_adaptor(self, code, start=None, end=None, ktype=None, **kwargs) -> pd.DataFrame:
        """
        code: 股票代码, 即6位数字代码, 或者指数代码(sh=上证指数 sz=深圳成指 hs300=沪深300指数 sz50=上证50 zxb=中小板 cyb=创业板）
        start: 开始日期, 格式YYYYMMDD
        end: 结束日期, 格式YYYYMMDD
        retry_count: 当网络异常后重试次数, 默认为3
        pause: 重试时停顿秒数, 默认为0
        """
        if '.' in code:
            # Convert 000333.SZ to 000333
            code = code.split('.')[0]

        def _normalize_date(date: str) -> str:
            # Convert YYYYMMDD to YYYY-MM-DD
            dt_obj = date_parser.parse(date)
            return dt_obj.strftime('%Y-%m-%d')

        if start:
            start = _normalize_date(start)

        if end:
            end = _normalize_date(end)

        df = self.api.get_hist_data(code, start, end, ktype, **kwargs)

        if not isinstance(df, pd.DataFrame):
            raise Exception(f'No data is fetched for code {code}. Result is {df}')

        return convert_tushare_v1_hist_data(df.reset_index())

    def get_daily(self, *args, **kwargs) -> pd.DataFrame:
        kwargs['ktype'] = 'D'
        return self._get_hist_data_adaptor(*args, **kwargs)
    
    def get_weekly(self, *args, **kwargs) -> pd.DataFrame:
        kwargs['ktype'] = 'W'
        return self._get_hist_data_adaptor(*args, **kwargs)
    
    def get_monthly(self, *args, **kwargs) -> pd.DataFrame:
        kwargs['ktype'] = 'M'
        return self._get_hist_data_adaptor(*args, **kwargs)
    
    def get_15m(self, *args, **kwargs) -> pd.DataFrame:
        kwargs['ktype'] = '15'
        return self._get_hist_data_adaptor(*args, **kwargs)

    def get_60m(self, *args, **kwargs) -> pd.DataFrame:
        kwargs['ktype'] = '60'
        return self._get_hist_data_adaptor(*args, **kwargs)


def with_retry_and_normalize(wait_interval, normalizer=None):
    def decor(fetch_fun):
        @wraps(fetch_fun)
        def inner(self, *args, **kwargs):
            def do_fetch():
                df = fetch_fun(self, *args, **kwargs)
                if normalizer:
                    return normalizer(df)

            try:
                return do_fetch()
            except Exception as exc:
                if '最多访问该接口' in str(exc):
                    time.sleep(wait_interval)
                    return do_fetch()
                raise exc
        return inner
    return decor


class TushareClientPro(TushareClient):

    def __init__(self, token: str):
        self.api = tushare.pro_api(token=token)

    @with_retry_and_normalize(65, normalizer=convert_tushare_v2_hist_data)
    def get_daily(self, *args, **kwargs) -> pd.DataFrame:
        return self.api.daily(*args, **kwargs)
        # return self.api.stk_factor(*args, **kwargs)

    @with_retry_and_normalize(65, normalizer=convert_tushare_v2_fundamentals_data)
    def get_company_fundamentals(self, trade_date=None, ts_code=None) -> pd.DataFrame:
        if trade_date:
            assert len(trade_date) == 8, date  # e.g, 20220101
        else:
            _day = get_latest_trade_date()
            trade_date = _day.strftime('%Y%m%d')

        print(f'Fetch listings on {trade_date}. TS Code = {ts_code}')

        df = self.api.bak_basic(trade_date=trade_date, ts_code=ts_code)
        return df

    def get_trade_cal(self, start_date: date | str | None=None, end_date: date | str | None=None) -> pd.DataFrame:
        _start = start_date or date.fromisoformat("2000-01-01")
        _end = end_date or get_latest_trade_date()

        if isinstance(_start, str):
            _start = date_parser.parse(_start)

        if isinstance(_end, str):
            _end = date_parser.parse(_end)

        df = self.api.trade_cal(
            start_date=convert_to_ts_date(_start),
            end_date=convert_to_ts_date(_end),
            is_open="1"
        )
        df["cal_date"] = df["cal_date"].map(convert_ts_date_to_iso_format)
        return df.set_index("cal_date").sort_index()


class TushareProQuotas:

    DAILY = 500 # per min
