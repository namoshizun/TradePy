from datetime import date


class classproperty:
    def __init__(self, method=None):
        self.fget = method

    def __get__(self, instance, cls=None):
        return self.fget(cls)

    def getter(self, method):
        self.fget = method
        return self


class CacheKeys:
    _prefix = f'tradepy:{date.today()}'

    account = f"{_prefix}:broker:account"
    orders = f"{_prefix}:broker:orders"
    positions = f"{_prefix}:broker:positions"

    indicators_df = f'{_prefix}:dataset:indicators-dataframe'

    compute_open_indicators = f'{_prefix}:lock:compute-open-indicators'
    compute_close_indicators = f'{_prefix}:lock:compute-close-indicators'


class Timeouts:
    download_quote = 3
    handle_pre_market_open_call = 60 * 4
    compute_open_indicators = 60 * 3.55
    compute_close_indicators = 60 * 3
    handle_cont_trade = 2
    handle_cont_trade_pre_close = 60 * 2
