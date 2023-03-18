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

    cash_amount = f"{_prefix}:account-free-cash-amount"
    indicators_df = f'{_prefix}:indicators-dataframe'
    latest_adjust_factors = f'{_prefix}:latest-adjust-factors'

    compute_indicators = f'{_prefix}:lock:compute-indicators'


class Timeouts:
    download_quote = 3
    handle_pre_market_open_call = 60 * 4
    pre_compute_indicators = 60 * 3
    handle_cont_trade = 2
    handle_cont_trade_pre_close = 60 * 3
