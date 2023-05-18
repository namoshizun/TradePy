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
    @classproperty
    def prefix(cls):
        return f"tradepy:{date.today()}"

    @classproperty
    def account(self):
        return f"{self.prefix}:broker:account"

    @classproperty
    def orders(self):
        return f"{self.prefix}:broker:orders"

    @classproperty
    def positions(self):
        return f"{self.prefix}:broker:positions"

    @classproperty
    def indicators_df(self):
        return f"{self.prefix}:dataset:indicators-dataframe"

    @classproperty
    def close_indicators_df(self):
        return f"{self.prefix}:dataset:close-indicators-dataframe"

    @classproperty
    def compute_open_indicators(self):
        return f"{self.prefix}:lock:compute-open-indicators"

    @classproperty
    def compute_close_indicators(self):
        return f"{self.prefix}:lock:compute-close-indicators"

    @classproperty
    def update_assets(self):
        return f"{self.prefix}:lock:cache-update"


class Timeouts:
    download_quote = 3
    handle_pre_market_open_call = 60 * 4
    compute_open_indicators = 60 * 3.55
    compute_close_indicators = 60 * 3
    handle_cont_trade = 2
    handle_cont_trade_pre_close = 60 * 2
