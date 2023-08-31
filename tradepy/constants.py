from datetime import date

from tradepy.types import MarketPhase


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
    def update_assets(self):
        return f"{self.prefix}:lock:cache-update"

    @classproperty
    def hist_k(self):
        return "hist-k-cache"


TRADABLE_PHASES = (
    MarketPhase.PRE_OPEN_CALL_P2,
    MarketPhase.CONT_TRADE,
    MarketPhase.CONT_TRADE_PRE_CLOSE,
)
