import functools
import tradepy
from tradepy.constants import CacheKeys


class BrokerAPI:

    def get_account_info(self):
        return {
            "free_cash_amount": 1e5,
        }


def _cache(key: str):
    def inner(fun):
        @functools.wraps(fun)
        def decor(*args, **kwargs):
            redis_client = tradepy.config.get_redis_client()
            if redis_client.exists(key):
                return redis_client.get(key)

            result = fun(*args, **kwargs)
            redis_client.set(key, result)
            return result
        return decor
    return inner


@_cache(CacheKeys.cash_amount)
def get_account_free_cash_amount() -> float:
    account_info = BrokerAPI().get_account_info()
    return account_info["free_cash_amount"]
