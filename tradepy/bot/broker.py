import functools
import json
import os
import tradepy

from tradepy.constants import CacheKeys
from tradepy.core.position import Position
from tradepy.core.order import Order


LOG = tradepy.LOG


class BrokerAPI:

    broker_api_url = os.environ["TRADE_BROKER_API_URL"]

    @staticmethod
    def get_orders():
        return [
            {
                "id": "10086",
                "timestamp": "2021-01-01 09:30:00",
                "code": "000333",
                "company": "美的集团",
                "price": 50,
                "filled_price": None,
                "shares": 1000,
                "status": "pending",
                "direction": "buy",
            }
        ]

    @staticmethod
    def get_positions():
        return [
            {
                "id": "10086",
                "timestamp": "2021-01-01 09:30:00",
                "code": "000333",
                "company": "美的集团",
                "price": 50.0,
                "shares": 1000,
            }
        ]

    @staticmethod
    def get_account_info():
        return {
            "free_cash_amount": 1e5,
        }


def get_or_set_source_cache(key: str, refetch_fun):
    """
    Read the original data from redis store if exists, otherwise call the refetch function
    to retrieve it from the Broker API and then cache it. It is expected that the
    broker service will update the cache once the original data is updated.
    """
    def inner(fun):
        @functools.wraps(fun)
        def decor(*args, **kwargs):
            redis_client = tradepy.config.get_redis_client()
            if value := redis_client.get(key):
                return fun(json.loads(value), *args, **kwargs)

            result = refetch_fun()
            if not redis_client.exists(key):
                # In case the data is updated by the broker service in the meantime
                redis_client.set(key, json.dumps(result))
            else:
                # Oops there it is, the broker service has updated the data
                value = redis_client.get(key)
                result = json.loads(value)  # type: ignore

            return fun(result, *args, **kwargs)
        return decor
    return inner


@get_or_set_source_cache(CacheKeys.cash_amount, refetch_fun=BrokerAPI.get_account_info)
def get_account_free_cash_amount(account_info) -> float:
    return account_info["free_cash_amount"]


@get_or_set_source_cache(CacheKeys.orders, refetch_fun=BrokerAPI.get_orders)
def get_orders(orders) -> list[Order]:
    return [
        Order(
            id=x["id"],
            timestamp=x["timestamp"],
            code=x["code"],
            price=x["price"],
            filled_price=x["filled_price"],
            vol=x["vol"],
            status=x["status"],
            direction=x["direction"],
        )
        for x in orders
    ]


@get_or_set_source_cache(CacheKeys.positions, refetch_fun=BrokerAPI.get_positions)
def get_positions(positions, available_only: bool = False) -> list[Position]:
    if available_only:
        on_sell_lst = set(o.code for o in get_orders() if o.direction == "sell")  # type: ignore
    else:
        on_sell_lst = set()

    return [
        Position(
            id=x["id"],
            timestamp=x["timestamp"],
            code=x["code"],
            price=x["price"],
            vol=x["shares"],
        )
        for x in positions
        if x["code"] not in on_sell_lst
    ]


def place_orders(orders: list[Order]):
    # TODO
    ...
