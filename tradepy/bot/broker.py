import os
import tradepy
import requests as rq

from tradepy.core.position import Position
from tradepy.core.order import Order


LOG = tradepy.LOG
BROKER_API_URL = f'http://{os.environ["TRADE_BROKER_HOST"]}:{os.environ["TRADE_BROKER_PORT"]}'


def get_url(path) -> str:
    return os.path.join(BROKER_API_URL, path)


class BrokerAPI:

    @staticmethod
    def get_orders() -> list[Order]:
        res = rq.get(get_url("orders"))
        return [Order(**x) for x in res.json()]

    @staticmethod
    def get_positions(available_only=False) -> list[Position]:
        res = rq.get(get_url("positions"), params={
            "available": available_only
        })
        return [Position(**x) for x in res.json()]

    @staticmethod
    def get_account_free_cash_amount() -> float:
        res = rq.get(get_url("account"))
        return res.json()["free_cash"]

    @staticmethod
    def place_orders(orders: list[Order]):
        res = rq.post(get_url("orders"), json=[
            o.dict()
            for o in orders
        ]).json()

        if res["succ"] != len(orders):
            tradepy.LOG.warn(f'部分下单失败: {res}')
