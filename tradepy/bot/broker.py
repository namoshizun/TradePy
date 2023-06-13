import os
import tradepy
import requests as rq

import tradepy
from tradepy.core.models import Account, Position, Order


LOG = tradepy.LOG
BROKER_API_URL = (
    f"http://{tradepy.config.trading.broker.host}:{tradepy.config.trading.broker.port}"
)


def get_url(path) -> str:
    return os.path.join(BROKER_API_URL, path)


class BrokerAPI:
    @staticmethod
    def warm_db() -> str:
        res = rq.get(get_url("control/warm-db"))
        return res.text

    @staticmethod
    def flush_cache() -> str:
        res = rq.get(get_url("control/flush-cache"))
        return res.text

    @staticmethod
    def get_orders() -> list[Order]:
        res = rq.get(get_url("orders"))
        return [Order(**x) for x in res.json()]

    @staticmethod
    def get_positions(available_only=False) -> list[Position]:
        res = rq.get(get_url("positions"), params={"available": available_only})
        return [Position(**x) for x in res.json()]

    @staticmethod
    def get_account() -> Account:
        res = rq.get(get_url("account"))
        return Account(**res.json())

    @staticmethod
    def place_orders(orders: list[Order]):
        res = rq.post(get_url("orders"), json=[o.dict() for o in orders]).json()

        if len(res["succ"]) != len(orders):
            tradepy.LOG.warn(f"部分下单失败: {res}")

    @staticmethod
    def cancel_orders(orders: list[Order]):
        res = rq.delete(
            get_url("orders"), params={"order_ids": [o.id for o in orders]}
        ).json()

        if len(res["succ"]) != len(orders):
            tradepy.LOG.warn(f"部分撤单失败: {res}")
