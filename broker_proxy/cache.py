import abc
import json
import tradepy
from tradepy.constants import CacheKeys
from tradepy.core.position import Position
from tradepy.core.order import Order


def get_redis():
    return tradepy.config.get_redis_client()


class CacheItem:

    @staticmethod
    @abc.abstractmethod
    def exists(*args):
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def set(*args):
        raise NotImplementedError

    @classmethod
    def set_many(cls, instances: list):
        for instance in instances:
            cls.set(instance)

    @staticmethod
    @abc.abstractmethod
    def get(*args):
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def get_many(*args):
        raise NotImplementedError


class PositionCache(CacheItem):

    @staticmethod
    def set(position: Position):
        get_redis().hset(
            CacheKeys.positions,
            position.id,
            position.json()
        )

    @staticmethod
    def get(position_id: str) -> Position | None:
        if raw := get_redis().hget(CacheKeys.positions, position_id):
            return Position.parse_raw(raw)

    @staticmethod
    def get_many() -> list[Position] | None:
        r = get_redis()
        if not r.exists(CacheKeys.positions):
            return None
        return [
            Position.parse_raw(raw)
            for _, raw in get_redis().hgetall(CacheKeys.positions).items()
        ]


class OrderCache(CacheItem):

    @staticmethod
    def exists(order: Order):
        return get_redis().hexists(CacheKeys.orders, order.id)

    @staticmethod
    def set(order: Order):
        assert order.id is not None
        get_redis().hset(
            CacheKeys.orders,
            order.id,
            order.json()
        )

    @staticmethod
    def get(order_id: str) -> Order | None:
        if raw := get_redis().hget(CacheKeys.orders, order_id):
            return Order.parse_raw(raw)

    @staticmethod
    def get_many() -> list[Order] | None:
        r = get_redis()
        if not r.exists(CacheKeys.orders):
            return None
        return [
            Order.parse_raw(raw)
            for _, raw in get_redis().hgetall(CacheKeys.orders).items()
        ]


class AccountCache(CacheItem):

    @staticmethod
    def set(account: dict):
        get_redis().set(
            CacheKeys.account,
            json.dumps(account)
        )

    @staticmethod
    def get() -> dict | None:
        if raw := get_redis().get(CacheKeys.account):
            return json.loads(raw)
