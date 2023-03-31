import abc

import tradepy
import redis
import contextvars
from pydantic import BaseModel
from typing import Iterable, TypeVar, Generic
from contextlib import contextmanager
from tradepy.constants import CacheKeys
from tradepy.core.account import Account
from tradepy.core.position import Position
from tradepy.core.order import Order


client_var = contextvars.ContextVar("redis_client")


@contextmanager
def use_redis(client: redis.Redis):
    token = client_var.set(client)
    try:
        yield
        client = client_var.get(token)
        if isinstance(client, redis.client.Pipeline):
            client.execute()
    finally:
        client_var.reset(token)


def get_redis() -> redis.Redis:
    try:
        return client_var.get()
    except LookupError:
        return tradepy.config.get_redis_client()


ItemType = TypeVar("ItemType", bound=BaseModel)


class CacheItem(Generic[ItemType]):

    @staticmethod
    @abc.abstractmethod
    def exists(*args):
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def set(item: ItemType):
        raise NotImplementedError

    @classmethod
    def set_many(cls, instances: Iterable[ItemType]):
        for instance in instances:
            cls.set(instance)

    @staticmethod
    @abc.abstractmethod
    def get(*args) -> ItemType | None:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def get_many(*args) -> list[ItemType] | None:
        raise NotImplementedError


class PositionCache(CacheItem[Position]):

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


class OrderCache(CacheItem[Order]):

    @staticmethod
    def exists(order_id: str):
        return get_redis().hexists(CacheKeys.orders, order_id)

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


class AccountCache(CacheItem[Account]):

    @staticmethod
    def set(account: Account):
        get_redis().set(
            CacheKeys.account,
            account.json()
        )

    @staticmethod
    def get() -> Account | None:
        if raw := get_redis().get(CacheKeys.account):
            return Account.parse_raw(raw)
