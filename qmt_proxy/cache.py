import abc
import tradepy
import redis
import contextvars
from pydantic import BaseModel
from typing import Callable, Iterable, TypeVar, Generic
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

    get_key: Callable[..., str]
    item_type: type[ItemType]

    @classmethod
    @abc.abstractmethod
    def exists(cls, *args):
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def set(cls, item: ItemType):
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def set_many(cls, items: Iterable[ItemType]):
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def get(cls, *args) -> ItemType | None:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def get_many(cls, *args) -> list[ItemType] | None:
        raise NotImplementedError


class HashmapCacheItem(CacheItem[ItemType]):

    @classmethod
    def set(cls, item: ItemType):
        get_redis().hset(cls.get_key(), item.id, item.json())

    @classmethod
    def set_many(cls, items: Iterable[ItemType]):
        r = get_redis()
        with r.pipeline() as pipe:
            pipe.delete(cls.get_key())
            if items:
                pipe.hset(cls.get_key(), mapping={
                    i.id: i.json()
                    for i in items
                })
            pipe.execute()

    @classmethod
    def get(cls, id: str) -> ItemType | None:
        if raw := get_redis().hget(cls.get_key(), id):
            return cls.item_type.parse_raw(raw)

    @classmethod
    def get_many(cls) -> list[ItemType] | None:
        r = get_redis()
        if not r.exists(cls.get_key()):
            return None
        return [
            cls.item_type.parse_raw(raw)
            for _, raw in r.hgetall(cls.get_key()).items()
        ]


class PositionCache(HashmapCacheItem[Position]):

    get_key = lambda: CacheKeys.positions
    item_type = Position


class OrderCache(HashmapCacheItem[Order]):

    get_key = CacheKeys.orders
    item_type = Order


class AccountCache(CacheItem[Account]):

    get_key = CacheKeys.account

    @classmethod
    def set(cls, account: Account):
        get_redis().set(cls.get_key(), account.json())

    @classmethod
    def get(cls) -> Account | None:
        if raw := get_redis().get(cls.get_key()):
            return Account.parse_raw(raw)
