from __future__ import annotations

import asyncio
from abc import ABCMeta, abstractmethod
from collections import UserList
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    ClassVar,
    Collection,
    Generic,
    Iterable,
    Iterator,
    List,
    MutableSet,
    Type,
    TypeVar,
)

from aioredis.client import Pipeline, Redis

from .exception import ModelNotFoundException
from .fields import deserialize, serialize
from .session import connection, transaction
from .types import IModel, Key, RedisValue, Serializable

T = TypeVar("T", bound=Serializable)

_generic_types_cache: dict[tuple[type[Any], Any | tuple[Any, ...]], type[Any]] = {}
GenericCollectionT = TypeVar("GenericCollectionT", bound="GenericCollection")


class GenericCollection:
    def __class_getitem__(
        cls: type[GenericCollectionT],
        params: type[Any] | tuple[type[Any], ...],
        **kwargs: Any,
    ) -> type[GenericCollectionT]:

        cached = _generic_types_cache.get((cls, params))
        if cached is not None:
            return cached

        if not isinstance(params, tuple):
            params = (params,)
        if not hasattr(cls, "__parameters__"):
            raise TypeError(
                f"Type {cls.__name__} must inherit from "
                "typing.Generic before being parameterized"
            )

        cls_dict = dict(cls.__dict__)
        cls_dict["item_class"] = params[0]
        generic_collection = type(cls.__name__, (cls,), cls_dict)
        _generic_types_cache[(cls, params[0])] = generic_collection
        return generic_collection


class RedisCollection(
    GenericCollection,
    Collection[T],
    IModel,
    Generic[T],
    AsyncIterable[T],
    Iterable[T],
    metaclass=ABCMeta,
):
    item_class: ClassVar[type]

    def __set_name__(self, owner: Any, name: str) -> None:
        self.id: Key | None = name

    def __init__(self, values: Collection[T] | None = None, id: Key | None = None):
        self.id = id
        self.values = values

    @property
    def values(self) -> Collection[T] | None:
        return self._values

    @values.setter
    def values(self, values: Collection[T] | None) -> None:
        self._values = values

    def db_id(self) -> str:
        if not self.id:
            raise AttributeError("id must be set")
        return f"{self.prefix()}:{str(self.id)}"

    @abstractmethod
    async def save_redis_values(
        self,
        tr: Pipeline,
        values: Collection[Any],
    ) -> None:
        pass

    async def save(self, optimistic: bool = False, cascade: bool = False) -> None:
        if not self.values:
            return

        if not self.id:
            raise AttributeError("id must be set")

        watch = [self.db_id()] if optimistic else []
        async with transaction(*watch) as tr:
            await tr.delete(self.db_id())
            await self.save_redis_values(tr, [serialize(v) for v in self.values])
            await tr.sadd(self.prefix(), self.id)
            if cascade:
                await asyncio.gather(
                    *[v.save(optimistic) for v in self.values if isinstance(v, IModel)]
                )

    @classmethod
    async def get(cls: Type[RedisCollection[T]], id: Key) -> RedisCollection[T]:
        key = f"{cls.prefix()}:{str(id)}"
        async with connection() as conn:
            if not bool(await conn.exists(key)):
                raise ModelNotFoundException(f"Collection with key {key} not found")
            return cls(id=id)

    async def load(self) -> None:
        async with connection() as redis:
            self.values = await asyncio.gather(
                *[
                    deserialize(self.item_class, value)
                    for value in await self.get_redis_values(redis)
                ]
            )

    @abstractmethod
    async def get_redis_values(self, redis: Redis) -> Collection[RedisValue]:
        pass

    async def delete(self, cascade: bool = False) -> None:
        if not self.id:
            raise AttributeError("id must be set")

        async with transaction() as tr:
            await tr.delete(self.db_id())
            await tr.srem(self.prefix(), self.id)
            if cascade and self.values:
                await asyncio.gather(
                    *[v.delete() for v in self.values if isinstance(v, IModel)]
                )

    def __len__(self) -> int:
        return 0 if self.values is None else len(self.values)

    def __iter__(self) -> Iterator[T]:
        return iter(self.values or [])

    def __repr__(self) -> str:
        return repr(self.values)

    def __eq__(self, other: Any) -> bool:
        return self.values == (
            other.values if isinstance(other, RedisCollection) else other
        )

    def __contains__(self, item: Any) -> bool:
        return self.values is not None and item in self.values


class RedisSet(RedisCollection[T], MutableSet[T]):
    @RedisCollection.values.setter  # type: ignore[attr-defined,misc]
    def values(self, values: Collection[T] | None) -> None:
        super(RedisSet, type(self)).values.fset(  # type: ignore[attr-defined]
            self, set(values) if values else set()
        )

    async def get_redis_values(self, redis: Redis) -> Collection[RedisValue]:
        return set(await redis.smembers(self.db_id()))

    async def save_redis_values(
        self, tr: Pipeline, values: Collection[RedisValue]
    ) -> None:
        await tr.sadd(self.db_id(), *values)

    async def async_add(
        self, value: T, optimistic: bool = False, cascade: bool = False
    ) -> None:
        async with connection() as conn:
            await conn.sadd(self.db_id(), serialize(value))
            if cascade and isinstance(value, IModel):
                await value.save(optimistic)
        self.add(value)

    async def async_discard(self, value: T, cascade: bool = False) -> None:
        async with connection() as conn:
            await conn.srem(self.db_id(), serialize(value))
            if cascade and isinstance(value, IModel):
                await value.delete()
        self.discard(value)

    async def total_count(self) -> int:
        async with connection() as conn:
            return int(await conn.scard(self.db_id()))

    async def __aiter__(self) -> AsyncIterator[T]:
        async with connection() as conn:
            async for value in conn.sscan_iter(self.db_id()):
                item = await deserialize(self.item_class, value)
                self.add(item)
                yield item

    def add(self, value: T) -> None:
        self.values.add(value)

    def discard(self, value: T) -> None:
        self.values.discard(value)


class RedisList(RedisCollection[T], UserList):  # type: ignore[type-arg]
    @property
    def values(self) -> List[T]:
        return self.data

    @values.setter
    def values(self, values: Collection[T] | None) -> None:
        self.data = list(values) if values else []

    async def get_redis_values(self, redis: Redis) -> Collection[RedisValue]:
        return list(await redis.lrange(self.db_id(), 0, -1))

    async def total_count(self) -> int:
        async with connection() as conn:
            return int(await conn.llen(self.db_id()))

    async def save_redis_values(
        self, tr: Pipeline, values: Collection[RedisValue]
    ) -> None:
        await tr.rpush(self.db_id(), *values)

    async def async_append(
        self, value: T, optimistic: bool = False, cascade: bool = False
    ) -> None:
        async with connection() as conn:
            await conn.rpush(self.db_id(), serialize(value))
            if cascade and isinstance(value, IModel):
                await value.save(optimistic)
        self.append(value)

    async def __aiter__(self) -> AsyncIterator[T]:
        async with connection() as conn:
            for index in range(0, await conn.llen(self.db_id())):
                value = await conn.lindex(self.db_id(), index)
                item = await deserialize(self.item_class, value)
                self.insert(index, item)
                yield item


@serialize.register(RedisCollection)
def _(value: RedisCollection[Any]) -> RedisValue | None:
    return value.id if value else None
