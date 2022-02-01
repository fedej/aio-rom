from __future__ import annotations

import asyncio
from abc import ABC, ABCMeta, abstractmethod
from collections import UserList
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    ClassVar,
    Collection,
    Generic,
    Iterable,
    List,
    MutableSequence,
    Set,
    Type,
    TypeVar,
)

from aioredis.client import Pipeline, Redis

from .exception import ModelNotFoundException
from .fields import Field, deserialize, serialize
from .session import connection, transaction
from .types import IModel, Key, RedisValue, Serializable

T = TypeVar("T", bound=Serializable)

maps: dict[tuple[type, type], type] = {}


class GenericCollection(Generic[T]):
    def __class_getitem__(
        cls,
        params: type[Any] | tuple[type[Any], ...],
        **kwargs: Any,
    ) -> type[RedisCollection[T]]:
        if not isinstance(params, tuple):
            params = (params,)
        # if not isinstance(params[0], type):
        #    raise TypeError(f"{cls.__name__}[{params[0]}], {params[0]} is not a type")

        global maps
        if (cls, params[0]) not in maps:
            cls_dict = dict(cls.__dict__)
            cls_dict["item_class"] = params[0]
            maps[cls, params[0]] = type(cls.__name__, (cls,), cls_dict)
        return maps[cls, params[0]]


class RedisCollection(
    GenericCollection[T],
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

    def __getattr__(self, item: str) -> Any:
        return getattr(self.values, item)


class RedisSet(RedisCollection[T], Set[T]):
    @RedisCollection.values.setter  # type: ignore[attr-defined,misc]
    def values(self, values: Collection[T] | None) -> None:
        super(RedisSet, type(self)).values.fset(  # type: ignore[attr-defined]
            self, set(values) if values else []
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


@deserialize.register(RedisList)
@deserialize.register(RedisSet)
async def _(
    field_type: type[RedisList[T]] | type[RedisSet[T]],
    value: Key,
    field: Field | None = None,
) -> RedisCollection[T]:
    collection = await field_type.get(value)
    if field and field.eager:
        await collection.load()
    return collection
