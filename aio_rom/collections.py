from __future__ import annotations

import asyncio
import typing
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

from aio_rom.fields import deserialize, serialize
from aio_rom.session import connection, transaction
from aio_rom.types import IModel, Key, RedisValue, Serializable

if typing.TYPE_CHECKING:
    from redis.asyncio.client import Pipeline, Redis

T = TypeVar("T", bound=Serializable)
M = TypeVar("M", bound=IModel)

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

        cls_dict = dict(cls.__dict__)
        cls_dict["item_class"] = params[0]
        slots = cls_dict.pop("__slots__", None)
        generic_collection = type(cls.__name__, (cls,), cls_dict)
        if slots:
            generic_collection.__slots__ = slots  # type: ignore[attr-defined]
        _generic_types_cache[(cls, params[0])] = generic_collection
        return generic_collection


class RedisCollection(
    Generic[T],
    GenericCollection,
    Collection[T],
    IModel,
    AsyncIterable[T],
    Iterable[T],
    metaclass=ABCMeta,
):
    __slots__ = ("_values",)

    item_class: ClassVar[type]

    def __init__(self, values: Collection[T] | None = None, id: Key | None = None):
        self.id = id  # type: ignore[assignment]
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
        return super().db_id()

    @abstractmethod
    async def save_redis_values(
        self,
        tr: Pipeline[str],
        values: Collection[Any],
    ) -> None:
        pass

    async def save(self, *, optimistic: bool = False, cascade: bool = False) -> None:
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
                    *(
                        v.save(optimistic=optimistic)
                        for v in self.values
                        if isinstance(v, IModel)
                    )
                )

    @classmethod
    async def get(cls: Type[RedisCollection[T]], id: Key) -> RedisCollection[T]:
        async with connection() as redis:
            values = []
            for value in await cls.get_redis_values(redis, f"{cls.prefix()}:{str(id)}"):
                deserialized = deserialize(cls.item_class, value)
                if isinstance(deserialized, IModel):
                    await deserialized.refresh()
                    deserialized = getattr(deserialized, "__wrapped__", deserialized)
                values.append(deserialized)

        return cls(id=id, values=values)

    async def refresh(self) -> None:
        fresh = await type(self).get(self.id)
        self.values = fresh.values

    @classmethod
    @abstractmethod
    async def get_redis_values(
        cls, redis: Redis[str], id: Key
    ) -> Collection[RedisValue]:
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

    def __eq__(self, other: object) -> bool:
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

    @classmethod
    async def get_redis_values(
        cls, redis: Redis[str], id: Key
    ) -> Collection[RedisValue]:
        return set(await redis.smembers(id))

    async def save_redis_values(
        self, tr: Pipeline[str], values: Collection[RedisValue]
    ) -> None:
        await tr.sadd(self.db_id(), *values)

    async def async_add(
        self, value: T, optimistic: bool = False, cascade: bool = False
    ) -> None:
        async with connection() as conn:
            await conn.sadd(self.db_id(), serialize(value))
            if cascade and isinstance(value, IModel):
                await value.save(optimistic=optimistic)
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
                item = deserialize(self.item_class, value)
                if isinstance(item, IModel):
                    await item.refresh()
                    item = getattr(item, "__wrapped__", item)
                self.add(item)
                yield item

    def add(self, value: T) -> None:
        self.values.add(value)

    def discard(self, value: T) -> None:
        self.values.discard(value)


class RedisList(RedisCollection[T], UserList):  # type: ignore[type-arg]
    @property  # type: ignore[override]
    def values(self) -> List[T] | None:
        return self.data

    @values.setter
    def values(self, values: Collection[T] | None) -> None:
        self.data = list(values) if values else []

    @classmethod
    async def get_redis_values(
        cls, redis: Redis[str], id: Key
    ) -> Collection[RedisValue]:
        return list(await redis.lrange(id, 0, -1))

    async def total_count(self) -> int:
        async with connection() as conn:
            return int(await conn.llen(self.db_id()))

    async def save_redis_values(
        self, tr: Pipeline[str], values: Collection[RedisValue]
    ) -> None:
        await tr.rpush(self.db_id(), *values)

    async def async_append(
        self, value: T, optimistic: bool = False, cascade: bool = False
    ) -> None:
        async with connection() as conn:
            await conn.rpush(self.db_id(), serialize(value))
            if cascade and isinstance(value, IModel):
                await value.save(optimistic=optimistic)
        self.append(value)

    async def __aiter__(self) -> AsyncIterator[T]:
        async with connection() as conn:
            for index in range(0, await conn.llen(self.db_id())):
                value = await conn.lindex(self.db_id(), index)
                item = deserialize(self.item_class, value)
                if isinstance(item, IModel):
                    await item.refresh()
                    item = getattr(item, "__wrapped__", item)
                self[index] = item
                yield item
