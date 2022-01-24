from __future__ import annotations

import asyncio
import collections.abc
from abc import ABCMeta, abstractmethod
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Collection,
    Generic,
    Iterable,
    Iterator,
    List,
    MutableSequence,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from aioredis.client import Pipeline, Redis
from typing_extensions import get_args

from .exception import ModelNotFoundException
from .fields import deserialize, serialize
from .session import connection, transaction
from .types import IModel, Key, RedisValue, Serializable

T = TypeVar("T")
S = TypeVar("S", bound=Serializable)
M = TypeVar("M", bound=IModel)


class GenericCollection:
    def __class_getitem__(
        cls: type, params: Union[type[Any], tuple[type[Any], ...]], **kwargs: Any
    ) -> type[Any]:
        setattr(cls, "__parameters__", params)
        return cls


class RedisCollection(
    GenericCollection,
    Collection[T],
    IModel,
    Generic[T],
    AsyncIterable[T],
    metaclass=ABCMeta,
):
    def __init__(self, values: Collection[T] | None = None, id: Key | None = None):
        self.id = id
        self.values = values or self.wrap([])

    def db_id(self) -> Key:
        return self.id

    def __eq__(self, other: object) -> bool:
        return self.values == getattr(other, "values", other)

    @property
    def item_class(self) -> type[T]:
        return (
            get_args(self.__orig_class__)[0]
            if hasattr(self, "__orig_class__")
            else self.__parameters__[0]
            if isinstance(self.__parameters__, tuple)
            else self.__parameters__
        )

    @abstractmethod
    async def do_save(
        self,
        tr: Pipeline,
        values: Collection[Any],
        optimistic: bool,
        cascade: bool,
    ) -> None:
        pass

    async def save(self, optimistic: bool = False, cascade: bool = False) -> None:
        if self.values:
            watch = [self.db_id()] if optimistic else []
            async with transaction(*watch) as tr:
                await tr.delete(self.db_id())
                await self.do_save(tr, self.values, optimistic, cascade)

    @classmethod
    async def get(cls: Type[RedisCollection[T]], id: Key) -> RedisCollection[T]:
        async with connection() as conn:
            if not bool(await conn.exists(id)):
                raise ModelNotFoundException(f"Collection with key {str(id)} not found")
            collection = cls(id=id)
            collection.values = await collection.get_values()
            return collection

    async def get_values(self) -> Collection[T]:
        async with connection() as redis:
            return type(self).wrap(
                await asyncio.gather(
                    *[
                        deserialize(self.item_class, value)
                        for value in await self.get_redis_values(redis)
                    ]
                )
            )

    @abstractmethod
    async def get_redis_values(self, redis: Redis) -> Collection[RedisValue]:
        pass

    @classmethod
    @abstractmethod
    def wrap(cls, values: Iterable[T]) -> Collection[T]:
        pass

    def __iter__(self) -> Iterator[T]:
        return iter(self.values or [])

    def __len__(self) -> int:
        return len(self.values) if self.values else 0

    def __contains__(self, x: object) -> bool:
        return self.values is not None and x in self.values

    async def delete(self, cascade: bool = False) -> None:
        async with connection() as conn:
            await conn.delete(self.db_id())


class RedisSet(RedisCollection[S], Generic[S], set[S]):
    @classmethod
    def wrap(cls, values: Iterable[S]) -> Set[S]:
        return set(values)

    async def get_redis_values(self, redis: Redis):
        return await redis.smembers(self.db_id())

    async def do_save(
        self,
        tr: Pipeline,
        values: Collection[RedisValue],
        optimistic: bool,
        cascade: bool,
    ) -> None:
        await tr.sadd(self.db_id(), *values)

    def add(self, value: S) -> None:
        cast(Set[S], self.values).add(value)

    async def async_add(self, value: S, **_: Any) -> None:
        async with connection() as conn:
            await conn.sadd(self.db_id(), getattr(value, "id", value))  # type: ignore
        self.add(value)

    def discard(self, value: S) -> None:
        cast(Set[S], self.values).discard(value)

    async def async_discard(self, value: S) -> None:
        async with connection() as conn:
            await conn.srem(self.db_id(), getattr(value, "id", value))  # type: ignore
        self.discard(value)

    async def total_count(self) -> int:
        async with connection() as conn:
            return int(await conn.scard(self.db_id()))

    async def __aiter__(self) -> AsyncIterator[S]:
        async with connection() as conn:
            async for value in conn.sscan_iter(self.db_id()):
                yield serialize(value)


class RedisList(RedisCollection[S], MutableSequence[S]):
    @classmethod
    def wrap(cls, values: Iterable[S]) -> List[S]:
        return list(values)

    async def get_redis_values(self, redis: Redis) -> Collection[RedisValue]:
        return await redis.lrange(self.db_id(), 0, -1)

    async def total_count(self) -> int:
        async with connection() as conn:
            return int(await conn.llen(self.db_id()))

    async def do_save(
        self,
        tr: Pipeline,
        values: Collection[RedisValue],
        optimistic: bool,
        cascade: bool,
    ) -> None:
        await tr.rpush(self.db_id(), *values)

    def insert(self, index: int, value: S) -> None:
        cast(List[S], self.values).insert(index, value)

    @overload
    def __getitem__(self, i: int) -> S:
        ...

    @overload
    def __getitem__(self, i: slice) -> List[S]:
        ...

    def __getitem__(self, i: int | slice) -> S | List[S]:
        return cast(List[S], self.values).__getitem__(i)

    def __setitem__(self, i: int | slice, o: S | Iterable[S]) -> None:
        self.values.__setitem__(i, o)  # type: ignore

    def __delitem__(self, i: int | slice) -> None:
        self.values.__delitem__(i)  # type: ignore

    async def async_append(self, value: S, **_: Any) -> None:
        async with connection() as conn:
            await conn.rpush(self.db_id(), getattr(value, "id", value))  # type: ignore
        self.append(value)

    async def __aiter__(self) -> AsyncIterator[S]:
        async with connection() as conn:
            for index in range(0, await conn.llen(self.db_id())):
                value = await conn.lindex(self.db_id(), index)
                yield await deserialize(self.item_class, value)


class ModelCollection(RedisCollection[M], Generic[M], metaclass=ABCMeta):
    async def do_save(
        self,
        tr: Pipeline,
        values: Collection[Any],
        optimistic: bool = False,
        cascade: bool = False,
    ) -> None:
        ids = [value.id for value in values if hasattr(value, "id")]
        await super().do_save(tr, ids, optimistic, cascade)
        if cascade:
            await asyncio.gather(*[v.save(optimistic=optimistic) for v in values])

    async def load(self: ModelCollection[IModel]) -> None:
        self.values = await self.get_values()

    async def delete(self, cascade: bool = False) -> None:
        await super().delete()
        if cascade:
            await asyncio.gather(*[m.delete() for m in self])


class RedisModelSet(ModelCollection[M], RedisSet[M]):
    async def async_add(
        self, value: M, optimistic: bool = False, cascade: bool = False, **_: Any
    ) -> None:
        await super().async_add(value)
        if cascade:
            await value.save(optimistic=optimistic)

    async def async_discard(self, value: M, cascade: bool = False, **_: Any) -> None:
        await super().async_discard(value)
        if cascade:
            await value.delete()


class RedisModelList(ModelCollection[M], RedisList[M]):
    async def async_append(
        self, value: M, optimistic: bool = False, cascade: bool = False, **_: Any
    ) -> None:
        await super().async_append(value)
        if cascade:
            await value.save(optimistic=optimistic)


@serialize.register(collections.abc.Set)
def _(values: collections.abc.Set[Any], key: str) -> RedisSet[Any]:
    item_class: type = get_args(values.__orig_class__)[0]
    return (
        RedisModelSet[item_class](values, id=key)
        if issubclass(item_class, IModel)
        else RedisSet[item_class](values, id=key)
    )


@serialize.register(collections.abc.MutableSequence)
def _(values: collections.abc.MutableSequence[Any], key: str) -> RedisList[Any]:
    item_class: type = get_args(values.__orig_class__)[0]
    return (
        RedisModelList[item_class](values, id=key)
        if issubclass(item_class, IModel)
        else RedisList[item_class](values, id=key)
    )


@deserialize.register(collections.abc.Set)
async def _(t: type[set[Any]], value: Key) -> RedisCollection[Any]:
    item_class: type = get_args(t)[0]
    if issubclass(item_class, IModel):
        return await RedisModelSet[item_class].get(value)
    else:
        return await RedisSet[item_class].get(value)


@deserialize.register(collections.abc.MutableSequence)
async def _(t: type[list[Any]], value: Key) -> RedisCollection[Any]:
    item_class: type = get_args(t)[0]
    if issubclass(item_class, IModel):
        return await RedisModelList[item_class].get(value)
    else:
        return await RedisList[item_class].get(value)
