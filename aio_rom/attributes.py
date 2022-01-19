from __future__ import annotations

import asyncio
import json
from abc import ABCMeta, abstractmethod
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Collection,
    Generic,
    Iterable,
    Iterator,
    List,
    MutableSequence,
    Set,
    Type,
    TypeVar,
    cast,
    overload,
)

from aioredis.client import Pipeline

from .exception import ModelNotFoundException
from .session import connection, transaction
from .types import IModel, Key, RedisValue, Serializable

T = TypeVar("T")
S = TypeVar("S", bound=Serializable)
M = TypeVar("M", bound=IModel)


class RedisCollection(
    Collection[T], IModel, Generic[T], AsyncIterable[T], metaclass=ABCMeta
):
    def __init__(
        self, id: Key, values: Collection[T] | None, item_class: Type[T], **kwargs: Any
    ):
        self.id = id
        self.values = values or self.wrap([])
        self.item_class = item_class

    def db_id(self) -> Key:
        return self.id

    def __eq__(self, other: object) -> bool:
        return self.values == getattr(other, "values", other)

    @abstractmethod
    async def do_save(
        self,
        tr: Pipeline,
        values: Collection[Any],
        optimistic: bool,
    ) -> None:
        pass

    async def save(self, optimistic: bool = False) -> None:
        if self.values:
            watch = [self.id] if optimistic else []
            async with transaction(*watch) as tr:
                await tr.delete(self.id)
                await self.do_save(tr, self.values, optimistic)

    @classmethod
    async def get(
        cls: Type[RedisCollection[T]],
        id: Key,
        eager: bool = False,
        item_class: Type[T] | None = None,
        **kwargs: Any,
    ) -> RedisCollection[T]:
        if item_class is None:
            raise TypeError("item_class is required")
        async with connection() as conn:
            if not bool(await conn.exists(id)):
                raise ModelNotFoundException(f"Collection with key {str(id)} not found")
            values = await cls.get_values(id, item_class) if eager else None
            return cls(id, values, item_class, **kwargs)

    @classmethod
    async def get_values(cls, id: Key, item_class: Type[T]) -> Collection[T]:
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


class RedisSet(RedisCollection[S], Set[S]):
    @classmethod
    async def get_values(cls, id: Key, item_class: Type[S]) -> Set[S]:
        async with connection() as conn:
            if issubclass(item_class, str):
                return set(await conn.smembers(id))
            else:
                return {json.loads(value) for value in await conn.smembers(id)}

    @classmethod
    def wrap(cls, values: Iterable[S]) -> Set[S]:
        return set(values)

    async def do_save(
        self,
        tr: Pipeline,
        values: Collection[RedisValue],
        optimistic: bool,
    ) -> None:
        await tr.sadd(self.id, *values)

    def add(self, value: S) -> None:
        cast(Set[S], self.values).add(value)

    def discard(self, value: S) -> None:
        cast(Set[S], self.values).discard(value)

    async def total_count(self) -> int:
        async with connection() as conn:
            return int(await conn.scard(self.id))

    async def __aiter__(self) -> AsyncIterator[S]:
        async with connection() as conn:
            async for value in conn.sscan_iter(self.id):
                yield value if issubclass(self.item_class, str) else json.loads(value)


class RedisList(RedisCollection[S], MutableSequence[S]):
    @classmethod
    def wrap(cls, values: Iterable[S]) -> List[S]:
        return list(values)

    @classmethod
    async def get_values(cls, id: Key, item_class: Type[S]) -> List[S]:
        async with connection() as redis:
            if issubclass(item_class, str):
                return list(await redis.lrange(id, 0, -1))
            else:
                return [json.loads(value) for value in await redis.lrange(id, 0, -1)]

    async def total_count(self) -> int:
        async with connection() as conn:
            return int(await conn.llen(self.id))

    async def do_save(
        self,
        tr: Pipeline,
        values: Collection[RedisValue],
        optimistic: bool,
    ) -> None:
        await tr.rpush(self.id, *values)

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

    @overload
    def __setitem__(self, index: int, o: S) -> None:
        ...

    @overload
    def __setitem__(self, index: slice, o: Iterable[S]) -> None:
        ...

    def __setitem__(self, i: int | slice, o: S | Iterable[S]) -> None:
        self.values.__setitem__(i, o)  # type: ignore

    def __delitem__(self, i: int | slice) -> None:
        cast(List[S], self.values).__delitem__(i)

    async def __aiter__(self) -> AsyncIterator[S]:
        async with connection() as conn:
            for index in range(0, await conn.llen(self.id)):
                value = await conn.lindex(self.id, index)
                yield value if issubclass(self.item_class, str) else json.loads(value)


class ModelCollection(RedisCollection[M], Generic[M], metaclass=ABCMeta):
    def __init__(
        self,
        key: Key,
        values: Collection[M] | None,
        item_class: Type[M],
        cascade: bool = False,
        **kwargs: Any,
    ):
        super().__init__(key, values, item_class, **kwargs)
        self._cascade = cascade
        self._iter: Iterator[Awaitable[M]] | None = None

    async def do_save(
        self, tr: Pipeline, values: Collection[Any], optimistic: bool
    ) -> None:
        ids = [value.id for value in values if hasattr(value, "id")]
        await super().do_save(tr, ids, optimistic)
        if self._cascade:
            await asyncio.gather(*[v.save(optimistic=optimistic) for v in values])

    @classmethod
    async def get_values(cls, id: Key, item_class: Type[M]) -> Any:
        if not issubclass(item_class, IModel) or item_class is IModel:
            raise TypeError(f"{item_class} is not a subclass of IModel")
        async with connection():
            keys = cast(Collection[Key], await super().get_values(id, str))  # type: ignore  # noqa: E501
            return cls.wrap(
                await asyncio.gather(*[item_class.get(key) for key in keys])
            )

    async def __aiter__(self) -> AsyncIterator[M]:
        key: Key
        async for key in super().__aiter__():  # type: ignore
            yield await self.item_class.get(key)

    async def load(self: ModelCollection[IModel]) -> None:
        self.values = await type(self).get_values(self.id, item_class=self.item_class)


class RedisModelSet(ModelCollection[M], RedisSet[M]):
    pass


class RedisModelList(ModelCollection[M], RedisList[M]):
    pass
