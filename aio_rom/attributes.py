from __future__ import annotations

import asyncio
import json
from abc import ABCMeta, abstractmethod
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Collection,
    Generic,
    Iterable,
    Iterator,
    List,
    MutableSequence,
    Set,
    TypeVar,
    cast,
    overload,
)

from aioredis.client import Pipeline

from .session import connection, transaction
from .types import IModel, Key, RedisValue, Serializable

T = TypeVar("T")
S = TypeVar("S", bound=Serializable)
M = TypeVar("M", bound=IModel)


class RedisCollection(Collection[T], IModel, Generic[T], metaclass=ABCMeta):
    def __init__(self, id: Key, values: Collection[T] | None, **kwargs: Any):
        self.id = id
        self.values = values

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
        cls: type[RedisCollection[T]],
        id: Key,
        **kwargs: Any,
    ) -> RedisCollection[T]:
        async with connection() as conn:
            if not bool(await conn.exists(id)):
                raise RuntimeError()
            eager = kwargs.pop("eager", False)
            values = await cls.get_values(id, **kwargs) if eager else None
            return cls(id, values, **kwargs)

    @classmethod
    async def get_values(cls, id: Key, **kwargs: Any) -> Collection[T]:
        pass

    @classmethod
    @abstractmethod
    def wrap(cls, values: Iterable[T]) -> Collection[T]:
        pass

    def __iter__(self) -> Iterator[T]:
        return filter(None, iter(self.values or []))

    def __len__(self) -> int:
        return len(self.values) if self.values else 0

    def __contains__(self, x: object) -> bool:
        return self.values is not None and x in self.values


class RedisSet(RedisCollection[S], Set[S]):
    @classmethod
    async def get_values(cls, id: Key, **kwargs: Any) -> set[S]:
        async with connection() as conn:
            key = str(id) if isinstance(id, int) else id
            return {json.loads(value) for value in await conn.smembers(key)}

    @classmethod
    def wrap(cls, values: Iterable[S]) -> set[S]:
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


class RedisList(RedisCollection[S], MutableSequence[S]):
    @classmethod
    def wrap(cls, values: Iterable[S]) -> MutableSequence[S]:
        return list(values)

    @classmethod
    async def get_values(cls, id: Key, **kwargs: Any) -> MutableSequence[S]:
        async with connection() as redis:
            key = str(id) if isinstance(id, int) else id
            return [json.loads(value) for value in await redis.lrange(key, 0, -1)]

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
    def __getitem__(self, i: slice) -> list[S]:
        ...

    def __getitem__(self, i: int | slice) -> S | list[S]:
        return cast(List[S], self.values).__getitem__(i)

    @overload
    def __setitem__(self, index: int, o: S) -> None:
        ...

    @overload
    def __setitem__(self, index: slice, o: Iterable[S]) -> None:
        ...

    def __setitem__(self, i: int | slice, o: S | Iterable[S]) -> None:
        if isinstance(i, slice) and isinstance(o, Iterable):
            cast(List[S], self.values).__setitem__(i, o)
        elif isinstance(i, int) and not isinstance(o, Iterable):
            cast(List[S], self.values).insert(i, o)

    def __delitem__(self, i: int | slice) -> None:
        cast(List[S], self.values).__delitem__(i)


class ModelCollection(
    RedisCollection[M], AsyncIterator[M], Generic[M], metaclass=ABCMeta
):
    def __init__(
        self,
        key: Key,
        values: Collection[M] | None,
        model_class: type[M] | None = None,
        cascade: bool = False,
    ):
        super().__init__(key, values)
        self._model_class = model_class
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
    async def get_values(cls, id: Key, **kwargs: Any) -> Any:
        model_class = kwargs.get("model_class")
        if not model_class:
            raise TypeError("Missing model_class for ModelCollection")
        if not issubclass(model_class, IModel):
            raise TypeError(f"{model_class} is not a subclass of IModel")
        async with connection():
            keys = await super().get_values(id, **kwargs)
            return cls.wrap(
                await asyncio.gather(*[model_class.get(key) for key in keys])
            )

    def __aiter__(self) -> AsyncIterator[M]:
        self._iter = None
        return self

    async def __anext__(self) -> M:
        if self._iter is None:
            if self._model_class is None:
                raise TypeError("model class is missing")
            keys = cast(Collection[Key], await super().get_values(self.id))
            self._iter = map(self._model_class.get, keys)
        try:
            return await next(self._iter)
        except StopIteration as err:
            raise StopAsyncIteration from err

    async def load(self: ModelCollection[IModel]) -> None:
        self.values = await type(self).get_values(
            self.id, model_class=self._model_class
        )


class RedisModelSet(ModelCollection[IModel], RedisSet[IModel]):
    pass


class RedisModelList(ModelCollection[IModel], RedisList[IModel]):
    pass
