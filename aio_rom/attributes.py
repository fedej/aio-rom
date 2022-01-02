from __future__ import annotations

import asyncio
import json
from abc import ABCMeta, abstractmethod
from dataclasses import MISSING, is_dataclass
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Collection,
    Generic,
    Iterable,
    Iterator,
    List,
    MutableSequence,
    Optional,
    Set,
    Type,
    Union,
    cast,
    overload,
)

from aioredis.client import Pipeline
from typing_extensions import TypeGuard

from .session import connection, transaction
from .types import C, Key, M


def is_model(model: object) -> TypeGuard[M]:
    return is_dataclass(model) and hasattr(model, "prefix")


class RedisCollection(Collection[C], Generic[C], metaclass=ABCMeta):
    def __init__(self, key: Key, values: Optional[Collection[C]], **kwargs: Any):
        self.key = str(key) if isinstance(key, int) else key
        self.values = values

    @classmethod
    async def deserialize(
        cls,
        default_factory: Callable[[], "RedisCollection"[C]],
        key: Key,
        eager: bool = False,
        **kwargs: Any,
    ) -> "RedisCollection"[C]:
        redis_key = str(key) if isinstance(key, int) else key
        async with connection() as conn:
            if not bool(await conn.exists(redis_key)):
                if default_factory is MISSING:  # type: ignore[comparison-overlap]
                    raise AttributeError(f"Missing {str(key)} needs a default_factory")
                return default_factory()

        values = await cls._load(redis_key, **kwargs) if eager else None
        return cls(redis_key, values, **kwargs)

    def __eq__(self, other: object) -> bool:
        return self.values == getattr(other, "values", other)

    @abstractmethod
    async def do_save(
        self,
        tr: Pipeline,
        values: Collection[Union[str, bool, int, float, bytes, memoryview]],
    ) -> None:
        pass

    async def save(self, optimistic: bool = False) -> None:
        if self.values:
            watch = [self.key] if optimistic else []
            async with transaction(*watch) as tr:
                await tr.delete(self.key)
                await self.do_save(tr, self.values)  # type: ignore[arg-type]

    @classmethod
    @abstractmethod
    async def _load(cls, key: Key, **_: Any) -> Collection[C]:
        pass

    def __iter__(self) -> Iterator[C]:
        return filter(None, iter(self.values or []))

    def __len__(self) -> int:
        return len(self.values) if self.values else 0

    def __contains__(self, x: object) -> bool:
        return self.values is not None and x in self.values


class RedisSet(RedisCollection[C], Set[C], Generic[C]):
    @classmethod
    async def _load(cls, key: Key, **_: Any) -> Set[C]:
        async with connection() as conn:
            key = str(key) if isinstance(key, int) else key
            return {json.loads(value) for value in await conn.smembers(key)}

    async def do_save(
        self,
        tr: Pipeline,
        values: Collection[Union[str, bool, int, float, bytes, memoryview]],
    ) -> None:
        await tr.sadd(self.key, *values)

    def add(self, value: C) -> None:
        cast(Set[C], super().values).add(value)

    def discard(self, value: C) -> None:
        cast(Set[C], super().values).discard(value)


class RedisList(RedisCollection[C], MutableSequence[C], Generic[C]):
    @classmethod
    async def _load(cls, key: Key, **_: Any) -> List[C]:
        async with connection() as redis:
            key = str(key) if isinstance(key, int) else key
            return [json.loads(value) for value in await redis.lrange(key, 0, -1)]

    async def do_save(
        self,
        tr: Pipeline,
        values: Collection[Union[str, bool, int, float, bytes, memoryview]],
    ) -> None:
        await tr.rpush(self.key, *values)

    def insert(self, index: int, value: C) -> None:
        cast(List[C], super().values).insert(index, value)

    @overload
    def __getitem__(self, i: int) -> C:
        ...

    @overload
    def __getitem__(self, i: slice) -> List[C]:
        ...

    def __getitem__(self, i: Union[int, slice]) -> Union[C, List[C]]:
        return cast(List[C], super().values).__getitem__(i)

    @overload
    def __setitem__(self, index: int, o: C) -> None:
        ...

    @overload
    def __setitem__(self, index: slice, o: Iterable[C]) -> None:
        ...

    def __setitem__(self, i: Union[int, slice], o: Union[C, Iterable[C]]) -> None:
        if isinstance(i, slice) and isinstance(o, Iterable):
            cast(List[C], super().values).__setitem__(i, o)
        elif isinstance(i, int) and not isinstance(o, Iterable):
            cast(List[C], super().values).insert(i, o)

    def __delitem__(self, i: Union[int, slice]) -> None:
        cast(List[C], super().values).__delitem__(i)


class ModelCollection(RedisCollection[M], AsyncIterator[M], metaclass=ABCMeta):
    def __init__(
        self,
        key: Key,
        values: Optional[Collection[M]],
        model_class: Type[M],
        cascade: bool,
    ):
        super().__init__(key, values)
        self._model_class = model_class
        self._cascade = cascade
        self._iter: Optional[Iterator[Awaitable[M]]] = None

    async def do_save(self, tr: Pipeline, values: Collection[C]) -> None:
        await super().do_save(tr, [value.id for value in (self.values or [])])

    async def save(self, optimistic: bool = False) -> None:
        async with transaction():
            await super().save(optimistic=optimistic)
            if self._cascade and self.values:
                await asyncio.gather(
                    *[v.save(optimistic=optimistic) for v in self.values]
                )

    @classmethod
    async def _load(
        cls,
        key: Key,
        model_class: Optional[Type[M]] = None,
        cascade: bool = False,
        **kwargs: Any,
    ) -> Collection[M]:
        if model_class is None:
            raise TypeError("Missing class for ModelCollection")
        async with connection():
            keys = cast(Collection[Key], await super()._load(key))
            return type(keys)(await asyncio.gather(*[model_class.get(key) for key in keys]))  # type: ignore  # noqa: E501

    def __aiter__(self) -> AsyncIterator[M]:
        self._iter = None
        return self

    async def __anext__(self) -> M:
        if self._iter is None:
            keys = cast(Collection[Key], await super()._load(self.key))
            self._iter = map(self._model_class.get, keys)
        try:
            return await next(self._iter)
        except StopIteration as err:
            raise StopAsyncIteration from err

    async def load(self: ModelCollection[M]) -> None:
        self.values = await self._load(self.key, model_class=self._model_class)


class RedisModelSet(ModelCollection[M], RedisSet[M], Generic[M]):  # type: ignore[misc]
    def add(self, value: M) -> None:
        cast(Set[M], self.values).add(value)

    def discard(self, value: M) -> None:
        cast(Set[M], self.values).discard(value)


class RedisModelList(ModelCollection[M], RedisList[M], Generic[M]):  # type: ignore[misc]  # noqa: E501
    def insert(self, index: int, value: M) -> None:
        cast(MutableSequence[M], self.values).insert(index, value)

    def __getitem__(self, index: Any) -> Any:
        return cast(MutableSequence[M], self.values).__getitem__(index)

    def __setitem__(self, index: Any, o: Any) -> None:
        cast(MutableSequence[M], self.values).__setitem__(index, o)

    def __delitem__(self, i: Union[int, slice]) -> None:
        del cast(MutableSequence[M], self.values)[i]
