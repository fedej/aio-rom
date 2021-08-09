from __future__ import annotations

import asyncio
import json
from abc import ABCMeta, abstractmethod
from dataclasses import MISSING, is_dataclass
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Collection,
    Dict,
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
from typing_extensions import SupportsIndex, TypeGuard

from .exception import ModelNotLoadedException
from .session import connection, transaction
from .types import C, Key, M, RedisValue


def is_model(model: object) -> TypeGuard[M]:
    return is_dataclass(model) and hasattr(model, "prefix")


class RedisCollection(Collection[C], Generic[C], metaclass=ABCMeta):
    def __init__(self, key: Key, values: Collection[C], **kwargs: Any):
        self.key = str(key) if isinstance(key, int) else key
        self.values = values

    @classmethod
    async def from_key(
        cls,
        default_factory: Callable[[], Collection[C]],
        key: Key,
        eager: bool = False,
        **kwargs: Any,
    ) -> Collection[C]:
        values = await cls.get_values_for_key(key)
        if values is None:
            return default_factory() if default_factory != MISSING else None

        collection = cls(key, values, **kwargs)
        if eager:
            await collection.load()
            return collection.values
        return collection

    @abstractmethod
    async def do_save(self, tr: Pipeline, values: Collection[RedisValue]) -> None:
        pass

    async def save(self, optimistic: bool = False) -> None:
        watch = [self.key] if optimistic else []
        async with transaction(*watch) as tr:
            await tr.delete(self.key)
            if self.values:
                await self.do_save(tr, self.values)  # type: ignore[arg-type]

    async def load(self) -> None:
        pass

    @classmethod
    @abstractmethod
    async def get_values_for_key(cls, key: Key) -> Collection[C]:
        pass

    @property
    def values(self) -> Collection[C]:
        return self._values

    @values.setter
    def values(self, values: Collection[C]) -> None:
        self._values: Collection[C] = values

    def __iter__(self) -> Iterator[C]:
        return filter(None, iter(self.values))

    def __len__(self) -> int:
        return len(self.values)

    def __contains__(self, x: object) -> bool:
        return x in self.values


class RedisSet(RedisCollection[C], Set[C], Generic[C]):
    @classmethod
    async def get_values_for_key(cls, key: Key) -> Set[C]:
        async with connection() as conn:
            key = str(key) if isinstance(key, int) else key
            return {json.loads(value) for value in await conn.smembers(key)}

    async def do_save(self, tr: Pipeline, values: Collection[RedisValue]) -> None:
        await tr.sadd(self.key, *values)

    def add(self, value: C) -> None:
        cast(Set[C], super().values).add(value)

    def discard(self, value: C) -> None:
        cast(Set[C], super().values).discard(value)


class RedisList(RedisCollection[C], MutableSequence[C], Generic[C]):
    @classmethod
    async def get_values_for_key(cls, key: Key) -> List[C]:
        async with connection() as redis:
            key = str(key) if isinstance(key, int) else key
            return [json.loads(value) for value in await redis.lrange(key, 0, -1)]

    async def do_save(self, tr: Pipeline, values: Collection[RedisValue]) -> None:
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
        keys: Collection[Union[Key, M]],
        model_class: Type[M],
        cascade: bool,
    ):
        super().__init__(
            key, cast(List[M], [value for value in keys if is_model(value)])
        )
        self._cache.update(
            {key: None for key in keys if isinstance(key, (int, str, bytes))}
        )
        self._model_class = model_class
        self._cascade = cascade

    @classmethod
    def serialize(
        cls,
        key: Key,
        keys: Union[Collection[Key], ModelCollection[M]],
        model_class: Type[M],
        cascade: bool = False,
    ) -> ModelCollection[M]:
        return (
            keys
            if isinstance(keys, ModelCollection)
            else cls(key, keys, model_class, cascade)
        )

    async def do_save(self, tr: Pipeline, values: Collection[RedisValue]) -> None:
        await super().do_save(tr, list(self._cache.keys()))

    async def save(self, optimistic: bool = False) -> None:
        async with transaction():
            await super().save(optimistic=optimistic)
            if self._cascade and self.values:
                await asyncio.gather(
                    *[v.save(optimistic=optimistic) for v in self.values]
                )

    async def load(self) -> None:
        async with connection():
            await asyncio.gather(*[self._get_item(key) for key in self._cache.keys()])

    @property
    def values(self) -> Collection[M]:
        return list(filter(None, self._cache.values()))

    @values.setter
    def values(self, values: Collection[M]) -> None:
        self._cache: Dict[Key, Optional[M]] = {v.id: v for v in values}

    async def _get_item(self, key: Key) -> M:
        value = self._cache.get(key, None)
        if not value:
            value = await self._model_class.get(key)
            self._cache[key] = value
        return value

    def __aiter__(self) -> AsyncIterator[M]:
        self._iter = map(self._get_item, self._cache.keys())
        return self

    async def __anext__(self) -> M:
        try:
            return await next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class RedisModelSet(ModelCollection[M], RedisSet[M], Generic[M]):
    @property
    def values(self) -> Set[M]:
        return set(super().values)

    @values.setter
    def values(self, values: Collection[M]) -> None:
        super(RedisModelSet, type(self)).values.fset(self, values)  # type: ignore[attr-defined] # noqa: E501

    def add(self, value: M) -> None:
        self._cache[value.id] = value

    def discard(self, value: M) -> None:
        self._cache.pop(value.id)


class RedisModelList(ModelCollection[M], RedisList[M], Generic[M]):
    @property
    def values(self) -> List[M]:
        return list(super().values)

    @values.setter
    def values(self, values: Collection[C]) -> None:
        super(RedisModelList, type(self)).values.fset(self, values)  # type: ignore[attr-defined] # noqa: E501

    def _get_cached_item(self, key: Key) -> M:
        if key in self._cache:
            value = self._cache[key]
            if not value:
                raise ModelNotLoadedException(
                    f"{self._model_class.__name__} id {key!r} was not loaded"
                )
            return value
        else:
            raise KeyError

    def insert(self, index: int, value: M) -> None:
        super().insert(index, value)
        self._cache[value.id] = value

    @overload
    def __getitem__(self, index: int) -> M:
        ...

    @overload
    def __getitem__(self, index: slice) -> List[M]:
        ...

    def __getitem__(self, i: Union[int, slice]) -> Union[M, List[M]]:
        if isinstance(i, slice):
            return [
                self._get_cached_item(super().__getitem__(index).id)
                for index in range(*i.indices(len(self)))
            ]
        else:
            return self._get_cached_item(super().__getitem__(i).id)

    @overload
    def __setitem__(self, index: int, o: M) -> None:
        ...

    @overload
    def __setitem__(self, index: slice, o: Iterable[M]) -> None:
        ...

    def __setitem__(self, i: Union[int, slice], o: Union[M, Iterable[M]]) -> None:
        if isinstance(i, slice) and isinstance(o, SupportsIndex):
            for obj_index, index in enumerate(range(*i.indices(len(self)))):
                self.insert(index, o[obj_index])
        elif not isinstance(i, slice) and not isinstance(o, Iterable):
            self.insert(i, o)
        else:
            raise ValueError

    def __delitem__(self, i: Union[int, slice]) -> None:
        if isinstance(i, slice):
            for index in range(*i.indices(len(self))):
                del self._cache[super().__getitem__(index).id]
        else:
            del self._cache[super().__getitem__(i).id]
        super().__delitem__(i)
