import asyncio
import json
from abc import ABCMeta, abstractmethod
from asyncio.coroutines import iscoroutine
from dataclasses import MISSING
from typing import (TYPE_CHECKING, AbstractSet, Any, Collection, Dict, Generic,
                    Iterator, MutableSequence, MutableSet, Optional, Sequence,
                    Type, TypeVar, Union, cast)

if TYPE_CHECKING:
    from .model import Model

from .exception import ModelNotLoadedException
from .session import connection, transaction

T = TypeVar("T", bound="Model")


class RedisCollection(Collection, metaclass=ABCMeta):
    def __init__(self, key: str, values: Sequence):
        self._key = key
        self.values = values

    @classmethod
    async def from_key(cls, default_factory, key: str, eager: bool = False, **kwargs):
        values = await cls.get_values_for_key(key)
        if values is None:
            return default_factory() if default_factory != MISSING else None

        collection = cls(
            key, type(values)([json.loads(value) for value in values]), **kwargs
        )
        if eager:
            await collection.load()
            return collection.values
        return collection

    @abstractmethod
    async def save(self, optimistic: bool = False):
        pass

    async def load(self):
        pass

    @classmethod
    @abstractmethod
    async def get_values_for_key(cls, key: str) -> Sequence:
        pass

    @property
    def values(self):
        return self._values

    @values.setter
    def values(self, value):
        self._values = value

    def __iter__(self) -> Iterator:
        return filter(None, iter(self.values))

    def __len__(self) -> int:
        return len(self.values)

    def __contains__(self, x) -> bool:
        return x in self.values

    def __aiter__(self):
        self._iter = iter(self.values)
        return self

    async def __anext__(self):
        try:
            val = next(self._iter)
            if iscoroutine(val):
                return await val
            return val
        except StopIteration:
            raise StopAsyncIteration


class RedisSet(MutableSet, RedisCollection):
    @classmethod
    async def get_values_for_key(cls, id: int) -> AbstractSet:
        async with connection() as conn:
            return set(await conn.smembers(id))

    async def save(self, optimistic: bool = False):
        async with connection() as conn:
            if optimistic:
                conn.watch(self._key)
            async with transaction() as tr:
                tr.delete(self._key)
                values = super().values
                if values:
                    tr.sadd(self._key, *values)

    def add(self, value):
        super().values.add(value)


    def discard(self, value):
        super().values.discard(value)


class RedisList(MutableSequence, RedisCollection):
    @classmethod
    async def get_values_for_key(cls, id: int) -> Sequence:
        async with connection() as redis:
            return await redis.lrange(id, 0, -1)

    async def save(self, optimistic: bool = False):
        async with connection() as conn:
            if optimistic:
                conn.watch(self._key)
            async with transaction() as tr:
                tr.delete(self._key)
                values = super().values
                if values:
                    tr.rpush(self._key, *values)

    def insert(self, index: int, value: T):
        super().values.insert(index, value)

    def __getitem__(self, i: Union[int, slice]) -> Union[Any, Sequence[Any]]:
        return super().values.__getitem__(i)

    def __setitem__(self, i: Union[int, slice], o: Union[Any, Sequence[Any]]):
        super().values.__setitem__(i, o)

    def __delitem__(self, i: int):
        super().values.__delitem__(i)


class ModelCollection(RedisCollection, Generic[T], metaclass=ABCMeta):
    def __init__(
        self, key: str, keys: Sequence[int], model_class: Type[T], cascade: bool
    ):
        self._cache: Dict[int, T] = {}
        self._collection_type = type(keys)
        self._model_class = model_class
        self._cascade = cascade
        super().__init__(key, keys)

    @classmethod
    def serialize(
        cls, key: str, keys: Sequence, model_class: Type[T] = None, cascade=False
    ):
        return (
            keys
            if isinstance(keys, ModelCollection)
            else cls(key, keys, model_class, cascade)
        )

    async def save(self, optimistic: bool = False):
        async with transaction():
            await super().save(optimistic=optimistic)
            if self._cascade:
                for v in self.values:
                    await v.save(optimistic=optimistic)

    async def load(self):
        async with connection():
            await asyncio.gather(*[self._get_item(key) for key in super().values])

    @property
    def values(self):
        return self._collection_type(self._cache.values())

    @values.setter
    def values(self, values):
        self._cache = {v.id: v for v in values if hasattr(v, "id")}
        keys = self._collection_type(
            self._cache.keys()
            if isinstance(values, ModelCollection)
            else [getattr(v, "id", v) for v in values]
        )
        super(ModelCollection, type(self)).values.fset(self, keys)

    async def _get_item(self, key: int) -> Optional[T]:
        if key in self._cache:
            return self._cache[key]
        value = cast(T, await self._model_class.get(key))
        self._cache[key] = value
        return value

    def __aiter__(self):
        self._iter = map(self._get_item, super().values)
        return self


class RedisModelSet(ModelCollection[T], RedisSet):
    def add(self, value: T) -> None:
        key = value.id
        super().add(key)
        self._cache[key] = value

    def discard(self, value: T) -> None:
        key = value.id
        super().discard(key)
        self._cache.pop(key)


class RedisModelList(ModelCollection[T], RedisList):
    def _get_cached_item(self, key: int):
        try:
            return self._cache[key]
        except KeyError:
            raise ModelNotLoadedException(
                f"{self._model_class.__name__} object with id {key} was not loaded"
            )

    def insert(self, index: int, value: T) -> None:
        key = value.id
        super().insert(index, key)
        self._cache[key] = value

    def __getitem__(self, i: Union[int, slice]) -> Union[T, Sequence[T]]:
        if isinstance(i, slice):
            return [
                self._get_cached_item(self.values[index])
                for index in range(*i.indices(len(self)))
            ]
        else:
            return self._get_cached_item(self.values[i])

    def __setitem__(self, i: Union[int, slice], o: Union[T, Sequence[T]]) -> None:
        if isinstance(i, slice) and isinstance(o, Sequence):
            for obj_index, index in enumerate(range(*i.indices(len(self)))):
                self.insert(index, o[obj_index])
        elif not isinstance(i, slice) and not isinstance(o, Sequence):
            self.insert(i, o)
        else:
            raise ValueError

    def __delitem__(self, i: int) -> None:
        key = self.values[i]
        super().__delitem__(i)
        del self._cache[key]
