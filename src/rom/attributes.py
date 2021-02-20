import json
from abc import ABCMeta, abstractmethod
from asyncio.coroutines import iscoroutine
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
    async def from_key(cls, key: str, eager: bool = False, **kwargs):
        values = await cls.get_values_for_key(key) or []
        collection = cls(
            key, type(values)([json.loads(value) for value in values]), **kwargs
        )
        if eager:
            async for _ in collection:
                pass
            return collection.values
        return collection

    @abstractmethod
    async def save(self, optimistic: bool = False):
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
                tr.sadd(self._key, *super().values)

    def add(self, value):
        self.values.append(value)

    def discard(self, value):
        self.values.discard(value)


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
                tr.rpush(self._key, *super().values)

    def insert(self, index: int, value: T):
        self.values.insert(index, value)

    def __getitem__(self, i: Union[int, slice]) -> Union[Any, Sequence[Any]]:
        return self.values.__getitem__(i)

    def __setitem__(self, i: Union[int, slice], o: Union[Any, Sequence[Any]]):
        self.values.__setitem__(i, o)

    def __delitem__(self, i: int):
        self.values.__delitem__(i)


class ModelCollection(RedisCollection, Generic[T], metaclass=ABCMeta):
    def __init__(
        self, key: str, keys: Sequence[int], model_class: Type[T] = None, cascade=False
    ):
        self._cache: Dict[int, T] = {}
        self._collection_type = type(keys)
        self._model_class = model_class
        self._cascade = cascade
        super().__init__(key, keys)

    async def save(self, optimistic: bool = False):
        async with transaction():
            await super().save(optimistic=optimistic)
            if self._cascade:
                async for v in self.values:
                    v.save(optimistic=optimistic)

    @property
    def values(self):
        return self._collection_type(self._cache.values())

    @values.setter
    def values(self, values):
        if values and isinstance(next(iter(values)), int):  # Lazy loading using keys
            super(ModelCollection, type(self)).values.fset(
                self, self._collection_type(values)
            )
        else:  # values: Collection[Model]
            super(ModelCollection, type(self)).values.fset(
                self, self._collection_type([v.id for v in values])
            )
            self._cache = {v.id: v for v in values}

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
