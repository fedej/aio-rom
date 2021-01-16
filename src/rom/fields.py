import json
import logging
from abc import abstractmethod
from asyncio.coroutines import iscoroutine
from collections.abc import MutableSequence, MutableSet
from dataclasses import Field
from dataclasses import field as dc_field
from dataclasses import fields, is_dataclass
from enum import Enum
from functools import partial
from typing import (AbstractSet, Any, Collection, Dict, Generic, Iterator,
                    List, Optional, Sequence, Type, TypeVar, Union, cast)

from typing_inspect import get_args, get_origin

from .exception import ModelNotLoadedException

_logger = logging.getLogger(__name__)

from typing import TYPE_CHECKING

from . import model

if TYPE_CHECKING:
    from .model import Model

T = TypeVar("T", bound="Model")

_DB_TRANSIENT = "_db_transient"
_DB_EAGER = "_db_transient"


def _is_transient(field: Field):
    return field.metadata.get(_DB_TRANSIENT, False)


def _is_eager(field: Field):
    return field.metadata.get(_DB_EAGER, False)


def field(transient: bool = False, eager: bool = False, **kwargs) -> Any:
    metadata = kwargs.pop("metadata", {})
    metadata[_DB_TRANSIENT] = transient
    metadata[_DB_EAGER] = eager
    return dc_field(metadata=metadata, **kwargs)


class LazyModelCollection(Collection[T], Generic[T]):
    def __init__(self, keys: Sequence[Union[int, str]], model_class: Type) -> None:
        self._cache = {}
        self._model_class = model_class
        self.keys = list(map(str, keys))

    @classmethod
    def default_factory(cls, model_class: Type):
        return partial(cls, [], model_class)

    @classmethod
    async def create(
        cls,
        id: Union[int, str],
        model_class: Type = type(None),
        eager_type: Type = None,
    ):
        keys = await cls.get_keys(id)
        val = cls(keys, model_class)
        if eager_type is not None:
            # Load all
            async for _ in val:
                pass
            return eager_type(val)
        return val

    @classmethod
    @abstractmethod
    async def get_keys(cls, id: Union[int, str]) -> Sequence[int]:
        pass

    def __iter__(self) -> Iterator[T]:
        return filter(None, iter(self._cache.values()))

    def __len__(self) -> int:
        return len(self._cache)

    def __contains__(self, x: T) -> bool:
        return str(x.id) in self._cache.keys()

    def _get_cached_item(self, key: str):
        try:
            return self._cache[key]
        except KeyError:
            raise ModelNotLoadedException(
                f"{self._model_class.__name__} object with id {key} was not loaded"
            )

    async def _get_item(self, id: Union[str, int]) -> Optional[T]:
        key = str(id)
        value = self._cache.get(key)
        if value is not None:
            return value
        value = cast(T, await self._model_class.get(key))
        if value is not None:
            self._cache[key] = value
            return value
        else:
            _logger.warning(f"{self._model_class.__name__}: {key} orphaned")
        return None

    def __aiter__(self):
        self._iter = iter(self.keys)
        return self

    async def __anext__(self):
        try:
            return await self._get_item(next(self._iter))
        except StopIteration:
            raise StopAsyncIteration


class LazyModelSet(MutableSet, LazyModelCollection[T]):
    @classmethod
    async def get_keys(cls, id: Union[int, str]) -> Sequence[int]:
        return await model.redis.smembers(id)

    def add(self, value: T) -> None:
        key = str(value.id)
        self.keys.append(key)
        self._cache[key] = value

    def discard(self, value: T) -> None:
        key = str(value.id)
        if value in self:
            self.keys.remove(key)
            del self._cache[key]


class LazyModelList(MutableSequence, LazyModelCollection[T]):
    @classmethod
    async def get_keys(cls, id: Union[int, str]) -> Sequence[int]:
        return await model.redis.lrange(id, 0, -1)

    def insert(self, index: int, value: T) -> None:
        key = str(value.id)
        self._cache[key] = value
        self.keys.insert(index, key)

    def __getitem__(self, i: Union[int, slice]) -> Union[T, Sequence[T]]:
        if isinstance(i, slice):
            return [
                self._get_cached_item(self.keys[index])
                for index in range(*i.indices(len(self)))
            ]
        else:
            return self._get_cached_item(self.keys[i])

    def __setitem__(self, i: Union[int, slice], o: Union[T, Sequence[T]]) -> None:
        if isinstance(i, slice) and isinstance(o, Sequence):
            for obj_index, index in enumerate(range(*i.indices(len(self)))):
                self.insert(index, o[obj_index])
        elif not isinstance(i, slice) and not isinstance(o, Sequence):
            self.insert(i, o)
        else:
            raise ValueError

    def __delitem__(self, i: int) -> None:
        key = self.keys.pop(i)
        del self._cache[key]


async def get_model_ref(
    id: Union[int, str], model_class: Type[T] = None, **kwargs
) -> Optional[T]:
    if id is None:
        return None
    return cast(T, await model_class.get(id))


def _serialize_collection(val):
    if isinstance(val, (LazyModelList, LazyModelSet)):
        return val.keys
    else:
        return [str(i.id) for i in val]


class ModelFieldType(Enum):
    DEFAULT = str, json.dumps, json.loads
    STRING = str, lambda x: x, lambda x: x
    SET = set, lambda x: x, set
    LIST = list, lambda x: x, list
    MODEL = str, lambda x: str(x.id), get_model_ref
    MODEL_OPTIONAL = str, lambda x: str(x.id) if x is not None else None, get_model_ref
    MODEL_SET = set, _serialize_collection, LazyModelSet.create
    MODEL_LIST = list, _serialize_collection, LazyModelList.create

    def __new__(cls, *args, **kwds):
        value = len(cls.__members__) + 1
        obj = object.__new__(cls)
        obj._value_ = value
        return obj

    def __init__(self, redis_type: Type, serializer, deserializer):
        self.redis_type = redis_type
        self.serializer = serializer
        self.deserializer = deserializer


class ModelField:
    name: str
    model_type: ModelFieldType
    transient: bool
    eager: bool

    def __init__(self, name, type, model_type, transient, eager) -> None:
        self.name = name
        self.model_type = model_type
        self._serializer = model_type.serializer
        if model_type in [
            ModelFieldType.MODEL_SET,
            ModelFieldType.MODEL_LIST,
            ModelFieldType.MODEL_OPTIONAL,
            ModelFieldType.MODEL,
        ]:
            self._deserializer = partial(
                model_type.deserializer,
                model_class=type,
                eager_type=model_type.redis_type if eager else None,
            )
        else:
            self._deserializer = model_type.deserializer
        self.transient = transient

    def serialize(self, model: Any) -> Any:
        return self._serializer(getattr(model, self.name))

    async def deserialize(self, model: Dict[str, Any]) -> Optional[Any]:
        serialized = model.get(self.name)
        if serialized is not None:
            val = self._deserializer(serialized)
            if iscoroutine(val):
                val = await val
            return val


def _is_model(model: Union[Type, object]):
    return (
        is_dataclass(model)
        and hasattr(model, "prefix")
        and any([f.name == "id" for f in fields(model)])
    )


def model_fields(model_class: Type) -> List[ModelField]:
    model_fields = []
    for f in fields(model_class):
        origin = get_origin(f.type)
        args = get_args(f.type)
        transient = _is_transient(f)
        eager = True
        field_type = f.type
        model_field_type = ModelFieldType.DEFAULT

        if isinstance(f.type, type) and _is_model(f.type):
            model_field_type = ModelFieldType.MODEL
        elif f.type == str:
            model_field_type = ModelFieldType.STRING
        elif origin is Union and len(args) == 2 and args[1] == type(None):
            field_type = args[0]
            if _is_model(field_type):
                model_field_type = ModelFieldType.MODEL_OPTIONAL
            elif field_type == str:
                model_field_type = ModelFieldType.STRING
        elif isinstance(origin, type):
            if issubclass(origin, AbstractSet):
                field_type = args[0]
                model_field_type = (
                    ModelFieldType.MODEL_SET
                    if _is_model(args[0])
                    else ModelFieldType.SET
                )
            elif issubclass(origin, Sequence):
                field_type = args[0]
                model_field_type = (
                    ModelFieldType.MODEL_LIST
                    if _is_model(args[0])
                    else ModelFieldType.LIST
                )
            if issubclass(origin, LazyModelCollection):
                eager = False
        model_fields.append(
            ModelField(f.name, field_type, model_field_type, transient, eager)
        )
    return model_fields
