from __future__ import annotations

import collections.abc
import dataclasses
import functools
import json
from asyncio.coroutines import iscoroutine
from collections.abc import MutableSequence
from dataclasses import MISSING
from functools import partial
from typing import AbstractSet, Any, Awaitable, Callable, TypeVar

from typing_extensions import TypeGuard, get_args, get_origin, get_type_hints

from .attributes import RedisList, RedisModelList, RedisModelSet, RedisSet
from .types import Deserializer, IModel, Key, RedisValue, Serializable, Serialized

T = TypeVar("T")


@dataclasses.dataclass
class Metadata:
    transient: bool = False
    cascade: bool = False
    eager: bool = False


@dataclasses.dataclass(unsafe_hash=True)
class Field:
    name: str
    origin: type
    args: tuple[type, ...]
    transient: bool = False
    cascade: bool = False
    eager: bool = False

    @property
    def optional(self) -> bool:
        return len(self.args) == 2 and type(None) in self.args

    @property
    def type(self) -> type:
        if self.optional or (not isinstance(self.origin, type) and self.args):
            return self.args[0]
        else:
            return self.origin


@functools.lru_cache()
def type_fields(obj: type) -> list[Field]:
    fields_list: list[Field] = []
    origin: type
    args: tuple[type, ...]
    for name, value in get_type_hints(obj, include_extras=True).items():
        if isinstance(value, type):
            fields_list.append(Field(name, value, ()))
        elif hasattr(value, "__metadata__"):
            if isinstance(value.__args__[0], type):
                origin = value.__args__[0]
                args = ()
            else:
                origin = get_origin(value.__args__[0])  # type: ignore[assignment]
                args = get_args(value.__args__[0])
            metadata = next(
                filter(
                    lambda x: isinstance(x, Metadata),
                    getattr(value, "__metadata__", ()),
                ),
                None,
            )
            metadata = dataclasses.asdict(metadata) if metadata else {}
            fields_list.append(Field(name, origin, args, **metadata))
        else:
            origin = get_origin(value)  # type: ignore[assignment]
            args = get_args(value)
            fields_list.append(Field(name, origin, args))

    return fields_list


def fields(obj: Any) -> list[Field]:
    return type_fields(obj if isinstance(obj, type) else type(obj))


def default_is_not_missing(value: object) -> TypeGuard[Serializable | None]:
    return value is not MISSING


def factory_is_not_missing(value: object) -> TypeGuard[Callable[[], Serializable]]:
    return value is not MISSING


async def deserialize(field: Field, value: RedisValue | None) -> Any:
    if value is None and field.optional:
        return None
    deserializer = field_deserializer(field)
    deserialized_value = deserializer(value)
    if iscoroutine(deserialized_value):
        return await deserialized_value
    else:
        return deserialized_value


def _is_coroutine_serializable(val: Any) -> TypeGuard[Awaitable[Serializable]]:
    return iscoroutine(val)


@functools.singledispatch
def serialize(value: Any, *_: Any) -> Serialized:
    raise TypeError(f"{type(value)} not supported")


@serialize.register(int)
@serialize.register(float)
@serialize.register(bool)
def _(value: int | float | bool, *_: Any) -> str:
    return json.dumps(value)


@serialize.register(type(None))
@serialize.register(str)
def _(value: str | None, *_: Any) -> str | None:
    return value


@serialize.register(collections.abc.Set)
def _(values: collections.abc.Set[Any], key: str, field: Field) -> RedisSet[Any]:
    return (
        RedisModelSet(key, values, model_class=field.args[0], cascade=field.cascade)
        if field.args and issubclass(field.args[0], IModel)
        else RedisSet(key, values)
    )


@serialize.register(collections.abc.MutableSequence)
def _(
    values: collections.abc.MutableSequence[Any], key: str, field: Field
) -> RedisList[Any]:
    return (
        RedisModelList(key, values, model_class=field.args[0], cascade=field.cascade)
        if field.args and issubclass(field.args[0], IModel)
        else RedisList(key, values)
    )


@functools.lru_cache()
def field_deserializer(
    field: Field,
) -> Deserializer:
    deserializer: Deserializer = json.loads
    if issubclass(field.type, (str, bytes, memoryview)):

        def deserializer(value: Any) -> Any:
            return value

    elif issubclass(field.type, IModel):
        model_class = field.type

        async def deserializer(
            key: Key,
        ) -> IModel | None:
            if key is None:
                return None
            return await model_class.get(key)

    elif issubclass(field.type, AbstractSet):
        model_class = field.args[0]
        if issubclass(model_class, IModel):
            deserializer = partial(
                RedisModelSet.get,
                model_class=model_class,
                eager=field.eager,
                cascade=field.cascade,
            )
        else:
            deserializer = partial(
                RedisSet.get,
                eager=field.eager,
            )
    elif issubclass(field.type, MutableSequence):
        model_class = field.args[0]
        if issubclass(model_class, IModel):
            deserializer = partial(
                RedisModelList.get,
                model_class=model_class,
                eager=field.eager,
                cascade=field.cascade,
            )
        else:
            deserializer = partial(
                RedisList.get,
                eager=field.eager,
            )

    return deserializer
