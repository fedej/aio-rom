from __future__ import annotations

import collections.abc
import functools
import json
from asyncio.coroutines import iscoroutine
from collections.abc import MutableSequence
from dataclasses import MISSING, Field
from functools import partial
from typing import AbstractSet, Any, Awaitable, Callable, TypeVar

from typing_extensions import TypedDict, TypeGuard, get_args, get_origin

from .attributes import RedisList, RedisModelList, RedisModelSet, RedisSet
from .types import Deserializer, IModel, Key, RedisValue, Serializable, Serialized

T = TypeVar("T")


class Metadata(TypedDict, total=False):
    transient: bool
    cascade: bool
    eager: bool


def is_transient(dataclass_field: Field[Any]) -> bool:
    return dataclass_field.metadata.get("transient", False)


def has_default(dataclass_field: Field[Any]) -> bool:
    return (
        dataclass_field.default is not MISSING
        or dataclass_field.default_factory is not MISSING  # type: ignore[comparison-overlap]  # noqa: E501
    )


def is_eager(dataclass_field: Field[Any]) -> bool:
    return dataclass_field.metadata.get("eager", False)


def is_cascade(dataclass_field: Field[Any]) -> bool:
    return dataclass_field.metadata.get("cascade", False)


def default_is_not_missing(value: object) -> TypeGuard[Serializable | None]:
    return value is not MISSING


def factory_is_not_missing(value: object) -> TypeGuard[Callable[[], Serializable]]:
    return value is not MISSING


async def deserialize(dataclass_field: Field[T], value: RedisValue | None) -> Any:
    if value is None:
        if type(None) in get_args(dataclass_field.type):
            return None
        elif default_is_not_missing(dataclass_field.default):
            return dataclass_field.default
        elif factory_is_not_missing(dataclass_field.default_factory):
            return dataclass_field.default_factory()
        raise AttributeError(f"Missing value for {dataclass_field.name}")
    deserializer = field_deserializer(dataclass_field)
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
def _(
    values: collections.abc.Set[Any], key: str, dataclass_field: Field[T]
) -> RedisSet[Any]:
    type_args = get_args(dataclass_field.type)
    return (
        RedisModelSet(
            key, values, model_class=type_args[0], cascade=is_cascade(dataclass_field)
        )
        if type_args and issubclass(type_args[0], IModel)
        else RedisSet(key, values)
    )


@serialize.register(collections.abc.MutableSequence)
def _(
    values: collections.abc.MutableSequence[Any], key: str, dataclass_field: Field[T]
) -> RedisList[Any]:
    type_args = get_args(dataclass_field.type)
    return (
        RedisModelList(
            key, values, model_class=type_args[0], cascade=is_cascade(dataclass_field)
        )
        if type_args and issubclass(type_args[0], IModel)
        else RedisList(key, values)
    )


def field_deserializer(
    dataclass_field: Field[T],
) -> Deserializer:
    eager = is_eager(dataclass_field)
    cascade = is_cascade(dataclass_field)

    field_type = dataclass_field.type
    type_args = get_args(field_type)
    if type(None) in type_args:
        # Unwrap optional type
        field_type = type_args[0]

    origin = get_origin(field_type) or field_type

    deserializer: Deserializer = json.loads
    if dataclass_field.name == "id":

        def deserializer(value: Any) -> Any:
            return value

    elif isinstance(field_type, type):
        if issubclass(field_type, (str, bytes, memoryview)):

            def deserializer(value: Any) -> Any:
                return value

        elif issubclass(field_type, IModel):
            model_class = field_type

            async def deserializer(
                key: Key,
            ) -> IModel | None:
                if key is None:
                    return None
                return await model_class.get(key)

    elif isinstance(origin, type):
        type_args = get_args(field_type)
        if issubclass(origin, AbstractSet):
            model_class = type_args[0]
            if issubclass(model_class, IModel):
                deserializer = partial(
                    RedisModelSet.deserialize,
                    dataclass_field.default_factory,
                    model_class=model_class,
                    eager=eager,
                    cascade=cascade,
                )
            else:
                deserializer = partial(
                    RedisSet.deserialize,
                    dataclass_field.default_factory,
                    eager=eager,
                )
        elif issubclass(origin, MutableSequence):
            model_class = type_args[0]
            if issubclass(model_class, IModel):
                deserializer = partial(
                    RedisModelList.deserialize,
                    dataclass_field.default_factory,
                    model_class=model_class,
                    eager=eager,
                    cascade=cascade,
                )
            else:
                deserializer = partial(
                    RedisList.deserialize,
                    dataclass_field.default_factory,
                    eager=eager,
                )

    return deserializer
