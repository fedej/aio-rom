from __future__ import annotations

import collections.abc
import functools
import json
from asyncio.coroutines import iscoroutine
from collections.abc import MutableSequence
from dataclasses import MISSING, Field
from functools import partial
from typing import (
    AbstractSet,
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Optional,
    Type,
    Union,
    cast,
)

from typing_extensions import TypedDict, TypeGuard, get_args, get_origin

from .attributes import (
    RedisCollection,
    RedisList,
    RedisModelList,
    RedisModelSet,
    RedisSet,
    is_model,
)
from .types import C, Deserializer, F, Key, M, RedisValue, Serializable, Serialized


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


def is_model_type(model_type: Type[Any]) -> TypeGuard[Type[M]]:
    return is_model(model_type) and isinstance(model_type, type)


def default_is_not_missing(value: object) -> TypeGuard[Optional[C]]:
    return value is not MISSING


def factory_is_not_missing(value: object) -> TypeGuard[Callable[[], C]]:
    return value is not MISSING


def _deserialize_reference(
    model_class: Type[M],
) -> Callable[[Key], Coroutine[Any, Any, Optional[M]]]:
    async def deserializer(
        key: Key,
    ) -> Optional[M]:
        if key is None:
            return None
        return await model_class.get(key)

    return deserializer


async def deserialize(
    dataclass_field: Field[C], value: Optional[RedisValue]
) -> Optional[Union[C, RedisCollection[C]]]:
    if value is None:
        if type(None) in get_args(dataclass_field.type):
            return None
        elif default_is_not_missing(dataclass_field.default):
            return dataclass_field.default
        elif factory_is_not_missing(dataclass_field.default_factory):
            return dataclass_field.default_factory()

    deserializer = field_deserializer(dataclass_field)
    deserialized_value = deserializer(value)
    if iscoroutine(deserialized_value):
        return await cast(Awaitable[Union[C, RedisCollection[C]]], deserialized_value)
    else:
        return cast(Union[C, RedisCollection[C]], deserialized_value)


def _is_coroutine_serializable(val: Any) -> TypeGuard[Awaitable[Serializable]]:
    return iscoroutine(val)


@functools.singledispatch
def serialize(value: Any, *_: Any) -> Serialized:
    raise TypeError(f"{type(value)} not supported")


@serialize.register(int)
@serialize.register(float)
@serialize.register(bool)
def _(value: Union[int, float, bool], *_: Any) -> str:
    return json.dumps(value)


@serialize.register(type(None))
@serialize.register(str)
def _(value: Optional[str], *_: Any) -> Optional[str]:
    return value


@serialize.register(collections.abc.Set)
def _(
    values: collections.abc.Set[Any], key: str, dataclass_field: Field[F]
) -> RedisSet[Any]:
    type_args = get_args(dataclass_field.type)
    return (
        RedisModelSet(
            key, values, model_class=type_args[0], cascade=is_cascade(dataclass_field)
        )
        if type_args and is_model_type(type_args[0])
        else RedisSet(key, values)
    )


@serialize.register(collections.abc.MutableSequence)
def _(
    values: collections.abc.MutableSequence[Any], key: str, dataclass_field: Field[F]
) -> RedisList[Any]:
    type_args = get_args(dataclass_field.type)
    return (
        RedisModelList(
            key, values, model_class=type_args[0], cascade=is_cascade(dataclass_field)
        )
        if type_args and is_model_type(type_args[0])
        else RedisList(key, values)
    )


def field_deserializer(
    dataclass_field: Field[C],
) -> Deserializer[Union[C, RedisCollection[C]]]:
    eager = is_eager(dataclass_field)
    cascade = is_cascade(dataclass_field)

    field_type = dataclass_field.type
    type_args = get_args(field_type)
    if type(None) in type_args:
        # Unwrap optional type
        field_type = type_args[0]

    origin = get_origin(field_type) or field_type

    deserializer: Deserializer[Union[C, RedisCollection[C]]] = json.loads
    if isinstance(field_type, type) and issubclass(field_type, str):

        def deserializer(value: Any) -> Any:
            return value

    elif is_model_type(field_type):
        deserializer = _deserialize_reference(field_type)
    elif isinstance(origin, type):
        type_args = get_args(field_type)
        if issubclass(origin, AbstractSet):
            model_class = type_args[0]
            if is_model_type(model_class):
                deserializer = partial(
                    RedisModelSet.from_key,
                    dataclass_field.default_factory,
                    model_class=model_class,
                    eager=eager,
                    cascade=cascade,
                )
            else:
                deserializer = partial(
                    RedisSet.from_key,
                    dataclass_field.default_factory,
                    eager=eager,
                )
        elif issubclass(origin, MutableSequence):
            model_class = type_args[0]
            if is_model(model_class):
                deserializer = partial(
                    RedisModelList.from_key,
                    dataclass_field.default_factory,
                    model_class=model_class,
                    eager=eager,
                    cascade=cascade,
                )
            else:
                deserializer = partial(
                    RedisList.from_key,
                    dataclass_field.default_factory,
                    eager=eager,
                )

    return deserializer
