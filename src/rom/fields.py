from __future__ import annotations

import json
import sys
from asyncio.coroutines import iscoroutine
from collections.abc import MutableSequence
from dataclasses import Field
from dataclasses import field as dc_field
from dataclasses import is_dataclass
from enum import Enum, auto
from functools import partial
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Awaitable,
    Callable,
    Collection,
    Coroutine,
    Dict,
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

from typing_inspect import get_args, get_origin, is_optional_type  # type: ignore

if sys.version_info >= (3, 8):
    from typing import TypedDict, Literal, overload  # pylint: disable=no-name-in-module
else:
    from typing_extensions import TypedDict, Literal, overload

from .attributes import (
    RedisList,
    RedisModelList,
    RedisModelSet,
    RedisSet,
    RedisCollection,
    Key,
    RedisValue,
)

if TYPE_CHECKING:
    from .model import Model

T = TypeVar("T", bound="Model")
F = TypeVar("F", str, bool, int, float)

_Serializable = Union[RedisValue, "Model", RedisCollection]
_Serializer = Callable[..., Union[F, _Serializable, Awaitable[_Serializable]]]
_Deserialized = Union[Optional[F], Awaitable[Optional[F]]]
_Deserializer = Callable[..., _Deserialized[F]]


class Metadata(TypedDict, total=False):
    transient: bool
    cascade: bool
    eager: bool
    optional: bool
    serializer: _Serializer[Any]
    deserializer: _Deserializer[Any]


def is_transient(dataclass_field: Field[F]) -> bool:
    return dataclass_field.metadata.get("transient", False)


def is_eager(dataclass_field: Field[F]) -> bool:
    return dataclass_field.metadata.get("eager", False)


def is_cascade(dataclass_field: Field[F]) -> bool:
    return dataclass_field.metadata.get("cascade", False)


def is_optional(dataclass_field: Field[F]) -> bool:
    return dataclass_field.metadata.get("optional", False)


def is_model(model: object) -> bool:
    return is_dataclass(model) and hasattr(model, "prefix")


def field(
    transient: bool = False, cascade: bool = False, eager: bool = False, **kwargs: Any
) -> F:
    metadata: Metadata = kwargs.pop("metadata", {})
    metadata.update(
        {
            "transient": transient,
            "cascade": cascade,
            "eager": eager,
        }
    )
    return cast(F, dc_field(metadata=metadata, **kwargs))


def deserialize_reference(
    model_class: Type[T],
) -> Callable[[Key], Awaitable[Optional[T]]]:
    async def deserializer(
        key: Key,
    ) -> Optional[T]:
        if key is None:
            return None
        return await model_class.get(key)

    return deserializer


async def deserialize(
    dataclass_field: Field[F], value: Optional[RedisValue]
) -> Optional[F]:
    if value is not None:
        metadata = cast(Metadata, dataclass_field.metadata)
        deserializer = metadata["deserializer"]
        deserialized_value: _Deserialized[F] = deserializer(value)
        if iscoroutine(deserialized_value):
            return await cast(Awaitable[F], deserialized_value)
        else:
            return cast(F, deserialized_value)
    elif is_optional(dataclass_field):
        return None
    else:
        raise TypeError(f"")


async def serialize(dataclass_field: Field[F], key: str, value: F) -> _Serializable:
    metadata = cast(Metadata, dataclass_field.metadata)
    serializer = metadata["serializer"]
    val = serializer(key, value)
    if iscoroutine(val):
        return await cast(Awaitable[_Serializable], val)
    else:
        return cast(_Serializable, val)


def passthrough_serializer(key: str, value: F) -> F:
    return value


def passthrough_deserializer(value: Any) -> Any:
    return value


def update_field(
    name: str, field_type: Type[Union[F, T]], fields: Dict[str, Field[F]]
) -> None:
    dataclass_field = fields.get(name, dc_field()) or dc_field()
    eager = is_eager(dataclass_field)
    cascade = is_cascade(dataclass_field)
    optional = False
    # Unwrap optional type

    unwrapped_field_type = field_type
    if is_optional_type(field_type):
        optional = True
        unwrapped_field_type = get_args(field_type)[0]

    origin = get_origin(unwrapped_field_type) or unwrapped_field_type
    type_args = get_args(unwrapped_field_type)

    deserializer: _Deserializer[Union[T, F]] = json.loads
    serializer: _Serializer[F] = lambda key, value: json.dumps(value)

    if isinstance(unwrapped_field_type, type) and issubclass(unwrapped_field_type, str):
        deserializer = passthrough_deserializer
        serializer = passthrough_serializer
    elif isinstance(unwrapped_field_type, cast(Type[T], type)) and is_model(
        unwrapped_field_type
    ):
        deserializer = deserialize_reference(unwrapped_field_type)
        serializer = passthrough_serializer

    if isinstance(origin, type):
        if issubclass(origin, AbstractSet):
            model_class = type_args[0]
            if isinstance(unwrapped_field_type, cast(Type[T], type)) and is_model(
                model_class
            ):
                deserializer = partial(
                    RedisModelSet.from_key,
                    dataclass_field.default_factory,
                    model_class=model_class,
                    eager=eager,
                    cascade=cascade,
                )
                serializer = partial(
                    RedisModelSet.serialize, model_class=model_class, cascade=cascade
                )
            else:
                deserializer = cast(
                    Callable[[], F],
                    partial(
                        RedisSet.from_key,
                        dataclass_field.default_factory,  # type: ignore[misc]
                        eager=eager,
                    ),
                )
                serializer = RedisSet
        elif issubclass(origin, MutableSequence):
            if is_model(type_args[0]):
                deserializer = cast(
                    Callable[[], F],
                    partial(
                        RedisModelList.from_key,
                        dataclass_field.default_factory,  # type: ignore[misc]
                        model_class=type_args[0],
                        eager=eager,
                        cascade=cascade,
                    ),
                )
                serializer = partial(
                    RedisModelList.serialize, model_class=type_args[0], cascade=cascade
                )
            else:
                deserializer = cast(
                    Callable[[], F],
                    partial(
                        RedisList.from_key,
                        dataclass_field.default_factory,  # type: ignore[misc]
                        eager=eager,
                    ),
                )
                serializer = RedisList

    metadata = cast(Metadata, dict(dataclass_field.metadata))
    metadata.update(
        {
            "deserializer": deserializer,
            "serializer": serializer,
            "optional": optional,
        }
    )
    dataclass_field.metadata = MappingProxyType(metadata)
    fields[name] = dataclass_field
