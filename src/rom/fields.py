from __future__ import annotations

import json
from asyncio.coroutines import iscoroutine
from collections.abc import MutableSequence
from dataclasses import Field
from dataclasses import field as dc_field
from dataclasses import is_dataclass
from functools import partial
from types import MappingProxyType
from typing import (
    AbstractSet,
    Any,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Type,
    Union,
    cast,
)

from typing_extensions import TypedDict
from typing_extensions import get_args, get_origin

from .attributes import (
    RedisList,
    RedisModelList,
    RedisModelSet,
    RedisSet,
)
from .types import (
    Deserializer,
    Serializer,
    F,
    Serializable,
    RedisValue,
    Deserialized,
    M,
    Key,
)


class Metadata(TypedDict, total=False):
    transient: bool
    cascade: bool
    eager: bool
    optional: bool
    serializer: Serializer[Any]
    deserializer: Deserializer[Any]


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


def _deserialize_reference(
    model_class: Type[M],
) -> Callable[[Key], Awaitable[Optional[M]]]:
    async def deserializer(
        key: Key,
    ) -> Optional[M]:
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
        deserialized_value: Deserialized[F] = deserializer(value)
        if iscoroutine(deserialized_value):
            return await cast(Awaitable[F], deserialized_value)
        else:
            return cast(F, deserialized_value)
    elif is_optional(dataclass_field):
        return None
    else:
        raise TypeError(f"Value missing for required field {dataclass_field.name}")


async def serialize(dataclass_field: Field[F], key: str, value: F) -> Serializable:
    metadata = cast(Metadata, dataclass_field.metadata)
    serializer = metadata["serializer"]
    val = serializer(key, value)
    if iscoroutine(val):
        return await cast(Awaitable[Serializable], val)
    else:
        return cast(Serializable, val)


def pass_through_serializer(key: str, value: F) -> F:
    return value


def pass_through_deserializer(value: Any) -> Any:
    return value


def update_field(
    name: str, field_type: Type[Union[F, M]], fields: Dict[str, Field[F]]
) -> None:
    dataclass_field = fields.get(name, dc_field()) or dc_field()
    eager = is_eager(dataclass_field)
    cascade = is_cascade(dataclass_field)

    optional = False
    if type(None) in get_args(field_type):
        # Unwrap optional type
        optional = True
        field_type = get_args(field_type)[0]

    origin = get_origin(field_type) or field_type
    type_args = get_args(field_type)

    deserializer: Deserializer[Union[M, F]] = json.loads
    serializer: Serializer[F] = lambda key, value: json.dumps(value)

    if isinstance(field_type, type) and issubclass(field_type, str):
        deserializer = pass_through_deserializer
        serializer = pass_through_serializer
    elif isinstance(field_type, cast(Type[M], type)) and is_model(field_type):
        deserializer = _deserialize_reference(field_type)
        serializer = pass_through_serializer

    if isinstance(origin, type):
        if issubclass(origin, AbstractSet):
            model_class = type_args[0]
            if is_model(model_class):
                deserializer = cast(
                    Callable[[], F],
                    partial(
                        RedisModelSet.from_key,
                        dataclass_field.default_factory,  # type: ignore[misc]
                        model_class=model_class,
                        eager=eager,
                        cascade=cascade,
                    ),
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
            model_class = type_args[0]
            if is_model(model_class):
                deserializer = cast(
                    Callable[[], F],
                    partial(
                        RedisModelList.from_key,
                        dataclass_field.default_factory,  # type: ignore[misc]
                        model_class=model_class,
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
