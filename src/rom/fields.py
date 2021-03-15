import json
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
    Dict,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

from typing_inspect import get_args, get_origin, is_optional_type

from .attributes import RedisList, RedisModelList, RedisModelSet, RedisSet

if TYPE_CHECKING:
    from .model import Model

T = TypeVar("T", bound="Model")


class FieldMetadata(str, Enum):
    TRANSIENT = auto()
    EAGER = auto()
    CASCADE = auto()
    OPTIONAL = auto()
    DESERIALIZER = auto()
    SERIALIZER = auto()


def is_transient(field: Field):
    return field.metadata.get(FieldMetadata.TRANSIENT, False)


def is_eager(field: Field):
    return field.metadata.get(FieldMetadata.EAGER, False)


def is_cascade(field: Field):
    return field.metadata.get(FieldMetadata.CASCADE, False)


def is_optional(field: Field):
    return field.metadata.get(FieldMetadata.OPTIONAL, False)


def is_model(model: Union[Type, object]):
    return is_dataclass(model) and hasattr(model, "prefix")


def field(
    transient: bool = False, cascade: bool = False, eager: bool = False, **kwargs
) -> Any:
    metadata = kwargs.pop("metadata", {})
    metadata[FieldMetadata.TRANSIENT] = transient
    metadata[FieldMetadata.CASCADE] = cascade
    metadata[FieldMetadata.EAGER] = eager
    return dc_field(metadata=metadata, **kwargs)


def deserialize_reference(model_class: Type[T]):
    async def deserializer(
        key: Union[int, str],
    ) -> Optional[T]:
        if key is None:
            return None
        return cast(T, await model_class.get(key))

    return deserializer


async def deserialize(field: Field, value):
    if value is None and is_optional(field):
        return None
    val = field.metadata.get(FieldMetadata.DESERIALIZER)(value)
    if iscoroutine(val):
        val = await val
    return val


async def serialize(field: Field, key: str, value):
    val = field.metadata.get(FieldMetadata.SERIALIZER)(key, value)
    if iscoroutine(val):
        val = await val
    return val


def default_serializer(key, value):
    return json.dumps(value)


def passthrough_serializer(key, value):
    return value


def passthrough_deserializer(value):
    return value


def update_field(name, field_type, fields: Dict[str, Field]):
    field = fields.get(name, dc_field()) or dc_field()
    origin = get_origin(field_type) or field_type
    type_args = get_args(field_type)
    metadata = dict(getattr(field, "metadata", {}))
    eager = is_eager(field)
    cascade = is_cascade(field)
    optional = False
    deserializer = json.loads
    serializer = default_serializer

    # Unwrap optional type
    if is_optional_type(field_type):
        optional = True
        field_type = type_args[0]
        type_args = get_args(field_type)

    if isinstance(field_type, type) and issubclass(field_type, str):
        deserializer = passthrough_deserializer
        serializer = passthrough_serializer
    elif is_model(field_type):
        deserializer = deserialize_reference(field_type)
        serializer = passthrough_serializer
    elif issubclass(origin, AbstractSet):
        if is_model(type_args[0]):
            deserializer = partial(
                RedisModelSet.from_key,
                field.default_factory,
                model_class=type_args[0],
                eager=eager,
                cascade=cascade,
            )
            serializer = partial(
                RedisModelSet.serialize, model_class=type_args[0], cascade=cascade
            )
        else:
            deserializer = partial(
                RedisSet.from_key, field.default_factory, eager=eager
            )
            serializer = RedisSet
    elif issubclass(origin, MutableSequence):
        if is_model(type_args[0]):
            deserializer = partial(
                RedisModelList.from_key,
                field.default_factory,
                model_class=type_args[0],
                eager=eager,
                cascade=cascade,
            )
            serializer = partial(
                RedisModelList.serialize, model_class=type_args[0], cascade=cascade
            )
        else:
            deserializer = partial(
                RedisList.from_key, field.default_factory, eager=eager
            )
            serializer = RedisList

    metadata[FieldMetadata.DESERIALIZER] = deserializer
    metadata[FieldMetadata.SERIALIZER] = serializer
    metadata[FieldMetadata.OPTIONAL] = optional
    field.metadata = MappingProxyType(metadata)
    fields[name] = field
