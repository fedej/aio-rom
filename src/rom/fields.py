import json
from asyncio.coroutines import iscoroutine
from collections.abc import MutableSequence
from dataclasses import Field
from dataclasses import field as dc_field
from dataclasses import is_dataclass
from enum import Enum, auto
from functools import partial
from types import MappingProxyType
from typing import (TYPE_CHECKING, AbstractSet, Any, Dict, Optional, Type,
                    TypeVar, Union, cast)

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


async def deserialize_reference(model_class: Type[T]):
    async def deserializer(
        key: Union[int, str],
    ) -> Optional[T]:
        if key is None:
            return None
        return cast(T, await model_class.get(id))

    return deserializer


async def deserialize(field: Field, value):
    val = field.metadata.get(FieldMetadata.DESERIALIZER)(value)
    if iscoroutine(val):
        val = await val
    return val


async def serialize(field: Field, key: str, value):
    val = field.metadata.get(FieldMetadata.SERIALIZER)(key, value)
    if iscoroutine(val):
        val = await val
    return val


def update_field(name, field_type, fields: Dict[str, Field]):
    field = fields.get(name, dc_field()) or dc_field()
    origin = get_origin(field_type) or field_type
    args = get_args(field_type)
    metadata = dict(getattr(field, "metadata", {}))
    eager = is_eager(field)
    cascade = is_cascade(field)
    optional = False
    deserializer = json.loads
    serializer = lambda _, value: json.dumps(value)
    if isinstance(field_type, type) and issubclass(field_type, str):
        deserializer = lambda x: x
        serializer = lambda _, v: v
    elif is_model(field_type):
        deserializer = deserialize_reference(field_type)
    elif is_optional_type(field_type):
        optional = True
        if is_model(args[0]):
            deserializer = deserialize_reference(args[0])
    elif issubclass(origin, AbstractSet):
        optional = True
        if is_model(args[0]):
            deserializer = partial(
                RedisModelSet.from_key,
                model_class=args[0],
                eager=eager,
                cascade=cascade,
            )
            serializer = partial(RedisModelSet, model_class=args[0], cascade=cascade)
        else:
            deserializer = partial(RedisSet.from_key, eager=eager)
            serializer = RedisSet
    elif issubclass(origin, MutableSequence):
        optional = True
        if is_model(args[0]):
            deserializer = partial(
                RedisModelList.from_key,
                model_class=args[0],
                eager=eager,
                cascade=cascade,
            )
            serializer = partial(RedisModelList, model_class=args[0], cascade=cascade)
        else:
            deserializer = partial(RedisList.from_key, eager=eager)
            serializer = RedisList

    if deserializer:
        metadata[FieldMetadata.DESERIALIZER] = deserializer
    if serializer:
        metadata[FieldMetadata.SERIALIZER] = serializer
    metadata[FieldMetadata.OPTIONAL] = optional
    field.metadata = MappingProxyType(metadata)
    fields[name] = field
