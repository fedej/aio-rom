from __future__ import annotations

import dataclasses
import functools
import json
from typing import Any, Callable, ClassVar, TypeVar

from typing_extensions import get_args, get_origin, get_type_hints

from .types import IModel, Key, RedisValue, Serializable
from .utils import type_dispatch

T = TypeVar("T")

serializer: Callable[[Serializable], RedisValue] = json.dumps
deserializer: Callable[[RedisValue], Any] = json.loads


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
def type_fields(obj: type) -> dict[str, Field]:
    fields_dict: dict[str, Field] = {}
    for name, value in get_type_hints(obj, include_extras=True).items():
        if isinstance(value, type):
            fields_dict[name] = Field(name, value, ())
        elif hasattr(value, "__metadata__"):
            if isinstance(value.__args__[0], type):
                origin = value.__args__[0]
                args: tuple[type, ...] = ()
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
            if not metadata.get("transient"):
                fields_dict[name] = Field(name, origin, args, **metadata)
        else:
            origin = get_origin(value)  # type: ignore[assignment]
            if origin is not ClassVar:  # type: ignore
                args = get_args(value)
                fields_dict[name] = Field(name, origin, args)

    return fields_dict


def fields(obj: Any) -> dict[str, Field]:
    return type_fields(obj if isinstance(obj, type) else type(obj))


def serialize_dict(changes: dict[str, Serializable]) -> dict[str, RedisValue]:
    return {f: serialize(value) for f, value in changes.items()}


@functools.singledispatch
def serialize(value: Any) -> RedisValue:
    return serializer(value)


@serialize.register(IModel)
def _(value: IModel) -> RedisValue:
    model_id = getattr(value, "id", None)
    if not model_id:
        raise AttributeError(f"{value} has no id")
    if not isinstance(model_id, (bytes, str, memoryview)):
        raise TypeError(f"Type '{type(model_id)}' invalid for id '{model_id}'")
    return model_id


@serialize.register(type(None))
@serialize.register(str)
@serialize.register(bytes)
@serialize.register(memoryview)
def _(value: str | bytes | memoryview) -> RedisValue:
    return value


@type_dispatch
async def deserialize(_: type[Any], value: RedisValue) -> Any:
    return deserializer(value) if isinstance(value, (str, bytes)) else value


@deserialize.register(str)
@deserialize.register(bytes)
@deserialize.register(memoryview)
async def _(_: type[str | bytes | memoryview], value: RedisValue) -> RedisValue:
    return value


@deserialize.register(IModel)
async def _(value_type: type[IModel], value: Key) -> IModel:
    return await value_type.get(value)
