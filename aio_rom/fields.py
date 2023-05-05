from __future__ import annotations

import functools
from typing import Any, ClassVar, TypeVar

from typing_extensions import get_args, get_origin, get_type_hints

from aio_rom.proxy import ProxyModel
from aio_rom.types import IModel, RedisValue, Serializable
from aio_rom.utils import type_dispatch

T = TypeVar("T")


class Metadata:
    __slots__ = "transient", "cascade", "eager"

    transient: bool
    cascade: bool
    eager: bool

    def __init__(
        self, transient: bool = False, cascade: bool = False, eager: bool = False
    ):
        self.transient = transient
        self.cascade = cascade
        self.eager = eager


class Field:
    __slots__ = (
        "name",
        "transient",
        "cascade",
        "eager",
        "optional",
        "field_type",
    )

    name: str
    transient: bool
    cascade: bool
    eager: bool
    optional: bool
    field_type: type

    def __init__(
        self,
        name: str,
        origin: type,
        args: tuple[type, ...],
        transient: bool = False,
        cascade: bool = False,
        eager: bool = False,
    ):
        self.name = name
        self.transient = transient
        self.cascade = cascade
        self.eager = eager
        self.optional = len(args) == 2 and type(None) in args
        if self.optional or (not isinstance(origin, type) and args):
            self.field_type = args[0]
        else:
            self.field_type = origin


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
                origin = get_origin(value.__args__[0])
                args = get_args(value.__args__[0])
            metadata: Metadata | None = next(
                filter(
                    lambda x: isinstance(x, Metadata),
                    getattr(value, "__metadata__", ()),
                ),
                None,
            )
            if metadata and not metadata.transient:
                fields_dict[name] = Field(
                    name,
                    origin,
                    args,
                    transient=metadata.transient,
                    cascade=metadata.cascade,
                    eager=metadata.eager,
                )
        else:
            origin = get_origin(value)
            if origin is not ClassVar:  # type: ignore
                args = get_args(value)
                fields_dict[name] = Field(name, origin, args)

    return fields_dict


def fields(obj: Any) -> dict[str, Field]:
    return type_fields(obj if isinstance(obj, type) else type(obj))


@functools.singledispatch
def serialize(value: Serializable) -> RedisValue:
    if isinstance(value, (str, int, float, bytes, memoryview)):
        return int(value) if isinstance(value, bool) else value
    if isinstance(value, IModel):
        if not value.id:
            raise AttributeError(f"{value} has no id")
        return serialize(value.id)
    if value is None:
        return None
    raise TypeError(f"Serialization of {type(value)!r} not supported")


@type_dispatch
def deserialize(value_type: type[Any], value: RedisValue) -> Any:
    if issubclass(value_type, (str, bytes, memoryview)):
        return value
    if issubclass(value_type, (int, float)):
        return value_type(value)
    if issubclass(value_type, IModel):
        return ProxyModel(value_type, value)  # type: ignore[arg-type]
    if issubclass(value_type, bool):
        return bool(int(value))
    raise TypeError(f"Cannot deserialize {value!r} to type {value_type}")
