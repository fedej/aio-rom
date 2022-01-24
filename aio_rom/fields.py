from __future__ import annotations

import dataclasses
import functools
import json
from asyncio.coroutines import iscoroutine
from typing import Any, Awaitable, ClassVar, TypeVar

from typing_extensions import TypeGuard, get_args, get_origin, get_type_hints

from .types import IModel, Key, RedisValue, Serializable, Serialized
from .utils import type_dispatch

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
            if origin is not ClassVar:  # type: ignore
                args = get_args(value)
                fields_list.append(Field(name, origin, args))

    return fields_list


def fields(obj: Any) -> list[Field]:
    return type_fields(obj if isinstance(obj, type) else type(obj))


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


@type_dispatch
async def deserialize(_: type[Any], value: RedisValue) -> Any:
    return json.loads(value) if isinstance(value, (str, bytes)) else value


@deserialize.register(str)
@deserialize.register(bytes)
@deserialize.register(memoryview)
async def _(_: type[str | bytes | memoryview], value: RedisValue) -> RedisValue:
    return value


@deserialize.register(IModel)
async def _(value_type: type[IModel], value: Key) -> IModel:
    return await value_type.get(value)
