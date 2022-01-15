from __future__ import annotations

import collections.abc
import dataclasses
import functools
import json
from asyncio.coroutines import iscoroutine
from typing import Any, Awaitable, TypeVar

from typing_extensions import TypeGuard, get_args, get_origin, get_type_hints

from .attributes import (
    RedisCollection,
    RedisList,
    RedisModelList,
    RedisModelSet,
    RedisSet,
)
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
            args = get_args(value)
            fields_list.append(Field(name, origin, args))

    return fields_list


def fields(obj: Any) -> list[Field]:
    return type_fields(obj if isinstance(obj, type) else type(obj))


async def deserialize(field: Field, value: RedisValue | None) -> Any:
    if value is None and field.optional:
        return None
    return await deserialize_field(field.type, value, field)


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
        RedisModelSet(key, values, item_class=field.args[0], cascade=field.cascade)
        if field.args and issubclass(field.args[0], IModel)
        else RedisSet(key, values, item_class=field.args[0])
    )


@serialize.register(collections.abc.MutableSequence)
def _(
    values: collections.abc.MutableSequence[Any], key: str, field: Field
) -> RedisList[Any]:
    return (
        RedisModelList(key, values, item_class=field.args[0], cascade=field.cascade)
        if field.args and issubclass(field.args[0], IModel)
        else RedisList(key, values, item_class=field.args[0])
    )


@type_dispatch
async def deserialize_field(_: type[Any], value: RedisValue, field: Field) -> Any:
    return json.loads(value) if isinstance(value, (str, bytes)) else value


@deserialize_field.register(str)
@deserialize_field.register(bytes)
@deserialize_field.register(memoryview)
async def _(
    _: type[str | bytes | memoryview], value: RedisValue, field: Field
) -> RedisValue:
    return value


@deserialize_field.register(IModel)
async def _(value_type: type[IModel], value: Key, _: Field) -> IModel:
    return await value_type.get(value)


@deserialize_field.register(collections.abc.Set)
async def _(_: type[set[Any]], value: Key, field: Field) -> RedisCollection[Any]:
    item_class = field.args[0]
    if issubclass(item_class, IModel):
        return await RedisModelSet.get(
            value,
            item_class=item_class,
            eager=field.eager,
            cascade=field.cascade,
        )
    else:
        return await RedisSet.get(
            value,
            eager=field.eager,
            item_class=item_class,
        )


@deserialize_field.register(collections.abc.MutableSequence)
async def _(_: type[list[Any]], value: Key, field: Field) -> RedisCollection[Any]:
    item_class = field.args[0]
    if issubclass(item_class, IModel):
        return await RedisModelList.get(
            value,
            item_class=item_class,
            eager=field.eager,
            cascade=field.cascade,
        )
    else:
        return await RedisList.get(
            value,
            eager=field.eager,
            item_class=item_class,
        )
