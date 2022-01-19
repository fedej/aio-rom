from __future__ import annotations

from typing import Any, TypeVar, Union

from aioredis.client import FieldT, KeyT
from typing_extensions import Protocol, runtime_checkable

Key = KeyT
T = TypeVar("T", bound="IModel")


@runtime_checkable
class IModel(Protocol):
    def db_id(self) -> Key:
        ...

    async def save(self, optimistic: bool) -> None:
        ...

    @classmethod
    async def get(cls: type[T], id: Key, **kwargs: Any) -> T:
        ...

    async def total_count(self) -> int:
        ...


RedisValue = FieldT
Serializable = Union[RedisValue, IModel]
Serialized = Union[RedisValue, IModel, None]
