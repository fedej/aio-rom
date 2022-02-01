from __future__ import annotations

from typing import Optional, TypeVar, Union

from aioredis.client import FieldT, KeyT
from typing_extensions import Protocol, runtime_checkable

Key = KeyT
T = TypeVar("T", bound="IModel")


@runtime_checkable
class IModel(Protocol):
    @classmethod
    def prefix(cls) -> str:
        return f"{cls.__name__.lower()}"

    def db_id(self) -> Key:
        ...

    async def save(self, optimistic: bool = False, cascade: bool = False) -> None:
        ...

    @classmethod
    async def get(cls: type[T], id: Key) -> T:
        ...

    async def total_count(self) -> int:
        ...

    async def delete(self, cascade: bool = False) -> None:
        ...


RedisValue = FieldT
Serializable = Union[RedisValue, IModel]
Serialized = Optional[RedisValue]
