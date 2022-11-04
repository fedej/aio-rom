from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Optional, Type, TypeVar, Union

from redis.typing import FieldT, KeyT
from typing_extensions import TypeAlias

from aio_rom.exception import ModelNotFoundException
from aio_rom.session import connection

T = TypeVar("T", bound="IModel")


class IModel(ABC):
    __slots__ = ("id",)

    NotFoundException: ClassVar[Type[ModelNotFoundException]]
    id: Key

    @classmethod
    def prefix(cls) -> str:
        return f"{cls.__name__.lower()}"

    def __init_subclass__(cls: type[T], **kwargs: Any) -> None:
        cls.NotFoundException = type("NotFoundException", (ModelNotFoundException,), {})

    def db_id(self) -> str:
        return f"{self.prefix()}:{str(self.id)}"

    @abstractmethod
    async def save(self, *, optimistic: bool = False, cascade: bool = False) -> None:
        ...

    @classmethod
    @abstractmethod
    async def get(cls: type[T], id: Key) -> T:
        ...

    @classmethod
    async def persisted(cls: type[T], id: int) -> bool:
        async with connection() as conn:
            return bool(await conn.exists(f"{cls.prefix()}:{id}"))

    @classmethod
    async def delete_all(cls: type[T]) -> None:
        key_prefix = cls.prefix()
        async with connection() as conn:
            keys = await conn.keys(f"{key_prefix}:*")
            await conn.delete(key_prefix, *keys)

    @abstractmethod
    async def total_count(self) -> int:
        ...

    @abstractmethod
    async def delete(self, cascade: bool = False) -> None:
        ...

    async def exists(self) -> bool:
        async with connection() as conn:
            return bool(await conn.exists(self.db_id()))

    @abstractmethod
    async def refresh(self) -> None:
        ...


Key: TypeAlias = KeyT
RedisValue: TypeAlias = FieldT
Serializable = Union[RedisValue, IModel]
Serialized = Optional[RedisValue]

__all__ = ["IModel", "Key", "RedisValue", "Serializable", "Serialized"]
