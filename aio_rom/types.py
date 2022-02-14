from __future__ import annotations

from abc import ABC
from typing import Any, ClassVar, Optional, Type, TypeVar, Union

from aioredis.client import FieldT, KeyT

from aio_rom.exception import ModelNotFoundException

Key = KeyT
T = TypeVar("T", bound="IModel")


class IModel(ABC):
    NotFoundException: ClassVar[Type[ModelNotFoundException]]
    id: Key

    @classmethod
    def prefix(cls) -> str:
        return f"{cls.__name__.lower()}"

    def __init_subclass__(cls: type[T], **kwargs: Any) -> None:
        cls.NotFoundException = type("NotFoundException", (ModelNotFoundException,), {})

    def db_id(self) -> str:
        return f"{self.prefix()}:{str(self.id)}"

    async def save(self, *, optimistic: bool = False, cascade: bool = False) -> None:
        ...

    @classmethod
    async def get(cls: type[T], id: Key) -> T:
        ...

    async def total_count(self) -> int:
        ...

    async def delete(self, cascade: bool = False) -> None:
        ...

    async def load(self) -> None:
        """Model references are eager by default"""


RedisValue = FieldT
Serializable = Union[RedisValue, IModel]
Serialized = Optional[RedisValue]
