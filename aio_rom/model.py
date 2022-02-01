from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from operator import attrgetter
from typing import Any, AsyncIterator, Awaitable, ClassVar, Type, TypeVar

from .exception import ModelNotFoundException
from .fields import deserialize, fields, serialize_dict
from .session import connection, transaction
from .types import IModel, Key, RedisValue

_logger = logging.getLogger(__name__)

M = TypeVar("M", bound="Model")


class Model:
    NotFoundException: ClassVar[Type[ModelNotFoundException]]
    id: Key

    def __init_subclass__(cls: type[M], **kwargs: Any) -> None:
        cls.NotFoundException = type("NotFoundException", (ModelNotFoundException,), {})

    @classmethod
    def prefix(cls) -> str:
        return f"{cls.__name__.lower()}"

    @classmethod
    async def get(cls: type[M], id: Key) -> M:
        key = f"{cls.prefix()}:{str(id)}"
        async with connection() as conn:
            db_item: dict[str, RedisValue] = await conn.hgetall(key)

        if not db_item:
            raise cls.NotFoundException(f"{key} not found")

        model_fields = [
            f for field_name, f in fields(cls).items() if field_name in db_item
        ]
        deserialized = await asyncio.gather(
            *(deserialize(f.type, db_item[f.name], field=f) for f in model_fields)
        )

        return cls(**dict(zip(map(attrgetter("name"), model_fields), deserialized)))

    @classmethod
    async def scan(cls: type[M], **kwargs: str | None | int | None) -> AsyncIterator[M]:
        async with connection() as conn:
            found = set()
            async for key in conn.sscan_iter(cls.prefix(), **kwargs):  # type: ignore[arg-type] # noqa
                if key not in found:
                    value = await cls.get(key)
                    if value:
                        yield value
                        found.add(key)
                    else:
                        _logger.warning(f"{cls.__name__} Key: {key} orphaned")

    @classmethod
    async def all(cls: type[M]) -> Iterable[M]:
        async with connection() as conn:
            keys = await conn.smembers(cls.prefix())
            return await asyncio.gather(*[cls.get(key) for key in keys])

    @classmethod
    async def total_count(cls) -> int:
        async with connection() as conn:
            return int(await conn.scard(cls.prefix()))

    @classmethod
    async def delete_all(cls: type[M]) -> None:
        key_prefix = cls.prefix()
        async with connection() as conn:
            keys = await conn.keys(f"{key_prefix}:*")
            await conn.delete(key_prefix, *keys)

    @classmethod
    async def persisted(cls: type[M], id: int) -> bool:
        async with connection() as conn:
            return bool(await conn.exists(f"{cls.prefix()}:{id}"))

    def db_id(self) -> str:
        return f"{self.prefix()}:{str(self.id)}"

    async def save(self, optimistic: bool = False, _: bool = False) -> None:
        watch = [self.db_id()] if optimistic else []
        async with transaction(*watch) as tr:
            await self.update(optimistic=optimistic)
            await tr.sadd(self.prefix(), self.id)

    async def update(self, optimistic: bool = False, **changes: Any) -> None:
        model_fields = fields(self)
        values = {
            field_name: changes.get(field_name, getattr(self, field_name))
            for field_name, f in model_fields.items()
            if (not changes or field_name in changes)
        }

        model_dict = serialize_dict(
            {k: v for k, v in values.items() if not model_fields[k].optional or v}
        )
        watch = [self.db_id()] if optimistic else []
        operations: list[Awaitable[None]] = [
            value.save(optimistic=optimistic, cascade=model_fields[field_name].cascade)
            for field_name, value in values.items()
            if isinstance(value, IModel)
        ]
        keys_to_delete = [k for k, v in model_dict.items() if v is None]
        async with transaction(*watch) as tr:
            if keys_to_delete:
                operations.append(tr.hdel(self.db_id(), *keys_to_delete))
            await asyncio.gather(
                tr.hset(
                    self.db_id(),
                    mapping={k: v for k, v in model_dict.items() if v is not None},
                ),
                *operations,
            )

        for name, value in changes.items():
            if name in model_dict:
                setattr(self, name, value)

    async def delete(self, _: bool = False) -> None:
        key = self.db_id()
        async with connection() as conn:
            keys = await conn.keys(f"{key}:*")
            async with transaction() as tr:
                await tr.delete(*keys, key)
                await tr.srem(self.prefix(), key)

    async def exists(self) -> bool:
        async with connection() as conn:
            return bool(await conn.exists(self.db_id()))

    async def refresh(self: M) -> M:
        return await type(self).get(self.id)
