from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Awaitable, ClassVar, Mapping, Type, TypeVar

from aio_rom.exception import ModelNotFoundException
from aio_rom.fields import deserialize, fields, serialize
from aio_rom.session import connection, transaction
from aio_rom.types import IModel, Key, RedisValue

_logger = logging.getLogger(__name__)

M = TypeVar("M", bound="Model")


class Model(IModel):
    NotFoundException: ClassVar[Type[ModelNotFoundException]]

    @classmethod
    async def get(cls: type[M], id: Key) -> M:
        key = f"{cls.prefix()}:{str(id)}"
        async with connection() as conn:
            db_item: Mapping[str, RedisValue] = await conn.hgetall(key)

        if not db_item:
            raise cls.NotFoundException(f"{key} not found")

        deserialized = {}
        for f in [f for f in fields(cls).values() if f.name in db_item]:
            value = deserialize(f.field_type, db_item[f.name])
            if f.eager and isinstance(value, IModel):
                await value.refresh()
                value = getattr(value, "__wrapped__", value)
            deserialized[f.name] = value

        return cls(**deserialized)

    @classmethod
    async def scan(cls: type[M], **kwargs: str | None | int | None) -> AsyncIterator[M]:
        async with connection() as conn:
            async for key in conn.sscan_iter(
                cls.prefix(), **kwargs  # type: ignore[arg-type]
            ):
                try:
                    yield await cls.get(key)
                except cls.NotFoundException:
                    _logger.warning(f"{cls.__name__} Key: {key} orphaned")

    @classmethod
    async def all(cls: type[M]) -> list[M]:
        async with connection() as conn:
            keys = await conn.smembers(cls.prefix())
            return list(await asyncio.gather(*[cls.get(key) for key in keys]))

    @classmethod
    async def total_count(cls) -> int:
        async with connection() as conn:
            return int(await conn.scard(cls.prefix()))

    async def save(self, *, optimistic: bool = False, cascade: bool = False) -> None:
        watch = [self.db_id()] if optimistic else []
        async with transaction(*watch) as tr:
            await self.update(optimistic=optimistic)
            await tr.sadd(self.prefix(), self.id)

    async def update(self, optimistic: bool = False, **changes: Any) -> None:
        model_fields = fields(self)
        for name, value in changes.items():
            if name in model_fields:
                setattr(self, name, value)

        values = [
            (field_name, getattr(self, field_name))
            for field_name, f in model_fields.items()
            if (not changes or field_name in changes)
        ]

        model_dict = {
            k: serialize(v)
            for k, v in values
            if not (model_fields[k].optional and v is None)
        }
        operations: list[Awaitable[None]] = [
            value.save(optimistic=optimistic, cascade=model_fields[field_name].cascade)
            for field_name, value in values
            if isinstance(value, IModel)
        ]
        keys_to_delete = [k for k, v in model_dict.items() if v is None]
        update_mapping = {k: v for k, v in model_dict.items() if v is not None}
        async with transaction(*([self.db_id()] if optimistic else [])) as tr:
            if keys_to_delete:
                operations.append(tr.hdel(self.db_id(), *keys_to_delete))
            if update_mapping:
                operations.append(
                    tr.hset(
                        self.db_id(),
                        mapping=update_mapping,  # type: ignore[arg-type]
                    )
                )
            if operations:
                await asyncio.gather(*operations)

    async def delete(self, _: bool = False) -> None:
        key = self.db_id()
        async with connection() as conn:
            keys = await conn.keys(f"{key}:*")
            async with transaction() as tr:
                await tr.delete(*keys, key)
                await tr.srem(self.prefix(), key)

    async def refresh(self: M) -> None:
        fresh = await type(self).get(self.id)
        for name, field in fields(self).items():
            if not field.transient:
                setattr(self, name, getattr(fresh, name))

    def __setattr__(self, key: str, value: Any) -> None:
        if isinstance(value, IModel) and not value.id:
            value.id = f"{self.db_id()}:{key}"
        super().__setattr__(key, value)
