from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Awaitable, ClassVar, Type, TypeVar

from .exception import ModelNotFoundException
from .fields import deserialize, fields, serialize_dict
from .session import connection, transaction
from .types import IModel, Key, RedisValue

_logger = logging.getLogger(__name__)

M = TypeVar("M", bound="Model")


class Model(IModel):
    NotFoundException: ClassVar[Type[ModelNotFoundException]]

    @classmethod
    async def get(cls: type[M], id: Key) -> M:
        key = f"{cls.prefix()}:{str(id)}"
        async with connection() as conn:
            db_item: dict[str, RedisValue] = await conn.hgetall(key)

        if not db_item:
            raise cls.NotFoundException(f"{key} not found")

        deserialized = {}
        for field_name, f in filter(
            lambda item: item[0] in db_item, fields(cls).items()
        ):
            field_type = f.type
            value = deserialize(field_type, db_item[field_name])
            if issubclass(field_type, IModel):
                value = await value
                if f.eager:
                    await value.refresh()
            deserialized[field_name] = value

        return cls(**deserialized)

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
            setattr(self, name, value)

        values = {
            field_name: getattr(self, field_name)
            for field_name, f in model_fields.items()
            if (not changes or field_name in changes)
        }

        model_dict = serialize_dict(
            {
                k: v
                for k, v in values.items()
                if not (model_fields[k].optional and v is None)
            }
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
            update_mapping = {k: v for k, v in model_dict.items() if v is not None}
            if update_mapping:
                operations.append(
                    tr.hset(
                        self.db_id(),
                        mapping=update_mapping,
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
        model_fields = fields(self)
        if isinstance(value, IModel) and not value.id:
            value.id = f"{self.db_id()}:{model_fields[key].name}"
        super().__setattr__(key, value)
