from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from inspect import signature
from typing import Any, AsyncIterator, ClassVar, Type, TypeVar, Generic

from .exception import ModelNotFoundException
from .fields import deserialize, fields, serialize
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
        async with connection() as conn:
            db_item: dict[str, RedisValue] = await conn.hgetall(
                f"{cls.prefix()}:{str(id)}"
            )

        if not db_item:
            raise cls.NotFoundException(f"{str(id)} not found")

        model_fields = {f.name: f for f in fields(cls) if not f.transient}
        deserialized = await asyncio.gather(
            *[
                deserialize(model_fields[field_name].type, value)
                for field_name, value in db_item.items()
            ]
        )

        return cls.from_dict(
            {f: value for f, value in zip(db_item.keys(), deserialized)},
            strict=False,
        )

    @classmethod
    def from_dict(cls: type[M], model: dict[str, Any], strict: bool = True) -> M:
        parameters = signature(cls).parameters
        return (
            cls(**{k: v for k, v in model.items() if k in parameters})
            if strict
            else cls(**model)
        )

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
        watch = [self.db_id()] if optimistic else []
        async with transaction(*watch) as tr:
            model_dict = await self.serialize(changes)
            references = []
            for name, value in model_dict.items():
                if isinstance(value, IModel):
                    references.append(value)
                    model_dict[name] = value.db_id()
            await asyncio.gather(*[ref.save(optimistic) for ref in references])
            await tr.hset(self.db_id(), mapping=model_dict)
        for name, value in changes.items():
            setattr(self, name, value)

    async def serialize(self, changes: dict[str, Any]) -> dict[str, Any]:
        model_fields = {}
        for f in [
            f
            for f in fields(self)
            if not f.transient and (not changes or f.name in changes)
        ]:
            value = changes.get(f.name, getattr(self, f.name))
            if value:
                key = f"{self.db_id()}:{f.name}"
                serialized = serialize(value, key, f)
                model_fields[f.name] = serialized

        return model_fields

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
