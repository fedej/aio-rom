from __future__ import annotations

import asyncio
import dataclasses
import logging
from collections.abc import Iterable
from dataclasses import field, fields, replace
from inspect import signature
from typing import Any, AsyncIterator, Generic, Mapping, Type, TypeVar, cast

from .exception import ModelNotFoundException
from .fields import deserialize, has_default, is_transient, serialize
from .session import connection, transaction
from .types import IModel, Key, RedisValue

_logger = logging.getLogger(__name__)


M = TypeVar("M", bound="Model")


class ModelDataclassType(type, Generic[M]):
    @classmethod
    def __prepare__(  # type: ignore[override]
        mcs, name: str, bases: tuple[type, ...], **kwds: Any
    ) -> Mapping[str, Any]:
        ns = super().__prepare__(name, bases)
        return {
            "NotFoundException": type(
                "NotFoundException", (ModelNotFoundException,), {}
            ),
            **ns,
        }

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        init: bool = True,
        repr: bool = True,
        eq: bool = True,
        order: bool = False,
        unsafe_hash: bool = False,
        frozen: bool = False,
    ) -> ModelDataclassType[M]:
        cls = cast(Type[M], super().__new__(mcs, name, bases, namespace))
        return dataclasses.dataclass(
            init=init,
            repr=repr,
            eq=eq,
            order=order,
            unsafe_hash=unsafe_hash,
        )(cls)


class Model(metaclass=ModelDataclassType):
    id: Key = field(init=True, repr=False, compare=False)

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

        model_fields = [f for f in fields(cls) if not is_transient(f)]
        deserialized = await asyncio.gather(
            *[deserialize(f, db_item.get(f.name)) for f in model_fields]
        )

        return cls.from_dict(
            {f.name: value for f, value in zip(model_fields, deserialized)},
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

    @property
    def db_id(self) -> str:
        return f"{self.prefix()}:{str(self.id)}"

    async def save(self, optimistic: bool = False) -> None:
        watch = [self.db_id] if optimistic else []
        async with transaction(*watch) as tr:
            model_dict = await self._serialized_model(optimistic)
            await tr.hset(self.db_id, mapping=model_dict)
            await tr.sadd(self.prefix(), self.id)

    async def update(self: M, optimistic: bool = False, **changes: Any) -> M:
        watch = [self.db_id] if optimistic else []
        async with transaction(*watch) as tr:
            model_dict = await self._serialized_model(optimistic, **changes)
            for key, value in model_dict.items():
                await tr.hset(self.db_id, key, value)
            return replace(self, **changes)

    async def _serialized_model(
        self: M, optimistic: bool, **changes: Any
    ) -> dict[str, Any]:
        model_fields = {}
        for f in [
            f
            for f in fields(self)
            if not is_transient(f)
            and hasattr(self, f.name)
            and (not changes or f.name in changes)
        ]:
            value = changes.get(f.name, getattr(self, f.name))

            if not (
                has_default(f)
                and isinstance(value, (Iterable, type(None)))
                and not value
            ):
                key = f"{self.db_id}:{f.name}"
                serialized = serialize(value, key, f)
                if isinstance(serialized, IModel):
                    await serialized.save(optimistic=optimistic)
                    serialized = key
                model_fields[f.name] = serialized

        return model_fields

    async def delete(self) -> None:
        key = self.db_id
        async with connection() as conn:
            keys = await conn.keys(f"{key}:*")
            async with transaction() as tr:
                await tr.delete(*keys, key)
                await tr.srem(self.prefix(), key)

    async def exists(self) -> bool:
        async with connection() as conn:
            return bool(await conn.exists(self.db_id))

    async def refresh(self: M) -> M:
        return await type(self).get(self.id)
