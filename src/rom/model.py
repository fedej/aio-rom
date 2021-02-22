from contextlib import asynccontextmanager
import logging
from dataclasses import dataclass, field, fields, replace
from typing import (Any, AsyncGenerator, AsyncIterator, Collection, Dict, Type,
                    TypeVar, Union, cast)

from .exception import ModelNotFoundException
from .fields import (deserialize, is_optional, is_transient, serialize,
                     update_field)
from .session import connection, transaction

_logger = logging.getLogger(__name__)


class ModelDataclassType(type):
    def __new__(cls, name, bases, dict):
        for field_name, field_type in dict.get("__annotations__", {}).items():
            update_field(field_name, field_type, dict)
        model_class = dataclass(
            super().__new__(cls, name, bases, dict), unsafe_hash=True
        )
        setattr(
            model_class,
            "NotFoundException",
            type("NotFoundException", (ModelNotFoundException,), {}),
        )
        return model_class


T = TypeVar("T", bound="Model")


class Model(metaclass=ModelDataclassType):
    id: int = field(init=True, repr=False, compare=False)

    @classmethod
    def prefix(cls) -> str:
        return f"{cls.__name__.lower()}"

    @classmethod
    async def get(cls: Type[T], id: Union[int, str]) -> T:
        async with connection() as conn:
            db_item = cast(Dict[str, Any], await conn.hgetall(f"{cls.prefix()}:{id}"))

        if not db_item:
            raise cls.NotFoundException(f"{id} not found")  # type: ignore # pylint: disable=no-member

        model_dict = {}
        for f in [f for f in fields(cls) if not is_transient(f)]:
            value = db_item.get(f.name)
            model_dict[f.name] = None if is_optional(f) and value is None else await deserialize(f, value)

        return cls(**model_dict)

    @classmethod
    async def all(cls: Type[T]) -> AsyncIterator[T]:
        async with connection() as conn:
            for key in await conn.smembers(cls.prefix()):
                value = cast(T, await cls.get(key))
                if value:
                    yield value
                else:
                    _logger.warning(f"{cls.__name__} Key: {key} orphaned")

    @staticmethod
    async def flush():
        async with connection() as conn:
            await conn.flushdb()

    @classmethod
    async def count(cls) -> int:
        async with connection() as conn:
            return await conn.scard(cls.prefix())

    @classmethod
    async def delete_all(cls: Type):
        key_prefix = cls.prefix()
        async with connection() as conn:
            keys = await conn.keys(f"{key_prefix}:*")
            await conn.delete(key_prefix, *keys)

    @classmethod
    async def persisted(cls: Type, id: int) -> bool:
        async with connection() as conn:
            return await conn.exists(f"{cls.prefix()}:{id}")

    @property
    def db_id(self) -> str:
        return f"{self.prefix()}:{self.id}"

    async def save(self, optimistic=False):
        async with self._serialized_model(optimistic) as model_dict:
            async with transaction() as tr:
                tr.hmset_dict(self.db_id, model_dict)
                tr.sadd(self.prefix(), self.id)

    async def update(self, optimistic=False, **changes: Dict[str, Any]):
        async with self._serialized_model(optimistic, **changes) as model_dict:
            async with transaction() as tr:
                for key, value in model_dict.items():
                    tr.hset(self.db_id, key, value)
                return replace(self, **changes)

    @asynccontextmanager
    async def _serialized_model(
        self, optimistic, **changes
    ) -> AsyncGenerator[Dict[str, Any], None]:
        async with connection() as conn:
            if optimistic:
                conn.watch(self.db_id)
            model_dict = {}
            for field in [
                f
                for f in fields(self)
                if not is_transient(f)
                and hasattr(self, f.name)
                and (not changes or f.name in changes)
            ]:
                value = changes.get(field.name, getattr(self, field.name))
                ref_key = f"{self.db_id}:{field.name}"
                serialized = await serialize(field, ref_key, value)
                if isinstance(serialized, (Collection, Model)) and not isinstance(
                    serialized, str
                ):
                    model_dict[field.name] = ref_key
                    await serialized.save(optimistic=optimistic)
                elif serialized:
                    model_dict[field.name] = serialized
            yield model_dict

    async def delete(self):
        key = self.db_id
        async with connection() as conn:
            keys = await conn.keys(f"{key}:*")
            await conn.delete(*keys, key)

    async def exists(self) -> bool:
        async with connection() as conn:
            return await conn.exists(self.db_id)

    async def refresh(self: T) -> T:
        refreshed = await type(self).get(self.id)
        return cast(T, refreshed)
