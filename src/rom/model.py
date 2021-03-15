import asyncio
import logging
from dataclasses import dataclass, field, fields, replace
from inspect import iscoroutinefunction, signature
from typing import (
    Any,
    AsyncIterator,
    Collection,
    Dict,
    List,
    Type,
    TypeVar,
    Union,
    cast,
)

from .exception import ModelNotFoundException
from .fields import deserialize, is_cascade, is_transient, serialize, update_field
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
            raise cls.NotFoundException(f"{id} not found")  # type: ignore

        model_fields = [f for f in fields(cls) if not is_transient(f)]
        deserialized = await asyncio.gather(
            *[deserialize(f, db_item.get(f.name)) for f in model_fields]
        )

        return cls.from_dict(
            {f.name: value for f, value in zip(model_fields, deserialized)},
            strict=False,
        )

    @classmethod
    def from_dict(cls: Type[T], model: Dict[str, Any], strict: bool = True) -> T:
        parameters = signature(cls).parameters
        return (
            cls(**{k: v for k, v in model.items() if k in parameters})
            if strict
            else cls(**model)
        )

    @classmethod
    async def scan(cls: Type[T], **kwargs) -> AsyncIterator[T]:
        async with connection() as conn:
            found = set()
            async for key in conn.isscan(cls.prefix(), **kwargs):
                if key not in found:
                    value = cast(T, await cls.get(key))
                    if value:
                        yield value
                        found.add(key)
                    else:
                        _logger.warning(f"{cls.__name__} Key: {key} orphaned")

    @classmethod
    async def all(cls: Type[T]) -> List[T]:
        async with connection() as conn:
            keys = await conn.smembers(cls.prefix())
            return await asyncio.gather(*[cls.get(key) for key in keys])

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
        async with transaction() as tr:
            model_dict = await self._serialized_model(optimistic)
            tr.hmset_dict(self.db_id, model_dict)
            tr.sadd(self.prefix(), self.id)

    async def update(self, optimistic=False, **changes) -> T:
        async with transaction() as tr:
            model_dict = await self._serialized_model(optimistic, **changes)
            for key, value in model_dict.items():
                tr.hset(self.db_id, key, value)
            return replace(self, **changes)

    async def _serialized_model(self, optimistic, **changes) -> Dict[str, Any]:
        async with connection() as conn:
            if optimistic:
                conn.watch(self.db_id)
            model_fields = {
                f: changes.get(f.name, getattr(self, f.name))
                for f in fields(self)
                if not is_transient(f)
                and hasattr(self, f.name)
                and (not changes or f.name in changes)
            }

            serialized = await asyncio.gather(
                *[
                    serialize(field, f"{self.db_id}:{field.name}", value)
                    for field, value in model_fields.items()
                ]
            )
            serialized_fields = list(zip(model_fields.keys(), serialized))

            await asyncio.gather(
                *[
                    value.save(optimistic=optimistic)
                    for field, value in serialized_fields
                    if iscoroutinefunction(getattr(value, "save", None))
                    and (is_cascade(field) or isinstance(value, Collection))
                ]
            )
            return {
                field.name: getattr(value, "id", f"{self.db_id}:{field.name}")
                if iscoroutinefunction(getattr(value, "save", None))
                else value
                for field, value in serialized_fields
                if value is not None
                or iscoroutinefunction(getattr(value, "save", None))
            }

    async def delete(self):
        key = self.db_id
        async with connection() as conn:
            keys = await conn.keys(f"{key}:*")
            async with transaction() as tr:
                tr.delete(*keys, key)
                tr.srem(self.prefix(), key)

    async def exists(self) -> bool:
        async with connection() as conn:
            return await conn.exists(self.db_id)

    async def refresh(self: T) -> T:
        refreshed = await type(self).get(self.id)
        return cast(T, refreshed)
