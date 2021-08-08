import asyncio
import dataclasses
import logging
from collections import Iterable
from dataclasses import field, fields, replace
from inspect import iscoroutinefunction, signature
from typing import (
    Any,
    AsyncIterator,
    Collection,
    Dict,
    Generic,
    List,
    Mapping,
    Tuple,
    Type,
    Union,
    cast,
)

from .exception import ModelNotFoundException
from .fields import (
    deserialize,
    has_default,
    is_cascade,
    is_transient,
    serialize,
    update_field,
)
from .session import connection, transaction
from .types import Key, M, RedisValue

_logger = logging.getLogger(__name__)


class ModelDataclassType(type, Generic[M]):
    @classmethod
    def __prepare__(  # type: ignore[override]
        mcs, name: str, bases: Tuple[type, ...], **kwds: Any
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
        bases: Tuple[type, ...],
        namespace: Dict[str, Any],
        init: bool = True,
        repr: bool = True,
        eq: bool = True,
        order: bool = False,
        unsafe_hash: bool = False,
        frozen: bool = False,
    ) -> "ModelDataclassType[M]":
        new_namespace = namespace.copy()
        for field_name, field_type in namespace.get("__annotations__", {}).items():
            update_field(field_name, field_type, new_namespace)

        cls = cast(Type[M], super().__new__(mcs, name, bases, new_namespace))
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
    async def get(cls: Type[M], id: Key) -> M:
        async with connection() as conn:
            db_item: Dict[str, RedisValue] = await conn.hgetall(
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
    def from_dict(cls: Type[M], model: Dict[str, Any], strict: bool = True) -> M:
        parameters = signature(cls).parameters
        return (
            cls(**{k: v for k, v in model.items() if k in parameters})
            if strict
            else cls(**model)
        )

    @classmethod
    async def scan(cls: Type[M], **kwargs: Union[str, int]) -> AsyncIterator[M]:
        async with connection() as conn:
            found = set()
            async for key in conn.isscan(cls.prefix(), **kwargs):
                if key not in found:
                    value = await cls.get(key)
                    if value:
                        yield value
                        found.add(key)
                    else:
                        _logger.warning(f"{cls.__name__} Key: {key} orphaned")

    @classmethod
    async def all(cls: Type[M]) -> List[M]:
        async with connection() as conn:
            keys = await conn.smembers(cls.prefix())
            return await asyncio.gather(*[cls.get(key) for key in keys])

    @staticmethod
    async def flush() -> None:
        async with connection() as conn:
            await conn.flushdb()

    @classmethod
    async def count(cls) -> int:
        async with connection() as conn:
            return int(await conn.scard(cls.prefix()))

    @classmethod
    async def delete_all(cls: Type[M]) -> None:
        key_prefix = cls.prefix()
        async with connection() as conn:
            keys = await conn.keys(f"{key_prefix}:*")
            await conn.delete(key_prefix, *keys)

    @classmethod
    async def persisted(cls: Type[M], id: int) -> bool:
        async with connection() as conn:
            return bool(await conn.exists(f"{cls.prefix()}:{id}"))

    @property
    def db_id(self) -> str:
        return f"{self.prefix()}:{str(self.id)}"

    async def save(self, optimistic: bool = False) -> None:
        async with transaction() as tr:
            model_dict = await self._serialized_model(optimistic)
            tr.hmset_dict(self.db_id, model_dict)
            tr.sadd(self.prefix(), self.id)

    async def update(self: M, optimistic: bool = False, **changes: Any) -> M:
        async with transaction() as tr:
            model_dict = await self._serialized_model(optimistic, **changes)
            for key, value in model_dict.items():
                tr.hset(self.db_id, key, value)
            return replace(self, **changes)

    async def _serialized_model(
        self: M, optimistic: bool, **changes: Any
    ) -> Dict[str, Any]:
        async with connection() as conn:
            if optimistic:
                conn.watch(self.db_id)

            model_fields = {}
            for f in [
                f
                for f in fields(self)
                if not is_transient(f)
                and hasattr(self, f.name)
                and (not changes or f.name in changes)
            ]:
                value = changes.get(f.name, getattr(self, f.name))
                if has_default(f):
                    if not (isinstance(value, (Iterable, type(None))) and not value):
                        model_fields[f] = value
                else:
                    model_fields[f] = value

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

    async def delete(self) -> None:
        key = self.db_id
        async with connection() as conn:
            keys = await conn.keys(f"{key}:*")
            async with transaction() as tr:
                tr.delete(*keys, key)
                tr.srem(self.prefix(), key)

    async def exists(self) -> bool:
        async with connection() as conn:
            return bool(await conn.exists(self.db_id))

    async def refresh(self: M) -> M:
        return await type(self).get(self.id)
