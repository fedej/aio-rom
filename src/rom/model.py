import asyncio
import logging
from dataclasses import dataclass, field
from typing import (Any, AsyncIterator, Dict, Iterable, List, Optional, Type,
                    TypeVar, Union, cast)

from aioredis.commands import Redis, create_redis_pool

from .exception import ModelNotFoundException
from .fields import ModelField, ModelFieldType, model_fields

_logger = logging.getLogger(__name__)

lock = asyncio.Lock()
redis: Optional[Redis] = None

async def init(address, *args, **kwargs):
    async with lock:
        global redis
        if redis is None:
            redis = await create_redis_pool(f"{address}?encoding=utf-8", *args, **kwargs)

class ModelDataclassType(type):
    def __new__(cls, name, bases, dict):
        model_class = dataclass(super().__new__(cls, name, bases, dict))
        setattr(model_class, "fields", model_fields(model_class))
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
    async def from_dict(cls: Type[T], model: Dict[str, Any]) -> T:
        fields: List[ModelField] = cls.fields  # type: ignore # pylint: disable=no-member
        deserialized = {}
        for field in fields:
            value = await field.deserialize(model)
            if value is not None:
                deserialized[field.name] = value
        return cls(**deserialized)

    def to_dict(self) -> Dict[str, Any]:
        fields: List[ModelField] = self.fields  # type: ignore # pylint: disable=no-member
        # dict_model_class: Type = self.dict_model_class  # type: ignore # pylint: disable=no-member
        return {field.name: field.serialize(self) for field in fields}

    @classmethod
    def prefix(cls) -> str:
        return f"{cls.__name__.lower()}"

    @classmethod
    async def get(cls: Type[T], id: Union[int, str]) -> T:
        db_item = cast(Dict[str, Any], await redis.hgetall(f"{cls.prefix()}:{id}"))
        if not db_item:
            raise cls.NotFoundException(f"{id} not found")  # type: ignore # pylint: disable=no-member
        return cast(T, await cls.from_dict(db_item))

    @classmethod
    async def all(cls: Type[T]) -> AsyncIterator[T]:
        for key in await redis.smembers(cls.prefix()):
            value = cast(T, await cls.get(key))
            if value:
                yield value
            else:
                _logger.warning(f"{cls.__name__} Key: {key} orphaned")

    @staticmethod
    async def flush():
        await redis.flushdb()

    @classmethod
    async def count(cls) -> int:
        return await redis.scard(cls.prefix())

    @classmethod
    async def delete_all(cls: Type):
        key_prefix = cls.prefix()
        with await cast(Redis, redis) as conn:
            keys = await conn.keys(f"{key_prefix}:*")
            await conn.delete(*keys, key_prefix)

    @classmethod
    async def persisted(cls: Type, id: int) -> bool:
        return await redis.exists(f"{cls.prefix()}:{id}")

    def _db_id(self) -> str:
        return f"{self.prefix()}:{self.id}"

    # TODO Support optimistic locking, WATCH?
    async def save(self, cascade=False):
        tr = redis.multi_exec()
        model_dict = self.to_dict()
        for field in self.fields:  # type: ignore # pylint: disable=no-member
            value = getattr(self, field.name)
            if field.transient or value is None:
                del model_dict[field.name]
                continue

            if field.model_type == [
                ModelFieldType.MODEL,
                ModelFieldType.MODEL_OPTIONAL,
            ]:
                if cascade:
                    await value.update(cascade=True)
            elif field.model_type in [
                ModelFieldType.MODEL_LIST,
                ModelFieldType.LIST,
                ModelFieldType.MODEL_SET,
                ModelFieldType.SET,
            ]:
                if cascade and isinstance(value, Iterable[T]):
                    for v in filter(None, value):
                        await v.update(cascade=True)
                ref_key = f"{self.prefix()}:{self.id}:{field.name}"
                tr.delete(ref_key)
                if field.model_type in [ModelFieldType.MODEL_LIST, ModelFieldType.LIST]:
                    tr.rpush(ref_key, *model_dict[field.name])
                else:
                    tr.sadd(ref_key, *model_dict[field.name])
                model_dict[field.name] = ref_key
        tr.hmset_dict(self._db_id(), model_dict)
        tr.sadd(self.prefix(), self.id)
        await tr.execute()

    async def delete(self):
        key = self._db_id()
        keys = await redis.keys(f"{key}:*")
        await redis.delete(*keys, key)

    async def exists(self) -> bool:
        return await redis.exists(self._db_id())

    async def refresh(self: T) -> T:
        refreshed = await type(self).get(self.id)
        return cast(T, refreshed)
