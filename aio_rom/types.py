from typing import TYPE_CHECKING, Awaitable, Callable, Optional, TypeVar, Union

from aioredis.client import FieldT, KeyT

if TYPE_CHECKING:
    from .attributes import RedisCollection
    from .model import Model

F = TypeVar("F", str, bool, int, float, bytes, memoryview)
M = TypeVar("M", bound="Model")
C = TypeVar("C", bound=Union[str, bool, int, float, bytes, memoryview, "Model"])

RedisValue = FieldT
Key = Union[int, KeyT]
Serializable = Union[RedisValue, "Model", "RedisCollection"]
Serializer = Callable[..., Union[F, Serializable, Awaitable[Serializable]]]
Deserialized = Union[Optional[F], Awaitable[Optional[F]]]
Deserializer = Callable[..., Deserialized[F]]
