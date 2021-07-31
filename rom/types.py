from typing import TYPE_CHECKING, Awaitable, Callable, Optional, TypeVar, Union

if TYPE_CHECKING:
    from .attributes import RedisCollection
    from .model import Model

F = TypeVar("F", str, bool, int, float)
M = TypeVar("M", bound="Model")
C = TypeVar("C", bound=Union[str, bool, int, float, "Model"])

RedisValue = Union[str, bytes]
Key = Union[int, RedisValue]
Serializable = Union[RedisValue, "Model", "RedisCollection"]
Serializer = Callable[..., Union[F, Serializable, Awaitable[Serializable]]]
Deserialized = Union[Optional[F], Awaitable[Optional[F]]]
Deserializer = Callable[..., Deserialized[F]]
