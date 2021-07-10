from typing import Union, Awaitable, Callable, Optional, TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from .model import Model
    from .attributes import RedisCollection

F = TypeVar("F", str, bool, int, float)
M = TypeVar("M", bound="Model")
C = TypeVar("C", bound=Union[str, bool, int, float, "Model"])

RedisValue = Union[str, bytes]
Key = Union[int, RedisValue]
Serializable = Union[RedisValue, "Model", "RedisCollection"]
Serializer = Callable[..., Union[F, Serializable, Awaitable[Serializable]]]
Deserialized = Union[Optional[F], Awaitable[Optional[F]]]
Deserializer = Callable[..., Deserialized[F]]
