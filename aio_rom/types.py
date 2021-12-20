from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Collection,
    Optional,
    TypeVar,
    Union,
)

from typing_extensions import Protocol, runtime_checkable

if TYPE_CHECKING:
    from .attributes import RedisCollection
    from .model import Model

F = TypeVar("F", str, bool, int, float)
M = TypeVar("M", bound="Model")
C = TypeVar("C", bound=Union[str, bool, int, float, "Model"])

RedisValue = Union[str, bytes]
Key = Union[int, RedisValue]
Serializable = Union[RedisValue, "Model", "RedisCollection"]
Serialized = Union[RedisValue, "Model", "RedisCollection", None]
Deserialized = Union[
    Optional[F],
    Awaitable[Optional[F]],
    Awaitable[Collection[Any]],
]
Deserializer = Callable[..., Deserialized[F]]


@runtime_checkable
class SupportsSave(Protocol):
    async def save(self, optimistic: bool) -> None:
        ...
