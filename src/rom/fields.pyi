from dataclasses import Field
from typing import (
    Any,
    Mapping,
    Optional,
    overload,
    TypeVar,
    Callable,
    Type,
    Union,
    Dict,
)

from .types import RedisValue, Serializable, F

_T = TypeVar("_T")

def is_transient(dataclass_field: Field[_T]) -> bool: ...
def is_eager(dataclass_field: Field[_T]) -> bool: ...
def is_cascade(dataclass_field: Field[_T]) -> bool: ...
def is_optional(dataclass_field: Field[_T]) -> bool: ...
def is_model(model: object) -> bool: ...
@overload  # `default` and `default_factory` are optional and mutually exclusive.
def field(
    *,
    default: _T,
    transient: bool = ...,
    cascade: bool = ...,
    eager: bool = ...,
    init: bool = ...,
    repr: bool = ...,
    hash: Optional[bool] = ...,
    compare: bool = ...,
    metadata: Optional[Mapping[str, Any]] = ...,
) -> _T: ...
@overload
def field(
    *,
    default_factory: Callable[[], _T],
    transient: bool = ...,
    cascade: bool = ...,
    eager: bool = ...,
    init: bool = ...,
    repr: bool = ...,
    hash: Optional[bool] = ...,
    compare: bool = ...,
    metadata: Optional[Mapping[str, Any]] = ...,
) -> _T: ...
@overload
def field(
    *,
    transient: bool = ...,
    cascade: bool = ...,
    eager: bool = ...,
    init: bool = ...,
    repr: bool = ...,
    hash: Optional[bool] = ...,
    compare: bool = ...,
    metadata: Optional[Mapping[str, Any]] = ...,
) -> Any: ...
async def deserialize(
    dataclass_field: Field[_T], value: Optional[RedisValue]
) -> Optional[_T]: ...
async def serialize(
    dataclass_field: Field[_T], key: str, value: _T
) -> Serializable: ...
def update_field(
    name: str, field_type: Type[Union[F, _T]], fields: Dict[str, Field[F]]
) -> None: ...
