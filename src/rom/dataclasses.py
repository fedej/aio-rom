import dataclasses
from typing import Type, Any, TypeVar

from rom.model import ModelDataclassType, Model

_T = TypeVar("_T", bound=Model)


def dataclass(cls: Type[_T], **kwargs: Any) -> Type[_T]:
    return dataclasses.dataclass(  # type: ignore
        ModelDataclassType(cls.__name__, cls.__bases__, vars(cls)), **kwargs
    )
