from typing import (
    Callable,
    Optional,
    Type as TypingType,
)

from mypy.plugin import (
    ClassDefContext,
    Plugin,
)
from mypy.plugins import dataclasses

DATACLASS_FULLNAME = "rom.dataclasses.dataclass"
MODEL_DATACLASS_TYPE_FULLNAME = "rom.model.ModelDataclassType"
MODEL_FULLNAME = "rom.model.Model"


def plugin(version: str) -> TypingType[Plugin]:
    return AioRomPlugin


class AioRomPlugin(Plugin):
    def get_class_decorator_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:
        if fullname == DATACLASS_FULLNAME:
            return dataclasses.dataclass_class_maker_callback
        return None

    def get_base_class_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:
        if fullname == MODEL_FULLNAME:
            return dataclasses.dataclass_class_maker_callback
        return None

    def get_metaclass_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:
        if fullname == MODEL_DATACLASS_TYPE_FULLNAME:
            return dataclasses.dataclass_class_maker_callback
        return None
