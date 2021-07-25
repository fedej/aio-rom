from typing import (
    Callable,
    Optional,
    Type as TypingType,
)

from mypy.nodes import ClassDef, SymbolTableNode, NameExpr, TypeInfo
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
    @staticmethod
    def transform(ctx: ClassDefContext) -> None:
        cls: ClassDef = ctx.cls
        if not cls.base_type_exprs or not any(
            [
                expr.fullname == MODEL_FULLNAME
                for expr in cls.base_type_exprs
                if isinstance(expr, NameExpr)
            ]
        ):
            node = ctx.api.lookup_fully_qualified("rom.Model").node
            if node and isinstance(node, TypeInfo):
                if cls.info.mro:
                    cls.info.mro = cls.info.mro[:-1] + [node] + [cls.info.mro[-1]]
                else:
                    cls.info.mro = [node]
        dataclasses.dataclass_class_maker_callback(ctx)

    def get_class_decorator_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:
        if fullname == DATACLASS_FULLNAME:
            return AioRomPlugin.transform
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
