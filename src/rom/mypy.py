from typing import (
    Callable,
    Optional,
    Type as TypingType,
)

from mypy.nodes import (
    ClassDef,
    NameExpr,
    TypeInfo,
    Block,
    SymbolTableNode,
    MDEF,
    SymbolTable,
    PassStmt,
)
from mypy.plugin import (
    ClassDefContext,
    Plugin,
)
from mypy.plugins import dataclasses

MODEL_DATACLASS_TYPE_FULLNAME = "rom.model.ModelDataclassType"
MODEL_FULLNAME = "rom.model.Model"


def plugin(_: str) -> TypingType[Plugin]:
    return AioRomPlugin


class AioRomPlugin(Plugin):
    @staticmethod
    def transform(ctx: ClassDefContext) -> None:
        cls: ClassDef = ctx.cls
        not_found_exception_def = ClassDef(
            "NotFoundException",
            Block([PassStmt()]),
            base_type_exprs=[NameExpr("rom.exception.ModelNotFoundException")],
        )
        not_found_exception_def.fullname = f"{cls.fullname}.NotFoundException"
        not_found_attr = TypeInfo(
            SymbolTable({}),
            defn=not_found_exception_def,
            module_name=cls.info.module_name,
        )
        cls.info.names["NotFoundException"] = SymbolTableNode(MDEF, not_found_attr)
        dataclasses.dataclass_class_maker_callback(ctx)

    def get_base_class_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:
        if fullname == MODEL_FULLNAME:
            return AioRomPlugin.transform
        return None

    def get_metaclass_hook(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], None]]:
        if fullname == MODEL_DATACLASS_TYPE_FULLNAME:
            return AioRomPlugin.transform
        return None
