from typing import Callable, Optional
from typing import Type as TypingType

from mypy.nodes import (
    MDEF,
    Block,
    ClassDef,
    NameExpr,
    PassStmt,
    SymbolTable,
    SymbolTableNode,
    TypeInfo,
    Var,
)
from mypy.plugin import ClassDefContext, Plugin
from mypy.plugins import dataclasses
from mypy.types import Instance, TypeType

MODEL_DATACLASS_TYPE_FULLNAME = "aio_rom.model.ModelDataclassType"
MODEL_FULLNAME = "aio_rom.model.Model"


def plugin(_: str) -> TypingType[Plugin]:
    return AioRomPlugin


class AioRomPlugin(Plugin):
    @staticmethod
    def transform(ctx: ClassDefContext) -> None:
        AioRomPlugin.add_exception_to_class(ctx)
        dataclasses.dataclass_class_maker_callback(ctx)

    @staticmethod
    def add_exception_to_class(ctx: ClassDefContext) -> None:
        cls: ClassDef = ctx.cls
        not_found_exception_def = ClassDef(
            "NotFoundException",
            Block([PassStmt()]),
            base_type_exprs=[NameExpr("aio_rom.exception.ModelNotFoundException")],
        )
        not_found_exception_def.fullname = f"{cls.fullname}.NotFoundException"
        not_found_class = TypeInfo(
            SymbolTable({}),
            defn=not_found_exception_def,
            module_name=cls.info.module_name,
        )
        sym = ctx.api.lookup_fully_qualified_or_none(
            "aio_rom.exception.ModelNotFoundException"
        )
        if sym is None:
            raise TypeError("ModelNotFoundException class missing")
        elif not isinstance(sym.node, TypeInfo):
            raise TypeError(f"{sym.node} needs to be TypeInfo")
        not_found_class.bases = [Instance(sym.node, [])]
        not_found_attr = Var(
            "NotFoundException",
            TypeType(
                Instance(sym.node, [])
            ),  # TODO: this is ModelNotFoundException, should be NotFoundException
        )
        not_found_attr.info = not_found_class
        cls.info.names["NotFoundException"] = SymbolTableNode(MDEF, not_found_attr)

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
