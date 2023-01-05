from __future__ import annotations

import typing

import wrapt

if typing.TYPE_CHECKING:
    from aio_rom.types import IModel, Key

    T = typing.TypeVar("T", bound=IModel)


class ProxyModel(wrapt.ObjectProxy):  # type: ignore[misc]
    __slots__ = ("proxied_type", "id")

    def __init__(self, proxied_type: type[T], id: Key):
        self.id: Key = id
        self.proxied_type: type[T] = proxied_type

    @property  # type: ignore[misc]
    def __class__(self) -> type[T]:  # type: ignore[override]
        return self.proxied_type

    async def save(self, *, optimistic: bool = False, cascade: bool = False) -> None:
        try:
            wrapped = self.__wrapped__
        except ValueError:
            pass
        else:
            await wrapped.save(optimistic=optimistic, cascade=cascade)

    async def refresh(self) -> None:
        proxied = await self.proxied_type.get(self.id)
        super().__init__(proxied)

    def __repr__(self) -> str:
        try:
            wrapped = self.__wrapped__
        except ValueError:
            return super().__repr__()  # type: ignore[no-any-return]
        else:
            return repr(wrapped)
