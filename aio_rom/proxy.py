from __future__ import annotations

import typing

import wrapt

if typing.TYPE_CHECKING:
    from aio_rom.types import IModel, Key

    T = typing.TypeVar("T", bound=IModel)


class ProxyModel(wrapt.ObjectProxy):
    __slots__ = ("proxied_type", "id")

    def __init__(self, proxied_type: type[T], id: Key):
        self.id: Key = id
        self.proxied_type: type[T] = proxied_type

    @property
    def __class__(self):
        return self.proxied_type

    @__class__.setter
    def __class__(self, value):
        self.proxied_type = value

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
