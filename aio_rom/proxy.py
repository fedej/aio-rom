from __future__ import annotations

import typing

from aio_rom.types import IModel, Key

T = typing.TypeVar("T", bound=IModel)


class ProxyModel(IModel, typing.Generic[T]):
    __slots__ = ("proxied", "proxied_type", "id")

    def __init__(self, proxied_type: type[T], id: Key):
        self.id: Key = id
        self.proxied: T | None = None
        self.proxied_type: type[T] = proxied_type

    async def save(self, *, optimistic: bool = False, cascade: bool = False) -> None:
        await self.proxied.save(optimistic=optimistic, cascade=cascade)

    @classmethod
    async def get(cls, *args):
        raise TypeError("Proxy objects can't be fetched")

    async def total_count(self) -> int:
        return await self.proxied.total_count()

    async def delete(self, cascade: bool = False) -> None:
        return await self.proxied.delete(cascade=cascade)

    async def refresh(self) -> None:
        self.proxied = await self.proxied_type.get(self.id)

    def __iter__(self):
        return iter(self.proxied)

    def __getattr__(self, item):
        if not self.proxied:
            raise ValueError("You must call `refresh()` before you can use this proxy")
        return getattr(self.proxied, item)
